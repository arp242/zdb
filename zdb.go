package zdb

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"text/tabwriter"

	"github.com/jmoiron/sqlx"
	"zgo.at/zlog"
)

// Date format for SQL.
const Date = "2006-01-02 15:04:05"

// DB wraps sqlx.DB so we can add transactions and logging.
type DB interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	Rebind(query string) string
	SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	QueryxContext(ctx context.Context, query string, args ...interface{}) (*sqlx.Rows, error)
}

var (
	ctxkey = &struct{ n string }{"d"}
	l      = zlog.Module("zdb")
)

// With returns a copy of the context with the DB instance.
func With(ctx context.Context, db DB) context.Context {
	return context.WithValue(ctx, ctxkey, db)
}

// MustGet gets the DB from the context, panicking if this fails.
func MustGet(ctx context.Context) DB {
	db, ok := ctx.Value(ctxkey).(DB)
	if !ok {
		panic("zdb.MustGet: no ctxkey value")
	}
	return db
}

// TX runs the given function in a transaction.
//
// The DB on the passed context is a copy with a transaction, which is is
// committed if the error is nil, or rolled back if it's not.
func TX(ctx context.Context, fn func(context.Context, DB) error) error {
	txctx, tx, err := Begin(ctx)
	if err != nil {
		return fmt.Errorf("zdb.TX: %w", err)
	}

	defer tx.Rollback()

	err = fn(txctx, tx)
	if err != nil {
		return fmt.Errorf("zdb.TX fn: %w", err)
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("zdb.TX commit: %w", err)
	}
	return nil
}

// Begin a new transaction.
//
// The returned context is a copy of the original with the DB replaced with a
// transaction. This transaction is also returned directly.
func Begin(ctx context.Context) (context.Context, *sqlx.Tx, error) {
	// TODO: to supported nested transactions we need to wrap it.
	// Also see: https://github.com/heetch/sqalx/blob/master/sqalx.go
	db := MustGet(ctx)
	if tx, ok := db.(*sqlx.Tx); ok {
		return ctx, tx, nil
	}

	tx, err := db.(*sqlx.DB).BeginTxx(ctx, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("zdb.Begin: %w", err)
	}

	return context.WithValue(ctx, ctxkey, tx), tx, nil
}

type ConnectOptions struct {
	Connect string // Connect string.
	Schema  []byte // Database schema to create on startup.
	Migrate *Migrate
}

// Connect to database.
func Connect(opts ConnectOptions) (*sqlx.DB, error) {
	var (
		db     *sqlx.DB
		exists bool
		err    error
	)
	if strings.HasPrefix(opts.Connect, "postgresql://") || strings.HasPrefix(opts.Connect, "postgres://") {
		trim := opts.Connect[strings.IndexRune(opts.Connect, '/')+2:]
		if strings.ContainsRune(trim, '/') {
			fmt.Println(opts.Connect)
			// "user=bob password=secret host=1.2.3.4 port=5432 dbname=mydb sslmode=verify-full"
			db, exists, err = connectPostgreSQL(opts.Connect)
		} else {
			fmt.Println(trim)
			// "postgres://bob:secret@1.2.3.4:5432/mydb?sslmode=verify-full"
			db, exists, err = connectPostgreSQL(trim)
		}
	} else if strings.HasPrefix(opts.Connect, "postgres://") {
		if !strings.ContainsRune(opts.Connect[11:], '/') {
			opts.Connect = opts.Connect[11:]
		}
		db, exists, err = connectPostgreSQL(opts.Connect)
	} else if strings.HasPrefix(opts.Connect, "sqlite://") {
		db, exists, err = connectSQLite(opts.Connect[9:], opts.Schema != nil)
	} else {
		err = fmt.Errorf("zdb.Connect: unrecognized database engine in connect string %q", opts.Connect)
	}
	if err != nil {
		return nil, fmt.Errorf("zdb.Connect: %w", err)
	}

	if opts.Migrate != nil {
		opts.Migrate.DB = db
	}

	if !exists {
		l.Printf("database %q doesn't exist; loading new schema", opts.Connect)
		db.MustExec(string(opts.Schema))
		if opts.Migrate != nil {
			err := opts.Migrate.Run("all")
			if err != nil {
				return nil, fmt.Errorf("migration: %w", err)
			}
		}
	}

	if opts.Migrate != nil {
		err = opts.Migrate.Check()
	}
	return db, err
}

// Dump the results of a query to a writer in an aligned table. This is a
// convenience function intended just for testing/debugging.
//
// Combined with ztest.Diff() it can be an easy way to test the database state.
func Dump(ctx context.Context, out io.Writer, query string, args ...interface{}) {
	rows, err := MustGet(ctx).QueryxContext(ctx, query, args...)
	if err != nil {
		panic(err)
	}
	cols, err := rows.Columns()
	if err != nil {
		panic(err)
	}

	fmt.Fprintln(out, "=>", query)
	if len(args) > 0 {
		fmt.Fprintf(out, "%#v\n", args)
	}

	t := tabwriter.NewWriter(out, 8, 8, 2, ' ', 0)
	for _, c := range cols {
		t.Write([]byte(fmt.Sprintf("%v\t", c)))
	}
	t.Write([]byte("\n"))

	for rows.Next() {
		row, err := rows.SliceScan()
		if err != nil {
			panic(err)
		}
		for _, c := range row {
			t.Write([]byte(fmt.Sprintf("%v\t", c)))
		}
		t.Write([]byte("\n"))
	}
	t.Flush()
}

// DumpString is like Dump(), but returns the result as a string.
func DumpString(ctx context.Context, query string, args ...interface{}) string {
	b := new(bytes.Buffer)
	Dump(ctx, b, query, args...)
	return b.String()
}

// ErrNoRows reports if this error is sql.ErrNoRows.
func ErrNoRows(err error) bool {
	return errors.Is(err, sql.ErrNoRows)
}

func connectPostgreSQL(connect string) (*sqlx.DB, bool, error) {
	db, err := sqlx.Connect("postgres", connect)
	if err != nil {
		return nil, false, fmt.Errorf("connectPostgreSQL: %w", err)
	}

	db.SetMaxIdleConns(25) // Default 2
	db.SetMaxOpenConns(25) // Default 0

	// TODO: report if DB exists.
	return db, true, nil
}

func connectSQLite(connect string, create bool) (*sqlx.DB, bool, error) {
	exists := true
	if _, err := os.Stat(connect); os.IsNotExist(err) {
		exists = false
		if !create {
			return nil, false, fmt.Errorf("connectSQLite: database %q doesn't exist", connect)
		}

		err = os.MkdirAll(filepath.Dir(connect), 0755)
		if err != nil {
			return nil, false, fmt.Errorf("connectSQLite: create DB dir: %w", err)
		}
	}
	db, err := sqlx.Connect("sqlite3", connect)
	if err != nil {
		return nil, false, fmt.Errorf("connectSQLite: %w", err)
	}

	return db, exists, nil
}
