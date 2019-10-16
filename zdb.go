package zdb

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/mattn/go-sqlite3"
	"github.com/pkg/errors"
	"zgo.at/utils/sliceutil"
	"zgo.at/zlog"
)

var ctxkey = &struct{ n string }{"d"}

// DB wraps sqlx.DB so we can add transactions and logging.
type DB interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	Rebind(query string) string
	SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error

	// BeginTxx(ctx context.Context, opts *sql.TxOptions) (*sqlx.Tx, error)
	// Rollback() error
	// Commit() error
}

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

// Begin a new transaction.
func Begin(ctx context.Context) (context.Context, *sqlx.Tx, error) {
	// TODO: to supported nested transactions we need to wrap it.
	// Also see: https://github.com/heetch/sqalx/blob/master/sqalx.go
	db := MustGet(ctx)
	if tx, ok := db.(*sqlx.Tx); ok {
		return ctx, tx, nil
	}

	tx, err := db.(*sqlx.DB).BeginTxx(ctx, nil)
	return context.WithValue(ctx, ctxkey, tx), tx, err
}

func UniqueErr(err error) bool {
	if sqlErr, ok := err.(sqlite3.Error); ok && sqlErr.ExtendedCode == sqlite3.ErrConstraintUnique {
		return true
	}
	if pqErr, ok := err.(pq.Error); ok && pqErr.Code == "23505" {
		return true
	}
	return false
}

// Connect to database.
func Connect(connect string, pgSQL bool, schema []byte, migrations map[string][]byte) (*sqlx.DB, error) {
	var db *sqlx.DB
	if pgSQL {
		var err error
		db, err = sqlx.Connect("postgres", connect)
		if err != nil {
			return nil, errors.Wrap(err, "sqlx.Connect pgsql")
		}

		// db.SetConnMaxLifetime()
		db.SetMaxIdleConns(25) // Default 2
		db.SetMaxOpenConns(25) // Default 0
	} else {
		exists := true
		if _, err := os.Stat(connect); os.IsNotExist(err) {
			zlog.Printf("database %q doesn't exist; loading new schema", connect)
			exists = false
		}
		var err error
		db, err = sqlx.Connect("sqlite3", connect)
		if err != nil {
			return nil, errors.Wrap(err, "sqlx.Connect pgsql")
		}

		if !exists {
			db.MustExec(string(schema))
		}
	}

	return db, checkmig(db, migrations)

}

func checkmig(db *sqlx.DB, migrations map[string][]byte) error {
	// Check migrations.
	var haveMig []string
	if _, err := os.Stat("./db/migrate"); os.IsNotExist(err) {
		for k := range migrations {
			haveMig = append(haveMig, k)
		}
	} else {
		haveMig, err = filepath.Glob("./db/migrate/*.sql")
		if err != nil {
			return errors.Wrap(err, "glob")
		}
	}
	for i := range haveMig {
		haveMig[i] = filepath.Base(haveMig[i][:len(haveMig[i])-4])
	}
	sort.Strings(haveMig)

	var ranMig []string
	err := db.Select(&ranMig, `select name from version order by name asc`)
	if err != nil {
		return errors.Wrap(err, "select version")
	}

	if d := diff(haveMig, ranMig); len(d) > 0 {
		zlog.Errorf("pending migrations: %q", d)
	}
	if d := diff(ranMig, haveMig); len(d) > 0 {
		zlog.Errorf("migrations in the DB that don't exist: %q", d)
	}
	return nil
}

// diff returns a new slice with elements that are in "set" but not in "others".
func diff(set []string, others ...[]string) []string {
	var out []string
	for _, setItem := range set {
		found := false
		for _, o := range others {
			if sliceutil.InStringSlice(o, setItem) {
				found = true
				break
			}
		}

		if !found {
			out = append(out, setItem)
		}
	}

	return out
}

// Date formatted for SQL.
func Date(t time.Time) string { return t.Format("2006-01-02 15:04:05") }
