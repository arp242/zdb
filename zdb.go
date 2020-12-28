package zdb

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/jmoiron/sqlx"
	"zgo.at/zlog"
	"zgo.at/zstd/zbyte"
)

// Date format for SQL.
const Date = "2006-01-02 15:04:05"

// DB wraps sqlx.DB so we can add transactions.
type DB interface {
	// Execute a query without returning any result.
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)

	// Get a simple single-column value. dest needs to be a pointer to a
	// primitive.
	//
	// Returns sql.ErrNoRows if there are no rows.
	GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error

	// Select multiple rows, dest needs to be a pointer to a slice.
	//
	// Returns nil if there are no rows.
	SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error

	// Query one row.
	//
	// Returning row is never nil; use .Err() to check for errors. Row.Scan()
	// will return sql.ErrNoRows if there are no rows.
	QueryRowxContext(ctx context.Context, query string, args ...interface{}) *sqlx.Row

	// Query one or more rows.
	//
	// Returning rows is never nil; use .Err() to check for errors. Row
	QueryxContext(ctx context.Context, query string, args ...interface{}) (*sqlx.Rows, error)

	// Rebind :named to placeholders appropriate for this SQL connection.
	BindNamed(query string, arg interface{}) (newquery string, args []interface{}, err error)

	// Rebind ? to placeholders appropriate for this SQL connection.
	Rebind(query string) string

	// SQL driver name for this connection.
	DriverName() string
}

// DBCloser is like DB, but with the Close() method.
//
// sqlx.Db satisfies this interface, but sqlx.Tx does not. Usually you want to
// accept the DB interface in your functions.
//
//   db := connectDB() // Returns zdb.DBCloser
//   defer db.Close()
//
//   doWork(db)        // Accepts zdb.DB, so it can operate on both sql.DB and sqlx.Tx
type DBCloser interface {
	DB
	Close() error
}

// A is an alias for map[string]interface{}, just because it's less typing 🙃
type A map[string]interface{}

var (
	ctxkey = &struct{ n string }{"zdb"}
	l      = zlog.Module("zdb")
)

// With returns a copy of the context with the DB instance.
func With(ctx context.Context, db DB) context.Context {
	return context.WithValue(ctx, ctxkey, db)
}

// Get the DB from the context.
func Get(ctx context.Context) (DB, bool) {
	db, ok := ctx.Value(ctxkey).(DB)
	return db, ok
}

// MustGet gets the DB from the context, panicking if there is none.
func MustGet(ctx context.Context) DB {
	db, ok := Get(ctx)
	if !ok {
		panic(fmt.Sprintf("zdb.MustGet: no DB on this context (value: %#v)", db))
	}
	return db
}

// Unwrap this database, removing any of the zdb wrappers and returning the
// underlying sqlx.DB or sqlx.Tx.
func Unwrap(db DB) DB {
	uw, ok := db.(interface {
		Unwrap() DB
	})
	if !ok {
		return db
	}
	return Unwrap(uw.Unwrap())
}

// ErrTransactionStarted is returned when a transaction is already started.
var ErrTransactionStarted = errors.New("transaction already started")

// Begin a new transaction.
//
// The returned context is a copy of the original with the DB replaced with a
// transaction. The same transaction is also returned directly.
//
// Nested transactions return the original transaction together with
// ErrTransactionStarted (which is not a fatal error).
func Begin(ctx context.Context) (context.Context, *sqlx.Tx, error) {
	db := Unwrap(MustGet(ctx))

	// Could use savepoints, but that's probably more confusing than anything
	// else: almost all of the time you want the outermost transaction to be
	// completed in full or not at all. If you really want savepoints then you
	// can do it manually.
	if tx, ok := db.(*sqlx.Tx); ok {
		return ctx, tx, ErrTransactionStarted
	}

	tx, err := db.(*sqlx.DB).BeginTxx(ctx, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("zdb.Begin: %w", err)
	}
	return With(ctx, tx), tx, nil
}

// TX runs the given function in a transaction.
//
// The context passed to the callback has the DB replaced with a transaction.
//
// The transaction is committed if the fn returns nil, or will be rolled back if
// it's not.
//
// Multiple TX() calls can be nested, but they all run the same transaction.
//
// This is just a more convenient wrapper for Begin().
func TX(ctx context.Context, fn func(context.Context, DB) error) error {
	txctx, tx, err := Begin(ctx)
	if err == ErrTransactionStarted {
		err := fn(txctx, tx)
		if err != nil {
			return fmt.Errorf("zdb.TX fn: %w", err)
		}
		return nil
	}
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

type DumpArg int

const (
	_ DumpArg = iota
	DumpVertical
	DumpQuery
	DumpExplain
	DumpResult
)

// Dump the results of a query to a writer in an aligned table. This is a
// convenience function intended mostly for testing/debugging.
//
// Combined with ztest.Diff() it can be an easy way to test the database state.
//
// You can add some special sentinel values in the args to control the output
// (they're not sent as parameters to the DB):
//
//   DumpVertical   Show vertical output instead of horizontal columns.
//   DumpQuery      Show the query with placeholders substituted.
//   DumpExplain    Show the results of EXPLAIN (or EXPLAIN ANALYZE for PostgreSQL).
func Dump(ctx context.Context, out io.Writer, query string, args ...interface{}) {
	var showQuery, vertical, explain bool
	argsb := args[:0]
	for _, a := range args {
		b, ok := a.(DumpArg)
		if !ok {
			argsb = append(argsb, a)
			continue
		}
		// TODO: formatting could be better; also merge with explainDB
		// TODO: DumpQuery -> DumpQueryOnly and make "DumpQuery" do query+explain
		switch b {
		case DumpQuery:
			showQuery = true
		case DumpVertical:
			vertical = true
		case DumpExplain:
			explain = true
		}
	}
	args = argsb

	rows, err := MustGet(ctx).QueryxContext(ctx, query, args...)
	if err != nil {
		panic(err)
	}
	cols, err := rows.Columns()
	if err != nil {
		panic(err)
	}

	if showQuery {
		fmt.Fprintln(out, "Query:", ApplyPlaceholders(query, args...))
	}

	t := tabwriter.NewWriter(out, 4, 4, 2, ' ', 0)
	if vertical {
		for rows.Next() {
			row, err := rows.SliceScan()
			if err != nil {
				panic(err)
			}
			for i, c := range row {
				t.Write([]byte(fmt.Sprintf("%s\t%v\n", cols[i], formatArg(c, false))))
			}
			t.Write([]byte("\n"))
		}
	} else {
		t.Write([]byte(strings.Join(cols, "\t") + "\n"))
		for rows.Next() {
			row, err := rows.SliceScan()
			if err != nil {
				panic(err)
			}
			for i, c := range row {
				t.Write([]byte(fmt.Sprintf("%v", formatArg(c, false))))
				if i < len(row)-1 {
					t.Write([]byte("\t"))
				}
			}
			t.Write([]byte("\n"))
		}
	}
	t.Flush()

	if explain {
		if PgSQL(MustGet(ctx)) {
			fmt.Fprintln(out, "")
			Dump(ctx, out, "explain analyze "+query, args...)
		} else {
			fmt.Fprintln(out, "\nEXPLAIN:")
			Dump(ctx, out, "explain query plan "+query, args...)
		}
	}
}

// ApplyPlaceholders replaces parameter placeholders in query with the values.
//
// This is ONLY for printf-debugging, and NOT for actual usage. Security was NOT
// a consideration when writing this. Parameters in SQL are sent separately over
// the write and are not interpolated, so it's very different.
//
// This supports ? placeholders and $1 placeholders *in order* ($\d is simply
// replace with ?).
func ApplyPlaceholders(query string, args ...interface{}) string {
	query = regexp.MustCompile(`\$\d`).ReplaceAllString(query, "?")
	for _, a := range args {
		query = strings.Replace(query, "?", formatArg(a, true), 1)
	}
	query = deIndent(query)
	if !strings.HasSuffix(query, ";") {
		return query + ";"
	}
	return query
}

// ListTables lists all tables
func ListTables(ctx context.Context) ([]string, error) {
	db := MustGet(ctx)

	query := `select name from sqlite_master where type='table' order by name`
	if PgSQL(db) {
		query = `select c.relname as name
			from pg_catalog.pg_class c
			left join pg_catalog.pg_namespace n on n.oid = c.relnamespace
			where
				c.relkind = 'r' and
				n.nspname <> 'pg_catalog' and
				n.nspname <> 'information_schema' and
				n.nspname !~ '^pg_toast' and
				pg_catalog.pg_table_is_visible(c.oid)
			order by name`
	}

	var tables []string
	err := db.SelectContext(ctx, &tables, query)
	if err != nil {
		return nil, fmt.Errorf("zdb.ListTables: %w", err)
	}
	return tables, nil
}

func formatArg(a interface{}, quoted bool) string {
	if a == nil {
		return "NULL"
	}
	switch aa := a.(type) {
	case *string:
		if aa == nil {
			return "NULL"
		}
		a = *aa
	case *int:
		if aa == nil {
			return "NULL"
		}
		a = *aa
	case *int64:
		if aa == nil {
			return "NULL"
		}
		a = *aa
	case *time.Time:
		if aa == nil {
			return "NULL"
		}
		a = *aa
	}

	switch aa := a.(type) {
	case time.Time:
		// TODO: be a bit smarter about the precision, e.g. a date or time
		// column doesn't need the full date.
		return formatArg(aa.Format(Date), quoted)
	case int, int64:
		return fmt.Sprintf("%v", aa)
	case []byte:
		if zbyte.Binary(aa) {
			return fmt.Sprintf("%x", aa)
		} else {
			return formatArg(string(aa), quoted)
		}
	case string:
		if quoted {
			return fmt.Sprintf("'%v'", strings.ReplaceAll(aa, "'", "''"))
		}
		return aa
	default:
		if quoted {
			return fmt.Sprintf("'%v'", aa)
		}
		return fmt.Sprintf("%v", aa)
	}
}

func deIndent(in string) string {
	// Ignore comment at the start for indentation as I often write:
	//     SelectContext(`/* Comment for PostgreSQL logs */
	//             select [..]
	//     `)
	in = strings.TrimLeft(in, "\n\t ")
	comment := 0
	if strings.HasPrefix(in, "/*") {
		comment = strings.Index(in, "*/")
	}

	indent := 0
	for _, c := range strings.TrimLeft(in[comment+2:], "\n") {
		if c != '\t' {
			break
		}
		indent++
	}

	r := ""
	for _, line := range strings.Split(in, "\n") {
		r += strings.Replace(line, "\t", "", indent) + "\n"
	}

	return strings.TrimSpace(r)
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

// PgSQL reports if this database connection is to PostgreSQL.
func PgSQL(db DB) bool {
	return db.DriverName() == "postgres"
}

// SQLite reports if this database connection is to SQLite.
func SQLite(db DB) bool {
	return strings.HasPrefix(db.DriverName(), "sqlite3")
}

// InsertID runs a INSERT query and returns the ID column idColumn.
//
// If multiple rows are inserted it will return the ID of the last inserted row.
//
// This works for both PostgreSQL and SQLite.
func InsertID(ctx context.Context, idColumn, query string, args ...interface{}) (int64, error) {
	if PgSQL(MustGet(ctx)) {
		// TODO: would be better if we could automatically get the column name.
		var id []int64
		err := MustGet(ctx).SelectContext(ctx, &id, query+" returning "+idColumn, args...)
		if err != nil {
			return 0, err
		}
		return id[len(id)-1], nil
	}

	r, err := MustGet(ctx).ExecContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	return r.LastInsertId()
}
