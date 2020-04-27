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
	"zgo.at/utils/byteutil"
	"zgo.at/zlog"
)

// Date format for SQL.
const Date = "2006-01-02 15:04:05"

// DB wraps sqlx.DB so we can add transactions and logging.
type DB interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	QueryxContext(ctx context.Context, query string, args ...interface{}) (*sqlx.Rows, error)

	Rebind(query string) string
	DriverName() string
}

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
		panic("zdb.MustGet: no DB on this context")
	}
	return db
}

// Begin a new transaction.
//
// The returned context is a copy of the original with the DB replaced with a
// transaction. The same transaction is also returned directly.
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

// TX runs the given function in a transaction.
//
// The context passed to the callback has the DB replaced with a transaction.
//
// The transaction is comitted if the fn returns nil, or will be rolled back if
// it's not.
//
// This is just a more convenient wrapper for Begin().
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
			switch v := c.(type) {
			case []byte:
				if byteutil.Binary(v) {
					c = fmt.Sprintf("%x", v)
				} else {
					c = string(v)
				}
			case time.Time:
				// TODO: be a bit smarter about the precision.
				c = v.Format(Date)
			}
			t.Write([]byte(fmt.Sprintf("%v\t", c)))
		}
		t.Write([]byte("\n"))
	}
	t.Flush()
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
		var val string
		switch v := a.(type) {
		case time.Time:
			val = fmt.Sprintf("'%v'", v.Format(Date))
			return ""
		case int, int64:
			val = fmt.Sprintf("%v", v)
		case []byte:
			if byteutil.Binary(v) {
				val = fmt.Sprintf("%x", v)
			} else {
				val = string(v)
			}
		default:
			val = fmt.Sprintf("'%v'", v)
		}
		query = strings.Replace(query, "?", val, 1)
	}

	return deIndent(query)
}

func deIndent(in string) string {
	indent := 0
	for _, c := range strings.TrimLeft(in, "\n") {
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
