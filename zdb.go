package zdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/jmoiron/sqlx"
	"zgo.at/zlog"
)

// Date format for SQL.
const Date = "2006-01-02 15:04:05"

// DB satisfies both sqlx.DB and sqlx.Tx.
type DB interface {
	// Execute a query without returning any result.
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)

	// Get a single row.
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
// sqlx.DB satisfies this interface, but sqlx.Tx does not. Usually you want to
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

// A is an alias for map[string]interface{}, just because it's less typing and
// looks less noisy 🙃
type A map[string]interface{}

var (
	ctxkey = &struct{ n string }{"zdb"}
	l      = zlog.Module("zdb")
)

// WithDB returns a copy of the context with the DB instance.
func WithDB(ctx context.Context, db DB) context.Context {
	return context.WithValue(ctx, ctxkey, db)
}

// GetDB gets the DB from the context.
func GetDB(ctx context.Context) (DB, bool) {
	db, ok := ctx.Value(ctxkey).(DB)
	return db, ok
}

// MustGet gets the DB from the context, panicking if there is none.
func MustGetDB(ctx context.Context) DB {
	db, ok := GetDB(ctx)
	if !ok {
		panic(fmt.Sprintf("zdb.MustGet: no DB on this context (value: %#v)", db))
	}
	return db
}

// TODO: temporary alias to easy migration; GoatCounter has a lot of MustGet()
// calls predating the Query()/Get()/Exec()/Select() functions and uses a lot of
// positional arguments; I don't want to migrate everything in one go just for
// this.
func MustGet(ctx context.Context) DB { return MustGetDB(ctx) }

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

// ErrNoRows reports if this error is sql.ErrNoRows.
func ErrNoRows(err error) bool {
	return errors.Is(err, sql.ErrNoRows)
}

// PgSQL reports if this database connection is to PostgreSQL.
func PgSQL(ctx context.Context) bool {
	return MustGetDB(ctx).DriverName() == "postgres"
}

// SQLite reports if this database connection is to SQLite.
func SQLite(ctx context.Context) bool {
	return strings.HasPrefix(MustGetDB(ctx).DriverName(), "sqlite3")
}
