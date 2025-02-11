// Package sqlite3 provides a zdb driver for SQLite.
//
// This uses https://github.com/mattn/go-sqlite3/
//
// Several connection parameters are set to different defaults in SQLite:
//
//	_journal_mode=wal          Almost always faster with better concurrency,
//	                           with little drawbacks for most use cases.
//	                           https://www.sqlite.org/wal.html
//
//	_foreign_keys=on           Check FK constraints; by default they're not
//	                           enforced, which is probably not what you want.
//
//	_busy_timeout=200          Wait 200ms for locks instead of immediately
//	                           throwing an error.
//
//	_defer_foreign_keys=on     Delay FK checks until the transaction commit; by
//	                           default they're checked immediately (if
//	                           enabled).
//
//	_case_sensitive_like=on    LIKE is case-sensitive, like PostgreSQL.
//
//	_cache_size=-20000         20M cache size, instead of 2M. Can be a
//	                           significant performance improvement.
//
// You can still use "?_journal_mode=something_else" in the connection string to
// set something different.
//
//   - https://github.com/mattn/go-sqlite3/
//
// To use a ConnectHook, you can DefaultHook() to automatically set the given
// connection hook on every new connection. Alternatively, you can register it
// first using the regular method:
//
//	sql.Register("sqlite3-hook1", &sqlite3.SQLiteDriver{
//	    ConnectHook: func(c *sqlite3.SQLiteConn) error {
//	        return c.RegisterFunc("hook1", func() string { return "hook1" }, true)
//	    },
//	})
//
// And then call zdb.Connect() with "sqlite3-hook1" as the driver name. Note the
// driver name *must* start with "sqlite3".
package sqlite3

import (
	"context"
	"strings"
	"testing"

	"github.com/mattn/go-sqlite3"
	"zgo.at/zdb"
	"zgo.at/zdb/drivers"
)

func init() {
	drivers.RegisterDriver(driver{})
}

var defHook func(*sqlite3.SQLiteConn) error

// DefaultHook sets the default SQLite connection hook to use on every
// connection if no specific hook was specified.
//
// Note that connections made before this are not modified.
func DefaultHook(f func(*sqlite3.SQLiteConn) error) {
	defHook = f
}

type driver struct{}

func (driver) Name() string    { return "sqlite3" }
func (driver) Dialect() string { return "sqlite" }
func (driver) Match(dialect, driver string) bool {
	return dialect == "sqlite3" || strings.HasPrefix(driver, "sqlite3")
}

func (driver) StartTest(t *testing.T, opt *drivers.TestOptions) context.Context {
	t.Helper()

	if opt == nil {
		opt = &drivers.TestOptions{}
	}

	copt := zdb.ConnectOptions{Connect: "sqlite+:memory:?cache=shared", Create: true}
	if opt != nil && opt.Connect != "" {
		copt.Connect = opt.Connect
	}
	if opt != nil && opt.Files != nil {
		copt.Files = opt.Files
	}
	if opt != nil && opt.GoMigrations != nil {
		copt.GoMigrations = opt.GoMigrations
	}

	db, err := zdb.Connect(context.Background(), copt)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { db.Close() })
	return zdb.WithDB(context.Background(), db)
}
