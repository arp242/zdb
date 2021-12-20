// Package sqlite3 provides a zdb driver for SQLite.
//
// This uses https://github.com/mattn/go-sqlite3/
//
// Several connection parameters are set to different defaults in SQLite:
//
//   _journal_mode=wal          Almost always faster with better concurrency,
//                              with little drawbacks for most use cases.
//                              https://www.sqlite.org/wal.html
//
//   _foreign_keys=on           Check FK constraints; by default they're not
//                              enforced, which is probably not what you want.
//
//   _defer_foreign_keys=on     Delay FK checks until the transaction commit; by
//                              default they're checked immediately (if
//                              enabled).
//
//   _case_sensitive_like=on    LIKE is case-sensitive, like PostgreSQL.
//
//   _cache_size=-20000         20M cache size, instead of 2M. Can be a
//                              significant performance improvement.
//
// You can still use "?_journal_mode=something_else" in the connection string to
// set something different.
//
//   - https://github.com/mattn/go-sqlite3/
//
// This will set the maximum number of open and idle connections to 16 and 4,
// instead of Go's default of 0 and 2. To change this, you can use:
//
//    db.DBSQL().SetMaxOpenConns(100)
//
// To use a ConnectHook, you can DefaultHook() to automatically set the given
// connection hook on every new connection. Alternatively, you can register it
// first using the regular method:
//
//     sql.Register("sqlite3-hook1", &sqlite3.SQLiteDriver{
//         ConnectHook: func(c *sqlite3.SQLiteConn) error {
//             return c.RegisterFunc("hook1", func() string { return "hook1" }, true)
//         },
//     })
//
// And then call zdb.Connect() with "sqlite3-hook1" as the driver name. Note the
// driver name *must* start with "sqlite3".
package sqlite3

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/mattn/go-sqlite3"
	"zgo.at/zdb/drivers"
	"zgo.at/zstd/zstring"
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
func (driver) Connect(ctx context.Context, connect string, create bool) (*sql.DB, bool, error) {
	connect, driver := zstring.Split2(connect, "+++")
	if driver == "" {
		driver = "sqlite3"
	}

	if driver == "sqlite3" && defHook != nil {
		suffix := "_zdb_" + fmt.Sprintf("%p\n", defHook)[2:]
		driver += suffix

		found := false
		for _, d := range sql.Drivers() {
			if d == driver {
				found = true
				break
			}
		}
		if !found {
			sql.Register(driver, &sqlite3.SQLiteDriver{ConnectHook: defHook})
		}
	}

	memory := strings.HasPrefix(connect, ":memory:")
	exists := !memory
	file := strings.TrimPrefix(connect, "file:")

	var (
		i   = strings.IndexRune(connect, '?')
		q   = make(url.Values)
		err error
	)
	if i > -1 {
		file = connect[:i]
		q, err = url.ParseQuery(connect[i+1:])
		if err != nil {
			return nil, false, fmt.Errorf("sqlite3.Connect: parse connection string: %w", err)
		}
	}

	var set = func(value string, keys ...string) {
		for _, k := range keys {
			_, ok := q[k]
			if ok {
				return
			}
		}
		q.Set(keys[0], value)
	}

	if !memory {
		set("wal", "_journal_mode", "_journal") // More reliable for concurrent access
	}
	set("on", "_foreign_keys", "_fk")             // Check FK constraints
	set("on", "_defer_foreign_keys", "_defer_fk") // Check FKs after transaction commit
	set("on", "_case_sensitive_like", "_cslike")  // Same as PostgreSQL
	set("-20000", "_cache_size")                  // 20M max. cache, instead of 2M
	connect = fmt.Sprintf("file:%s?%s", file, q.Encode())

	if !memory {
		_, err = os.Stat(file)
		if os.IsNotExist(err) {
			exists = false
			if !create {
				if abs, err := filepath.Abs(file); err == nil {
					file = abs
				}
				return nil, false, &drivers.NotExistError{Driver: "sqlite3", DB: file, Connect: connect}
			}

			err = os.MkdirAll(filepath.Dir(file), 0755)
			if err != nil {
				return nil, false, fmt.Errorf("sqlite3.Connect: create DB dir: %w", err)
			}
		}
	}

	// TODO: if the file doesn't exist yet stat is nil, need to change this to
	// take a file path so we can check permission of the directory.
	// ok, err := zos.Writable(stat)
	// if err != nil {
	// 	return nil, false, fmt.Errorf("connectSQLite: %w", err)
	// }
	// if !ok {
	// 	return nil, false, fmt.Errorf("connectSQLite: %q is not writable", connect)
	// }

	db, err := sql.Open(driver, connect)
	if err != nil {
		return nil, false, fmt.Errorf("sqlite3.Connect: %w", err)
	}

	db.SetMaxOpenConns(16)
	db.SetMaxIdleConns(4)

	return db, exists, nil
}
