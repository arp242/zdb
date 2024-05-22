//go:build cgo

package sqlite3

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/mattn/go-sqlite3"
	"zgo.at/zdb/drivers"
)

func (driver) ErrUnique(err error) bool {
	var sqlErr sqlite3.Error
	return errors.As(err, &sqlErr) && sqlErr.ExtendedCode == sqlite3.ErrConstraintUnique
}

func (driver) Connect(ctx context.Context, connect string, create bool) (*sql.DB, bool, error) {
	connect, driver, _ := strings.Cut(connect, "+++")
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
	set("200", "_busy_timeout")                   // Default is to error immediately
	set("-20000", "_cache_size")                  // 20M max. cache, instead of 2M
	connect = fmt.Sprintf("file:%s?%s", file, q.Encode())

	if !memory {
		_, err = os.Stat(file)
		if err != nil && !os.IsNotExist(err) {
			return nil, false, fmt.Errorf("sqlite3.Connect: %w", err)
		}

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

	return db, exists, nil
}
