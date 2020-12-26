package zdb

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/jmoiron/sqlx"
	"github.com/mattn/go-sqlite3"
)

type ConnectOptions struct {
	Connect string // Connect string.
	Schema  []byte // Database schema to create on startup.
	Migrate *Migrate

	// ConnectHook for sqlite3.SQLiteDriver; mainly useful to add your own
	// functions:
	//
	//    opts.SQLiteHook = func(c *sqlite3.SQLiteConn) error {
	//        return c.RegisterFunc("percent_diff", func(start, final float64) float64 {
	//            return (final - start) / start * 100
	//        }, true)
	//    }
	//
	// It'll automatically register and connect to a new "sqlite3_zdb" driver.
	SQLiteHook func(*sqlite3.SQLiteConn) error
}

// Connect to a database.
//
// The database will be created automatically if the database doesn't exist and
// Schema is in ConnectOptions
//
// This will set the maximum number of open and idle connections to 25 each for
// PostgreSQL, and 1 and -1 for SQLite, instead of Go's default of 0 and 2.
//
// To change this, you can use:
//   db.(*sqlx.DB).SetMaxOpenConns(100)
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
func Connect(opts ConnectOptions) (DBCloser, error) {
	var (
		proto string
		conn  string
	)
	if i := strings.Index(opts.Connect, "://"); i > -1 {
		proto = opts.Connect[:i]
		if len(opts.Connect) >= i+3 {
			conn = opts.Connect[i+3:]
		}
	}

	var (
		db     *sqlx.DB
		exists bool
		err    error
	)
	switch proto {
	case "postgresql", "postgres":
		if strings.ContainsRune(conn, ' ') {
			// "user=bob password=secret host=1.2.3.4 port=5432 dbname=mydb sslmode=verify-full"
			db, exists, err = connectPostgreSQL(conn)
		} else {
			// "postgres://bob:secret@1.2.3.4:5432/mydb?sslmode=verify-full"
			db, exists, err = connectPostgreSQL(opts.Connect)
		}
	case "sqlite", "sqlite3":
		db, exists, err = connectSQLite(conn, opts.Schema != nil, opts.SQLiteHook)
	default:
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
		_, err := db.ExecContext(context.Background(), string(opts.Schema))
		if err != nil {
			return nil, fmt.Errorf("loading schema: %w", err)
		}

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

func connectSQLite(connect string, create bool, hook func(c *sqlite3.SQLiteConn) error) (*sqlx.DB, bool, error) {
	exists := true
	memory := strings.HasPrefix(connect, ":memory:")

	file := connect
	if strings.HasPrefix(file, "file:") {
		file = file[5:]
	}

	var (
		i   = strings.IndexRune(connect, '?')
		q   = make(url.Values)
		err error
	)
	if i > -1 {
		file = connect[:i]
		q, err = url.ParseQuery(connect[i+1:])
		if err != nil {
			return nil, false, fmt.Errorf("connectSQLite: parse connection string: %w", err)
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
				return nil, false, fmt.Errorf("connectSQLite: database %q doesn't exist", file)
			}

			err = os.MkdirAll(filepath.Dir(file), 0755)
			if err != nil {
				return nil, false, fmt.Errorf("connectSQLite: create DB dir: %w", err)
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

	c := "sqlite3"
	if hook != nil {
		// TODO: two connections with different hooks won't work.
		found := false
		for _, d := range sql.Drivers() {
			if d == "sqlite3_zdb" {
				found = true
				break
			}
		}
		if !found {
			sql.Register("sqlite3_zdb", &sqlite3.SQLiteDriver{ConnectHook: hook})
		}
		c += "_zdb"
	}

	db, err := sqlx.Connect(c, connect)
	if err != nil {
		return nil, false, fmt.Errorf("connectSQLite: %w", err)
	}

	if !memory { // Seems to break :memory: database? Hmm
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(-1)
	}

	return db, exists, nil
}
