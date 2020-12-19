package zdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/mattn/go-sqlite3"
)

type ConnectOptions struct {
	Connect string   // Connect string.
	Create  bool     // Create database if it doesn't exist yet.
	Migrate []string // Migrations to run; nil for none, "all" for all, or a migration name.

	// Schema and migrations; it's assumed that the schema lives as /schema.sql,
	// and migrations in /migrate/*.sql.
	//
	// If /{sql.DriverName} exists and a diretory, then it will try
	// /{sql.DriverName}/schema.sql and /{sql.DriverName}/migrate/*.sql
	Schemas fs.FS

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

// Connect to database.
func Connect(opts ConnectOptions) (*sqlx.DB, error) {
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
			db, exists, err = connectPostgreSQL(conn, opts.Create)
		} else {
			// "postgres://bob:secret@1.2.3.4:5432/mydb?sslmode=verify-full"
			db, exists, err = connectPostgreSQL(opts.Connect, opts.Create)
		}
	case "sqlite", "sqlite3":
		db, exists, err = connectSQLite(conn, opts.Create, opts.SQLiteHook)
	default:
		err = fmt.Errorf("zdb.Connect: unrecognized database engine in connect string %q", opts.Connect)
	}
	if err != nil {
		return nil, fmt.Errorf("zdb.Connect: %w", err)
	}

	if opts.Schemas == nil {
		return db, nil
	}

	_, err = fs.ReadDir(opts.Schemas, db.DriverName())
	if err == nil {
		opts.Schemas, _ = fs.Sub(opts.Schemas, db.DriverName())
	}

	if !exists {
		l.Printf("database %q doesn't exist; loading new schema", opts.Connect)

		s, err := fs.ReadFile(opts.Schemas, "schema.sql")
		if err != nil {
			return nil, fmt.Errorf("zdb.Connect: reading schema: %w", err)
		}

		_, err = db.ExecContext(context.Background(), string(s))
		if err != nil {
			return nil, fmt.Errorf("zdb.Connect: running schema: %w", err)
		}

		// TODO: pass gomig
		m := NewMigrate(db, opts.Schemas, nil)
		err = m.Run("all")
		if err != nil {
			return nil, fmt.Errorf("zdb.Connect: running migrations: %w", err)
		}
	}

	// TODO: pass gomig
	if opts.Migrate != nil {
		m := NewMigrate(db, opts.Schemas, nil)
		err = m.Run(opts.Migrate...)
		if err != nil {
			return nil, fmt.Errorf("zdb.Connect: running migrations: %w", err)
		}

		return db, m.Check()
	}
	return db, nil
}

func connectPostgreSQL(connect string, create bool) (*sqlx.DB, bool, error) {
	exists := true
	db, err := sqlx.Connect("postgres", connect)
	if err != nil && create {
		var pqErr *pq.Error
		if errors.As(err, &pqErr) && pqErr.Code == "3D000" {
			dbname := regexp.MustCompile(`pq: database "(.+?)" does not exist`).FindStringSubmatch(pqErr.Error())
			out, cerr := exec.Command("createdb", dbname[1]).CombinedOutput()
			if cerr != nil {
				return nil, false, fmt.Errorf("connectPostgreSQL: %w: %s", cerr, out)
			}

			db, err = sqlx.Connect("postgres", connect)
			exists = false
		}
	}
	if err != nil {
		return nil, false, fmt.Errorf("connectPostgreSQL: %w", err)
	}

	db.SetMaxIdleConns(25) // Default 2
	db.SetMaxOpenConns(25) // Default 0

	return db, exists, nil
}

func connectSQLite(connect string, create bool, hook func(c *sqlite3.SQLiteConn) error) (*sqlx.DB, bool, error) {
	memory := strings.HasPrefix(connect, ":memory:")
	exists := !memory

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
