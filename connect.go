package zdb

import (
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
		if strings.ContainsRune(conn, '/') {
			// "postgres://bob:secret@1.2.3.4:5432/mydb?sslmode=verify-full"
			db, exists, err = connectPostgreSQL(opts.Connect)
		} else {
			// "user=bob password=secret host=1.2.3.4 port=5432 dbname=mydb sslmode=verify-full"
			db, exists, err = connectPostgreSQL(conn)
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
	memory := connect == ":memory:"

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

	if !memory {
		if _, ok := q["_journal_mode"]; !ok {
			q.Set("_journal_mode", "wal")
		}
		connect = fmt.Sprintf("file:%s?%s", file, q.Encode())

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
