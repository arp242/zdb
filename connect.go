package zdb

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/jmoiron/sqlx"
	"zgo.at/zstd/zos"
)

type ConnectOptions struct {
	Connect string // Connect string.
	Schema  []byte // Database schema to create on startup.
	Migrate *Migrate
}

// Connect to database.
func Connect(opts ConnectOptions) (*sqlx.DB, error) {
	var (
		db     *sqlx.DB
		exists bool
		err    error
	)
	if strings.HasPrefix(opts.Connect, "postgresql://") || strings.HasPrefix(opts.Connect, "postgres://") {
		trim := opts.Connect[strings.IndexRune(opts.Connect, '/')+2:]
		if strings.ContainsRune(trim, '/') {
			// "user=bob password=secret host=1.2.3.4 port=5432 dbname=mydb sslmode=verify-full"
			db, exists, err = connectPostgreSQL(opts.Connect)
		} else {
			// "postgres://bob:secret@1.2.3.4:5432/mydb?sslmode=verify-full"
			db, exists, err = connectPostgreSQL(trim)
		}
	} else if strings.HasPrefix(opts.Connect, "postgres://") {
		if !strings.ContainsRune(opts.Connect[11:], '/') {
			opts.Connect = opts.Connect[11:]
		}
		db, exists, err = connectPostgreSQL(opts.Connect)
	} else if strings.HasPrefix(opts.Connect, "sqlite://") {
		db, exists, err = connectSQLite(opts.Connect[9:], opts.Schema != nil)
	} else {
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

func connectSQLite(connect string, create bool) (*sqlx.DB, bool, error) {
	exists := true
	stat, err := os.Stat(connect)
	if os.IsNotExist(err) {
		exists = false
		if !create {
			return nil, false, fmt.Errorf("connectSQLite: database %q doesn't exist", connect)
		}

		err = os.MkdirAll(filepath.Dir(connect), 0755)
		if err != nil {
			return nil, false, fmt.Errorf("connectSQLite: create DB dir: %w", err)
		}
	}

	ok, err := zos.Writable(stat)
	if err != nil {
		return nil, false, fmt.Errorf("connectSQLite: %w", err)
	}
	if !ok {
		return nil, false, fmt.Errorf("connectSQLite: %q is not writable", connect)
	}

	db, err := sqlx.Connect("sqlite3", connect)
	if err != nil {
		return nil, false, fmt.Errorf("connectSQLite: %w", err)
	}

	return db, exists, nil
}
