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
	"zgo.at/zstd/zfs"
)

type ConnectOptions struct {
	Connect string   // Connect string.
	Create  bool     // Create database if it doesn't exist yet.
	Migrate []string // Migrations to run; nil for none, "all" for all, or a migration name.

	// Will be called for every migration that gets run.
	MigrateLog func(name string)

	// Database files; the following layout is assumed:
	//
	//   Schema       schema-{driver}.sql, schema.sql, or schema.gotxt
	//   Migrations   migrate/foo-{schema}.sql or migrate/foo.sql
	//   Queries      query/foo-{schema}.sql or query/foo.sql
	//
	// It's okay if files are missing; e.g. no migrate directory simply means
	// that it won't attempt to run migrations.
	Files fs.FS

	// In addition to migrations from .sql files, you can run migrations from Go
	// functions. See the documentation on Migrate for details.
	GoMigrations map[string]func(context.Context) error

	// ConnectHook for sqlite3.SQLiteDriver; mainly useful to add your own
	// functions:
	//
	//    opt.SQLiteHook = func(c *sqlite3.SQLiteConn) error {
	//        return c.RegisterFunc("percent_diff", func(start, final float64) float64 {
	//            return (final - start) / start * 100
	//        }, true)
	//    }
	//
	// It'll automatically register and connect to a new "sqlite3_zdb_[addr]"
	// driver; note that DriverName() will now return "sqlite3_zdb_[addr]"
	// instead of "sqlite3"; use zdb.SQLite() to test if a connection is a
	// SQLite one.
	SQLiteHook func(*sqlite3.SQLiteConn) error
}

// Connect to a database.
//
// The database will be created automatically if the database doesn't exist and
// Schema is in ConnectOptions. It looks for the following files, in this order:
//
//   schema.gotxt           Run zdb.SchemaTemplate first.
//   schema-{driver}.sql    Driver-specific schema.
//   schema.sql
//
// This will set the maximum number of open and idle connections to 25 each for
// PostgreSQL, and 16 and 4 for SQLite, instead of Go's default of 0 and 2.
//
// To change this, you can use:
//   db.DBSQL().SetMaxOpenConns(100)
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
// For details on the connection string, see the documentation for go-sqlite3
// and pq:
// https://github.com/mattn/go-sqlite3/
// https://github.com/lib/pq
func Connect(opt ConnectOptions) (DB, error) {
	// TODO: look at pgx, which is the recommended driver now. sqlx still uses
	// pq though, but it's only referenced in tests.
	// https://github.com/jackc/pgx
	var proto, conn string
	if i := strings.Index(opt.Connect, "://"); i > -1 {
		proto = opt.Connect[:i]
		if len(opt.Connect) >= i+3 {
			conn = opt.Connect[i+3:]
		}
	}

	var (
		dbx    *sqlx.DB
		driver DriverType
		exists bool
		err    error
	)
	switch proto {
	case "postgresql", "postgres":
		// PostgreSQL supports two types of connection strings; a "URL-style"
		// and a "key-value style"; the following are identical:
		//
		//   "user=bob password=secret host=1.2.3.4 port=5432 dbname=mydb sslmode=verify-full"
		//   "postgres://bob:secret@1.2.3.4:5432/mydb?sslmode=verify-full"
		//
		// We don't know which style is being used as zdb.Connect() always uses
		// "postgresql://" prefix to determine the driver, so we just try to
		// connect with both.
		dbx, exists, err = connectPostgreSQL(conn, opt.Create) // k/v style
		if err != nil {
			dbx, exists, err = connectPostgreSQL(opt.Connect, opt.Create) // URL-style
		}
		driver = DriverPostgreSQL
	case "sqlite", "sqlite3":
		dbx, exists, err = connectSQLite(conn, opt.Create, opt.SQLiteHook)
		driver = DriverSQLite
	default:
		err = fmt.Errorf("zdb.Connect: unrecognized database engine %q in connect string %q", proto, opt.Connect)
	}
	if err != nil {
		return nil, fmt.Errorf("zdb.Connect: %w", err)
	}

	db := &zDB{db: dbx, driver: driver}

	// No files for DB creation and migration: can just return now.
	if opt.Files == nil {
		return db, nil
	}

	// Accept both "go:embed db/*" from the toplevel, and "go:embbed *" from the
	// db package.
	opt.Files, err = zfs.SubIfExists(opt.Files, "db")
	if err != nil {
		return nil, fmt.Errorf("zdb.Connect: %w", err)
	}
	db.queryFS, _ = fs.Sub(opt.Files, "query") // Optional, okay to ignore error.

	// Create schema.
	if !exists {
		s, err := fs.ReadFile(opt.Files, "schema.gotxt")
		if err == nil {
			s, err = SchemaTemplate(db.Driver(), string(s))
		} else {
			s, err = findFile(opt.Files, insertDriver(db, "schema")...)
		}
		if err != nil {
			return nil, fmt.Errorf("zdb.Connect: %w", err)
		}

		err = TX(WithDB(context.Background(), db), func(ctx context.Context) error {
			return Exec(ctx, string(s))
		})
		if err != nil {
			return nil, fmt.Errorf("zdb.Connect: running schema: %w", err)
		}

		// Always run migrations for new databases.
		opt.Migrate = []string{"all"}
	}

	// Run migrations.
	if opt.Migrate != nil {
		m, err := NewMigrate(db, opt.Files, opt.GoMigrations)
		if err != nil {
			return nil, fmt.Errorf("zdb.Connect: %w", err)
		}
		m.Log(opt.MigrateLog)
		err = m.Run(opt.Migrate...)
		if err != nil {
			return nil, fmt.Errorf("zdb.Connect: %w", err)
		}
		return db, m.Check()
	}
	return db, nil
}

func insertDriver(db DB, name string) []string {
	switch db.Driver() {
	case DriverSQLite:
		return []string{name + "-sqlite.sql", name + "-sqlite3.sql", name + ".sql"}
	case DriverPostgreSQL:
		return []string{name + "-postgres.sql", name + "-postgresql.sql", name + "-psql.sql", name + ".sql"}
	default:
		return []string{name + "-" + db.DriverName() + ".sql", name + ".sql"}
	}
}

func findFile(files fs.FS, paths ...string) ([]byte, error) {
	for _, f := range paths {
		s, err := fs.ReadFile(files, f)
		if err == nil {
			return s, nil
		}
	}
	return nil, fmt.Errorf("could not load any of the files: %s", paths)
}

func connectPostgreSQL(connect string, create bool) (*sqlx.DB, bool, error) {
	db, err := sqlx.Connect("postgres", connect)
	if err != nil {
		var (
			dbname string
			pqErr  *pq.Error
		)
		if errors.As(err, &pqErr) && pqErr.Code == "3D000" {
			x := regexp.MustCompile(`pq: database "(.+?)" does not exist`).FindStringSubmatch(pqErr.Error())
			if len(x) >= 2 {
				dbname = x[1]
			}
		}

		if create && dbname != "" {
			out, cerr := exec.Command("createdb", dbname).CombinedOutput()
			if cerr != nil {
				return nil, false, fmt.Errorf("connectPostgreSQL: %w: %s", cerr, out)
			}

			db, err = sqlx.Connect("postgres", connect)
			if err != nil {
				return nil, false, fmt.Errorf("connectPostgreSQL: %w", err)
			}

			return db, false, nil
		}

		if dbname != "" {
			return nil, false, &NotExistError{Driver: "postgres", DB: dbname, Connect: connect}
		}
		return nil, false, fmt.Errorf("connectPostgreSQL: %w", err)
	}

	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(25)

	return db, true, nil
}

// NotExistError is returned when a database doesn't exist and Create is false
// in the connection arguments.
type NotExistError struct {
	Driver  string // Driver name
	DB      string // Database name
	Connect string // Full connect string
}

func (err NotExistError) Error() string {
	return fmt.Sprintf("%s database %q doesn't exist (from connection string %q)",
		err.Driver, err.DB, err.Driver+"://"+err.Connect)
}

func connectSQLite(connect string, create bool, hook func(c *sqlite3.SQLiteConn) error) (*sqlx.DB, bool, error) {
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
				if abs, err := filepath.Abs(file); err == nil {
					file = abs
				}
				return nil, false, &NotExistError{Driver: "sqlite3", DB: file, Connect: connect}
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

	// Register a new driver for every unique hook we see, and re-use existing
	// drivers.
	driver := "sqlite3"
	if hook != nil {
		suffix := "_zdb_" + fmt.Sprintf("%p\n", hook)[2:]
		driver += suffix

		found := false
		for _, d := range sql.Drivers() {
			if d == driver {
				found = true
				break
			}
		}
		if !found {
			sql.Register(driver, &sqlite3.SQLiteDriver{ConnectHook: hook})
		}
	}

	db, err := sqlx.Connect(driver, connect)
	if err != nil {
		return nil, false, fmt.Errorf("connectSQLite: %w", err)
	}

	db.SetMaxOpenConns(16)
	db.SetMaxIdleConns(4)

	return db, exists, nil
}
