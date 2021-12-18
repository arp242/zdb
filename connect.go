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

	_ "github.com/go-sql-driver/mysql"
	"github.com/lib/pq"
	"github.com/mattn/go-sqlite3"
	"zgo.at/zdb/internal/sqlx"
	"zgo.at/zstd/zfs"
)

// ConnectOptions are options for Connect().
type ConnectOptions struct {
	Connect string   // Connect string.
	Create  bool     // Create database if it doesn't exist yet.
	Migrate []string // Migrations to run; nil for none, "all" for all, or a migration name.

	// Will be called for every migration that gets run.
	MigrateLog func(name string)

	// Database files; the following layout is assumed:
	//
	//   Schema       schema-{driver}.sql, schema.sql, or schema.gotxt
	//   Migrations   migrate/{name}-{driver}.sql, migrate/{name}.sql, or migrate/{name}.gotxt
	//   Queries      query/{name}-{driver}.sql, query/{name}.sql, or query/{name}.gotxt
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
// Create is true. It looks for the following files, in this order:
//
//   schema.gotxt           Run zdb.Template first.
//   schema-{driver}.sql    Driver-specific schema.
//   schema.sql
//
// This will set the maximum number of open and idle connections to 25 each for
// PostgreSQL, and 16 and 4 for SQLite, instead of Go's default of 0 and 2. To
// change this, you can use:
//
//    db.DBSQL().SetMaxOpenConns(100)
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
// For details on the connection string, see the documentation for go-sqlite3,
// pq, or myself:
//
//   - https://github.com/mattn/go-sqlite3/
//   - https://github.com/lib/pq
//   - https://github.com/go-sql-driver/mysql
func Connect(opt ConnectOptions) (DB, error) {
	var proto, conn string
	if i := strings.Index(opt.Connect, "://"); i > -1 {
		proto = opt.Connect[:i]
		if len(opt.Connect) >= i+3 {
			conn = opt.Connect[i+3:]
		}
	}

	var (
		dbx     *sqlx.DB
		dialect Dialect
		exists  bool
		err     error
		ctx     = context.TODO()
	)
	switch proto {
	case "postgresql", "postgres":
		// PostgreSQL supports two types of connection strings; a "URL-style"
		// and a "key-value style"; the following are identical:
		//
		//   "user=bob password=secret host=1.2.3.4 port=5432 dbname=mydb sslmode=verify-full"
		//   "postgres://bob:secret@1.2.3.4:5432/mydb?sslmode=verify-full"
		//
		// We don't know which style is being used as zdb.Connect() already uses
		// "postgresql://" prefix to determine the driver, so we just try to
		// connect with both.
		//
		// TODO: I hate how this works, and the errors it reports on invalid
		// connections (i.e. wrong dbname) are horrible.
		//
		// Maybe just tell people to use "postgres://postgres://[..]"? That's
		// pretty ugly though. Could shorten it to "postgres://://[..]",
		// "postgres://:[..]", or maybe something else.
		//
		//   -db sqlite3:file.foo
		//   -db sqlite:file.foo
		//   -db psql:...
		//   -db pgsql:...
		//   -db pg:...
		//   -db postgres:..
		//   -db postgresql:..
		dbx, exists, err = connectPostgreSQL(ctx, conn, opt.Create) // k/v style
		if err != nil {
			dbx, exists, err = connectPostgreSQL(ctx, opt.Connect, opt.Create) // URL-style
		}
		dialect = DialectPostgreSQL
	case "sqlite", "sqlite3":
		dbx, exists, err = connectSQLite(ctx, conn, opt.Create, opt.SQLiteHook)
		dialect = DialectSQLite
	case "mysql":
		dbx, exists, err = connectMariaDB(ctx, conn, opt.Create)
		dialect = DialectMariaDB
	default:
		err = fmt.Errorf("zdb.Connect: unrecognized database engine %q in connect string %q", proto, opt.Connect)
	}
	if err != nil {
		return nil, fmt.Errorf("zdb.Connect: %w", err)
	}

	db := &zDB{db: dbx, dialect: dialect}

	// These versions are required for zdb.
	info, err := db.Info(WithDB(context.Background(), db))
	if err != nil {
		return nil, fmt.Errorf("zdb.Connect: %w", err)
	}
	switch db.SQLDialect() {
	case DialectSQLite:
		if !info.Version.AtLeast("3.35") {
			err = errors.New("zdb.Connect: zdb requires SQLite 3.35.0 or newer")
		}
	case DialectMariaDB:
		if !info.Version.AtLeast("10.5") {
			err = errors.New("zdb.Connect: zdb requires MariaDB 10.5.0 or newer")
		}
	case DialectPostgreSQL:
		if !info.Version.AtLeast("12.0") {
			err = errors.New("zdb.Connect: zdb requires PostgreSQL 12.0 or newer")
		}
	}
	if err != nil {
		return nil, err
	}

	// No files for DB creation and migration: can just return now.
	if opt.Files == nil {
		return db, nil
	}

	// Accept both "go:embed db/*" from the toplevel, and "go:embed *" from the
	// db package.
	opt.Files, err = zfs.SubIfExists(opt.Files, "db")
	if err != nil {
		return nil, fmt.Errorf("zdb.Connect: %w", err)
	}
	db.queryFS, _ = fs.Sub(opt.Files, "query") // Optional, okay to ignore error.

	// The database can exist, but be empty. Consider a database to "exist" only
	// if there's more than one table (any table).
	if exists {
		exists, err = hasTables(db)
		if err != nil {
			return nil, fmt.Errorf("zdb.Connect: %w", err)
		}
	}

	// Create schema.
	if !exists {
		if !opt.Create {
			return nil, &NotExistError{Driver: dialect.String(), Connect: conn}
		}

		s, file, err := findFile(opt.Files, insertDialect(db, "schema")...)
		if err != nil {
			return nil, fmt.Errorf("zdb.Connect: %w", err)
		}
		if strings.HasSuffix(file, ".gotxt") {
			s, err = Template(db.SQLDialect(), string(s))
			if err != nil {
				return nil, fmt.Errorf("zdb.Connect: %w", err)
			}
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

func insertDialect(db DB, name string) []string {
	switch db.SQLDialect() {
	case DialectSQLite:
		return []string{name + "-sqlite.sql", name + "-sqlite3.sql", name + ".gotxt", name + ".sql"}
	case DialectPostgreSQL:
		return []string{name + "-postgres.sql", name + "-postgresql.sql", name + "-psql.sql", name + ".gotxt", name + ".sql"}
	case DialectMariaDB:
		return []string{name + "-mysql.sql", name + ".gotxt", name + ".sql"}
	default:
		return []string{name + ".gotxt", name + ".sql"}
	}
}

func findFile(files fs.FS, paths ...string) ([]byte, string, error) {
	for _, f := range paths {
		s, err := fs.ReadFile(files, f)
		if err == nil {
			return s, f, nil
		}
	}
	return nil, "", fmt.Errorf("could not load any of the files: %s", paths)
}

// NotExistError is returned when a database doesn't exist and Create is false
// in the connection arguments.
type NotExistError struct {
	Driver  string // Driver name
	DB      string // Database name
	Connect string // Full connect string
}

func (err NotExistError) Error() string {
	if err.Driver == "" {
		return fmt.Sprintf("%s database exists but is empty (from connection string %q)",
			err.Driver, err.Driver+"://"+err.Connect)
	}
	return fmt.Sprintf("%s database %q doesn't exist (from connection string %q)",
		err.Driver, err.DB, err.Driver+"://"+err.Connect)
}

func connectPostgreSQL(ctx context.Context, connect string, create bool) (*sqlx.DB, bool, error) {
	exists := true
	db, err := sqlx.ConnectContext(ctx, "postgres", connect)
	if err != nil {
		var (
			dbname string
			pqErr  *pq.Error
		)
		if errors.As(err, &pqErr) && pqErr.Code == "3D000" {
			// TODO: rather ugly way to get database name :-/ pq doesn't expose
			// any way to parse the connection string :-/
			//
			// This would also allow us to get rid of the "createdb" shell
			// command; we can connect with the same credentials to the
			// "postgres" database and try "create database [wantdb]", using
			// "createdb" only as a fallback. But to do this we need more
			// information.
			//
			// https://github.com/jackc/pgx does expose this, and it the
			// "recommended library" according to the pg readme, but this pulls
			// in a slew of 34 dependencies, so meh. There's a few issues about
			// this; might get fixed in v5.
			//
			// pg is in "maintainence mode" with many unmerged PRs; it works,
			// but is on its way out. Should really look into this. Maybe do a
			// "fork" which just fixed a bit of the dependency stuff(?) We don't
			// need all the logadapter stuff, can vendor the jackc/pg* things,
			// and maybe do some other simple automated changes.
			x := regexp.MustCompile(`pq: database "(.+?)" does not exist`).FindStringSubmatch(pqErr.Error())
			if len(x) >= 2 {
				dbname = x[1]
				exists = true
			}
		}

		if create && dbname != "" {
			out, cerr := exec.Command("createdb", dbname).CombinedOutput()
			if cerr != nil {
				return nil, false, fmt.Errorf("connectPostgreSQL: %w: %s", cerr, out)
			}

			db, err = sqlx.ConnectContext(ctx, "postgres", connect)
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

	return db, exists, nil
}

func connectMariaDB(ctx context.Context, connect string, create bool) (*sqlx.DB, bool, error) {
	db, err := sqlx.ConnectContext(ctx, "mysql", connect)
	if err != nil {
		return nil, false, fmt.Errorf("connectMariaDB: %w", err)
	}

	return db, true, nil
}

func connectSQLite(ctx context.Context, connect string, create bool, hook func(c *sqlite3.SQLiteConn) error) (*sqlx.DB, bool, error) {
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

	db, err := sqlx.ConnectContext(ctx, driver, connect)
	if err != nil {
		return nil, false, fmt.Errorf("connectSQLite: %w", err)
	}

	db.SetMaxOpenConns(16)
	db.SetMaxIdleConns(4)

	return db, exists, nil
}

func hasTables(db DB) (bool, error) {
	var (
		has int
		err error
	)
	switch db.SQLDialect() {
	case DialectPostgreSQL:
		err = db.Get(context.Background(), &has, `select
			(select count(*) from pg_views where schemaname = current_schema()) +
			(select count(*) from pg_tables where schemaname = current_schema() and tablename != 'version')`)
	case DialectSQLite:
		err = db.Get(context.Background(), &has, `select count(*) from sqlite_schema where tbl_name != 'version'`)
	case DialectMariaDB:
		// TODO: views?
		// TODO: exclude version; I don't have MariaDB running atm.
		err = db.Get(context.Background(), &has, `select count(*) from information_schema.TABLES`)
	}
	return has > 0, err
}
