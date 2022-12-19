package zdb

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"strings"

	"zgo.at/zdb/drivers"
	"zgo.at/zdb/internal/sqlx"
	"zgo.at/zstd/zfs"
)

// ConnectOptions are options for Connect().
type ConnectOptions struct {
	Connect    string            // Connect string.
	Create     bool              // Create database if it doesn't exist yet.
	Migrate    []string          // Migrations to run; nil for none, "all" for all, or a migration name.
	MigrateLog func(name string) // Called for every migration that gets run.

	// Set the maximum number of open and idle connections.
	//
	// The default for MaxOpenConns is 16, and the default for MaxIdleConns is
	// 4, instead of Go's default of 0 and 2. Use a value <0 to skip the
	// default.
	//
	// This can also be changed at runtime with:
	//
	//    db.DBSQL().SetMaxOpenConns(100)
	MaxOpenConns int
	MaxIdleConns int

	// In addition to migrations from .sql files, you can run migrations from Go
	// functions. See the documentation on Migrate for details.
	GoMigrations map[string]func(context.Context) error

	// Database files; the following layout is assumed:
	//
	//   Schema       schema-{dialect}.sql, schema.sql, or schema.gotxt
	//   Migrations   migrate/{name}-{dialect}.sql, migrate/{name}.sql, or migrate/{name}.gotxt
	//   Queries      query/{name}-{dialect}.sql, query/{name}.sql, or query/{name}.gotxt
	//
	// It's okay if files are missing; e.g. no migrate directory simply means
	// that it won't attempt to run migrations.
	Files fs.FS
}

// Connect to a database.
//
// To connect to a database you need to register a driver for it first. While
// zdb uses database/sql, it needs a zdb-specific driver which contains
// additional information. Several drivers are included in the
// zgo.at/zdb/drivers package. To register a driver simply import it:
//
//	import _ "zgo.at/zdb/drivers/pq"
//
// The connect string has the following layout (minus spaces):
//
//	dialect                           Use default connection parameters for this driver.
//	dialect              + connect    Pass driver-specific connection string.
//	driverName           + connect    Use a SQL driver name, instead of SQL dialect.
//	dialect / driverName + connect    Specify both.
//
// The connectString is driver-specific; see the documentation of the driver for
// details. The dialect is the "SQL dialect"; currently recognized dialects are:
//
//	postgresql    aliases: postgres psql pgsql
//	sqlite        aliases: sqlite3
//	mysql         aliases: mariadb
//
// For example, "postgresql+dbname=mydb", "pq+dbname=mydb", and
// "postgresql/pq+dbname=mydb" are all identical, assuming pq is the registered
// driver.
//
// If multiple drivers are registered for the same dialect then it will use the
// first one.
//
// If Create is set it will try to automatically create a database if it doesn't
// exist yet. If Files is given it will also look for the following files to set
// up the database if it doesn't exist or is empty:
//
//	schema.gotxt           Run zdb.Template first.
//	schema-{dialect}.sql   Schema for this SQL dialect.
//	schema.sql
//
// Migrate and GoMigrate are migrations to run, see the documentation of Migrate
// for details.
func Connect(ctx context.Context, opt ConnectOptions) (DB, error) {
	conn, driver, dialect := connectionString(opt.Connect)
	if dialect == DialectUnknown && driver == "" {
		return nil, errors.New("zdb.Connect: invalid syntax for connection string")
	}

	var useDriver drivers.Driver
	for _, d := range drivers.Drivers() {
		// Mostly for SQLite, which can have different driver names if a connect
		// hook was used. Note we pass the driver in the connect string, too.
		matcher, ok := d.(interface {
			Match(dialect, driver string) bool
		})
		if ok && matcher.Match(dialect.String(), driver) {
			conn += "+++" + driver
			useDriver = d
			break
		}

		if (dialect == DialectUnknown || dialectNames[d.Dialect()] == dialect) &&
			(driver == "" || d.Name() == driver) {
			useDriver = d
			break
		}
	}
	if useDriver == nil {
		return nil, fmt.Errorf("zdb.Connect: no driver found: dialect=%q; driver=%q", dialect, driver)
	}

	sqlDB, exists, err := useDriver.Connect(ctx, conn, opt.Create)
	if err != nil {
		return nil, fmt.Errorf("zdb.Connect: %w", err)
	}

	if opt.MaxOpenConns == 0 {
		opt.MaxOpenConns = 16
	}
	if opt.MaxIdleConns == 0 {
		opt.MaxIdleConns = 4
	}
	sqlDB.SetMaxOpenConns(opt.MaxOpenConns)
	sqlDB.SetMaxIdleConns(opt.MaxIdleConns)

	dialect = dialectNames[useDriver.Dialect()]
	db := &zDB{db: sqlx.NewDb(sqlDB, useDriver.Name()), dialect: dialect}

	// These versions are required for zdb.
	info, err := db.Info(WithDB(context.Background(), db))
	if err != nil {
		return nil, fmt.Errorf("zdb.Connect: %w", err)
	}
	switch db.SQLDialect() {
	case DialectSQLite:
		if !info.Version.AtLeast("3.35") {
			err = fmt.Errorf("zdb.Connect: zdb requires SQLite 3.35.0 or newer; have %q", info.Version)
		}
	case DialectMariaDB:
		if !info.Version.AtLeast("10.5") {
			err = fmt.Errorf("zdb.Connect: zdb requires MariaDB 10.5.0 or newer; have %q", info.Version)
		}
	case DialectPostgreSQL:
		if !info.Version.AtLeast("12.0") {
			err = fmt.Errorf("zdb.Connect: zdb requires PostgreSQL 12.0 or newer; have %q", info.Version)
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
			return nil, &drivers.NotExistError{Driver: dialect.String(), Connect: conn}
		}

		err := Create(db, opt.Files)
		if err != nil {
			return nil, fmt.Errorf("zdb.Connect: %w", err)
		}

		// Always run migrations for new databases.
		opt.Migrate = []string{"all"}
	}

	// Run migrations.
	if opt.Migrate != nil && zfs.Exists(opt.Files, "migrate") {
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

// Create tables based on db/schema.{sql,gotxt}
func Create(db DB, files fs.FS) error {
	s, file, err := findFile(files, insertDialect(db, "schema")...)
	if err != nil {
		return err
	}
	if strings.HasSuffix(file, ".gotxt") {
		s, err = Template(db.SQLDialect(), string(s))
		if err != nil {
			return err
		}
	}

	err = TX(WithDB(context.Background(), db), func(ctx context.Context) error {
		return Exec(ctx, string(s))
	})
	if err != nil {
		return fmt.Errorf("running schema: %w", err)
	}
	return nil
}

// We use "+" because this won't conflict with anything else; URL-style "://"
// would conflict with e.g. "postgresql://", just ":" would conflict with
// "sqlite::memory:.
//
// This is not a insurmountable problem as such, but using
// "postgresql://postgresl://..." or "sqlite::memory" is weird and easy to get
// wrong.
func connectionString(c string) (conn string, driver string, d Dialect) {
	dialect, conn, _ := strings.Cut(c, "+")
	if i := strings.IndexByte(dialect, '/'); i > -1 {
		dialect, driver = dialect[:i], dialect[i+1:]
	}
	d, ok := dialectNames[dialect]
	if !ok {
		driver = dialect
	}
	return conn, driver, d
}

func insertDialect(db DB, name string) []string {
	switch db.SQLDialect() {
	case DialectSQLite:
		return []string{name + "-sqlite.sql", name + "-sqlite3.sql", name + ".gotxt", name + ".sql"}
	case DialectPostgreSQL:
		return []string{name + "-postgres.sql", name + "-postgresql.sql", name + "-psql.sql", name + ".gotxt", name + ".sql"}
	case DialectMariaDB:
		return []string{name + "-maria.sql", name + "-mariadb.sql", name + "-mysql.sql", name + ".gotxt", name + ".sql"}
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
