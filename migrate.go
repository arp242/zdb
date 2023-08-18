package zdb

import (
	"context"
	"fmt"
	"io/fs"
	"path/filepath"
	"slices"
	"sort"
	"strings"

	"zgo.at/zstd/zfs"
	"zgo.at/zstd/zslice"
	"zgo.at/zstd/zstring"
)

// Migrate allows running database migrations.
type Migrate struct {
	db         DB
	files      fs.FS
	gomig      map[string]func(context.Context) error
	log        func(name string)
	test, show bool
}

// NewMigrate creates a new migration instance.
//
// Migrations are loaded from the filesystem, as described in ConnectOptions.
//
// You can optionally pass a list of Go functions to run as a "migration".
//
// Every migration is automatically run in a transaction; and an entry in the
// version table is inserted.
func NewMigrate(db DB, files fs.FS, gomig map[string]func(context.Context) error) (*Migrate, error) {
	files, err := zfs.SubIfExists(files, "db/migrate")
	if err != nil {
		return nil, fmt.Errorf("zdb.NewMigrate: %w", err)
	}

	err = db.Exec(context.Background(), `create table if not exists version (name varchar(512))`)
	if err != nil {
		return nil, fmt.Errorf("zdb.NewMigrate: create version table: %w", err)
	}
	return &Migrate{db: db, files: files, gomig: gomig}, nil
}

// Log sets a log function for migrations; this gets called for every migration
// that gets run.
//
// This only gets called if the migration was run successfully.
func (m *Migrate) Log(f func(name string)) { m.log = f }

// Test sets the "test" flag: it won't commit any transactions.
//
// This will work correctly for SQLite and PostgreSQL, but not MariaDB as most
// ALTER and CREATE commands will automatically imply COMMIT. See:
// https://mariadb.com/kb/en/sql-statements-that-cause-an-implicit-commit/
func (m *Migrate) Test(t bool) { m.test = t }

// Show sets the "show" flag; it won't run anything, just print the queries it
// would run to stdout.
func (m *Migrate) Show(v bool) { m.show = v }

// List all migrations we know about, and all migrations that have already been
// run.
func (m Migrate) List() (haveMig, ranMig []string, err error) {
	ls, err := fs.ReadDir(m.files, ".")
	if err != nil {
		return nil, nil, fmt.Errorf("read migrations: %w", err)
	}

	dialect := m.db.SQLDialect()
	for _, f := range ls {
		if !zstring.HasSuffixes(f.Name(), ".sql", "gotxt") {
			continue
		}

		isFor := DialectUnknown
		if zstring.HasSuffixes(f.Name(), "-postgres.sql", "-postgresql.sql") {
			isFor = DialectPostgreSQL
		} else if zstring.HasSuffixes(f.Name(), "-sqlite3.sql", "-sqlite.sql") {
			isFor = DialectSQLite
		} else if zstring.HasSuffixes(f.Name(), "-mariadb.sql", "-maria.sql", "mysql.sql") {
			isFor = DialectMariaDB
		}
		if isFor != DialectUnknown && isFor != dialect {
			continue
		}

		haveMig = append(haveMig, zstring.TrimSuffixes(f.Name(), ".sql", ".gotxt",
			"-postgres", "-postgresql", "-sqlite3", "-sqlite", "-maria", "-mariadb", "-mysql"))
	}
	for k := range m.gomig {
		haveMig = append(haveMig, k)
	}
	sort.Strings(haveMig)

	err = m.db.Select(context.Background(), &ranMig,
		`select name from version order by name asc`)
	if err != nil {
		return nil, nil, fmt.Errorf("select version: %w", err)
	}
	return haveMig, ranMig, nil
}

// Schema of a migration by name.
func (m Migrate) Schema(name string) (string, error) {
	if m.findGoMig(name) != nil {
		return "", fmt.Errorf("%q is a Go migration", name)
	}

	b, file, err := findFile(m.files, insertDialect(m.db, zstring.TrimSuffixes(name, ".sql", ".gotxt"))...)
	if err != nil {
		return "", err
	}

	if strings.HasSuffix(file, ".gotxt") {
		b, err = Template(m.db.SQLDialect(), string(b))
		if err != nil {
			return "", err
		}
	}

	return string(b), nil
}

// PendingMigrationsError is a non-fatal error used to indicate there are
// migrations that have not yet been run.
type PendingMigrationsError struct{ Pending []string }

func (err PendingMigrationsError) Error() string {
	s := make([]string, 0, len(err.Pending))
	for _, p := range err.Pending {
		s = append(s, fmt.Sprintf("%q", p))
	}
	return fmt.Sprintf("%d pending migrations: %s", len(err.Pending), strings.Join(s, ", "))
}

// Check if there are pending migrations; will return the (non-fatal)
// PendingMigrationsError if there are.
func (m Migrate) Check() error {
	haveMig, ranMig, err := m.List()
	if err != nil {
		return fmt.Errorf("zdb.Migrate.Check: %w", err)
	}

	if d := zslice.Difference(haveMig, ranMig); len(d) > 0 {
		return &PendingMigrationsError{Pending: d}
	}
	return nil
}

// Run a migration, or all of then if which contains "all" or "auto".
func (m Migrate) Run(which ...string) error {
	haveMig, ranMig, err := m.List()
	if err != nil {
		return fmt.Errorf("zdb.Migrate.Run: %w", err)
	}

	if zslice.ContainsAny(which, "all", "auto") {
		which = zslice.Difference(haveMig, ranMig)
	}

	ctx := WithDB(context.Background(), m.db)
	for _, run := range which {
		if run == "pending" {
			continue
		}

		if m.log != nil {
			msg := run
			if m.test {
				msg += " (test mode; not committing)"
			}
			m.log(msg)
		}

		if m.db.SQLDialect() == DialectSQLite {
			err := Exec(ctx, `pragma foreign_keys = off`)
			if err != nil {
				return fmt.Errorf("zdb.Migrate.Run: %w", err)
			}
		}

		version := strings.TrimSuffix(filepath.Base(run), ".sql")
		if slices.Contains(ranMig, version) {
			return fmt.Errorf("migration already run: %q (version entry: %q)", run, version)
		}

		ctx, tx, err := m.db.Begin(ctx)
		if err != nil {
			return fmt.Errorf("zdb.Migrate.Run: %w", err)
		}
		defer tx.Rollback()

		ok, err := m.runGoMig(ctx, run)
		if err != nil {
			return fmt.Errorf("zdb.Migrate.Run: running %q: %w", run, err)
		}
		if !ok {
			s, err := m.Schema(run)
			if err != nil {
				return fmt.Errorf("zdb.Migrate.Run: running %q: %w", run, err)
			}
			if m.show {
				query, _, err := prepareImpl(ctx, MustGetDB(ctx), s)
				if err != nil {
					return fmt.Errorf("zdb.Migrate.Run: running %q: %w", run, err)
				}

				fmt.Println("-- " + run)
				fmt.Println(query)
				return nil
			}
			err = Exec(ctx, s)
			if err != nil {
				return fmt.Errorf("zdb.Migrate.Run: running %q: %w", run, err)
			}
		}

		err = Exec(ctx, `insert into version (name) values (?)`, version)
		if err != nil {
			return fmt.Errorf("zdb.Migrate.Run: running %q: %w", run, err)
		}

		if !m.test {
			err := tx.Commit()
			if err != nil {
				return fmt.Errorf("zdb.Migrate.Run: running %q: %w", run, err)
			}
		}
	}

	return nil
}

func (m Migrate) findGoMig(name string) func(context.Context) error {
	for k, f := range m.gomig {
		if k == name {
			return f
		}
	}
	return nil
}

func (m Migrate) runGoMig(ctx context.Context, name string) (bool, error) {
	for k, f := range m.gomig {
		if k == name {
			return true, f(ctx)
		}
	}
	return false, nil
}
