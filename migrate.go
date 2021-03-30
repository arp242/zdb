package zdb

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"

	"zgo.at/zstd/zfs"
	"zgo.at/zstd/zstring"
)

// Migrate allows running database migrations.
type Migrate struct {
	db    DB
	files fs.FS
	gomig map[string]func(context.Context) error
	log   func(name string)
	test  bool
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

	err = db.Exec(context.Background(), `create table if not exists version (name varchar)`)
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

// Test sets the "test" flags: it won't commit any transactions.
func (m *Migrate) Test(t bool) { m.test = t }

// List all migrations we know about, and all migrations that have already been
// run.
func (m Migrate) List() (haveMig, ranMig []string, err error) {
	ls, err := fs.ReadDir(m.files, ".")
	if err != nil {
		return nil, nil, fmt.Errorf("read migrations: %w", err)
	}

	driver := m.db.Driver()
	for _, f := range ls {
		if !zstring.HasSuffixes(f.Name(), ".sql", "gotxt") {
			continue
		}

		isFor := DriverUnknown
		if zstring.HasSuffixes(f.Name(), "-postgres.sql", "-postgresql.sql") {
			isFor = DriverPostgreSQL
		} else if zstring.HasSuffixes(f.Name(), "-sqlite3.sql", "-sqlite.sql") {
			isFor = DriverSQLite
		} else if zstring.HasSuffixes(f.Name(), "-mariadb.sql") {
			isFor = DriverMariaDB
		}
		if isFor != DriverUnknown && isFor != driver {
			continue
		}

		haveMig = append(haveMig, zstring.TrimSuffixes(f.Name(), ".sql", ".gotxt",
			"-postgres", "-postgresql", "-sqlite3", "-sqlite", "-mariadb"))
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

	b, file, err := findFile(m.files, insertDriver(m.db, zstring.TrimSuffixes(name, ".sql", ".gotxt"))...)
	if err != nil {
		return "", err
	}

	if strings.HasSuffix(file, ".gotxt") {
		b, err = SchemaTemplate(m.db.Driver(), string(b))
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

	if d := zstring.Difference(haveMig, ranMig); len(d) > 0 {
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

	if zstring.ContainsAny(which, "all", "auto") {
		which = zstring.Difference(haveMig, ranMig)
	}

	ctx := WithDB(context.Background(), m.db)
	for _, run := range which {
		if run == "pending" {
			continue
		}

		if m.db.Driver() == DriverSQLite {
			err := Exec(ctx, `pragma foreign_keys = off`)
			if err != nil {
				return fmt.Errorf("zdb.Migrate.Run: %w", err)
			}
		}

		err := TX(ctx, func(ctx context.Context) error {
			version := strings.TrimSuffix(filepath.Base(run), ".sql")

			// Go migration.
			f := m.findGoMig(run)
			if f != nil {
				return f(ctx)
			}

			// SQL migration.
			if zstring.Contains(ranMig, version) {
				return fmt.Errorf("migration already run: %q (version entry: %q)", run, version)
			}

			s, err := m.Schema(run)
			if err != nil {
				return err
			}

			err = Exec(ctx, s)
			if err != nil {
				return err
			}
			err = Exec(ctx, `insert into version (name) values (?)`, version)
			if err != nil {
				return err
			}

			if m.test {
				return errors.New("TESTTEST")
			}
			return nil
		})
		if err != nil && !strings.HasSuffix(err.Error(), "TESTTEST") {
			fmt.Printf("%q\n", err.Error())
			return fmt.Errorf("zdb.Migrate.Run: error running %q: %w", run, err)
		}
		if m.log != nil {
			m.log(run)
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
