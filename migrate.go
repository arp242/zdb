package zdb

import (
	"context"
	"fmt"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"

	"zgo.at/zstd/zstring"
)

// Migrate allows running database migrations.
type Migrate struct {
	db    DB
	files fs.FS
	gomig map[string]func(context.Context) error
}

func subIfExists(files fs.FS, dir string) (fs.FS, error) {
	ls, err := fs.ReadDir(files, ".")
	if err != nil {
		return nil, err
	}
	if len(ls) == 0 {
		return files, nil
	}

	for _, e := range ls {
		if e.IsDir() && e.Name() == dir {
			return fs.Sub(files, dir)
		}
	}
	return files, nil
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
	files, err := subIfExists(files, "db")
	if err != nil {
		return nil, fmt.Errorf("zdb.NewMigrate: %w", err)
	}
	files, err = subIfExists(files, "migrate")
	if err != nil {
		return nil, fmt.Errorf("zdb.NewMigrate: %w", err)
	}

	err = db.Exec(context.Background(), `create table if not exists version (name varchar)`)
	if err != nil {
		return nil, fmt.Errorf("zdb.NewMigrate: create version table: %w", err)
	}
	return &Migrate{db: db, files: files, gomig: gomig}, nil
}

// List all migrations we know about, and all migrations that have already been
// run.
func (m Migrate) List() (haveMig, ranMig []string, err error) {
	ls, err := fs.ReadDir(m.files, ".")
	if err != nil {
		return nil, nil, fmt.Errorf("read migrations: %w", err)
	}

	ctx := WithDB(context.Background(), m.db)
	for _, f := range ls {
		if SQLite(ctx) && zstring.HasSuffixes(f.Name(), "-postgres.sql", "-postgresql.sql", "-psql.sql") {
			continue
		}
		if PgSQL(ctx) && zstring.HasSuffixes(f.Name(), "-sqlite3.sql", "-sqlite.sql") {
			continue
		}

		if strings.HasSuffix(f.Name(), ".sql") {
			haveMig = append(haveMig, zstring.TrimSuffixes(f.Name(),
				".sql", "-postgres", "-postgresql", "-psql.sql", "-sqlite3", "-sqlite"))
		}
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

	b, err := findFile(m.files, insertDriver(m.db, strings.TrimSuffix(name, ".sql"))...)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// PendingMigrationsError is a non-fatal error used to indicate there are
// migrations that have not yet been run.
type PendingMigrationsError error

// Check if there are pending migrations; will return the (non-fatal)
// PendingMigrationsError if there are.
func (m Migrate) Check() error {
	haveMig, ranMig, err := m.List()
	if err != nil {
		return fmt.Errorf("zdb.Migrate.Check: %w", err)
	}

	if d := zstring.Difference(haveMig, ranMig); len(d) > 0 {
		return PendingMigrationsError(fmt.Errorf("pending migrations: %s", d))
	}
	return nil
}

// Run a migration, or all of then if which contains "all" or "auto".
func (m Migrate) Run(which ...string) error {
	haveMig, ranMig, err := m.List()
	if err != nil {
		return fmt.Errorf("zdb.Migrate.Run: %w", err)
	}

	if zstring.Contains(which, "all") || zstring.Contains(which, "auto") {
		which = zstring.Difference(haveMig, ranMig)
	}

	for _, run := range which {
		if run == "show" || run == "list" {
			continue
		}

		err := TX(WithDB(context.Background(), m.db), func(ctx context.Context) error {
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

			//l.Field("name", run).Print("SQL migration")
			err = Exec(ctx, s)
			if err != nil {
				return err
			}
			return Exec(ctx, `insert into version (name) values (?)`, version)
		})
		if err != nil {
			return fmt.Errorf("zdb.Migrate.Run %q: %w", run, err)
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
