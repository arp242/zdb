package zdb

// TODO: more advanced -migrate flag
//   Show migration status and exit:             ./goatcounter -migrate
//   Migrate all pending migrations and exit:    ./goatcounter -migrate all
//   Migrate one and exit:                       ./goatcounter -migrate 2019-10-16-1-geoip
//   Rollback last migration:                    ./goatcounter -migrate rollback:last
//   Rollback specific migration:                ./goatcounter -migrate rollback:2019-10-16-1-geoip

import (
	"context"
	"fmt"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"

	"zgo.at/zstd/zstring"
)

type Migrate struct {
	db    DB
	mig   fs.FS
	gomig map[string]func(DB) error
}

// NewMigrate creates a new migration instance.
//
// Migrations are loaded from the filesystem, as described in ConnectOptions.
//
// You can optionally pass a list of Go functions to run as a "migration";
func NewMigrate(db DB, mig fs.FS, gomig map[string]func(DB) error) *Migrate {
	_, err := fs.ReadDir(mig, db.DriverName())
	if err == nil {
		mig, _ = fs.Sub(mig, db.DriverName())
	}
	_, err = fs.ReadDir(mig, "migrate")
	if err == nil {
		mig, _ = fs.Sub(mig, "migrate")
	}

	_, err = db.ExecContext(context.Background(), `create table if not exists version (name varchar)`)
	if err != nil {
		panic(fmt.Sprintf("create version table: %s", err))
	}

	return &Migrate{db, mig, gomig}
}

// List all migrations we know about, and all migrations that have already been
// run.
func (m Migrate) List() (haveMig, ranMig []string, err error) {
	ls, err := fs.ReadDir(m.mig, ".")
	if err != nil {
		return nil, nil, fmt.Errorf("read migrations: %w", err)
	}
	for _, f := range ls {
		if strings.HasSuffix(f.Name(), ".sql") {
			haveMig = append(haveMig, strings.TrimSuffix(f.Name(), ".sql"))
		}
	}
	for k := range m.gomig {
		haveMig = append(haveMig, k)
	}
	sort.Strings(haveMig)

	err = m.db.SelectContext(context.Background(), &ranMig,
		`select name from version order by name asc`)
	if err != nil {
		return nil, nil, fmt.Errorf("select version: %w", err)
	}
	return haveMig, ranMig, nil
}

// Schema of a migration by name.
func (m Migrate) Schema(n string) (string, error) {
	for k, _ := range m.gomig {
		if n == k {
			return "", fmt.Errorf("%q is a Go migration", n)
		}
	}

	if !strings.HasSuffix(n, ".sql") {
		n += ".sql"
	}

	b, err := fs.ReadFile(m.mig, n)
	if err != nil {
		return "", err
	}

	return string(b), nil
}

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

// Run a migration, or all of then if which is "all" or "auto".
func (m Migrate) Run(which ...string) error {
	haveMig, ranMig, err := m.List()
	if err != nil {
		return fmt.Errorf("zdb.Migrate.Run: %w", err)
	}

	if zstring.Contains(which, "all") {
		which = zstring.Difference(haveMig, ranMig)
	}

outer:
	for _, run := range which {
		if run == "show" || run == "list" {
			continue
		}

		for k, f := range m.gomig {
			if k == run {
				err = f(m.db)
				if err != nil {
					return fmt.Errorf("zdb.Migrate.Run %q: %w", run, err)
				}
				continue outer
			}
		}

		version := strings.TrimSuffix(filepath.Base(run), ".sql")
		if zstring.Contains(ranMig, version) {
			return fmt.Errorf("zdb.Migrate.Run: migration already run: %q (version entry: %q)", run, version)
		}

		s, err := m.Schema(run)
		if err != nil {
			return fmt.Errorf("zdb.Migrate.Run: %w", err)
		}

		l.Field("name", run).Print("SQL migration")
		_, err = m.db.ExecContext(context.Background(), s)
		if err != nil {
			return fmt.Errorf("zdb.Migrate.Run %q: %w", run, err)
		}
	}

	return nil
}
