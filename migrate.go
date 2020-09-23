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
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"zgo.at/zstd/zstring"
)

type Migrate struct {
	DB           DB
	Which        []string                  // List of migrations to run.
	Migrations   map[string][]byte         // List of all migrations, for production.
	GoMigrations map[string]func(DB) error // Go migration functions.
	MigratePath  string                    // Path to migrations, for dev.
}

func NewMigrate(db DB, which []string, mig map[string][]byte, gomig map[string]func(DB) error, path string) *Migrate {
	return &Migrate{db, which, mig, gomig, path}
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

		for k, f := range m.GoMigrations {
			if k == run {
				err = f(m.DB)
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
		_, err = m.DB.ExecContext(context.Background(), s)
		if err != nil {
			return fmt.Errorf("zdb.Migrate.Run %q: %w", run, err)
		}
	}

	return nil
}

var printedWarning bool

// Get a list of all migrations we know about, and all migrations that have
// already been run.
func (m Migrate) List() (haveMig, ranMig []string, err error) {
	if _, err := os.Stat(m.MigratePath); os.IsNotExist(err) {
		// Compiled migrations.
		for k := range m.Migrations {
			haveMig = append(haveMig, k)
		}
		for k := range m.GoMigrations {
			haveMig = append(haveMig, k)
		}
	} else {
		if !printedWarning {
			l.Printf("WARNING: using migrations from filesystem; make sure the version of your source code matches the binary")
			printedWarning = true
		}

		// Load from filesystem.
		haveMig, err = filepath.Glob(m.MigratePath + "/*.sql")
		if err != nil {
			return nil, nil, fmt.Errorf("glob: %w", err)
		}
		for k := range m.GoMigrations {
			haveMig = append(haveMig, k)
		}
	}
	for i := range haveMig {
		haveMig[i] = strings.TrimSuffix(filepath.Base(haveMig[i]), ".sql")
	}
	sort.Strings(haveMig)

	err = m.DB.SelectContext(context.Background(), &ranMig,
		`select name from version order by name asc`)
	if err != nil {
		return nil, nil, fmt.Errorf("select version: %w", err)
	}
	return haveMig, ranMig, nil
}

// Schema of a migration by name.
func (m Migrate) Schema(n string) (string, error) {
	for k, _ := range m.GoMigrations {
		if n == k {
			return "", fmt.Errorf("%q is a Go migration", n)
		}
	}

	if !strings.HasSuffix(n, ".sql") {
		n += ".sql"
	}

	var (
		b   []byte
		err error
	)
	if _, serr := os.Stat(m.MigratePath); os.IsNotExist(serr) {
		// Compiled migrations.
		err = fmt.Errorf("no migration found: %q", n)
		for k, v := range m.Migrations {
			if strings.HasSuffix(k, n) {
				b = v
				err = nil
				break
			}
		}
	} else {
		path := filepath.Clean(n)
		if !strings.HasPrefix(path, m.MigratePath) {
			path = filepath.Join(m.MigratePath, path)
		}

		// Load from filesystem.
		b, err = ioutil.ReadFile(path)
	}
	if err != nil {
		return "", fmt.Errorf("Migrate.Schema: %w", err)
	}

	return string(b), nil
}

// Check if there are pending migrations and zlog.Error() if there are.
func (m Migrate) Check() error {
	if m.Migrations == nil && m.MigratePath == "" {
		return nil
	}
	if zstring.Contains(m.Which, "show") || zstring.Contains(m.Which, "list") {
		return nil
	}

	if len(m.Which) > 0 {
		err := m.Run(m.Which...)
		if err != nil {
			return fmt.Errorf("zdb.Migrate.Check: %w", err)
		}
	}

	haveMig, ranMig, err := m.List()
	if err != nil {
		return fmt.Errorf("zdb.Migrate.Check: %w", err)
	}

	if d := zstring.Difference(haveMig, ranMig); len(d) > 0 {
		l.Field("migrations", d).Errorf("pending migrations")
	}
	return nil
}
