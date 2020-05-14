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

	"zgo.at/utils/stringutil"
)

type Migrate struct {
	DB          DB
	Which       []string          // List of migrations to run.
	Migrations  map[string][]byte // List of all migrations, for production.
	MigratePath string            // Path to migrations, for dev.
}

func NewMigrate(db DB, which []string, mig map[string][]byte, path string) *Migrate {
	return &Migrate{db, which, mig, path}
}

// Run a migration, or all of then if which is "all" or "auto".
func (m Migrate) Run(which ...string) error {
	haveMig, ranMig, err := m.List()
	if err != nil {
		return fmt.Errorf("zdb.Migrate.Run: %w", err)
	}

	if stringutil.Contains(which, "all") {
		which = stringutil.Difference(haveMig, ranMig)
	}

	for _, run := range which {
		if run == "show" {
			continue
		}
		version := strings.TrimSuffix(filepath.Base(run), ".sql")
		if stringutil.Contains(ranMig, version) {
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

// Get a list of all migrations we know about, and all migrations that have
// already been run.
func (m Migrate) List() (haveMig, ranMig []string, err error) {
	if _, err := os.Stat(m.MigratePath); os.IsNotExist(err) {
		// Compiled migrations.
		for k := range m.Migrations {
			haveMig = append(haveMig, k)
		}
	} else {
		// Load from filesystem.
		haveMig, err = filepath.Glob(m.MigratePath + "/*.sql")
		if err != nil {
			return nil, nil, fmt.Errorf("glob: %w", err)
		}
	}
	for i := range haveMig {
		haveMig[i] = filepath.Base(haveMig[i][:len(haveMig[i])-4])
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
	if stringutil.Contains(m.Which, "show") {
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

	if d := stringutil.Difference(haveMig, ranMig); len(d) > 0 {
		l.Field("migrations", d).Errorf("pending migrations")
	}
	return nil
}
