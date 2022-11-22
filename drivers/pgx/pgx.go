// Package pgx provides a zdb driver for PostgreSQL.
//
// This uses https://github.com/jackc/pgx
package pgx

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib"
	"zgo.at/zdb"
	"zgo.at/zdb/drivers"
)

func init() {
	drivers.RegisterDriver(driver{})
}

type driver struct{}

func (driver) Name() string    { return "pgx" }
func (driver) Dialect() string { return "postgresql" }
func (driver) ErrUnique(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}
func (driver) Connect(ctx context.Context, connect string, create bool) (*sql.DB, bool, error) {
	exists := true
	db, err := sql.Open("pgx", connect)
	if err != nil {
		return nil, false, fmt.Errorf("pgx.Connect: %w", err)
	}

	err = db.PingContext(ctx)
	if err != nil {
		var (
			dbname string
			pgErr  *pgconn.PgError
		)
		if errors.As(err, &pgErr) && pgErr.Code == "3D000" {
			// TODO: we can parse the connection string with pgx now to get the
			// database name.
			x := regexp.MustCompile(`database "(.+?)" does not exist`).FindStringSubmatch(pgErr.Error())
			if len(x) >= 2 {
				dbname = x[1]
				exists = true
			}
		}

		if create && dbname != "" {
			out, cerr := exec.Command("createdb", dbname).CombinedOutput()
			if cerr != nil {
				return nil, false, fmt.Errorf("pgx.Connect: %w: %s", cerr, out)
			}

			db, err = sql.Open("pgx", connect)
			if err != nil {
				return nil, false, fmt.Errorf("pgx.Connect: %w", err)
			}

			return db, false, nil
		}

		if dbname != "" {
			return nil, false, &drivers.NotExistError{Driver: "postgres", DB: dbname, Connect: connect}
		}
		return nil, false, fmt.Errorf("pgx.Connect: %w", err)
	}

	return db, exists, nil
}

// StartTest starts a new test.
//
// TODO: document.
func (driver) StartTest(t *testing.T, opt *drivers.TestOptions) context.Context {
	t.Helper()

	if e := os.Getenv("PGDATABASE"); e == "" {
		os.Setenv("PGDATABASE", "zdb_test")
	}

	copt := zdb.ConnectOptions{Connect: "postgresql+", Create: true}
	if opt != nil && opt.Connect != "" {
		copt.Connect = opt.Connect
	}
	if opt != nil && opt.Files != nil {
		copt.Files = opt.Files
	}
	db, err := zdb.Connect(context.Background(), copt)
	if err != nil {
		t.Fatalf("pgx.StartTest: connecting to %q: %s", copt.Connect, err)
	}

	// The first test will create the zdb_test database, and every test after
	// that runs in its own schema.
	schema := fmt.Sprintf(`"zdb_test_%s"`, time.Now().Format("20060102T15:04:05.9999"))
	err = db.Exec(context.Background(), `create schema `+schema)
	if err != nil {
		t.Fatalf("pgx.StartTest: creating schema %s: %s", schema, err)
	}
	err = db.Exec(context.Background(), "set search_path to "+schema)
	if err != nil {
		t.Fatalf("pgx.StartTest: setting search_path to %s: %s", schema, err)
	}

	// No easy way to copy the public schema, so just run the create again.
	// TODO: migrate, too?
	if copt.Files != nil {
		err = zdb.Create(db, copt.Files)
		if err != nil {
			t.Fatalf("pgx.StartTest: creating database in schema %s: %s", schema, err)
		}
	}

	t.Cleanup(func() {
		db.Exec(context.Background(), "drop schema "+schema+" cascade")
		db.Close()
	})
	return zdb.WithDB(context.Background(), db)
}
