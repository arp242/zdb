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
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"zgo.at/zdb"
	"zgo.at/zdb/drivers"
	"zgo.at/zstd/zcrypto"
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
func (driver) Connect(ctx context.Context, connect string, create bool) (*sql.DB, any, bool, error) {
	exists := true

	cfg, err := pgxpool.ParseConfig(connect)
	if err != nil {
		return nil, nil, false, fmt.Errorf("pgx.Connect: %w", err)
	}
	schema := os.Getenv("PGSEARCHPATH")
	if schema != "" {
		cfg.ConnConfig.RuntimeParams["search_path"] = schema
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		return nil, nil, false, fmt.Errorf("pgx.Connect: %w", err)
	}
	db := stdlib.OpenDBFromPool(pool)

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
				return nil, nil, false, fmt.Errorf("pgx.Connect: %w: %s", cerr, out)
			}

			db, err = sql.Open("pgx", connect)
			if err != nil {
				return nil, nil, false, fmt.Errorf("pgx.Connect: %w", err)
			}

			return db, pool, false, nil
		}

		if dbname != "" {
			return nil, nil, false, &drivers.NotExistError{Driver: "postgres", DB: dbname, Connect: connect}
		}
		return nil, nil, false, fmt.Errorf("pgx.Connect: %w", err)
	}

	if schema != "" {
		_, err = db.ExecContext(ctx, `create schema if not exists `+schema)
		if err != nil {
			return nil, nil, false, fmt.Errorf("pgx.Connect: %w", err)
		}
	}

	return db, pool, exists, nil
}

// StartTest starts a new test.
//
// TODO: document.
func (driver) StartTest(t *testing.T, opt *drivers.TestOptions) context.Context {
	t.Helper()

	// The psql way to pas this is with:
	//
	//   export PGOPTIONS='-csearch_path=foo'
	//
	// But pgx doesn't support PGOPTIONS. So invent our own that we pick up on.
	//
	// TODO: don't use environment to pass information to Connect(), as this is
	// racy.
	schema := fmt.Sprintf(`"zdb_test_%s_%s"`, time.Now().Format("20060102T15:04:05.9999"),
		zcrypto.SecretString(4, ""))
	os.Setenv("PGSEARCHPATH", schema)

	if e := os.Getenv("PGDATABASE"); e == "" {
		os.Setenv("PGDATABASE", "zdb_test")
	}
	if opt == nil {
		opt = &drivers.TestOptions{}
	}

	//copt := zdb.ConnectOptions{Connect: "postgresql+", Create: true, Schema: schema}
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

	t.Cleanup(func() {
		db.Exec(context.Background(), "drop schema "+schema+" cascade")
		db.Close()
	})
	return zdb.WithDB(context.Background(), db)
}
