// Package pgx provides a zdb driver for PostgreSQL.
//
// This uses https://github.com/jackc/pgx
package pgx

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
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
func (d driver) Connect(ctx context.Context, connect string, create bool) (*sql.DB, any, error) {
	_, schema := getSearchPath(connect)
	cfg, err := pgxpool.ParseConfig(connect)
	if err != nil {
		return nil, nil, fmt.Errorf("pgx.Connect: %w", err)
	}

	if schema != "" {
		cfg.ConnConfig.RuntimeParams["search_path"] = `"` + schema + `"`
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		return nil, nil, fmt.Errorf("pgx.Connect: %w", err)
	}
	db := stdlib.OpenDBFromPool(pool)

	err = db.PingContext(ctx)
	if err != nil {
		var (
			dbname string
			pgErr  *pgconn.PgError
		)
		if errors.As(err, &pgErr) && pgErr.Code == "3D000" {
			dbname = cfg.ConnConfig.Database
		}

		if create && dbname != "" {
			cfg.ConnConfig.Database = "postgres"
			pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
			if err != nil {
				return nil, nil, fmt.Errorf("pgx.Connect: %w", err)
			}
			defer pool.Close()
			_, err = pool.Exec(ctx, `create database `+dbname)
			if err != nil {
				return nil, nil, fmt.Errorf("pgx.Connect: %w", err)
			}
			pool.Close()

			// Restart the function with "create" to false to avoid loops.
			return d.Connect(ctx, connect, false)
		}

		if dbname != "" {
			return nil, nil, &drivers.NotExistError{Driver: "postgres", DB: dbname, Connect: connect}
		}
		return nil, nil, fmt.Errorf("pgx.Connect: %w", err)
	}

	if schema != "" {
		_, err = db.ExecContext(ctx, `create schema if not exists "`+schema+`"`)
		if err != nil {
			return nil, nil, fmt.Errorf("pgx.Connect: %w", err)
		}
	}

	return db, pool, nil
}

// StartTest starts a new test.
//
// TODO: document.
func (driver) StartTest(t *testing.T, opt *drivers.TestOptions) context.Context {
	t.Helper()

	if e := os.Getenv("PGDATABASE"); e == "" {
		os.Setenv("PGDATABASE", "zdb_test")
	}
	if opt == nil {
		opt = &drivers.TestOptions{}
	}

	copt := zdb.ConnectOptions{Connect: "postgresql+", Create: true}
	if opt != nil && opt.Connect != "" {
		copt.Connect = opt.Connect
	}
	if opt != nil && opt.Files != nil {
		copt.Files = opt.Files
	}
	if opt != nil && opt.GoMigrations != nil {
		copt.GoMigrations = opt.GoMigrations
	}

	schema := fmt.Sprintf(`zdb_test_%s_%s`, time.Now().Format("20060102T15:04:05.9999"),
		zcrypto.SecretString(4, ""))
	copt.Connect = withSearchPath(copt.Connect, schema)
	db, err := zdb.Connect(context.Background(), copt)
	if err != nil {
		t.Fatalf("pgx.StartTest: connecting to %q: %s", copt.Connect, err)
	}

	t.Cleanup(func() {
		db.Exec(context.Background(), `drop schema "`+schema+`" cascade`)
		db.Close()

		// TODO: Just closing the sql.DB isn't enough, as that won't close all
		// the connections made from pgxpool.Pool. Need to explicitly close
		// both.
		//
		// This should really be done on db.Close() for everything, but not so
		// easy to wrap that as sql.DB is a struct rather than interface. Would
		// probably be best to send a patch to pgx to add option or something.
		// Need to look into it properly.
		//
		// However, in "real applications" it's usually not a big deal as
		// connections as only closed on application exit (that is: basically
		// never). So for now, it's an okay hack to just do this for tests.
		zdb.DriverConnection(db).(*pgxpool.Pool).Close()
	})
	return zdb.WithDB(context.Background(), db)
}

func withSearchPath(s, p string) string {
	u, err := url.Parse(s)
	if err != nil {
		panic(err)
	}
	q := u.Query()
	q.Set("search_path", p)
	u.RawQuery = q.Encode()
	return u.String()
}

func getSearchPath(s string) (string, string) {
	u, err := url.Parse(s)
	if err != nil {
		panic(err)
	}

	q := u.Query()
	p := q.Get("search_path")
	q.Del("search_path")
	u.RawQuery = q.Encode()
	return u.String(), p
}
