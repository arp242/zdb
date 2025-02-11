// Package mariadb provides a zdb driver for MariaDB.
//
// This uses https://github.com/go-sql-driver/mysql
//
// Only "sql_mode=ansi" is supported. This means that identifiers have to be
// quoted with a " instead of a `. This is set automatically.
package mariadb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os/exec"
	"testing"

	"github.com/go-sql-driver/mysql"
	"zgo.at/zdb"
	"zgo.at/zdb/drivers"
	"zgo.at/zstd/zcrypto"
)

func init() {
	drivers.RegisterDriver(driver{})
}

type driver struct{}

func (driver) Name() string    { return "mysql" }
func (driver) Dialect() string { return "mariadb" }
func (driver) ErrUnique(err error) bool {
	var mErr *mysql.MySQLError
	if errors.As(err, &mErr) {
		return mErr.Number == 1062
	}
	return false
}
func (driver) Connect(ctx context.Context, connect string, create bool) (*sql.DB, any, error) {
	// TODO: pass these better; can't just use "set sql_mode because of
	// connection pooling, and should allow overriding parseTime like SQLite
	db, err := sql.Open("mysql", connect+"?sql_mode=concat(@@sql_mode, ',ansi')&parseTime=true")
	if err != nil {
		return nil, nil, fmt.Errorf("mariadb.Connect: %w", err)
	}

	err = db.PingContext(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("mariadb.Connect: %w", err)
	}

	return db, nil, nil
}

// TODO: needs to be improved.
func (driver) StartTest(t *testing.T, opt *drivers.TestOptions) context.Context {
	t.Helper()

	dbname := "zdb_test_" + zcrypto.SecretString(10, "")
	if opt == nil {
		opt = &drivers.TestOptions{}
	}

	copt := zdb.ConnectOptions{Connect: "mysql+root@unix(/var/run/mysqld/mysqld.sock)/" + dbname, Create: true}
	if opt != nil && opt.Connect != "" {
		copt.Connect = opt.Connect
	}
	if opt != nil && opt.Files != nil {
		copt.Files = opt.Files
	}
	if opt != nil && opt.GoMigrations != nil {
		copt.GoMigrations = opt.GoMigrations
	}

	err := createdb(dbname)
	if err != nil {
		t.Fatal(err)
		return nil
	}

	db, err := zdb.Connect(context.Background(), copt)
	if err != nil {
		t.Fatal(err)
	}

	ctx := zdb.WithDB(context.Background(), db)
	t.Cleanup(func() {
		err := zdb.Exec(ctx, "drop database "+dbname)
		if err != nil {
			t.Fatal(err)
		}
		db.Close()
	})
	return ctx
}

func createdb(dbname string) error {
	out, err := exec.Command("mysql", "-u", "root", "-e", "create database "+dbname).CombinedOutput()
	if err != nil {
		return fmt.Errorf("createdb: %s → %s", err, out)
	}
	return nil
}
