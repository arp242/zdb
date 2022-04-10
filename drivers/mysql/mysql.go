// Package mysql provides a zdb driver for MySQL and MariaDB.
//
// This uses https://github.com/go-sql-driver/mysql
package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"os/exec"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"zgo.at/zdb"
	"zgo.at/zdb/drivers"
)

func init() {
	drivers.RegisterDriver(driver{})
}

type driver struct{}

func (driver) Name() string    { return "mysql" }
func (driver) Dialect() string { return "mysql" }
func (driver) ErrUnique(err error) bool {
	return false // TODO
}
func (driver) Connect(ctx context.Context, connect string, create bool) (*sql.DB, bool, error) {
	db, err := sql.Open("mysql", connect)
	if err != nil {
		return nil, false, fmt.Errorf("mysql.Connect: %w", err)
	}

	err = db.PingContext(ctx)
	if err != nil {
		return nil, false, fmt.Errorf("mysql.Connect: %w", err)
	}

	return db, true, nil
}

// TODO: needs to be improved.
func (driver) StartTest(t *testing.T, opt *drivers.TestOptions) context.Context {
	t.Helper()

	copt := zdb.ConnectOptions{Connect: "mysql:root@unix(/var/run/mysqld/mysqld.sock)/zdb_test", Create: true}
	if opt != nil && opt.Connect != "" {
		copt.Connect = opt.Connect
	}
	if opt != nil && opt.Files != nil {
		copt.Files = opt.Files
	}

	err := createdb()
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
		zdb.Exec(ctx, "drop database zdb_test")
		db.Close()
	})
	return ctx
}

func createdb() error {
	out, err := exec.Command("mysql", "-u", "root", "-e", "create database zdb_test").CombinedOutput()
	if err != nil {
		return fmt.Errorf("createdb: %s → %s", err, out)
	}
	return nil
}
