//go:build testmaria
// +build testmaria

package zdb

import (
	"context"
	"fmt"
	"os/exec"
	"testing"

	_ "zgo.at/zdb/drivers/mysql"
)

func connectTest() string {
	return "mysql+/zdb_test_newdb"
}

// TODO: needs to be improved.
func StartTest(t *testing.T, opt ...ConnectOptions) context.Context {
	t.Helper()

	if len(opt) > 1 {
		t.Fatal("zdb.StartTest: can only add one ConnectOptions")
	}
	var o ConnectOptions
	if len(opt) == 1 {
		o = opt[0]
	}
	o.Connect = "mysql:root@unix(/var/run/mysqld/mysqld.sock)/zdb_test"

	err := createdb()
	if err != nil {
		t.Fatal(err)
		return nil
	}

	db, err := Connect(context.Background(), o)
	if err != nil {
		t.Fatal(err)
	}

	ctx := WithDB(context.Background(), db)
	t.Cleanup(func() {
		Exec(ctx, "drop database zdb_test")
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
