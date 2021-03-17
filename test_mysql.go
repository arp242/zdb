// +build testmariadb

package zdb

import (
	"context"
	"fmt"
	"os/exec"
	"testing"
)

func connectTest() string {
	return "mysql:///zdb_test_newdb"
}

// TODO: needs to be improved.
func StartTest(t *testing.T) context.Context {
	t.Helper()

	err := createdb()
	if err != nil {
		t.Fatal(err)
		return nil
	}

	db, err := Connect(ConnectOptions{
		Connect: "mysql://root@unix(/var/run/mysqld/mysqld.sock)/zdb_test",
	})
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
