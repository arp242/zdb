// +build testpg

package zdb

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"testing"

	"github.com/lib/pq"
	"zgo.at/zstd/zcrypto"
)

func connectTest() string {
	os.Setenv("PGDATABASE", "zdb_test_newdb")
	return "postgresql://"
}

func StartTest(t *testing.T) context.Context {
	t.Helper()

	if _, ok := os.LookupEnv("PGDATABASE"); !ok {
		os.Setenv("PGDATABASE", "zdb_test")
	}
	db, err := Connect(ConnectOptions{
		Connect: "postgresql://",
	})
	if err != nil {
		var pqErr *pq.Error
		if errors.As(err, &pqErr) && pqErr.Code == "3D000" {
			err := createdb()
			if err != nil {
				t.Fatal(err)
			}
			return StartTest(t)
		}
		t.Fatal(err)
	}

	schema := fmt.Sprintf(`zdb_test_` + zcrypto.Secret64())
	err = db.Exec(context.Background(), `create schema `+schema)
	if err != nil {
		t.Fatal(err)
	}
	err = db.Exec(context.Background(), "set search_path to "+schema)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		db.Exec(context.Background(), "drop schema "+schema+" cascade")
		db.Close()
	})
	return WithDB(context.Background(), db)
}

func createdb() error {
	out, err := exec.Command("createdb", "zdb_test").CombinedOutput()
	if err != nil {
		return fmt.Errorf("createdb: %s → %s", err, out)
	}
	return nil
}
