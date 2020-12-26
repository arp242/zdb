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

func StartTest(t *testing.T) (context.Context, func()) {
	t.Helper()

	os.Setenv("PGDATABASE", "zdb_test")
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
	_, err = db.ExecContext(context.Background(), `create schema `+schema)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.ExecContext(context.Background(), "set search_path to "+schema)
	if err != nil {
		t.Fatal(err)
	}

	return With(context.Background(), db), func() {
		db.ExecContext(context.Background(), "drop schema "+schema+" cascade")
		db.Close()
	}
}

func createdb() error {
	out, err := exec.Command("createdb", "zdb_test").CombinedOutput()
	if err != nil {
		return fmt.Errorf("createdb: %s → %s", err, out)
	}
	return nil
}
