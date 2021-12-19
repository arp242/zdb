//go:build testpg
// +build testpg

package zdb

import (
	"context"
	"fmt"
	"os"
	"testing"

	_ "zgo.at/zdb/drivers/pq"
	"zgo.at/zstd/zcrypto"
)

func connectTest() string {
	os.Setenv("PGDATABASE", "zdb_test_newdb")
	return "postgresql+"
}

func StartTest(t *testing.T, opt ...ConnectOptions) context.Context {
	t.Helper()

	if len(opt) > 1 {
		t.Fatal("zdb.StartTest: can only add one ConnectOptions")
	}
	var o ConnectOptions
	if len(opt) == 1 {
		o = opt[0]
	}
	o.Connect = "postgresql:"

	if _, ok := os.LookupEnv("PGDATABASE"); !ok {
		os.Setenv("PGDATABASE", "zdb_test")
	}
	db, err := Connect(o)
	if err != nil {
		t.Fatal(err)
	}

	// The first test will create the zdb_test database, and every test after
	// that runs in its own schema.
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
