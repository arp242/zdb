//go:build !testpg && !testmaria
// +build !testpg,!testmaria

package zdb

import (
	"context"
	"testing"

	_ "zgo.at/zdb/drivers/sqlite3"
)

func connectTest() string {
	return "sqlite+:memory:"
}

// StartTest starts a new test.
//
// There are three versions of this function: for SQLite, PostgreSQL, and
// MariaDB. By default the SQLite version is used, which uses a :memory:
// database.
//
// The PostgreSQL version is compiled with the 'testpg' tag; this will use the
// "zdb_test" database (will be created if it doesn't exist) and will run every
// test in a new schema. The standard PG* environment variables (PGHOST, PGPORT)
// can be used to specify the connection to a PostgreSQL database; see psql(1)
// for details.
//
// The table (or schema) will be removed when the test ends, but the PostgreSQL
// zdb_test database is kept for re-use.
//
// The MariaDB version is used with the testmaria tag.
func StartTest(t *testing.T, opt ...ConnectOptions) context.Context {
	t.Helper()

	if len(opt) > 1 {
		t.Fatal("zdb.StartTest: can only add one ConnectOptions")
	}
	var o ConnectOptions
	if len(opt) == 1 {
		o = opt[0]
	}
	o.Connect = "sqlite+:memory:?cache=shared"

	db, err := Connect(context.Background(), o)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { db.Close() })
	return WithDB(context.Background(), db)
}
