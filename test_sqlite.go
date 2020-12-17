// +build !testpg

package zdb

import (
	"context"
	"testing"
)

// StartTest starts a new test.
//
// There are two versions of this function: one for SQLite and one for
// PostgreSQL; by default the SQLite version is used, which uses a :memory:
// database.
//
// The PostgreSQL version is compiled with the 'testpg' tag; this will use the
// "zdb_test" database (will be created if it doesn't exist) and will run every
// test in a new schema.
//
// The standard PG* environment variables (PGHOST, PGPORT) can be used to
// specify the connection to a PostgreSQL database; see psql(1) for details.
//
// The table (or schema) will be removed when the test ends, but the PostgreSQL
// zdb_test database is kept.
func StartTest(t *testing.T) (context.Context, func()) {
	t.Helper()

	db, err := Connect(ConnectOptions{
		Connect: "sqlite3://:memory:?cache=shared",
	})
	if err != nil {
		t.Fatal(err)
	}

	return With(context.Background(), db), func() {
		db.Close()
	}
}
