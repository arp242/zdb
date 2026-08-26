//go:build !testsqlite && !testpq && !testmaria

package zdb_test

import (
	"database/sql"
	"testing"

	"zgo.at/zdb"
	_ "zgo.at/zdb-drivers/go-sqlite3"
)

func TestFromSQLDB(t *testing.T) {
	sqlDB, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	db, err := zdb.FromSQLDB(sqlDB)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Info(zdb.WithDB(t.Context(), db))
	if err != nil {
		t.Fatal(err)
	}
	err = db.Close()
	if err != nil {
		t.Fatal(err)
	}
}
