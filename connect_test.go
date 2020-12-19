package zdb

import (
	"context"
	_ "embed"
	"testing"

	"zgo.at/zdb/testdata"
	"zgo.at/zstd/zembed"
)

func TestConnect(t *testing.T) {
	db, err := Connect(ConnectOptions{
		Connect: connectTest(),
		Create:  true,
		//Migrate: []string{"all"},
		Schemas: zembed.Dir(testdata.Files, ""),
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx := With(context.Background(), db)
	out := DumpString(ctx, `select * from t`)
	want := "col\nsqlite\n"
	if PgSQL(db) {
		want = "col\npgsql\n"
	}
	if out != want {
		t.Error(out)
	}
}
