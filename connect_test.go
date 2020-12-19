package zdb

import (
	"context"
	_ "embed"
	"testing"

	"zgo.at/zdb/testdata"
)

func TestConnect(t *testing.T) {
	db, err := Connect(ConnectOptions{
		Connect: connectTest(),
		Create:  true,
		Files:   testdata.Files,
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx := WithDB(context.Background(), db)
	out := DumpString(ctx, `select * from factions`)
	want := `
		faction_id  name
		1           Peacekeepers
		2           Moya`

	if d := Diff(out, want); d != "" {
		t.Error(d)
	}
}
