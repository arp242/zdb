package zdb

import (
	"context"
	_ "embed"
	"testing"

	"zgo.at/zdb/drivers/test"
	"zgo.at/zdb/testdata"
	"zgo.at/zstd/ztest"
)

func TestConnect(t *testing.T) {
	defer test.Use()()

	tests := []struct {
		in      string
		wantErr string
	}{
		{"postgresql+hello", ""},
		{"psql+hello", ""},
		{"psql+", ""},
		{"test+", ""},
		{"psql/test+", ""},

		{"", "invalid syntax"},
		{"+", "invalid syntax"},
		{"+connect", "invalid syntax"},
		{"/test+", "invalid syntax"},

		{"unknown+connect", "no driver found"},
		{"invalid", "no driver found"},
	}

	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			_, err := Connect(context.Background(), ConnectOptions{Connect: tt.in})
			if !ztest.ErrorContains(err, tt.wantErr) {
				t.Errorf("wrong error: %s", err)
			}
		})
	}
}

func TestConnectDB(t *testing.T) {
	db, err := Connect(context.Background(), ConnectOptions{
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
