package zdb_test

import (
	"context"
	_ "embed"
	"testing"

	"zgo.at/zdb"
	"zgo.at/zdb/drivers/test"
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
			_, err := zdb.Connect(context.Background(), zdb.ConnectOptions{Connect: tt.in})
			if !ztest.ErrorContains(err, tt.wantErr) {
				t.Errorf("wrong error: %s", err)
			}
		})
	}
}
