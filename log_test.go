package zdb

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"zgo.at/zstd/ztest"
)

func TestLogDB(t *testing.T) {
	tests := []struct {
		file string
		opts DumpArg
	}{
		{"default", 0},
		{"query", DumpQuery},
		{"explain", DumpExplain},
		{"query-explain", DumpQuery | DumpExplain},
		{"result", DumpResult},
		{"all", DumpAll},
	}

	ctx, clean := StartTest(t)
	defer clean()

	err := Exec(ctx, `create table x (i int); insert into x values (1), (2), (3), (4), (5)`)
	if err != nil {
		t.Fatal(err)
	}

	for _, tt := range tests {
		t.Run(tt.file, func(t *testing.T) {
			buf := new(bytes.Buffer)
			ctx = WithDB(context.Background(), NewLogDB(Unwrap(MustGetDB(ctx)), buf, tt.opts, ""))

			var i, j int
			err = Get(ctx, &i, `select i from x where i<3`)
			if err != nil {
				t.Fatal(err)
			}
			err = TX(ctx, func(ctx context.Context) error { return Get(ctx, &j, `select i from x where i<4`) })
			if err != nil {
				t.Fatal(err)
			}
			out := buf.String()

			want := string(ztest.Read(t, "testdata/logdb", tt.file))
			if i := strings.Index(want, "\n---\n"); i >= 0 {
				if PgSQL(ctx) {
					want = want[i+5:]
				} else {
					want = want[:i]
				}
			}
			want = strings.TrimSpace(want) + "\n\n"

			out, want = prep(ctx, out, want)
			if out != want {
				t.Errorf("\n==> OUT:\n%s==> WANT:\n%s\n\nout:  %[1]q\nwant: %[2]q\n\n%s", out, want, ztest.Diff(out, want))
			}
		})
	}
}
