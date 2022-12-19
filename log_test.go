package zdb_test

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"zgo.at/zdb"
	"zgo.at/zstd/ztest"
)

var _ zdb.E_dbImpl = &zdb.E_logDB{}

func TestLogDB(t *testing.T) {
	tests := []struct {
		file string
		opts zdb.DumpArg
	}{
		{"default", 0},
		{"query", zdb.DumpQuery},
		{"explain", zdb.DumpExplain},
		{"query-explain", zdb.DumpQuery | zdb.DumpExplain},
		{"result", zdb.DumpResult},
		{"all", zdb.DumpAll},
	}

	zdb.RunTest(t, func(t *testing.T, ctx context.Context) {
		err := zdb.Exec(ctx, `create table x (i int)`)
		if err != nil {
			t.Fatal(err)
		}
		err = zdb.Exec(ctx, `insert into x values (1), (2), (3), (4), (5)`)
		if err != nil {
			t.Fatal(err)
		}

		for _, tt := range tests {
			t.Run(tt.file, func(t *testing.T) {
				get := func(ctx context.Context, useCtx bool, dest any, query string, params ...any) error {
					if useCtx {
						return zdb.Get(ctx, dest, query, params...)
					}
					db, _ := zdb.GetDB(ctx) // Can be transaction.
					return db.Get(ctx, dest, query, params...)
				}

				test := func(t *testing.T, useCtx bool) {
					buf := new(bytes.Buffer)
					db := zdb.NewLogDB(zdb.Unwrap(zdb.MustGetDB(ctx)), buf, tt.opts, "")
					ctx = zdb.WithDB(context.Background(), db)

					var i, j int
					err = get(ctx, useCtx, &i, `select i from x where i<3`)
					if err != nil {
						t.Fatal(err)
					}
					err = zdb.TX(ctx, func(ctx context.Context) error { return get(ctx, useCtx, &j, `select i from x where i<4`) })
					if err != nil {
						t.Fatal(err)
					}
					out := buf.String()

					want := strings.Split(string(ztest.Read(t, "testdata/logdb", tt.file)), "\n---\n")[map[zdb.Dialect]int{
						zdb.DialectSQLite:     0,
						zdb.DialectPostgreSQL: 1,
						zdb.DialectMariaDB:    2,
					}[zdb.SQLDialect(ctx)]]
					want = strings.TrimSpace(want) + "\n\n"

					out, want = prep(ctx, out, want)
					if out != want {
						t.Errorf("\n==> OUT:\n%s==> WANT:\n%s\n\nout:  %[1]q\nwant: %[2]q\n\n%s", out, want, ztest.Diff(out, want))
					}
				}

				t.Run("db", func(t *testing.T) { test(t, false) })
				t.Run("ctx", func(t *testing.T) { test(t, true) })
			})
		}
	})
}
