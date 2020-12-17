package zdb

import (
	"context"
	"reflect"
	"testing"

	"github.com/jmoiron/sqlx"
	"zgo.at/zstd/ztest"
)

func TestQuery(t *testing.T) {
	tests := []struct {
		query string
		arg   interface{}
		conds []bool

		wantQuery string
		wantArg   []interface{}
		wantErr   string
	}{
		{`select foo from bar`, nil, []bool{}, `select foo from bar`, nil, ""},

		{`select foo from bar {{cond}}`, nil, []bool{true},
			`select foo from bar cond`, nil, ""},
		{`select foo from bar {{cond}}`, nil, []bool{false},
			`select foo from bar `, nil, ""},

		{`select foo from bar {{cond}} {{cond2}} `, nil, []bool{true, true},
			`select foo from bar cond cond2 `, nil, ""},
		{`select foo from bar {{cond}} {{cond2}} `, nil, []bool{false, false},
			`select foo from bar   `, nil, ""},

		{`select foo from bar {{x like :foo}} {{y = :bar}}`,
			map[string]interface{}{"foo": "qwe", "bar": "zxc"},
			[]bool{true, true},
			`select foo from bar x like $1 y = $2`,
			[]interface{}{"qwe", "zxc"},
			""},
		{`select foo from bar {{x like :foo}} {{y = :bar}}`,
			map[string]interface{}{"foo": "qwe", "bar": "zxc"},
			[]bool{false, true},
			`select foo from bar  y = $1`,
			[]interface{}{"zxc"},
			""},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			ctx, clean := StartTest(t)
			defer clean()

			if tt.arg == nil {
				tt.arg = make(map[string]interface{})
			}
			if tt.wantArg == nil {
				tt.wantArg = []interface{}{}
			}

			query, args, err := Query(ctx, tt.query, tt.arg, tt.conds...)
			query = sqlx.Rebind(sqlx.DOLLAR, query) // Always use $-binds for tests
			if !ztest.ErrorContains(err, tt.wantErr) {
				t.Fatal(err)
			}
			if query != tt.wantQuery {
				t.Errorf("wrong query\nout:  %q\nwant: %q", query, tt.wantQuery)
			}
			if !reflect.DeepEqual(args, tt.wantArg) {
				t.Errorf("wrong args\nout:  %v\nwant: %v", args, tt.wantArg)
			}
		})
	}
}

func BenchmarkQuery(b *testing.B) {
	query := `
		select foo from bar
		{{join x using (y) }}
		where site=:site and start=:start and end=:end
		{{and path like :path}}
		{{returning id}}`
	arg := map[string]interface{}{
		"path":  "/XXXX",
		"site":  42,
		"start": "2020-01-01",
		"end":   "2020-05-05",
	}

	db, err := Connect(ConnectOptions{
		Connect: "sqlite3://:memory:",
	})
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		_, _, _ = Query(With(context.Background(), db), query, arg, true, true, false)
	}
}
