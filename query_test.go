package zdb

import (
	"bytes"
	"context"
	"os"
	"reflect"
	"regexp"
	"testing"

	"github.com/jmoiron/sqlx"
	"zgo.at/zstd/ztest"
)

func TestQuery(t *testing.T) {
	tests := []struct {
		query string
		arg   interface{}

		wantQuery string
		wantArg   []interface{}
		wantErr   string
	}{
		{`select foo from bar`, nil, `select foo from bar`, nil, ""},

		{`select foo from bar {{:xxx cond}}`, map[string]interface{}{"xxx": true},
			`select foo from bar cond`, nil, ""},
		{`select foo from bar {{:xxx cond}}`, map[string]interface{}{"xxx": false},
			`select foo from bar `, nil, ""},

		{`select foo from bar {{:a cond}} {{:b cond2}} `, A{"a": true, "b": true},
			`select foo from bar cond cond2 `, nil, ""},
		{`select foo from bar {{:a cond}} {{:b cond2}} `, map[string]interface{}{"a": false, "b": false},
			`select foo from bar   `, nil, ""},

		{`select foo from bar {{:a x like :foo}} {{:b y = :bar}}`,
			map[string]interface{}{"foo": "qwe", "bar": "zxc", "a": true, "b": true},
			`select foo from bar x like $1 y = $2`,
			[]interface{}{"qwe", "zxc"},
			""},
		{`select foo from bar {{:a x like :foo}} {{:b y = :bar}}`,
			map[string]interface{}{"foo": "qwe", "bar": "zxc", "a": false, "b": true},
			`select foo from bar  y = $1`,
			[]interface{}{"zxc"},
			""},

		// Invalid syntax; just leave it alone.
		// {`select foo from bar {{cond}}`, map[string]interface{}{"xxx": false},
		// 	`select foo from bar `, nil, ""},

		// TODO: test some error conditions too
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

			query, args, err := Query(ctx, tt.query, tt.arg)
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

func TestQueryDump(t *testing.T) {
	ctx, clean := StartTest(t)
	defer clean()

	_, err := MustGet(ctx).ExecContext(ctx, `create table tbl (col1 varchar, col2 int);`)
	if err != nil {
		t.Fatal(err)
	}

	// Show just query.
	func() {
		defer func() { stderr = os.Stderr }()
		buf := new(bytes.Buffer)
		stderr = buf

		_, err = QueryExec(ctx, `insert into tbl values (:val, 1), {{:val2 (:val2, 2)}}`, map[string]interface{}{
			"val":  "hello",
			"val2": "world",
		}, DumpQuery)
		if err != nil {
			t.Fatal(err)
		}

		Dump(ctx, buf, `select * from tbl`)

		out := buf.String()
		want := "insert into tbl values ('hello', 1), ('world', 2);\ncol1   col2\nhello  1\nworld  2\n"
		if out != want {
			t.Errorf("wrong query\nout:  %q\nwant: %q", out, want)
		}
	}()

	// Show query and output.
	func() {
		defer func() { stderr = os.Stderr }()
		buf := new(bytes.Buffer)
		stderr = buf

		_, err = QueryExec(ctx, `select * from tbl where col1 = :val`, map[string]interface{}{
			"val": "hello",
		}, DumpResult)
		if err != nil {
			t.Fatal(err)
		}

		out := buf.String()
		want := "col1   col2\nhello  1\n"
		if out != want {
			t.Errorf("wrong query\nout:  %q\nwant: %q", out, want)
		}
	}()

	// Show explain
	func() {
		defer func() { stderr = os.Stderr }()
		buf := new(bytes.Buffer)
		stderr = buf

		_, err = QueryExec(ctx, `select * from tbl where col1 = :val`, map[string]interface{}{
			"val": "hello",
		}, DumpResult, DumpExplain)
		if err != nil {
			t.Fatal(err)
		}

		out := buf.String()
		want := `
			col1   col2
			hello  1

			EXPLAIN:
			id  parent  notused  detail
			2   0       0        SCAN TABLE tbl`

		if PgSQL(ctx) {
			out = regexp.MustCompile(`[0-9.]{4,}`).ReplaceAllString(out, "")
			want = `
				col1   col2
				hello  1

				QUERY PLAN
				Seq Scan on tbl  (cost= rows=6 width=36) (actual time= rows=1 loops=1)
				Filter: ((col1)::text = 'hello'::text)
				Rows Removed by Filter: 1
				Planning Time:  ms
				Execution Time:  ms`
		}

		if d := ztest.Diff(out, want, ztest.DiffNormalizeWhitespace); d != "" {
			t.Error(d)
		}
	}()
}

func BenchmarkQuery(b *testing.B) {
	query := `
		select foo from bar
		{{:join join x using (y)}}
		where site=:site and start=:start and end=:end
		{{:path and path like :path}}
		{{:psql returning id}}`
	arg := map[string]interface{}{
		"path":  "/XXXX",
		"site":  42,
		"start": "2020-01-01",
		"end":   "2020-05-05",
		"psql":  false,
		"join":  true,
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
		_, _, _ = Query(With(context.Background(), db), query, arg)
	}
}
