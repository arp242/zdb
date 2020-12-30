package zdb

import (
	"bytes"
	"context"
	"database/sql"
	"os"
	"reflect"
	"regexp"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"zgo.at/zstd/ztest"
)

func TestPrepare(t *testing.T) {
	type I []interface{}
	date := time.Date(2020, 06, 18, 01, 02, 03, 04, time.UTC)

	tests := []struct {
		query string
		args  []interface{}

		wantQuery string
		wantArg   []interface{}
		wantErr   string
	}{
		// No arguments.
		{`select foo from bar`, nil,
			`select foo from bar`, nil, ""},

		// Single named param from map
		{`select :x`, I{A{"x": "Y"}},
			`select $1`, I{"Y"}, ""},

		// Single named param from struct
		{`select :x`, I{struct{ X string }{"Y"}},
			`select $1`, I{"Y"}, ""},

		// Both a map and struct → merge
		{`select :x, :y`, I{A{"x": "Y"}, struct{ Y int }{42}},
			`select $1, $2`, I{"Y", 42}, ""},

		// One positional
		{`select $1`, I{"A"},
			`select $1`, I{"A"}, ""},
		{`select ?`, I{"A"},
			`select $1`, I{"A"}, ""},

		// Two positional
		{`select $1, $2`, I{"A", "B"},
			`select $1, $2`, I{"A", "B"}, ""},
		{`select ?, ?`, I{"A", "B"},
			`select $1, $2`, I{"A", "B"}, ""},

		// time.Time shouldn't be seen as a named argument.
		{`select ?`, I{date},
			`select $1`, I{date}, ""},
		{`select ?, ?`, I{date, date},
			`select $1, $2`, I{date, date}, ""},

		// Neither should structs implementing sql.Scanner
		{`select ?`, I{sql.NullBool{Valid: true}},
			`select $1`, I{sql.NullBool{Valid: true}}, ""},
		{`select ?, ?`, I{sql.NullString{}, sql.NullString{}},
			`select $1, $2`, I{sql.NullString{}, sql.NullString{}}, ""},

		// True conditional from bool
		{`select {{:xxx cond}} where 1=1`, I{A{"xxx": true}},
			`select cond where 1=1`, I{}, ""},
		{`select {{:xxx cond}} where 1=1`, I{struct{ XXX bool }{true}},
			`select cond where 1=1`, I{}, ""},
		{`select {{:xxx cond}} where 1=1`, I{A{"a": true}, struct{ XXX bool }{true}},
			`select cond where 1=1`, I{}, ""},

		// False conditional from bool
		{`select {{:xxx cond}} where 1=1`, I{A{"xxx": false}},
			`select  where 1=1`, I{}, ""},
		{`select {{:xxx cond}} where 1=1`, I{struct{ XXX bool }{false}},
			`select  where 1=1`, I{}, ""},
		{`select {{:xxx cond}} where 1=1`, I{A{"a": false}, struct{ XXX bool }{false}},
			`select  where 1=1`, I{}, ""},

		// Multiple conditionals
		{`select {{:a cond}} {{:b cond2}} `, I{A{"a": true, "b": true}},
			`select cond cond2 `, I{}, ""},
		{`select {{:a cond}} {{:b cond2}} `, I{A{"a": false, "b": false}},
			`select   `, I{}, ""},

		// Parameters inside conditionals
		{`select {{:a x like :foo}} {{:b y = :bar}}`, I{A{"foo": "qwe", "bar": "zxc", "a": true, "b": true}},
			`select x like $1 y = $2`, I{"qwe", "zxc"}, ""},
		{`select {{:a x like :foo}} {{:b y = :bar}}`, I{A{"foo": "qwe", "bar": "zxc", "a": false, "b": true}},
			`select  y = $1`, I{"zxc"}, ""},

		// Multiple conflicting params
		{`select :x`, I{A{"x": 1}, A{"x": 2}},
			``, nil, "more than once"},
		{`select {{:x cond}}`, I{A{"x": 1}, A{"x": 2}},
			``, nil, "more than once"},

		// Mixing positional and named
		{`select :x`, I{A{"x": 1}, 42},
			``, nil, "mix named and positional"},

		// Conditional not found
		{`select {{:x cond}}`, I{A{"z": 1}},
			``, nil, "could not find"},

		// Condtional with positional
		{`select {{:x cond}}`, I{"z", 1},
			`select {{:x cond}}`, I{"z", 1}, ""},

		// Invalid syntax for conditional; just leave it alone
		{`select {{cond}}`, I{A{"xxx": false}},
			`select {{cond}}`, I{}, ""},

		// Expand slice
		{`insert values (?)`, I{[]string{"a", "b"}},
			`insert values ($1, $2)`, I{"a", "b"}, ""},
		// TODO: this only works for "?"; sqlx.In() and named parameters.
		// {`insert values ($1)`, I{[]string{"a", "b"}},
		// 	`insert values ($1, $2)`, I{"a", "b"}, ""},
		{`insert values (:x)`, I{A{"x": []string{"a", "b"}}},
			`insert values ($1, $2)`, I{"a", "b"}, ""},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			ctx, clean := StartTest(t)
			defer clean()

			query, args, err := Prepare(ctx, tt.query, tt.args...)
			query = sqlx.Rebind(sqlx.DOLLAR, query) // Always use $-binds for tests
			if !ztest.ErrorContains(err, tt.wantErr) {
				t.Fatal(err)
			}
			if query != tt.wantQuery {
				t.Errorf("wrong query\nout:  %q\nwant: %q", query, tt.wantQuery)
			}
			if !reflect.DeepEqual(args, tt.wantArg) {
				t.Errorf("wrong args\nout:  %#v\nwant: %#v", args, tt.wantArg)
			}
		})
	}
}

func TestPrepareDump(t *testing.T) {
	ctx, clean := StartTest(t)
	defer clean()

	err := Exec(ctx, `create table tbl (col1 varchar, col2 int);`)
	if err != nil {
		t.Fatal(err)
	}

	// Show just query.
	func() {
		defer func() { stderr = os.Stderr }()
		buf := new(bytes.Buffer)
		stderr = buf

		err = Exec(ctx, `insert into tbl values (:val, 1), {{:val2 (:val2, 2)}}`, map[string]interface{}{
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

		err = Exec(ctx, `select * from tbl where col1 = :val`, map[string]interface{}{
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

		err = Exec(ctx, `select * from tbl where col1 = :val`, map[string]interface{}{
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

func TestInsertID(t *testing.T) {
	ctx, clean := StartTest(t)
	defer clean()

	tbl := `create table test (col_id integer primary key autoincrement, v varchar)`
	if PgSQL(ctx) {
		tbl = `create table test (col_id serial primary key, v varchar)`
	}
	err := Exec(ctx, tbl, nil)
	if err != nil {
		t.Fatal(err)
	}

	{ // One row
		id, err := InsertID(ctx, `col_id`, `insert into test (v) values (:val)`, A{"val": "aa"})
		if err != nil {
			t.Error(err)
		}
		if id != 1 {
			t.Errorf("id is %d, not 1", id)
		}
	}

	{ // Multiple rows
		id, err := InsertID(ctx, `col_id`, `insert into test (v) values (:val), ('bb')`, A{"val": "aa"})
		if err != nil {
			t.Error(err)
		}
		if id != 3 {
			t.Errorf("id is %d, not 3", id)
		}
	}

	{
		id, err := InsertID(ctx, `col_id`, `insert into test (v) values (?), (?)`,
			"X", "Y")
		if err != nil {
			t.Error(err)
		}
		if id != 5 {
			t.Errorf("id is %d, not 3", id)
		}
	}

	{ // Invalid SQL

		id, err := InsertID(ctx, `col_id`, `insert into test (no_such_col) values ($1)`, nil)
		if err == nil {
			t.Error("err is nil")
		}
		if id != 0 {
			t.Errorf("id is not 0: %d", id)
		}
	}

	out := "\n" + DumpString(ctx, `select * from test`)
	want := `
col_id  v
1       aa
2       aa
3       bb
4       X
5       Y
`
	if out != want {
		t.Errorf("\nwant: %v\ngot:  %v", want, out)
	}
}

func BenchmarkPrepare(b *testing.B) {
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
		_, _, _ = Prepare(WithDB(context.Background(), db), query, arg)
	}
}
