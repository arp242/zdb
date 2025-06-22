package zdb_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"testing"
	"time"

	"zgo.at/zdb"
	"zgo.at/zdb/drivers"
	"zgo.at/zdb/internal/sqlx"
	"zgo.at/zdb/testdata"
	"zgo.at/zstd/ztest"
)

func TestInfo(t *testing.T) {
	zdb.RunTest(t, func(t *testing.T, ctx context.Context) {
		db := zdb.MustGetDB(ctx)

		v, err := db.Info(ctx)
		if err != nil {
			t.Fatal(err)
		}
		t.Log(v)

		for _, tt := range [][]zdb.ServerVersion{
			{"3", "4"},
			{"3.35.0", "4"},
			{"3.35.0", "4.1.0"},
			{"3.35.0", "3.35.1"},
		} {
			have, want := tt[0], tt[1]
			if have.AtLeast(want) {
				t.Errorf("is true: %s.AtLeast(%s)", have, want)
			}
		}

		for _, tt := range [][]zdb.ServerVersion{
			{"4.0.0", "4"},
			{"4.1.0", "4"},
			{"4.1", "4"},
			{"4.0.1", "4"},
		} {
			have, want := tt[0], tt[1]
			if !have.AtLeast(want) {
				t.Errorf("is false: %s.AtLeast(%s)", have, want)
			}
		}
	})
}

func TestPrepare(t *testing.T) {
	var (
		s, s2      = "str", ""
		ptr1, ptr2 = &s, &s2
		date       = time.Date(2020, 06, 18, 01, 02, 03, 04, time.UTC)
	)
	type (
		strlist []string
		str     string
	)

	tests := []struct {
		query string
		args  []any

		wantQuery string
		wantArg   []any
		wantErr   string
	}{
		// No arguments.
		{`select foo from bar`, nil,
			`select foo from bar`, nil, ""},

		// Single named param from map
		{`select :x`, zdb.L{zdb.P{"x": "Y"}},
			`select $1`, zdb.L{"Y"}, ""},

		// Single named param from struct
		{`select :x`, zdb.L{struct{ X string }{"Y"}},
			`select $1`, zdb.L{"Y"}, ""},

		// Both a map and struct → merge
		{`select :x, :y`, zdb.L{zdb.P{"x": "Y"}, struct{ Y int }{42}},
			`select $1, $2`, zdb.L{"Y", 42}, ""},

		// One positional
		{`select $1`, zdb.L{"A"},
			`select $1`, zdb.L{"A"}, ""},
		{`select ?`, zdb.L{"A"},
			`select $1`, zdb.L{"A"}, ""},

		// Two positional
		{`select $1, $2`, zdb.L{"A", "B"},
			`select $1, $2`, zdb.L{"A", "B"}, ""},
		{`select ?, ?`, zdb.L{"A", "B"},
			`select $1, $2`, zdb.L{"A", "B"}, ""},

		// time.Time shouldn't be seen as a named argument.
		{`select ?`, zdb.L{date},
			`select $1`, zdb.L{date}, ""},
		{`select ?, ?`, zdb.L{date, date},
			`select $1, $2`, zdb.L{date, date}, ""},

		// Neither should structs implementing sql.Scanner
		{`select ?`, zdb.L{sql.NullBool{Valid: true}},
			`select $1`, zdb.L{sql.NullBool{Valid: true}}, ""},
		{`select ?, ?`, zdb.L{sql.NullString{}, sql.NullString{}},
			`select $1, $2`, zdb.L{sql.NullString{}, sql.NullString{}}, ""},

		// True conditional from bool
		{`select {{:xxx cond}} where 1=1`, zdb.L{zdb.P{"xxx": true}},
			`select cond where 1=1`, zdb.L{}, ""},
		{`select {{:xxx cond}} where 1=1`, zdb.L{struct{ XXX bool }{true}},
			`select cond where 1=1`, zdb.L{}, ""},
		{`select {{:xxx cond}} where 1=1`, zdb.L{zdb.P{"a": true}, struct{ XXX bool }{true}},
			`select cond where 1=1`, zdb.L{}, ""},

		// Negation with !
		{`select {{:xxx! cond}} where 1=1`, zdb.L{zdb.P{"xxx": true}},
			`select  where 1=1`, zdb.L{}, ""},
		// Negation with !
		{`select {{:xxx! cond}} where 1=1`, zdb.L{zdb.P{"xxx": false}},
			`select cond where 1=1`, zdb.L{}, ""},

		// False conditional from bool
		{`select {{:xxx cond}} where 1=1`, zdb.L{zdb.P{"xxx": false}},
			`select  where 1=1`, zdb.L{}, ""},
		{`select {{:xxx cond}} where 1=1`, zdb.L{struct{ XXX bool }{false}},
			`select  where 1=1`, zdb.L{}, ""},
		{`select {{:xxx cond}} where 1=1`, zdb.L{zdb.P{"a": false}, struct{ XXX bool }{false}},
			`select  where 1=1`, zdb.L{}, ""},

		// Multiple conditionals
		{`select {{:a cond}} {{:b cond2}} `, zdb.L{zdb.P{"a": true, "b": true}},
			`select cond cond2 `, zdb.L{}, ""},
		{`select {{:a cond}} {{:b cond2}} `, zdb.L{zdb.P{"a": false, "b": false}},
			`select   `, zdb.L{}, ""},

		// Parameters inside conditionals
		{`select {{:a x like :foo}} {{:b y = :bar}}`, zdb.L{zdb.P{"foo": "qwe", "bar": "zxc", "a": true, "b": true}},
			`select x like $1 y = $2`, zdb.L{"qwe", "zxc"}, ""},
		{`select {{:a x like :foo}} {{:b y = :bar}}`, zdb.L{zdb.P{"foo": "qwe", "bar": "zxc", "a": false, "b": true}},
			`select  y = $1`, zdb.L{"zxc"}, ""},

		// Multiple conflicting params
		{`select :x`, zdb.L{zdb.P{"x": 1}, zdb.P{"x": 2}},
			``, nil, "more than once"},
		{`select {{:x cond}}`, zdb.L{zdb.P{"x": 1}, zdb.P{"x": 2}},
			``, nil, "more than once"},

		// Mixing positional and named
		{`select :x`, zdb.L{zdb.P{"x": 1}, 42},
			``, nil, "mix named and positional"},

		// Conditional not found
		{`select {{:x cond}}`, zdb.L{zdb.P{"z": 1}},
			``, nil, "could not find"},

		// Condtional with positional
		{`select {{:x cond}}`, zdb.L{"z", 1},
			`select {{:x cond}}`, zdb.L{"z", 1}, ""},

		// Invalid syntax for conditional; just leave it alone
		{`select {{cond}}`, zdb.L{zdb.P{"xxx": false}},
			`select {{cond}}`, zdb.L{}, ""},

		// Conditional types
		// string
		{`select {{:x true :x}}{{:x! false :x}}`, zdb.L{zdb.P{"x": "str"}},
			`select true $1`, zdb.L{"str"}, ""},
		{`select {{:x true :x}}{{:x! false :x}}`, zdb.L{zdb.P{"x": str("str")}},
			`select true $1`, zdb.L{str("str")}, ""},
		{`select {{:x true :x}}{{:x! false :x}}`, zdb.L{zdb.P{"x": ""}},
			`select false $1`, zdb.L{""}, ""},
		{`select {{:x true :x}}{{:x! false :x}}`, zdb.L{zdb.P{"x": str("")}},
			`select false $1`, zdb.L{str("")}, ""},

		// Slice
		{`select {{:x true :x}}{{:x! false :x}}`, zdb.L{zdb.P{"x": []string{"str", "str2"}}},
			`select true $1, $2`, zdb.L{"str", "str2"}, ""},
		{`select {{:x true :x}}{{:x! false :x}}`, zdb.L{zdb.P{"x": strlist{"str", "str2"}}},
			`select true $1, $2`, zdb.L{"str", "str2"}, ""},
		{`select {{:x true}}{{:x! false}}`, zdb.L{zdb.P{"x": []string{}}},
			`select false`, zdb.L{}, ""},
		{`select {{:x true}}{{:x! false}}`, zdb.L{zdb.P{"x": strlist{}}},
			`select false`, zdb.L{}, ""},

		// Expand slice
		{`insert values (?)`, zdb.L{[]string{"a", "b"}},
			`insert values ($1, $2)`, zdb.L{"a", "b"}, ""},
		// TODO: this only works for "?"; sqlx.In() and named parameters.
		// {`insert values ($1)`, zdb.L{[]string{"a", "b"}},
		// 	`insert values ($1, $2)`, zdb.L{"a", "b"}, ""},
		{`insert values (:x)`, zdb.L{zdb.P{"x": []string{"a", "b"}}},
			`insert values ($1, $2)`, zdb.L{"a", "b"}, ""},

		// Pointer
		{`select {{:x true :x}}{{:x! false :x}}`, zdb.L{zdb.P{"x": ptr1}},
			`select true $1`, zdb.L{ptr1}, ""},
		{`select {{:x true :x}}{{:x! false :x}}`, zdb.L{zdb.P{"x": (*string)(nil)}},
			`select false $1`, zdb.L{(*string)(nil)}, ""},
		{`select {{:x true :x}}{{:x! false :x}}`, zdb.L{zdb.P{"x": ptr2}},
			`select false $1`, zdb.L{ptr2}, ""},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			zdb.RunTest(t, func(t *testing.T, ctx context.Context) {
				query, args, err := zdb.E_prepareImpl(ctx, zdb.MustGetDB(ctx), tt.query, tt.args...)
				query = sqlx.Rebind(sqlx.PlaceholderDollar, query) // Always use $-binds for tests
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
		})
	}
}

func TestPrepareDump(t *testing.T) {
	zdb.RunTest(t, func(t *testing.T, ctx context.Context) {
		err := zdb.Exec(ctx, `create table tbl (col1 text, col2 int);`)
		if err != nil {
			t.Fatal(err)
		}

		// Show just query.
		func() {
			buf := zdb.BufStderr(t)

			err = zdb.Exec(ctx, `insert into tbl values (:val, 1), {{:val2 (:val2, 2)}}`, map[string]any{
				"val":  "hello",
				"val2": "world",
			}, zdb.DumpQuery)
			if err != nil {
				t.Fatal(err)
			}

			zdb.Dump(ctx, buf, `select * from tbl`)

			out := buf.String()
			want := "insert into tbl values ('hello', 1), ('world', 2);\n\ncol1   col2\nhello  1\nworld  2\n\n"
			if out != want {
				t.Errorf("wrong query\nout:  %q\nwant: %q", out, want)
			}
		}()

		// Show query and output.
		func() {
			buf := zdb.BufStderr(t)

			err = zdb.Exec(ctx, `select * from tbl where col1 = :val`, map[string]any{
				"val": "hello",
			}, zdb.DumpResult)
			if err != nil {
				t.Fatal(err)
			}

			out := buf.String()
			want := "col1   col2\nhello  1\n\n"
			if out != want {
				t.Errorf("wrong query\nout:  %q\nwant: %q", out, want)
			}
		}()

		// Show explain
		func() {
			buf := zdb.BufStderr(t)

			err = zdb.Exec(ctx, `select * from tbl where col1 = :val`, map[string]any{
				"val": "hello",
			}, zdb.DumpResult, zdb.DumpExplain)
			if err != nil {
				t.Fatal(err)
			}

			out := buf.String()
			var want string
			switch zdb.SQLDialect(ctx) {
			case zdb.DialectSQLite:
				want = `
					[1mEXPLAIN[0m:
					  SCAN tbl
					  Time: 0.016 ms
					[1mRESULT[0m:
					  col1   col2
					  hello  1`
			case zdb.DialectPostgreSQL:
				want = `
					[1mEXPLAIN[0m:
					  Seq Scan on tbl  (cost=0.00..25.88 rows=6 width=36) (actual time=0.005..0.015 rows=1 loops=1)
						Filter: (col1 = 'hello'::text)
						Rows Removed by Filter: 1
					  Planning Time: 0.123 ms
					  Execution Time: 0.646 ms
					[1mRESULT[0m:
					  col1   col2
					  hello  1`
			case zdb.DialectMariaDB:
				want = `
					[1mEXPLAIN[0m:
					id  select_type  table  type  possible_keys  key   key_len  ref   rows  Extra
					1   SIMPLE       tbl    ALL   NULL           NULL  NULL     NULL  2     Using where
					[1mRESULT[0m:
					col1   col2
					hello  1
				`
			}

			out, want = prep(ctx, out, want)

			if d := ztest.Diff(out, want, ztest.DiffNormalizeWhitespace); d != "" {
				t.Error(d)
			}
		}()
	})
}

func prep(ctx context.Context, got, want string) (string, string) {
	re := []string{`([0-9]+.[0-9]+) ms`, `log_test\.go:(\d\d)`}
	if zdb.SQLDialect(ctx) == zdb.DialectPostgreSQL {
		re = append(re, `(?:cost|time)=([0-9.]+)\.\.([0-9.]+) `)
	}

	got = ztest.Replace(got, re...)
	want = ztest.Replace(want, re...)
	return got, want

}

func TestInsertID(t *testing.T) {
	zdb.RunTest(t, func(t *testing.T, ctx context.Context) {
		tbl := `create table test (col_id integer primary key autoincrement, v varchar)`
		if zdb.SQLDialect(ctx) == zdb.DialectPostgreSQL {
			tbl = `create table test (col_id serial primary key, v varchar)`
		}
		if zdb.SQLDialect(ctx) == zdb.DialectMariaDB {
			tbl = `create table test (col_id integer auto_increment, v varchar(255), primary key(col_id))`
		}
		err := zdb.Exec(ctx, tbl)
		if err != nil {
			t.Fatal(err)
		}

		{ // One row
			id, err := zdb.InsertID[int64](ctx, `col_id`, `insert into test (v) values (:val)`, zdb.P{"val": "aa"})
			if err != nil {
				t.Error(err)
			}
			if id != 1 {
				t.Errorf("id is %d, not 1", id)
			}
		}

		{ // Multiple rows
			id, err := zdb.InsertID[int32](ctx, `col_id`, `insert into test (v) values (:val), ('bb')`, zdb.P{"val": "aa"})
			if err != nil {
				t.Error(err)
			}
			if id != 3 {
				t.Errorf("id is %d, not 3\n%s", id, zdb.DumpString(ctx, `select * from test`))
			}
		}

		{
			id, err := zdb.InsertID[int](ctx, `col_id`, `insert into test (v) values (?), (?)`,
				"X", "Y")
			if err != nil {
				t.Error(err)
			}
			if id != 5 {
				t.Errorf("id is %d, not 5\n%s", id, zdb.DumpString(ctx, `select * from test`))
			}
		}

		{ // Invalid SQL
			id, err := zdb.InsertID[int](ctx, `col_id`, `insert into test (no_such_col) values ($1)`)
			if err == nil {
				t.Error("err is nil")
			}
			if id != 0 {
				t.Errorf("id is not 0: %d", id)
			}
		}

		out := "\n" + zdb.DumpString(ctx, `select * from test`)
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
	})
}

func TestQuery(t *testing.T) {
	zdb.RunTest(t, func(t *testing.T, ctx context.Context) {
		err := zdb.Exec(ctx, `
			create table tbl (
				s  text,
				i  int,
				t  timestamp,
				n  int null
			);
		`)
		if err != nil {
			t.Fatal(err)
		}
		err = zdb.Exec(ctx, `insert into tbl values
			('Hello', 42,  '2020-06-18', null),
			('Hello', 42,  '2020-06-18', null),
			('Hello', 42,  '2020-06-18', null),
			('Hello', 42,  '2020-06-18', null);
		`)
		if err != nil {
			t.Fatal(err)
		}

		rows, err := zdb.Query(ctx, `select * from tbl`)
		if err != nil {
			t.Fatal(err)
		}

		i := 0
		for rows.Next() {
			switch i {
			case 0:
				var (
					s  string
					i  int
					ti time.Time
					n  *int
				)
				err := rows.Scan(&s, &i, &ti, &n)
				if err != nil {
					t.Fatal(err)
				}
				//fmt.Println("scan", s, i, ti, n)
			case 1:
				var r map[string]any
				err := rows.Scan(&r)
				if err != nil {
					t.Fatal(err)
				}
				//fmt.Println("map", r)
			case 2:
				var r []any
				err := rows.Scan(&r)
				if err != nil {
					t.Fatal(err)
				}
				//fmt.Println("slice", r)
			case 3:
				var r struct {
					S string
					I int
					T time.Time
					N *int
				}
				err := rows.Scan(&r)
				if err != nil {
					t.Fatal(err)
				}
				//fmt.Println("struct", r)
			}
			i++
		}
	})
}

func TestLoad(t *testing.T) {
	zdb.RunTest(t, func(t *testing.T, ctx context.Context) {
		db := zdb.MustGetDB(ctx)

		{
			got, _, err := zdb.Load(db, "select-1")
			if err != nil {
				t.Fatal(err)
			}
			want := "/* select-1 */\nselect * from t where col like :find\n"
			if got != want {
				t.Errorf("\ngot:  %q\nwant: %q", got, want)
			}
		}

		{
			for _, n := range []string{"comment", "comment.sql"} {
				got, _, err := zdb.Load(db, n)
				if err != nil {
					t.Fatal(err)
				}
				want := "/* comment */\nselect 1\n\nfrom x;  -- xx\n"
				if got != want {
					t.Errorf("\ngot:  %q\nwant: %q", got, want)
				}
			}
		}
	}, drivers.TestOptions{Files: testdata.Files})
}

func TestBegin(t *testing.T) {
	zdb.RunTest(t, func(t *testing.T, ctx context.Context) {
		txctx, tx, err := zdb.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}

		err = tx.Rollback()
		if err != nil {
			t.Fatal(err)
		}

		t.Run("nested", func(t *testing.T) {
			txctx2, tx2, err := zdb.Begin(txctx)
			if err != zdb.ErrTransactionStarted {
				t.Fatal(err)
			}
			if tx2 != tx {
				t.Error("tx2 != tx")
			}
			if txctx2 != txctx {
				t.Error("txctx2 != txctx")
			}
		})
	})
}

func TestTX(t *testing.T) {
	zdb.RunTest(t, func(t *testing.T, ctx context.Context) {
		err := zdb.TX(ctx, func(ctx context.Context) error {
			_, ok := zdb.MustGetDB(ctx).(*zdb.E_zTX)
			if !ok {
				t.Errorf("not a tx: %T", zdb.MustGetDB(ctx))
			}

			return nil
		})
		if err != nil {
			t.Fatal(err)
		}

		t.Run("nested", func(t *testing.T) {
			err := zdb.TX(ctx, func(ctx context.Context) error {
				err := zdb.Exec(ctx, `create table test_tx (c text)`)
				if err != nil {
					return err
				}
				err = zdb.Exec(ctx, `insert into test_tx values ('outer')`)
				if err != nil {
					return err
				}

				return zdb.TX(ctx, func(ctx context.Context) error {
					err := zdb.Exec(ctx, `insert into test_tx values ('inner')`)
					return err
				})
			})
			if err != nil {
				t.Fatal(err)
			}

			got := zdb.DumpString(ctx, `select * from test_tx`)
			want := "c\nouter\ninner\n"
			if got != want {
				t.Errorf("\ngot:  %q\nwant: %q", got, want)
			}
		})

		t.Run("nested_inner_error", func(t *testing.T) {
			zdb.Exec(ctx, `create table test_tx2 (c text)`)
			err := zdb.TX(ctx, func(ctx context.Context) error {
				err := zdb.Exec(ctx, `insert into test_tx2 values ('outer')`)
				if err != nil {
					return err
				}

				return zdb.TX(ctx, func(ctx context.Context) error {
					zdb.Exec(ctx, `insert into test_tx2 values ('inner')`)
					return errors.New("oh noes")
				})
			})
			if err == nil {
				t.Fatal("err is nil")
			}

			have := zdb.DumpString(ctx, `select * from test_tx2`)
			want := "c\n"
			if have != want {
				t.Errorf("\nhave: %q\nwant: %q", have, want)
			}
		})

		t.Run("nested_outer_error", func(t *testing.T) {
			zdb.Exec(ctx, `create table test_tx3 (c text)`)

			err := zdb.TX(ctx, func(ctx context.Context) error {
				err := zdb.Exec(ctx, `insert into test_tx3 values ('outer')`)
				if err != nil {
					return err
				}

				err = zdb.TX(ctx, func(ctx context.Context) error {
					zdb.Exec(ctx, `insert into test_tx3 values ('inner')`)
					return nil
				})
				if err != nil {
					return err
				}

				return errors.New("oh noes")
			})
			if !ztest.ErrorContains(err, "oh noes") {
				t.Fatalf("wrong error: %v", err)
			}

			got := zdb.DumpString(ctx, `select * from test_tx3`)
			want := "c\n"
			if got != want {
				t.Errorf("\ngot:  %q\nwant: %q", got, want)
			}
		})
	})
}

func TestPrepareIn(t *testing.T) {
	zdb.RunTest(t, func(t *testing.T, ctx context.Context) {
		tests := []struct {
			query  string
			params []any
			want   string
		}{
			{``, nil, ` []interface {}(nil)`},
			{
				`select * from t where a=? and c in (?)`,
				[]any{1, []string{"A", "B"}},
				`select * from t where a=? and c in (?, ?) []interface {}{1, "A", "B"}`,
			},
			{
				`select * from t where a=? and c in (?)`,
				[]any{1, []int{1, 2}},
				`select * from t where a=? and c in (1, 2) []interface {}{1}`,
			},
			{
				`select * from t where a=? and c in (?)`,
				[]any{1, []int64{1, 2}},
				`select * from t where a=? and c in (1, 2) []interface {}{1}`,
			},
			{
				`? ? ? ? ? ?`,
				[]any{1, 2, 3, []int64{4}, 5, []int64{6}},
				`? ? ? 4 ? 6 []interface {}{1, 2, 3, 5}`,
			},

			// Note this is kinda wrong (or at least, unexpected), but this is how
			// sqlx.In() does it. There is no real way to know this is a []rune
			// rather than a []int32 :-/
			{
				`? (?) ?`,
				[]any{[]byte("ABC"), []rune("ZXC"), "C"},
				`? (?, ?, ?) ? []interface {}{[]uint8{0x41, 0x42, 0x43}, 90, 88, 67, "C"}`,
			},
		}

		for _, tt := range tests {
			t.Run("", func(t *testing.T) {
				query, params, err := zdb.E_prepareImpl(ctx, zdb.MustGetDB(ctx), tt.query, tt.params...)
				if err != nil {
					t.Fatal(err)
				}

				if zdb.SQLDialect(ctx) == zdb.DialectPostgreSQL {
					i := 0
					tt.want = regexp.MustCompile(`\?`).ReplaceAllStringFunc(tt.want, func(m string) string {
						i++
						return fmt.Sprintf("$%d", i)
					})
				}

				have := fmt.Sprintf("%s %#v", query, params)
				if have != tt.want {
					t.Errorf("\nhave: %#v\nwant: %#v", have, tt.want)
				}
			})
		}
	})
}

func BenchmarkPrepare(b *testing.B) {
	query := `
 		select foo from bar
 		{{:join join x using (y)}}
 		where site=:site and start=:start and end=:end
 		{{:path and path like :path}}
 		{{:psql returning id}}`
	arg := map[string]any{
		"path":  "/XXXX",
		"site":  42,
		"start": "2020-01-01",
		"end":   "2020-05-05",
		"psql":  false,
		"join":  true,
	}

	db, err := zdb.Connect(context.Background(), zdb.ConnectOptions{
		Connect: "sqlite3+:memory:",
	})
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		_, _, _ = zdb.E_prepareImpl(zdb.WithDB(context.Background(), db), db, query, arg)
	}
}

func BenchmarkLoad(b *testing.B) {
	db, err := zdb.Connect(context.Background(), zdb.ConnectOptions{
		Connect: "sqlite3+:memory:",
		Create:  true,
		Files:   testdata.Files,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		_, _, _ = zdb.Load(db, "hit_list.GetTotalCount")
	}
}
