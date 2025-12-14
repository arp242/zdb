package zdb

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"testing"
	"time"

	"zgo.at/zdb/drivers/test"
	_ "zgo.at/zdb/drivers/test"
	"zgo.at/zdb/internal/sqlx"
	"zgo.at/zstd/ztest"
)

func testdriver(t testing.TB) context.Context {
	test.Use()
	db, err := Connect(context.Background(), ConnectOptions{Connect: "test+"})
	if err != nil {
		t.Fatal(err)
	}
	return WithDB(context.Background(), db)
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
		{`select :x`, L{P{"x": "Y"}},
			`select $1`, L{"Y"}, ""},

		// Single named param from struct
		{`select :x`, L{struct{ X string }{"Y"}},
			`select $1`, L{"Y"}, ""},

		// Both a map and struct → merge
		{`select :x, :y`, L{P{"x": "Y"}, struct{ Y int }{42}},
			`select $1, $2`, L{"Y", 42}, ""},

		// One positional
		{`select $1`, L{"A"},
			`select $1`, L{"A"}, ""},
		{`select ?`, L{"A"},
			`select $1`, L{"A"}, ""},

		// Two positional
		{`select $1, $2`, L{"A", "B"},
			`select $1, $2`, L{"A", "B"}, ""},
		{`select ?, ?`, L{"A", "B"},
			`select $1, $2`, L{"A", "B"}, ""},

		// time.Time shouldn't be seen as a named argument.
		{`select ?`, L{date},
			`select $1`, L{date}, ""},
		{`select ?, ?`, L{date, date},
			`select $1, $2`, L{date, date}, ""},

		// Neither should structs implementing sql.Scanner
		{`select ?`, L{sql.NullBool{Valid: true}},
			`select $1`, L{sql.NullBool{Valid: true}}, ""},
		{`select ?, ?`, L{sql.NullString{}, sql.NullString{}},
			`select $1, $2`, L{sql.NullString{}, sql.NullString{}}, ""},

		// True conditional from bool
		{`select {{:yyy cond}} where 1=1`, L{P{"yyy": true}},
			`select cond where 1=1`, L{}, ""},
		{`select {{:yyy cond}} where 1=1`, L{struct{ YYY bool }{true}},
			`select cond where 1=1`, L{}, ""},
		{`select {{:yyy cond}} where 1=1`, L{P{"a": true}, struct{ YYY bool }{true}},
			`select cond where 1=1`, L{}, ""},

		// Negation with !
		{`select {{:yyy! cond}} where 1=1`, L{P{"yyy": true}},
			`select  where 1=1`, L{}, ""},
		// Negation with !
		{`select {{:yyy! cond}} where 1=1`, L{P{"yyy": false}},
			`select cond where 1=1`, L{}, ""},

		// False conditional from bool
		{`select {{:yyy cond}} where 1=1`, L{P{"yyy": false}},
			`select  where 1=1`, L{}, ""},
		{`select {{:yyy cond}} where 1=1`, L{struct{ YYY bool }{false}},
			`select  where 1=1`, L{}, ""},
		{`select {{:yyy cond}} where 1=1`, L{P{"a": false}, struct{ YYY bool }{false}},
			`select  where 1=1`, L{}, ""},

		// Multiple conditionals
		{`select {{:a cond}} {{:b cond2}} `, L{P{"a": true, "b": true}},
			`select cond cond2 `, L{}, ""},
		{`select {{:a cond}} {{:b cond2}} `, L{P{"a": false, "b": false}},
			`select   `, L{}, ""},

		// Parameters inside conditionals
		{`select {{:a x like :foo}} {{:b y = :bar}}`, L{P{"foo": "qwe", "bar": "zxc", "a": true, "b": true}},
			`select x like $1 y = $2`, L{"qwe", "zxc"}, ""},
		{`select {{:a x like :foo}} {{:b y = :bar}}`, L{P{"foo": "qwe", "bar": "zxc", "a": false, "b": true}},
			`select  y = $1`, L{"zxc"}, ""},

		// Multiple conflicting params
		{`select :x`, L{P{"x": 1}, P{"x": 2}},
			``, nil, "more than once"},
		{`select {{:x cond}}`, L{P{"x": 1}, P{"x": 2}},
			``, nil, "more than once"},

		// Mixing positional and named
		{`select :x`, L{P{"x": 1}, 42},
			``, nil, "mix named and positional"},

		// Conditional not found
		{`select {{:x cond}}`, L{P{"z": 1}},
			``, nil, "could not find"},

		// Condtional with positional
		{`select {{:x cond}}`, L{"z", 1},
			`select {{:x cond}}`, L{"z", 1}, ""},

		// Invalid syntax for conditional; just leave it alone
		{`select {{cond}}`, L{P{"yyy": false}},
			`select {{cond}}`, L{}, ""},

		// Conditional types
		// string
		{`select {{:x true :x}}{{:x! false :x}}`, L{P{"x": "str"}},
			`select true $1`, L{"str"}, ""},
		{`select {{:x true :x}}{{:x! false :x}}`, L{P{"x": str("str")}},
			`select true $1`, L{str("str")}, ""},
		{`select {{:x true :x}}{{:x! false :x}}`, L{P{"x": ""}},
			`select false $1`, L{""}, ""},
		{`select {{:x true :x}}{{:x! false :x}}`, L{P{"x": str("")}},
			`select false $1`, L{str("")}, ""},

		// Slice
		{`select {{:x true :x}}{{:x! false :x}}`, L{P{"x": []string{"str", "str2"}}},
			`select true $1, $2`, L{"str", "str2"}, ""},
		{`select {{:x true :x}}{{:x! false :x}}`, L{P{"x": strlist{"str", "str2"}}},
			`select true $1, $2`, L{"str", "str2"}, ""},
		{`select {{:x true}}{{:x! false}}`, L{P{"x": []string{}}},
			`select false`, L{}, ""},
		{`select {{:x true}}{{:x! false}}`, L{P{"x": strlist{}}},
			`select false`, L{}, ""},

		// Expand slice
		{`insert values (?)`, L{[]string{"a", "b"}},
			`insert values ($1, $2)`, L{"a", "b"}, ""},
		// TODO: this only works for "?"; sqlx.In() and named parameters.
		// {`insert values ($1)`, L{[]string{"a", "b"}},
		// 	`insert values ($1, $2)`, L{"a", "b"}, ""},
		{`insert values (:x)`, L{P{"x": []string{"a", "b"}}},
			`insert values ($1, $2)`, L{"a", "b"}, ""},

		// Pointer
		{`select {{:x true :x}}{{:x! false :x}}`, L{P{"x": ptr1}},
			`select true $1`, L{ptr1}, ""},
		{`select {{:x true :x}}{{:x! false :x}}`, L{P{"x": (*string)(nil)}},
			`select false $1`, L{(*string)(nil)}, ""},
		{`select {{:x true :x}}{{:x! false :x}}`, L{P{"x": ptr2}},
			`select false $1`, L{ptr2}, ""},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			ctx := testdriver(t)

			query, args, err := prepareImpl(ctx, MustGetDB(ctx), tt.query, tt.args...)
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
	}
}

func TestPrepareIn(t *testing.T) {
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
			`select * from t where a=? and c in (?, ?) []interface {}{1, 1, 2}`,
		},
		{
			`select * from t where a=? and c in (?)`,
			[]any{1, []int64{1, 2}},
			`select * from t where a=? and c in (?, ?) []interface {}{1, 1, 2}`,
		},
		{
			`? ? ? ? ? ?`,
			[]any{1, 2, 3, []int64{4}, 5, []int64{6}},
			`? ? ? ? ? ? []interface {}{1, 2, 3, 4, 5, 6}`,
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
			ctx := testdriver(t)
			query, params, err := prepareImpl(ctx, MustGetDB(ctx), tt.query, tt.params...)
			if err != nil {
				t.Fatal(err)
			}

			// if SQLDialect(ctx) == DialectPostgreSQL {
			// 	i := 0
			// 	tt.want = regexp.MustCompile(`\?`).ReplaceAllStringFunc(tt.want, func(m string) string {
			// 		i++
			// 		return fmt.Sprintf("$%d", i)
			// 	})
			// }

			have := fmt.Sprintf("%s %#v", query, params)
			if have != tt.want {
				t.Errorf("\nhave: %#v\nwant: %#v", have, tt.want)
			}
		})
	}
}

func TestSQLParameter(t *testing.T) {
	tests := []struct {
		query  string
		params map[string]any
		want   string
	}{
		{``, nil, ` []interface {}{}`},
		{
			`select * from y where z :in (:id)`,
			map[string]any{"in": SQL("in"), "id": 2},
			`select * from y where z in (?) []interface {}{2}`,
		},

		{
			`select * from y where z :in (:id)`,
			map[string]any{"in": SQL("in"), "notfound": SQL("unused"), "id": 2},
			`select * from y where z in (?) []interface {}{2}`,
		},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			ctx := testdriver(t)

			query, params, err := prepareImpl(ctx, MustGetDB(ctx), tt.query, tt.params)
			if err != nil {
				t.Fatal(err)
			}

			// if SQLDialect(ctx) == DialectPostgreSQL {
			// 	i := 0
			// 	tt.want = regexp.MustCompile(`\?`).ReplaceAllStringFunc(tt.want, func(m string) string {
			// 		i++
			// 		return fmt.Sprintf("$%d", i)
			// 	})
			// }

			have := fmt.Sprintf("%s %#v", query, params)
			if have != tt.want {
				t.Errorf("\nhave: %#v\nwant: %#v", have, tt.want)
			}
		})
	}
}

func BenchmarkPrepare(b *testing.B) {
	query := `
 		select foo from bar
 		{{:join join x using (y)}}
 		where site=:site and start=:start and end=:end
 		{{:path and path like :path}}
 		{{:psql returning id}}`
	arg := map[string]any{
		"path":  "/asdf",
		"site":  42,
		"start": "2020-01-01",
		"end":   "2020-05-05",
		"psql":  false,
		"join":  true,
	}

	ctx := testdriver(b)
	db := MustGetDB(ctx)

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		_, _, _ = prepareImpl(ctx, db, query, arg)
	}
}
