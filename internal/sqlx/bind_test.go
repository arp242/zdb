package sqlx

import (
	"database/sql"
	"reflect"
	"testing"

	"zgo.at/zstd/ztest"
)

func TestRebind(t *testing.T) {
	tests := []struct {
		in         string
		wantDollar string
		wantAt     string
		wantNamed  string
	}{
		{
			`INSERT INTO foo (a, b, c, d, e, f, g, h, i) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			`INSERT INTO foo (a, b, c, d, e, f, g, h, i) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
			`INSERT INTO foo (a, b, c, d, e, f, g, h, i) VALUES (@p1, @p2, @p3, @p4, @p5, @p6, @p7, @p8, @p9, @p10)`,
			`INSERT INTO foo (a, b, c, d, e, f, g, h, i) VALUES (:arg1, :arg2, :arg3, :arg4, :arg5, :arg6, :arg7, :arg8, :arg9, :arg10)`,
		},
		{
			`INSERT INTO foo (a, b, c) VALUES (?, ?, "foo"), ("Hi", ?, ?)`,
			`INSERT INTO foo (a, b, c) VALUES ($1, $2, "foo"), ("Hi", $3, $4)`,
			`INSERT INTO foo (a, b, c) VALUES (@p1, @p2, "foo"), ("Hi", @p3, @p4)`,
			`INSERT INTO foo (a, b, c) VALUES (:arg1, :arg2, "foo"), ("Hi", :arg3, :arg4)`,
		},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			if have := Rebind(PlaceholderDollar, tt.in); have != tt.wantDollar {
				t.Errorf("dollar wrong\nhave: %s\nwant: %s", have, tt.wantDollar)
			}
			if have := Rebind(PlaceholderAt, tt.in); have != tt.wantAt {
				t.Errorf("At wrong\nhave: %s\nwant: %s", have, tt.wantAt)
			}
			if have := Rebind(PlaceholderNamed, tt.in); have != tt.wantNamed {
				t.Errorf("Named wrong\nhave: %s\nwant: %s", have, tt.wantNamed)
			}
		})
	}
}

func TestBindMap(t *testing.T) {
	haveQuery, haveArgs, err := bindMap(PlaceholderQuestion,
		`INSERT INTO foo (a, b, c, d) VALUES (:name, :age, :first, :last)`,
		map[string]any{
			"name":  "Jason Moiron",
			"age":   30,
			"first": "Jason",
			"last":  "Moiron",
		})
	if err != nil {
		t.Fatal(err)
	}

	wantQuery := `INSERT INTO foo (a, b, c, d) VALUES (?, ?, ?, ?)`
	wantArgs := []any{"Jason Moiron", 30, "Jason", "Moiron"}

	if haveQuery != wantQuery {
		t.Errorf("Query wrong\nhave: %s\nwant: %s", haveQuery, wantQuery)
	}
	if !reflect.DeepEqual(haveArgs, wantArgs) {
		t.Errorf("Args\nhave: %v\nwant: %v", haveArgs, wantArgs)
	}
}

func TestIn(t *testing.T) {
	tests := []struct {
		in        string
		inArgs    []any
		wantQuery string
		wantArgs  []any
		wantErr   string
	}{
		{
			`SELECT * FROM foo WHERE x = ? AND v in (?) AND y = ?`,
			[]any{"foo", []int{0, 5, 7, 2, 9}, "bar"},

			`SELECT * FROM foo WHERE x = ? AND v in (?, ?, ?, ?, ?) AND y = ?`,
			[]any{"foo", 0, 5, 7, 2, 9, "bar"},
			"",
		},
		{
			`SELECT * FROM foo WHERE x in (?)`,
			[]any{[]int{1, 2, 3, 4, 5, 6, 7, 8}},

			`SELECT * FROM foo WHERE x in (?, ?, ?, ?, ?, ?, ?, ?)`,
			[]any{1, 2, 3, 4, 5, 6, 7, 8},
			"",
		},
		{ // Don't treat []byte as a slice to be processed.
			`SELECT * FROM foo WHERE x = ? AND y in (?)`,
			[]any{[]byte("foo"), []int{0, 5, 3}},

			`SELECT * FROM foo WHERE x = ? AND y in (?, ?, ?)`,
			[]any{[]byte("foo"), 0, 5, 3},
			"",
		},

		{
			`SELECT * FROM foo WHERE x = ? AND y IN (?)`,
			[]any{sql.NullString{Valid: true, String: "x"}, []string{"a", "b"}},

			`SELECT * FROM foo WHERE x = ? AND y IN (?, ?)`,
			[]any{"x", "a", "b"},
			"",
		},
		{ // NullString with Valid: false is the same as "nil".
			`SELECT * FROM foo WHERE x = ? AND y IN (?)`,
			[]any{sql.NullString{Valid: false}, []string{"a", "b"}},

			`SELECT * FROM foo WHERE x = ? AND y IN (?, ?)`,
			[]any{nil, "a", "b"},
			"",
		},

		// too many bindVars, but no slices, so short circuits parsing.
		// I'm not sure if this is the right behavior; this query/arg combo
		// might not work, but we shouldn't parse if we don't need to.
		{
			`SELECT * FROM foo WHERE x = ? AND y = ?`,
			[]any{"foo", "bar", "baz"},

			`SELECT * FROM foo WHERE x = ? AND y = ?`,
			[]any{"foo", "bar", "baz"},

			"",
		},

		// Too many bindvars;  slice present so should return error during parse
		{
			`SELECT * FROM foo WHERE x = ? and y = ?`,
			[]any{"foo", []int{1, 2, 3}, "bar"},

			``, nil, "more arguments than placeholders",
		},

		// Empty slice, should return error before parse
		{
			`SELECT * FROM foo WHERE x = ?`,
			[]any{[]int{}},

			``, nil, "empty slice passed",
		},

		// Too *few* bindvars, should return an error
		{
			`SELECT * FROM foo WHERE x = ? AND y in (?)`,
			[]any{[]int{1, 2, 3}},

			``, nil, "more placeholders than arguments",
		},

		// https://github.com/jmoiron/sqlx/issues/688
		{
			`SELECT * FROM people WHERE name IN (?)`,
			[]any{[]string{"gopher"}},

			`SELECT * FROM people WHERE name IN (?)`,
			[]any{"gopher"},
			"",
		},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			haveQuery, haveArgs, err := In(tt.in, tt.inArgs...)
			if !ztest.ErrorContains(err, tt.wantErr) {
				t.Error(err)
			}
			if haveQuery != tt.wantQuery {
				t.Errorf("Query wrong\nhave: %s\nwant: %s", haveQuery, tt.wantQuery)
			}
			if !reflect.DeepEqual(haveArgs, tt.wantArgs) {
				t.Errorf("Args\nhave: %v\nwant: %v", haveArgs, tt.wantArgs)
			}
		})
	}
}

// BenchmarkRebind/two-8            3237973               374.2 ns/op           144 B/op          2 allocs/op
// BenchmarkRebind/ten-8            1933748               633.9 ns/op           192 B/op          2 allocs/op
//
// sqltoken:
// BenchmarkRebind/two-8             324739              3696 ns/op            2160 B/op          5 allocs/op
// BenchmarkRebind/ten-8             243567              4956 ns/op            2880 B/op          5 allocs/op
func BenchmarkRebind(b *testing.B) {
	b.Run("two", func(b *testing.B) {
		q := `INSERT INTO foo (a, b, c) VALUES (?, ?, "foo"), ("Hi", ?, ?)`
		for i := 0; i < b.N; i++ {
			Rebind(PlaceholderDollar, q)
		}
	})
	b.Run("ten", func(b *testing.B) {
		q := `INSERT INTO foo (a, b, c, d, e, f, g, h, i) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
		for i := 0; i < b.N; i++ {
			Rebind(PlaceholderDollar, q)
		}
	})
}

// BenchmarkIn-8                    1000000              1060 ns/op             264 B/op          4 allocs/op
// BenchmarkIn1k-8                    64852             18586 ns/op           19480 B/op          3 allocs/op
// BenchmarkIn1kInt-8                 47572             25248 ns/op           19480 B/op          3 allocs/op
// BenchmarkIn1kString-8              46544             25653 ns/op           19480 B/op          3 allocs/op
func BenchmarkIn(b *testing.B) {
	q := `SELECT * FROM foo WHERE x = ? AND v in (?) AND y = ?`
	for i := 0; i < b.N; i++ {
		_, _, _ = In(q, []any{"foo", []int{0, 5, 7, 2, 9}, "bar"}...)
	}
}
func BenchmarkIn1k(b *testing.B) {
	q := `SELECT * FROM foo WHERE x = ? AND v in (?) AND y = ?`
	var vals [1000]any
	for i := 0; i < b.N; i++ {
		_, _, _ = In(q, []any{"foo", vals[:], "bar"}...)
	}
}
func BenchmarkIn1kInt(b *testing.B) {
	q := `SELECT * FROM foo WHERE x = ? AND v in (?) AND y = ?`
	var vals [1000]int
	for i := 0; i < b.N; i++ {
		_, _, _ = In(q, []any{"foo", vals[:], "bar"}...)
	}
}
func BenchmarkIn1kString(b *testing.B) {
	q := `SELECT * FROM foo WHERE x = ? AND v in (?) AND y = ?`
	var vals [1000]string
	for i := 0; i < b.N; i++ {
		_, _, _ = In(q, []any{"foo", vals[:], "bar"}...)
	}
}
