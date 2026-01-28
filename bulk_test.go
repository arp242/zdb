package zdb

import (
	"reflect"
	"testing"

	"zgo.at/zdb/internal/array"
)

func TestBuilder(t *testing.T) {
	t.Run("sqlite", func(t *testing.T) {
		b := newBuilder("TBL", false, "col1", "col2", "col3")
		b.returning = []string{"r1", "r2"}
		b.values("one", "two", "three")
		b.values("a", "b", "c")

		want := `insert into "TBL" (col1,col2,col3) values (?,?,?),(?,?,?) returning r1,r2`
		wantargs := []any{"one", "two", "three", "a", "b", "c"}

		query, args, err := b.SQL()
		if err != nil {
			t.Fatal(err)
		}
		if query != want {
			t.Errorf("wrong query\nwant: %q\nhave: %q", want, query)
		}
		if !reflect.DeepEqual(args, wantargs) {
			t.Errorf("wrong args\nwant: %#v\nhave: %#v", wantargs, args)
		}
	})

	t.Run("postgres", func(t *testing.T) {
		b := newBuilder("TBL", true, "col1", "col2", "col3")
		b.types = []string{"text", "text", "text"}
		b.returning = []string{"r1", "r2"}
		b.values("one", "two", "three")
		b.values("a", "b", "c")

		want := `insert into "TBL" (col1,col2,col3) select * from unnest(:p1::text[], :p2::text[], :p3::text[]) returning r1,r2`
		wantargs := []any{map[string]any{
			"p1": array.Array([]any{"one", "a"}),
			"p2": array.Array([]any{"two", "b"}),
			"p3": array.Array([]any{"three", "c"}),
		}}

		query, args, err := b.SQL()
		if err != nil {
			t.Fatal(err)
		}
		if query != want {
			t.Errorf("wrong query\nwant: %q\nhave: %q", want, query)
		}
		if !reflect.DeepEqual(args, wantargs) {
			t.Errorf("wrong args\nwant: %#v\nhave: %#v", wantargs, args)
		}
	})
}

func BenchmarkBuilder(b *testing.B) {
	b.Run("values", func(b *testing.B) {
		bld := newBuilder("TBL", false, "col1", "col2", "col3")
		bld.returning = []string{"r1", "r2"}

		b.ResetTimer()
		for b.Loop() {
			bld.values("one", "two", "three")
		}
	})

	b.Run("SQL", func(b *testing.B) {
		bld := newBuilder("TBL", false, "col1", "col2", "col3")
		bld.returning = []string{"r1", "r2"}
		bld.values("one", "two", "three")
		bld.values("a", "b", "c")

		b.ResetTimer()
		for b.Loop() {
			_, _, _ = bld.SQL()
		}
	})
}
