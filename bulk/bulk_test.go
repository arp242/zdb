package bulk

import (
	"reflect"
	"testing"

	"zgo.at/zdb"
)

func TestBuilder(t *testing.T) {
	b := newBuilder("TBL", "col1", "col2", "col3")
	b.values("one", "two", "three")
	b.values("a", "b", "c")

	want := `insert into TBL (col1,col2,col3) values ($1,$2,$3),($4,$5,$6)`
	wantargs := []interface{}{"one", "two", "three", "a", "b", "c"}

	query, args := b.SQL()
	if query != want {
		t.Errorf("wrong query\nwant: %q\ngot:  %q", want, query)
	}
	if !reflect.DeepEqual(args, wantargs) {
		t.Errorf("wrong args\nwant: %q\ngot:  %q", wantargs, args)
	}
}

func TestInsert(t *testing.T) {
	ctx, clean := zdb.StartTest(t)
	defer clean()

	err := zdb.Exec(ctx, `create table TBL (aa text, bb text, cc text);`)
	if err != nil {
		t.Fatal(err)
	}

	insert := NewInsert(ctx, "TBL", []string{"aa", "bb", "cc"})
	insert.Values("one", "two", "three")
	insert.Values("a", "b", "c")

	err = insert.Finish()
	if err != nil {
		t.Fatal(err)
	}
}

func TestError(t *testing.T) {
	ctx, clean := zdb.StartTest(t)
	defer clean()

	err := zdb.Exec(ctx, `create table TBL (aa text, bb text, cc text);`)
	if err != nil {
		t.Fatal(err)
	}

	insert := NewInsert(ctx, "TBL", []string{"aa", "bb", "cc"})
	insert.Values("one", "two")
	insert.Values("a", "b")

	err = insert.Finish()
	if err == nil {
		t.Fatal("error is nil")
	}

	want := `1 errors: 2 values for 3 columns (query="insert into TBL (aa,bb,cc) values ($1,$2),($3,$4)") (args=[one two a b])`
	if zdb.PgSQL(ctx) {
		want = `1 errors: pq: INSERT has more target columns than expressions (query="insert into TBL (aa,bb,cc) values ($1,$2),($3,$4)") (args=[one two a b])`
	}
	if err.Error() != want {
		t.Fatalf("wrong error:\n%v", err)
	}
}
