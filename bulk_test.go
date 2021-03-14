package zdb

import (
	"reflect"
	"testing"
	"time"
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

func TestBulkInsert(t *testing.T) {
	ctx, clean := StartTest(t)
	defer clean()

	err := Exec(ctx, `create table TBL (aa text, bb text, cc text);`)
	if err != nil {
		t.Fatal(err)
	}

	insert := NewBulkInsert(ctx, "TBL", []string{"aa", "bb", "cc"})
	insert.Values("one", "two", "three")
	insert.Values("a", "b", "c")

	err = insert.Finish()
	if err != nil {
		t.Fatal(err)
	}
}

func TestBulkInsertError(t *testing.T) {
	ctx, clean := StartTest(t)
	defer clean()

	err := Exec(ctx, `create table TBL (aa text, bb text, cc text);`)
	if err != nil {
		t.Fatal(err)
	}

	insert := NewBulkInsert(ctx, "TBL", []string{"aa", "bb", "cc"})
	insert.Values("'one\"", 2)
	a := "a"
	insert.Values(&a, time.Date(2021, 6, 18, 12, 00, 00, 0, time.UTC))

	err = insert.Finish()
	if err == nil {
		t.Fatal("error is nil")
	}

	want := `1 errors: 2 values for 3 columns (query="insert into TBL (aa,bb,cc) values ($1,$2),($3,$4)") (params=['''one"' 2 'a' '2021-06-18 12:00:00'])`
	if Driver(ctx) == DriverPostgreSQL {
		want = `1 errors: pq: INSERT has more target columns than expressions (query="insert into TBL (aa,bb,cc) values ($1,$2),($3,$4)") (params=['''one"' 2 'a' '2021-06-18 12:00:00'])`
	}
	if err.Error() != want {
		t.Fatalf("wrong error:\n%v", err)
	}
}
