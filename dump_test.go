package zdb

import (
	"reflect"
	"testing"
)

func TestListTables(t *testing.T) {
	ctx, clean := StartTest(t)
	defer clean()

	tables, err := ListTables(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var want []string
	if !reflect.DeepEqual(want, tables) {
		t.Errorf("\nwant: %v\ngot:  %v", want, tables)
	}

	_, err = MustGet(ctx).ExecContext(ctx, `create table test2 (col int)`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = MustGet(ctx).ExecContext(ctx, `create table test1 (col varchar)`)
	if err != nil {
		t.Fatal(err)
	}

	tables, err = ListTables(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want = []string{"test1", "test2"}
	if !reflect.DeepEqual(want, tables) {
		t.Errorf("\nwant: %v\ngot:  %v", want, tables)
	}
}
