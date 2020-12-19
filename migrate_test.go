package zdb

import (
	"reflect"
	"testing"

	"zgo.at/zdb/testdata"
)

func TestMigrate(t *testing.T) {
	ctx, clean := StartTest(t)
	defer clean()
	db := MustGet(ctx)

	_, err := db.ExecContext(ctx, `create table version (name varchar)`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.ExecContext(ctx, `insert into version (name) values ('one'), ('two')`)
	if err != nil {
		t.Fatal(err)
	}

	m := NewMigrate(db, testdata.Files, nil)
	have, ran, err := m.List()
	if err != nil {
		t.Fatal(err)
	}

	{
		want := []string{"test"}
		if !reflect.DeepEqual(have, want) {
			t.Errorf("\ngot:  %#v\nwant: %#v", have, want)
		}

		want = []string{"one", "two"}
		if !reflect.DeepEqual(ran, want) {
			t.Errorf("\ngot:  %#v\nwant: %#v", ran, want)
		}
	}

	{
		got, err := m.Schema("test")
		if err != nil {
			t.Fatal(err)
		}
		want := "select 'migrate-sqlite';\n"
		if PgSQL(db) {
			want = "select 'migrate-pgsql';\n"
		}
		if got != want {
			t.Errorf("\ngot:  %q\nwant: %q", got, want)
		}

		got, err = m.Schema("test.sql")
		if err != nil {
			t.Fatal(err)
		}
		want = "select 'migrate-sqlite';\n"
		if PgSQL(db) {
			want = "select 'migrate-pgsql';\n"
		}
		if got != want {
			t.Errorf("\ngot:  %q\nwant: %q", got, want)
		}
	}
}
