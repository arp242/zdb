package zdb_test

import (
	"context"
	"reflect"
	"testing"

	"zgo.at/zdb"
	"zgo.at/zdb/test/testdata"
)

func TestMigrateList(t *testing.T) {
	zdb.RunTest(t, func(t *testing.T, ctx context.Context) {
		err := zdb.Exec(ctx, `create table version (name text)`)
		if err != nil {
			t.Fatal(err)
		}
		err = zdb.Exec(ctx, `insert into version (name) values ('one'), ('two')`)
		if err != nil {
			t.Fatal(err)
		}

		m, err := zdb.NewMigrate(zdb.MustGetDB(ctx), testdata.Files, nil)
		if err != nil {
			t.Fatal(err)
		}
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
			if zdb.SQLDialect(ctx) == zdb.DialectPostgreSQL {
				want = "select 'migrate-pgsql';\n"
			}
			if zdb.SQLDialect(ctx) == zdb.DialectMariaDB {
				want = "select 'migrate-mariadb';\n"
			}
			if got != want {
				t.Errorf("\ngot:  %q\nwant: %q", got, want)
			}

			got, err = m.Schema("test.sql")
			if err != nil {
				t.Fatal(err)
			}
			want = "select 'migrate-sqlite';\n"
			if zdb.SQLDialect(ctx) == zdb.DialectPostgreSQL {
				want = "select 'migrate-pgsql';\n"
			}
			if zdb.SQLDialect(ctx) == zdb.DialectMariaDB {
				want = "select 'migrate-mariadb';\n"
			}
			if got != want {
				t.Errorf("\ngot:  %q\nwant: %q", got, want)
			}
		}
	})
}
