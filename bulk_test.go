package zdb_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"zgo.at/zdb"
)

func TestBulkInsert(t *testing.T) {
	zdb.RunTest(t, func(t *testing.T, ctx context.Context) {
		err := zdb.Exec(ctx, `create table TBL (aa text, bb text, cc text);`)
		if err != nil {
			t.Fatal(err)
		}

		insert := zdb.NewBulkInsert(ctx, "TBL", []string{"aa", "bb", "cc"})
		insert.Values("one", "two", "three")
		insert.Values("a", "b", "c")

		err = insert.Finish()
		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestBulkInsertError(t *testing.T) {
	zdb.RunTest(t, func(t *testing.T, ctx context.Context) {
		err := zdb.Exec(ctx, `create table TBL (aa text, bb text, cc text);`)
		if err != nil {
			t.Fatal(err)
		}

		insert := zdb.NewBulkInsert(ctx, "TBL", []string{"aa", "bb", "cc"})
		insert.Values("'one\"", 2)
		a := "a"
		insert.Values(&a, time.Date(2021, 6, 18, 12, 00, 00, 0, time.UTC))

		err = insert.Finish()
		if err == nil {
			t.Fatal("error is nil")
		}

		var want string
		switch zdb.SQLDialect(ctx) {
		case zdb.DialectSQLite:
			want = `2 values for 3 columns`
		case zdb.DialectPostgreSQL:
			want = `INSERT has more target columns than expressions`
		case zdb.DialectMariaDB:
			want = `Column count doesn't match value count at row 1`
		}
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("wrong error:\n%v", err)
		}
	})
}

func TestBulkInsertReturning(t *testing.T) {
	zdb.RunTest(t, func(t *testing.T, ctx context.Context) {
		err := zdb.Exec(ctx, fmt.Sprintf(`create table TBL (id %s, aa text, bb text, cc text)`,
			map[zdb.Dialect]string{
				zdb.DialectPostgreSQL: "serial   primary key",
				zdb.DialectSQLite:     "integer  primary key autoincrement",
				zdb.DialectMariaDB:    "integer  not null auto_increment primary key",
			}[zdb.SQLDialect(ctx)]))
		if err != nil {
			t.Fatal(err)
		}

		insert := zdb.NewBulkInsert(ctx, "TBL", []string{"aa", "bb", "cc"})
		insert.Returning("id")
		insert.Limit = 2

		w := make([][]any, 0, 49)
		ins := func() error {
			j := 1
			for i := 100; i < 150; i++ {
				insert.Values(i, i, i)
				w = append(w, []any{j})
				j++
			}
			return insert.Finish()
		}

		err = ins()
		if err != nil {
			t.Fatal(err)
		}

		{
			want := fmt.Sprintf("%v", w)
			have := fmt.Sprintf("%v", insert.Returned())
			if have != want {
				t.Errorf("\nhave: %s\nwant: %s", have, want)
			}
			if l := len(insert.Returned()); l != 0 {
				t.Errorf("len = %d", l)
			}
		}

		{
			for i := range w {
				w[i][0] = 51 + i
			}
			want := fmt.Sprintf("%v", w)
			err := ins()
			if err != nil {
				t.Fatal(err)
			}

			have := fmt.Sprintf("%v", insert.Returned())
			if have != want {
				t.Errorf("\nhave: %s\nwant: %s", have, want)
			}
			if l := len(insert.Returned()); l != 0 {
				t.Errorf("len = %d", l)
			}
		}
	})
}

func TestBulkInsertReturningMultiple(t *testing.T) {
	zdb.RunTest(t, func(t *testing.T, ctx context.Context) {
		err := zdb.Exec(ctx, fmt.Sprintf(`create table TBL (id %s, aa text, bb text, cc text)`,
			map[zdb.Dialect]string{
				zdb.DialectPostgreSQL: "serial   primary key",
				zdb.DialectSQLite:     "integer  primary key autoincrement",
				zdb.DialectMariaDB:    "integer  not null auto_increment primary key",
			}[zdb.SQLDialect(ctx)]))
		if err != nil {
			t.Fatal(err)
		}

		insert := zdb.NewBulkInsert(ctx, "TBL", []string{"aa", "bb", "cc"})
		insert.Returning("id", "aa", "bb")

		insert.Values("a 1", "b 1", "c 1")
		insert.Values("a 2", "b 2", "c 2")

		err = insert.Finish()
		if err != nil {
			t.Fatal(err)
		}

		want := `[][]interface {}{[]interface {}{1, "a 1", "b 1"}, []interface {}{2, "a 2", "b 2"}}`
		have := fmt.Sprintf("%#v", insert.Returned())
		if have != want {
			t.Errorf("\nhave: %s\nwant: %s", have, want)
		}
		if l := len(insert.Returned()); l != 0 {
			t.Errorf("len = %d", l)
		}
	})
}
