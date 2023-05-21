package zdb_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"zgo.at/zdb"
	"zgo.at/zstd/ztest"
)

var (
	_ zdb.DB = zdb.E_zDB{}
	_ zdb.DB = zdb.E_zTX{}
)

func TestUnwrap(t *testing.T) {
	zdb.RunTest(t, func(t *testing.T, ctx context.Context) {
		db := zdb.MustGetDB(ctx)

		if zdb.Unwrap(db) != db {
			t.Error()
		}

		ldb := zdb.NewLogDB(db, os.Stdout, 0, "")
		if zdb.Unwrap(ldb) != db {
			t.Error()
		}
		ldb2 := zdb.NewLogDB(ldb, os.Stdout, 0, "")
		if zdb.Unwrap(ldb2) != db {
			t.Error()
		}
	})
}

func TestError(t *testing.T) {
	tests := []struct {
		err   error
		check func(error) bool
		want  bool
	}{
		{sql.ErrNoRows, zdb.ErrNoRows, true},
		{fmt.Errorf("x: %w", sql.ErrNoRows), zdb.ErrNoRows, true},
		{errors.New("X"), zdb.ErrNoRows, false},
	}

	for i, tt := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			out := tt.check(tt.err)
			if out != tt.want {
				t.Errorf("out: %t; want: %t", out, tt.want)
			}
		})
	}
}

func TestErrUnique(t *testing.T) {
	zdb.RunTest(t, func(t *testing.T, ctx context.Context) {
		err := zdb.Exec(ctx, `create table t (c text)`)
		if err != nil {
			t.Fatal(err)
		}
		err = zdb.Exec(ctx, `create unique index test on t(c)`)
		if err != nil {
			t.Fatal(err)
		}

		err = zdb.Exec(ctx, `insert into t values ('a')`)
		if err != nil {
			t.Fatal(err)
		}

		err = zdb.Exec(ctx, `insert into t values ('a')`)
		if err == nil {
			t.Fatal("error is nil")
		}
		if !zdb.ErrUnique(err) {
			t.Fatalf("wrong error: %#v", err)
		}
	})
}

func TestDate(t *testing.T) {
	zdb.RunTest(t, func(t *testing.T, ctx context.Context) {
		err := zdb.Exec(ctx, `create table t (a timestamp, b timestamp)`)
		if err != nil {
			t.Fatal(err)
		}

		n := time.Now()
		err = zdb.Exec(ctx, `insert into t values (?)`, zdb.L{n, &n})
		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestDialect(t *testing.T) {
	zdb.RunTest(t, func(t *testing.T, ctx context.Context) {
		db := zdb.MustGetDB(ctx)
		t.Log(db.SQLDialect())
	})
}

func TestMissingFields(t *testing.T) {
	zdb.RunTest(t, func(t *testing.T, ctx context.Context) {
		err := zdb.Exec(ctx, `create table t (a text, b text, c text, d text)`)
		if err != nil {
			t.Fatal(err)
		}

		err = zdb.Exec(ctx, `insert into t values (?), (?)`,
			zdb.L{"1", "2", "3", "4"},
			zdb.L{"5", "6", "7", "8"},
		)
		if err != nil {
			t.Fatal(err)
		}

		var r []struct {
			A string `db:"a"`
			C string `db:"c"`
			D string `db:"d"`
		}
		err = zdb.Select(ctx, &r, `select * from t`)
		if err == nil || !zdb.ErrMissingField(err) {
			t.Errorf("wrong error: %#v", err)
		}
		if have := fmt.Sprintf("%s", r); have != `[{1 3 4} {5 7 8}]` {
			t.Error(have)
		}
	})
}

func TestSelect(t *testing.T) {
	zdb.RunTest(t, func(t *testing.T, ctx context.Context) {
		err := zdb.Exec(ctx, `create table t (a text, b text, c text, d text)`)
		if err != nil {
			t.Fatal(err)
		}
		err = zdb.Exec(ctx, `insert into t values (?), (?)`,
			zdb.L{"1", "2", "3", "4"},
			zdb.L{"5", "6", "7", "8"},
		)
		if err != nil {
			t.Fatal(err)
		}

		t.Run("slice of struct", func(t *testing.T) {
			var s []struct {
				A string
				B string
			}
			err := zdb.Select(ctx, &s, `select a, b from t`)
			if err != nil {
				t.Fatal(err)
			}
			want := `[{1 2} {5 6}]`
			if have := fmt.Sprintf("%s", s); have != want {
				t.Errorf("\nhave: %s\nwant: %s", have, want)
			}
		})

		t.Run("slice of map any", func(t *testing.T) {
			var s []map[string]any
			err := zdb.Select(ctx, &s, `select a, b from t`)
			if err != nil {
				t.Fatal(err)
			}
			want := `[map[a:1 b:2] map[a:5 b:6]]`
			if have := fmt.Sprintf("%s", s); have != want {
				t.Errorf("\nhave: %s\nwant: %s", have, want)
			}
		})

		t.Run("slice of map string", func(t *testing.T) {
			var s []map[int]string
			err := zdb.Select(ctx, &s, `select a, b from t`)
			if !ztest.ErrorContains(err, `dest map must by []map[string]any, not []map[int]string`) {
				t.Fatalf("wrong error: %v", err)
			}
		})

		t.Run("slice of slice any", func(t *testing.T) {
			var s [][]any
			err := zdb.Select(ctx, &s, `select a, b from t`)
			if err != nil {
				t.Fatal(err)
			}
			want := `[[1 2] [5 6]]`
			if have := fmt.Sprintf("%s", s); have != want {
				t.Errorf("\nhave: %s\nwant: %s", have, want)
			}
		})

		t.Run("slice of slice string", func(t *testing.T) {
			var s [][]string
			err := zdb.Select(ctx, &s, `select a, b from t`)
			if !ztest.ErrorContains(err, "dest slice must by [][]any, not [][]string") {
				t.Fatalf("wrong error: %v", err)
			}
		})
	})
}
