package zdb_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"zgo.at/zdb"
	"zgo.at/zstd/ztest"
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
		err = zdb.Exec(ctx, `insert into t values (?)`, []any{n, &n})
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
			[]any{"1", "2", "3", "4"},
			[]any{"5", "6", "7", "8"},
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
			[]any{"1", "2", "3", "4"},
			[]any{"5", "6", "7", "8"},
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

type Time struct{ time.Time }

func (t Time) Value() (driver.Value, error) {
	return t.Time.Truncate(time.Microsecond), nil
}
func (t *Time) Scan(v any) error {
	var s string
	switch vv := v.(type) {
	case nil: // Allow NULL; explicitly set to zero value.
		t.Time = time.Time{}
		return nil
	case time.Time:
		t.Time = vv
		return nil
	case *time.Time:
		t.Time = *vv
		return nil
	case string:
		s = vv
	case []byte:
		s = string(vv)
	default:
		return fmt.Errorf("Time.Scan: %#v", vv)
	}
	tt, err := time.Parse("2006-01-02 15:04:05.000000000", s)
	t.Time = tt
	return err
}

func TestNilPanic(t *testing.T) {
	zdb.RunTest(t, func(t *testing.T, ctx context.Context) {
		err := zdb.Exec(ctx, `create table tbl (a text, t timestamp)`)
		if err != nil {
			t.Fatal(err)
		}
		err = zdb.Exec(ctx, `insert into tbl values ('one', '1985-06-18 19:20:21')`)
		if err != nil {
			t.Fatal(err)
		}

		var rows []struct {
			A string `db:"a"`
			T Time   `db:"t"`
		}
		var x *Time
		err = zdb.Select(ctx, &rows, `select * from tbl where t = ?`, x)
		if err != nil {
			t.Fatal(err)
		}

		have := zdb.DumpString(ctx, `select * from tbl where t = ?`, x, zdb.DumpQuery)
		want := "select * from tbl where t = '<nil>';\n"
		if have != want {
			t.Errorf("\nhave: %q\nwant: %q", have, want)
		}
	})
}
