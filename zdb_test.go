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
		err := zdb.Exec(ctx, `create table t (c varchar); create unique index test on t(c)`)
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
