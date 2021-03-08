package zdb

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/lib/pq"
)

var (
	_ DB = zDB{}
	_ DB = zTX{}
)

func TestUnwrap(t *testing.T) {
	ctx, clean := StartTest(t)
	defer clean()

	db := MustGetDB(ctx)

	if Unwrap(db) != db {
		t.Error()
	}

	ldb := NewLogDB(db, os.Stdout, 0, "")
	if Unwrap(ldb) != db {
		t.Error()
	}
	ldb2 := NewLogDB(ldb, os.Stdout, 0, "")
	if Unwrap(ldb2) != db {
		t.Error()
	}
}

func TestError(t *testing.T) {
	tests := []struct {
		err   error
		check func(error) bool
		want  bool
	}{
		{sql.ErrNoRows, ErrNoRows, true},
		{fmt.Errorf("x: %w", sql.ErrNoRows), ErrNoRows, true},
		{errors.New("X"), ErrNoRows, false},

		{&pq.Error{}, ErrUnique, false},
		{&pq.Error{Code: "123"}, ErrUnique, false},
		{&pq.Error{Code: "23505"}, ErrUnique, true},
		{fmt.Errorf("X: %w", &pq.Error{Code: "23505"}), ErrUnique, true},
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
	ctx, clean := StartTest(t)
	defer clean()

	err := Exec(ctx, `create table t (c varchar); create unique index test on t(c)`)
	if err != nil {
		t.Fatal(err)
	}

	err = Exec(ctx, `insert into t values ('a')`)
	if err != nil {
		t.Fatal(err)
	}

	err = Exec(ctx, `insert into t values ('a')`)
	if err == nil {
		t.Fatal("error is nil")
	}
	if !ErrUnique(err) {
		t.Fatalf("wrong error: %#v", err)
	}
}

func TestDate(t *testing.T) {
	ctx, clean := StartTest(t)
	defer clean()

	err := Exec(ctx, `create table t (a timestamp, b timestamp)`)
	if err != nil {
		t.Fatal(err)
	}

	n := time.Now()
	err = Exec(ctx, `insert into t values (?)`, L{n, &n})
	if err != nil {
		t.Fatal(err)
	}
}
