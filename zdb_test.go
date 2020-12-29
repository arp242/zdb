package zdb

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

var (
	_ DB       = &sqlx.DB{}
	_ DBCloser = &sqlx.DB{}
	_ DB       = &sqlx.Tx{}
)

func TestUnwrap(t *testing.T) {
	ctx, clean := StartTest(t)
	defer clean()

	db := MustGetDB(ctx)

	if Unwrap(db) != db {
		t.Error()
	}

	edb := NewExplainDB(db.(DBCloser), os.Stdout, "")
	if Unwrap(edb) != db {
		t.Error()
	}

	edb2 := NewExplainDB(edb, os.Stdout, "")
	if Unwrap(edb2) != db {
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

func TestInsertID(t *testing.T) {
	ctx, clean := StartTest(t)
	defer clean()

	tbl := `create table test (col_id integer primary key autoincrement, v varchar)`
	if PgSQL(ctx) {
		tbl = `create table test (col_id serial primary key, v varchar)`
	}
	_, err := Exec(ctx, tbl, nil)
	if err != nil {
		t.Fatal(err)
	}

	{ // One row
		id, err := InsertID(ctx, `col_id`, `insert into test (v) values ($1)`, "aa")
		if err != nil {
			t.Error(err)
		}
		if id != 1 {
			t.Errorf("id is %d, not 1", id)
		}
	}

	{ // Multiple rows
		id, err := InsertID(ctx, `col_id`, `insert into test (v) values ($1), ('bb')`, "aa")
		if err != nil {
			t.Error(err)
		}
		if id != 3 {
			t.Errorf("id is %d, not 3", id)
		}
	}

	{ // Invalid SQL

		id, err := InsertID(ctx, `col_id`, `insert into test (no_such_col) values ($1)`)
		if err == nil {
			t.Error("err is nil")
		}
		if id != 0 {
			t.Errorf("id is not 0: %d", id)
		}
	}

	out := "\n" + DumpString(ctx, `select * from test`)
	want := `
col_id  v
1       aa
2       aa
3       bb
`
	if out != want {
		t.Errorf("\nwant: %v\ngot:  %v", want, out)
	}
}
