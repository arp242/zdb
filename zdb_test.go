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
