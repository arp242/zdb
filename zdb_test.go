package zdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

var (
	_ DB = &sqlx.DB{}
	_ DB = &sqlx.Tx{}
)

func TestBegin(t *testing.T) {
	ctx, clean := startTest(t)
	defer clean()

	txctx, tx, err := Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}

	err = tx.Rollback()
	if err != nil {
		t.Fatal(err)
	}

	t.Run("nested", func(t *testing.T) {
		// Just ensure it won't panic. Nested transactions aren't supported yet.
		_, _, err = Begin(txctx)
		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestTX(t *testing.T) {
	ctx, clean := startTest(t)
	defer clean()

	err := TX(ctx, func(ctx context.Context, tx DB) error {
		_, ok := tx.(*sqlx.Tx)
		if !ok {
			t.Errorf("not a tx: %T", tx)
		}

		_, ok = MustGet(ctx).(*sqlx.Tx)
		if !ok {
			t.Errorf("not a tx: %T", tx)
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
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

func TestListTables(t *testing.T) {
	ctx, clean := startTest(t)
	defer clean()

	db := MustGet(ctx).(*sqlx.DB)

	tables, err := ListTables(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var want []string
	if !reflect.DeepEqual(want, tables) {
		t.Errorf("\nwant: %v\ngot:  %v", want, tables)
	}

	db.MustExec(`create table test2 (col int)`)
	db.MustExec(`create table test1 (col varchar)`)

	tables, err = ListTables(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want = []string{"test1", "test2"}
	if !reflect.DeepEqual(want, tables) {
		t.Errorf("\nwant: %v\ngot:  %v", want, tables)
	}
}

// startTest a new database test.
func startTest(t *testing.T) (context.Context, func()) {
	t.Helper()
	db, err := Connect(ConnectOptions{
		Connect: "sqlite3://:memory:",
	})
	if err != nil {
		t.Fatal(err)
	}
	return With(context.Background(), db), func() { db.Close() }
}
