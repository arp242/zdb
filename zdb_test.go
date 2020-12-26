package zdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"zgo.at/zstd/ztest"
)

var (
	_ DB       = &sqlx.DB{}
	_ DBCloser = &sqlx.DB{}
	_ DB       = &sqlx.Tx{}
)

func TestUnwrap(t *testing.T) {
	ctx, clean := StartTest(t)
	defer clean()

	db := MustGet(ctx)

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

func TestBegin(t *testing.T) {
	ctx, clean := StartTest(t)
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
		txctx2, tx2, err := Begin(txctx)
		if err != ErrTransactionStarted {
			t.Fatal(err)
		}
		if tx2 != tx {
			t.Error("tx2 != tx")
		}
		if txctx2 != txctx {
			t.Error("txctx2 != txctx")
		}
	})
}

func TestTX(t *testing.T) {
	ctx, clean := StartTest(t)
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

	t.Run("nested", func(t *testing.T) {
		err := TX(ctx, func(ctx context.Context, tx DB) error {
			_, err := tx.ExecContext(ctx, `create table test_tx (c varchar)`)
			if err != nil {
				return err
			}
			_, err = tx.ExecContext(ctx, `insert into test_tx values ('outer')`)
			if err != nil {
				return err
			}

			return TX(ctx, func(ctx context.Context, tx DB) error {
				_, err := tx.ExecContext(ctx, `insert into test_tx values ('inner')`)
				return err
			})
		})
		if err != nil {
			t.Fatal(err)
		}

		got := DumpString(ctx, `select * from test_tx`)
		want := "c\nouter\ninner\n"
		if got != want {
			t.Errorf("\ngot:  %q\nwant: %q", got, want)
		}
	})

	t.Run("nested_inner_error", func(t *testing.T) {
		MustGet(ctx).ExecContext(ctx, `create table test_tx2 (c varchar)`)
		err := TX(ctx, func(ctx context.Context, tx DB) error {
			_, err := tx.ExecContext(ctx, `insert into test_tx2 values ('outer')`)
			if err != nil {
				return err
			}

			return TX(ctx, func(ctx context.Context, tx DB) error {
				tx.ExecContext(ctx, `insert into test_tx2 values ('inner')`)
				return errors.New("oh noes")
			})
		})
		if err == nil {
			t.Fatal("err is nil")
		}

		got := DumpString(ctx, `select * from test_tx2`)
		want := "c\n"
		if got != want {
			t.Errorf("\ngot:  %q\nwant: %q", got, want)
		}
	})

	t.Run("nested_outer_error", func(t *testing.T) {
		MustGet(ctx).ExecContext(ctx, `create table test_tx3 (c varchar)`)
		err := TX(ctx, func(ctx context.Context, tx DB) error {
			_, err := tx.ExecContext(ctx, `insert into test_tx3 values ('outer')`)
			if err != nil {
				return err
			}

			err = TX(ctx, func(ctx context.Context, tx DB) error {
				tx.ExecContext(ctx, `insert into test_tx3 values ('inner')`)
				return nil
			})
			if err != nil {
				return err
			}

			return errors.New("oh noes")
		})
		if !ztest.ErrorContains(err, "oh noes") {
			t.Fatalf("wrong error: %v", err)
		}

		got := DumpString(ctx, `select * from test_tx3`)
		want := "c\n"
		if got != want {
			t.Errorf("\ngot:  %q\nwant: %q", got, want)
		}
	})
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
	ctx, clean := StartTest(t)
	defer clean()

	tables, err := ListTables(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var want []string
	if !reflect.DeepEqual(want, tables) {
		t.Errorf("\nwant: %v\ngot:  %v", want, tables)
	}

	_, err = MustGet(ctx).ExecContext(ctx, `create table test2 (col int)`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = MustGet(ctx).ExecContext(ctx, `create table test1 (col varchar)`)
	if err != nil {
		t.Fatal(err)
	}

	tables, err = ListTables(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want = []string{"test1", "test2"}
	if !reflect.DeepEqual(want, tables) {
		t.Errorf("\nwant: %v\ngot:  %v", want, tables)
	}
}

func TestInsertID(t *testing.T) {
	ctx, clean := StartTest(t)
	defer clean()

	tbl := `create table test (col_id integer primary key autoincrement, v varchar)`
	if PgSQL(MustGet(ctx)) {
		tbl = `create table test (col_id serial primary key, v varchar)`
	}
	_, err := MustGet(ctx).ExecContext(ctx, tbl)
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
