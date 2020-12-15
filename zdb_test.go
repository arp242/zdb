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

	t.Run("nested", func(t *testing.T) {
		err := TX(ctx, func(ctx context.Context, tx DB) error {
			tx.ExecContext(ctx, `create table test_tx (c varchar)`)
			tx.ExecContext(ctx, `insert into test_tx values ("outer")`)
			return TX(ctx, func(ctx context.Context, tx DB) error {
				_, err := tx.ExecContext(ctx, `insert into test_tx values ("inner")`)
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
			tx.ExecContext(ctx, `insert into test_tx2 values ("outer")`)
			return TX(ctx, func(ctx context.Context, tx DB) error {
				tx.ExecContext(ctx, `insert into test_tx2 values ("inner")`)
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
			tx.ExecContext(ctx, `insert into test_tx3 values ("outer")`)
			TX(ctx, func(ctx context.Context, tx DB) error {
				tx.ExecContext(ctx, `insert into test_tx3 values ("inner")`)
				return nil
			})
			return errors.New("oh noes")
		})
		if err == nil {
			t.Fatal("err is nil")
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
	ctx, clean := startTest(t)
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

// startTest a new database test.
func startTest(t *testing.T) (context.Context, func()) {
	t.Helper()
	db, err := Connect(ConnectOptions{
		Connect: "sqlite3://:memory:",
	})
	if err != nil {
		t.Fatal(err)
	}
	return With(context.Background(), db), func() {
		db.Close()
	}
}
