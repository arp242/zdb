package zdb

import (
	"context"
	"errors"
	"testing"

	"github.com/jmoiron/sqlx"
	"zgo.at/zstd/ztest"
)

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

	err := TX(ctx, func(ctx context.Context) error {
		_, ok := MustGetDB(ctx).(*sqlx.Tx)
		if !ok {
			t.Errorf("not a tx: %T", MustGetDB(ctx))
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	t.Run("nested", func(t *testing.T) {
		err := TX(ctx, func(ctx context.Context) error {
			_, err := Exec(ctx, `create table test_tx (c varchar)`, nil)
			if err != nil {
				return err
			}
			_, err = Exec(ctx, `insert into test_tx values ('outer')`, nil)
			if err != nil {
				return err
			}

			return TX(ctx, func(ctx context.Context) error {
				_, err := Exec(ctx, `insert into test_tx values ('inner')`, nil)
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
		Exec(ctx, `create table test_tx2 (c varchar)`, nil)
		err := TX(ctx, func(ctx context.Context) error {
			_, err := Exec(ctx, `insert into test_tx2 values ('outer')`, nil)
			if err != nil {
				return err
			}

			return TX(ctx, func(ctx context.Context) error {
				Exec(ctx, `insert into test_tx2 values ('inner')`, nil)
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

		Exec(ctx, `create table test_tx3 (c varchar)`, nil)

		err := TX(ctx, func(ctx context.Context) error {
			_, err := Exec(ctx, `insert into test_tx3 values ('outer')`, nil)
			if err != nil {
				return err
			}

			err = TX(ctx, func(ctx context.Context) error {
				Exec(ctx, `insert into test_tx3 values ('inner')`, nil)
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
