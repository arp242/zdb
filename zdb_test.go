package zdb

import (
	"context"
	"testing"

	"github.com/jmoiron/sqlx"
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

// startTest a new database test.
func startTest(t *testing.T) (context.Context, func()) {
	t.Helper()
	db, err := sqlx.Connect("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	return With(context.Background(), db), func() { db.Close() }
}
