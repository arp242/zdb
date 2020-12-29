package zdb

import (
	"context"
	"errors"
	"fmt"

	"github.com/jmoiron/sqlx"
)

// ErrTransactionStarted is returned when a transaction is already started.
var ErrTransactionStarted = errors.New("transaction already started")

// Begin a new transaction.
//
// The returned context is a copy of the original with the DB replaced with a
// transaction. The same transaction is also returned directly.
//
// Nested transactions return the original transaction together with
// ErrTransactionStarted (which is not a fatal error).
func Begin(ctx context.Context) (context.Context, *sqlx.Tx, error) {
	db := Unwrap(MustGet(ctx))

	// Could use savepoints, but that's probably more confusing than anything
	// else: almost all of the time you want the outermost transaction to be
	// completed in full or not at all. If you really want savepoints then you
	// can do it manually.
	if tx, ok := db.(*sqlx.Tx); ok {
		return ctx, tx, ErrTransactionStarted
	}

	tx, err := db.(*sqlx.DB).BeginTxx(ctx, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("zdb.Begin: %w", err)
	}
	return With(ctx, tx), tx, nil
}

// TX runs the given function in a transaction.
//
// The context passed to the callback has the DB replaced with a transaction.
// The transaction is committed if the fn returns nil, or will be rolled back if
// it's not.
//
// Multiple TX() calls can be nested, but they all run the same transaction and
// are comitted only if the outermost transaction returns true.
//
// This is just a more convenient wrapper for Begin().
func TX(ctx context.Context, fn func(context.Context, DB) error) error {
	txctx, tx, err := Begin(ctx)
	if err == ErrTransactionStarted {
		err := fn(txctx, tx)
		if err != nil {
			return fmt.Errorf("zdb.TX fn: %w", err)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("zdb.TX: %w", err)
	}

	defer tx.Rollback()

	err = fn(txctx, tx)
	if err != nil {
		return fmt.Errorf("zdb.TX fn: %w", err)
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("zdb.TX commit: %w", err)
	}
	return nil
}
