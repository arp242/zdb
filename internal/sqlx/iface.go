package sqlx

import (
	"context"
	"database/sql"
)

// ExtContext is a union interface which can bind, query, and exec, with Context
// used by NamedQueryContext and NamedExecContext.
type ExtContext interface {
	binder
	QueryerContext
	ExecerContext
}

// ColScanner is an interface used by MapScan and SliceScan
type ColScanner interface {
	Columns() ([]string, error)
	Scan(dest ...interface{}) error
	Err() error
}

// QueryerContext is an interface used by GetContext and SelectContext
type QueryerContext interface {
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryxContext(ctx context.Context, query string, args ...interface{}) (*Rows, error)
	QueryRowxContext(ctx context.Context, query string, args ...interface{}) *Row
}

// PreparerContext is an interface used by PreparexContext.
type PreparerContext interface {
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
}

// ExecerContext is an interface used by LoadFileContext
type ExecerContext interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

// A union interface of contextPreparer and binder, required to be able to
// prepare named statements with context (as the bindtype must be determined).
type namedPreparerContext interface {
	PreparerContext
	binder
}

// Binder is an interface for something which can bind queries (Tx, DB)
type binder interface {
	DriverName() string
	Rebind(string) string
	BindNamed(string, interface{}) (string, []interface{}, error)
}
