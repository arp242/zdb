package sqlx

import (
	"context"
	"database/sql"
)

// Ext is a union interface which can bind, query, and exec, used by NamedQuery
// and NamedExec.
type Ext interface {
	binder
	Queryer
	Execer
}

// ColScanner is an interface used by MapScan and SliceScan
type ColScanner interface {
	Columns() ([]string, error)
	Scan(dest ...interface{}) error
	Err() error
}

// Queryer is an interface used by Get and Select
type Queryer interface {
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryxContext(ctx context.Context, query string, args ...interface{}) (*Rows, error)
	QueryRowxContext(ctx context.Context, query string, args ...interface{}) *Row
}

// Preparer is an interface used by Preparex.
type Preparer interface {
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
}

// Execer is an interface used by LoadFile.
type Execer interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

// A union interface of contextPreparer and binder, required to be able to
// prepare named statements with context (as the bindtype must be determined).
type namedPreparer interface {
	Preparer
	binder
}

// Binder is an interface for something which can bind queries (Tx, DB)
type binder interface {
	DriverName() string
	Rebind(string) string
	BindNamed(string, interface{}) (string, []interface{}, error)
}
