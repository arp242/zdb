package sqlx

import (
	"context"
	"database/sql"
)

// Ext is a union interface which can bind, query, and exec, used by NamedQuery
// and NamedExec.
type Ext interface {
	Queryer

	DriverName() string
	Rebind(string) string
	BindNamed(string, any) (string, []any, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// Queryer is an interface used by Get and Select
type Queryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryxContext(ctx context.Context, query string, args ...any) (*Rows, error)
	QueryRowxContext(ctx context.Context, query string, args ...any) *Row
}

// ColScanner is an interface used by MapScan and SliceScan
type ColScanner interface {
	Columns() ([]string, error)
	Scan(dest ...any) error
	Err() error
}
