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
	BindNamed(string, interface{}) (string, []interface{}, error)
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

// Queryer is an interface used by Get and Select
type Queryer interface {
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryxContext(ctx context.Context, query string, args ...interface{}) (*Rows, error)
	QueryRowxContext(ctx context.Context, query string, args ...interface{}) *Row
}

// ColScanner is an interface used by MapScan and SliceScan
type ColScanner interface {
	Columns() ([]string, error)
	Scan(dest ...interface{}) error
	Err() error
}
