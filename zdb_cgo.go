// +build cgo

package zdb

import (
	"errors"

	"github.com/lib/pq"
	"github.com/mattn/go-sqlite3"
)

// ErrUnique reports if this error reports a UNIQUE constraint violation.
//
// This is the cgo version which works for PostgreSQL and SQLite.
func ErrUnique(err error) bool {
	var sqlErr sqlite3.Error
	if errors.As(err, &sqlErr) && sqlErr.ExtendedCode == sqlite3.ErrConstraintUnique {
		return true
	}
	var pqErr *pq.Error
	if errors.As(err, &pqErr) && pqErr.Code == "23505" {
		return true
	}
	return false
}
