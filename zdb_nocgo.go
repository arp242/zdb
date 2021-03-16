// +build !cgo

package zdb

import (
	"errors"

	"github.com/lib/pq"
)

// ErrUnique reports if this error reports a UNIQUE constraint violation.
//
// This is the non-cgo version which works only for PostgreSQL.
func ErrUnique(err error) bool {
	var pqErr *pq.Error
	if errors.As(err, &pqErr) && pqErr.Code == "23505" {
		return true
	}

	// TODO: MySQL
	return false
}
