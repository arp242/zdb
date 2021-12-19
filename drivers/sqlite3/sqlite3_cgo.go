//go:build cgo
// +build cgo

package sqlite3

import (
	"errors"

	"github.com/mattn/go-sqlite3"
)

func (driver) ErrUnique(err error) bool {
	var sqlErr sqlite3.Error
	return errors.As(err, &sqlErr) && sqlErr.ExtendedCode == sqlite3.ErrConstraintUnique
}
