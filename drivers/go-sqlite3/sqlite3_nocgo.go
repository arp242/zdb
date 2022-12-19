//go:build !cgo

package sqlite3

import (
	"context"
	"database/sql"
	"errors"
)

// Accessing the error requires pulling in cgo;
//
// Patch to fix it was met with some "ThiS iS DoINg IT WrOnG I AM vERy SmARt"
// wankery: https://github.com/mattn/go-sqlite3/pull/899 🤷
func (driver) ErrUnique(err error) bool { return false }

func (driver) Connect(ctx context.Context, connect string, create bool) (*sql.DB, bool, error) {
	return nil, false, errors.New("go-sqlite3: not available: compiled with CGO_ENABLED=0")
}
