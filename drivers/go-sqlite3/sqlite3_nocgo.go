//go:build !cgo
// +build !cgo

package sqlite3

// Accessing the error requires pulling in cgo;
//
// Patch to fix it was met with some "ThiS iS DoINg IT WrOnG I AM vERy SmARt"
// wankery: https://github.com/mattn/go-sqlite3/pull/899 🤷
func (driver) ErrUnique(err error) bool { return false }
