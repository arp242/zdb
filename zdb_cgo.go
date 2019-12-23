// +build cgo

package zdb

import (
	"github.com/lib/pq"
	"github.com/mattn/go-sqlite3"
)

func UniqueErr(err error) bool {
	if sqlErr, ok := err.(sqlite3.Error); ok && sqlErr.ExtendedCode == sqlite3.ErrConstraintUnique {
		return true
	}
	if pqErr, ok := err.(pq.Error); ok && pqErr.Code == "23505" {
		return true
	}
	return false
}
