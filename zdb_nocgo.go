// +build !cgo

package zdb

import (
	"github.com/lib/pq"
)

func UniqueErr(err error) bool {
	if pqErr, ok := err.(pq.Error); ok && pqErr.Code == "23505" {
		return true
	}
	return false
}
