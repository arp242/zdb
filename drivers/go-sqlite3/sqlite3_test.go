//go:build cgo

package sqlite3

import (
	"fmt"
	"testing"

	"github.com/mattn/go-sqlite3"
)

func TestErrUnqiue(t *testing.T) {
	tests := []struct {
		err   error
		check func(error) bool
		want  bool
	}{
		{sqlite3.Error{}, driver{}.ErrUnique, false},
		{sqlite3.Error{ExtendedCode: 1 + sqlite3.ErrConstraintUnique}, driver{}.ErrUnique, false},
		{sqlite3.Error{ExtendedCode: sqlite3.ErrConstraintUnique}, driver{}.ErrUnique, true},
		{fmt.Errorf("X: %w", sqlite3.Error{ExtendedCode: sqlite3.ErrConstraintUnique}), driver{}.ErrUnique, true},
	}

	for i, tt := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			out := tt.check(tt.err)
			if out != tt.want {
				t.Errorf("out: %t; want: %t", out, tt.want)
			}
		})
	}
}
