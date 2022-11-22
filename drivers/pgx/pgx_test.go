package pgx

import (
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestErrUnqiue(t *testing.T) {
	tests := []struct {
		err   error
		check func(error) bool
		want  bool
	}{
		{&pgconn.PgError{}, driver{}.ErrUnique, false},
		{&pgconn.PgError{Code: "123"}, driver{}.ErrUnique, false},
		{&pgconn.PgError{Code: "23505"}, driver{}.ErrUnique, true},
		{fmt.Errorf("X: %w", &pgconn.PgError{Code: "23505"}), driver{}.ErrUnique, true},
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
