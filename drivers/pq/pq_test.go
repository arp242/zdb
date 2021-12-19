package pq

import (
	"fmt"
	"testing"

	"github.com/lib/pq"
)

func TestErrUnqiue(t *testing.T) {
	tests := []struct {
		err   error
		check func(error) bool
		want  bool
	}{
		{&pq.Error{}, driver{}.ErrUnique, false},
		{&pq.Error{Code: "123"}, driver{}.ErrUnique, false},
		{&pq.Error{Code: "23505"}, driver{}.ErrUnique, true},
		{fmt.Errorf("X: %w", &pq.Error{Code: "23505"}), driver{}.ErrUnique, true},
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
