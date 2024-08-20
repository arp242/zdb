//go:build testpgx

package pgx

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"zgo.at/zdb"
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

func TestSearchPath(t *testing.T) {
	var d driver
	ctx := d.StartTest(t, nil)

	var (
		wg sync.WaitGroup
		ch = make(chan string, 20)
	)
	wg.Add(20)
	for i := 0; i < 20; i++ {
		go func(i int) {
			defer wg.Done()
			var s string
			err := zdb.Get(ctx, &s, `show search_path`)
			if err != nil {
				t.Error(err)
			}
			err = zdb.Exec(ctx, fmt.Sprintf(`create table test_%02d (c int)`, i))
			if err != nil {
				t.Error(err)
			}
			ch <- s
		}(i)
	}
	wg.Wait()

	prev := <-ch
	for i := 0; i < 19; i++ {
		cur := <-ch
		if cur != prev {
			t.Fatalf("\nschema changed from:\n\t%s\nto:\n\t%s", prev, cur)
		}
		if strings.Contains(cur, "public") || strings.Contains(cur, "$user") {
			t.Fatalf("\nschema seens to be default schema:\n\t%s", cur)
		}
		prev = cur
	}

	//zdb.Dump(ctx, os.Stdout, `select schemaname, relname from pg_stat_user_tables order by relname`)
}
