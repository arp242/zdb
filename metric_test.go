package zdb

import (
	"context"
	"testing"
)

func TestMetricsDB(t *testing.T) {
	record := NewMetricsMemory(0)
	ctx := StartTest(t)
	ctx = WithDB(context.Background(), NewMetricsDB(Unwrap(MustGetDB(ctx)), record))

	var i int
	if err := Get(ctx, &i, `select 1`); err != nil || i != 1 {
		t.Fatalf("err: %s; i: %d", err, i)
	}
	if err := Get(ctx, &i, `select 2`); err != nil || i != 2 {
		t.Fatalf("err: %s; i: %d", err, i)
	}

	err := TX(ctx, func(ctx context.Context) error {
		if err := Get(ctx, &i, `select 1`); err != nil || i != 1 {
			t.Fatalf("err: %s; i: %d", err, i)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	q := record.Queries()
	if l := len(q); l != 2 {
		t.Fatalf("len(record.Queries()) == %d", l)
	}

	if q[0].Query != "select 1" {
		t.Errorf("q[0].Query = %q", q[0].Query)
	}
	if q[1].Query != "select 2" {
		t.Errorf("q[1].Query = %q", q[1].Query)
	}

	if l := q[0].Times.List(); len(l) != 2 || l[0] == 0 || l[1] == 0 {
		t.Errorf("q[0].Times = %s", l)
	}
	if l := q[1].Times.List(); len(l) != 1 || l[0] == 0 {
		t.Errorf("q[1].Times = %s", l)
	}
}
