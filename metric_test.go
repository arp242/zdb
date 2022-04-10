package zdb_test

import (
	"context"
	"testing"

	"zgo.at/zdb"
)

var _ zdb.E_dbImpl = &zdb.E_metricDB{}

func TestMetricsDB(t *testing.T) {
	zdb.RunTest(t, func(t *testing.T, ctx context.Context) {
		record := zdb.NewMetricsMemory(0)
		ctx = zdb.WithDB(context.Background(), zdb.NewMetricsDB(zdb.Unwrap(zdb.MustGetDB(ctx)), record))

		var i int
		if err := zdb.Get(ctx, &i, `select 1`); err != nil || i != 1 {
			t.Fatalf("err: %s; i: %d", err, i)
		}
		if err := zdb.Get(ctx, &i, `select 2`); err != nil || i != 2 {
			t.Fatalf("err: %s; i: %d", err, i)
		}

		err := zdb.TX(ctx, func(ctx context.Context) error {
			if err := zdb.Get(ctx, &i, `select 1`); err != nil || i != 1 {
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
	})
}
