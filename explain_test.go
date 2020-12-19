package zdb

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestExplain(t *testing.T) {
	ctx, clean := StartTest(t)
	defer clean()

	err := Exec(ctx, `create table x (i int)`)
	if err != nil {
		t.Fatal(err)
	}
	err = Exec(ctx, `insert into x values (1), (2), (3), (4), (5)`)
	if err != nil {
		t.Fatal(err)
	}

	buf := new(bytes.Buffer)
	ctx = WithDB(context.Background(), NewExplainDB(MustGetDB(ctx), buf, ""))

	var i int
	err = Get(ctx, &i, `select i from x where i<3`)
	if err != nil {
		t.Fatal(err)
	}

	var j int
	err = TX(ctx, func(ctx context.Context) error {
		return Get(ctx, &j, `select i from x where i<4`)
	})
	if err != nil {
		t.Fatal(err)
	}

	out := buf.String()
	want := "QUERY:\n\tselect i from x where i<3;\nEXPLAIN:\n\tSCAN TABLE x\n\tTime:"
	if PgSQL(ctx) {
		want = "QUERY:\n\tselect i from x where i<3;\nEXPLAIN:\n\tSeq Scan on x"
	}
	if !strings.HasPrefix(out, want) {
		t.Errorf("\nout:  %q\nwant: %q", out, want)
	}
	if !strings.Contains(out, "from x where i<4") { // Transaction
		t.Errorf("\nout:  %q\nwant: %q", out, want)
	}
}
