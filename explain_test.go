package zdb

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/jmoiron/sqlx"
)

func TestExplain(t *testing.T) {
	ctx, clean := StartTest(t)
	defer clean()
	db := MustGet(ctx).(*sqlx.DB)

	db.MustExec(`create table x (i int)`)
	db.MustExec(`insert into x values (1), (2), (3), (4), (5)`)

	buf := new(bytes.Buffer)
	dbe := NewExplainDB(db, buf, "")
	ctx = With(context.Background(), dbe)

	var i int
	err := dbe.GetContext(ctx, &i, `select i from x where i<3`)
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
}
