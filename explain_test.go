package zdb

import (
	"bytes"
	"context"
	"testing"
)

func TestExplain(t *testing.T) {
	db, err := Connect(ConnectOptions{
		Connect: "sqlite://:memory:",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.MustExec(`create table x (i int)`)
	db.MustExec(`insert into x values (1), (2), (3), (4), (5)`)

	buf := new(bytes.Buffer)
	dbe := NewExplainDB(db, buf, "")
	ctx := With(context.Background(), dbe)

	var i int
	err = dbe.GetContext(ctx, &i, `select i from x where i<3`)
	if err != nil {
		t.Fatal(err)
	}

	out := buf.String()
	want := "QUERY:\n\tselect i from x where i<3;\nEXPLAIN:\n\tSCAN TABLE x\n\tTime: 0s\n\n"
	if out != want {
		t.Errorf("\nout:  %q\nwant: %q", out, want)
	}
}
