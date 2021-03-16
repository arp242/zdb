package zdb

import (
	"testing"
)

func TestDump(t *testing.T) {
	ctx := StartTest(t)

	err := Exec(ctx, `create table tbl (
		v   varchar   not null,
		i   int not   null,
		t   timestamp not null,
		n   varchar   null
	);
	insert into tbl values
		('hello', 42, '2006-01-02 15:04:05', 'v'),
		('zxc',   0,  '2020-01-01 12:00:00', null);
	`)
	if err != nil {
		t.Fatal(err)
	}

	{
		got := DumpString(ctx, `select * from tbl`)
		want := `
			v      i   t                    n
			hello  42  2006-01-02 15:04:05  v
			zxc    0   2020-01-01 12:00:00  NULL`
		if d := Diff(got, want); d != "" {
			t.Error(d)
		}
	}

	{
		got := DumpString(ctx, `select * from tbl`, DumpVertical)
		want := `
			v   hello
			i   42
			t   2006-01-02 15:04:05
			n   v

			v   zxc
			i   0
			t   2020-01-01 12:00:00
			n   NULL`
		if d := Diff(got, want); d != "" {
			t.Error(d)
		}
	}
	{
		got := DumpString(ctx, `select * from tbl`, DumpCSV)
		want := `
			v,i,t,n
			hello,42,2006-01-02 15:04:05,v
			zxc,0,2020-01-01 12:00:00,NULL`
		if d := Diff(got, want); d != "" {
			t.Error(d)
		}
	}
	{
		got := DumpString(ctx, `select * from tbl`, DumpJSON)
		want := `[
			{
				"i": 42,
				"n": "v",
				"t": "2006-01-02T15:04:05Z",
				"v": "hello"
			},
			{
				"i": 0,
				"n": null,
				"t": "2020-01-01T12:00:00Z",
				"v": "zxc"
			}
		]`
		if d := Diff(got, want); d != "" {
			t.Error(d)
		}
	}
}
