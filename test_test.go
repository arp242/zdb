package zdb_test

import (
	"context"
	"testing"

	"zgo.at/zdb"
)

func TestDump(t *testing.T) {
	zdb.RunTest(t, func(t *testing.T, ctx context.Context) {
		err := zdb.Exec(ctx, `create table tbl (
			v   varchar   not null,
			i   int not   null,
			t   timestamp not null,
			n   varchar   null
		);
		insert into tbl values
			('hello',    42, '2006-01-02 15:04:05', 'v'),
			('<zxc>,"',   0,  '2020-01-01 12:00:00', null);
		`)
		if err != nil {
			t.Fatal(err)
		}

		{
			got := zdb.DumpString(ctx, `select * from tbl`)
			want := `
				v        i   t                    n
				hello    42  2006-01-02 15:04:05  v
				<zxc>,"  0   2020-01-01 12:00:00  NULL`
			if d := zdb.Diff(got, want); d != "" {
				t.Error(d)
			}
		}

		{
			got := zdb.DumpString(ctx, `select * from tbl`, zdb.DumpVertical)
			want := `
				v   hello
				i   42
				t   2006-01-02 15:04:05
				n   v

				v   <zxc>,"
				i   0
				t   2020-01-01 12:00:00
				n   NULL`
			if d := zdb.Diff(got, want); d != "" {
				t.Error(d)
			}
		}
		{
			got := zdb.DumpString(ctx, `select * from tbl`, zdb.DumpCSV)
			want := `
				v,i,t,n
				hello,42,2006-01-02 15:04:05,v
				"<zxc>,""",0,2020-01-01 12:00:00,NULL`
			if d := zdb.Diff(got, want); d != "" {
				t.Error(d)
			}
		}
		{
			got := zdb.DumpString(ctx, `select * from tbl`, zdb.DumpJSON)
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
					"v": "\u003czxc\u003e,\""
				}
			]`
			if d := zdb.Diff(got, want); d != "" {
				t.Error(d)
			}
		}
		{
			got := zdb.DumpString(ctx, `select * from tbl`, zdb.DumpHTML)
			want := `
				<table><thead><tr>
				  <th>v</th>
				  <th>i</th>
				  <th>t</th>
				  <th>n</th>
				</tr></thead><tbody>
				<tr>
				  <td>hello</td>
				  <td>42</td>
				  <td>2006-01-02 15:04:05</td>
				  <td>v</td>
				</tr>
				<tr>
				  <td>&lt;zxc&gt;,&#34;</td>
				  <td>0</td>
				  <td>2020-01-01 12:00:00</td>
				  <td>NULL</td>
				</tr>
				</tbody></table>`
			if d := zdb.Diff(got, want); d != "" {
				t.Error(d)
			}
		}
	})
}
