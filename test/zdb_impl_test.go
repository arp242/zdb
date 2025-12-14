package zdb_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"zgo.at/zdb"
	"zgo.at/zdb/drivers"
	"zgo.at/zdb/test/testdata"
	"zgo.at/zstd/ztest"
)

func TestInfo(t *testing.T) {
	zdb.RunTest(t, func(t *testing.T, ctx context.Context) {
		db := zdb.MustGetDB(ctx)

		v, err := db.Info(ctx)
		if err != nil {
			t.Fatal(err)
		}
		t.Log(v)

		for _, tt := range [][]zdb.ServerVersion{
			{"3", "4"},
			{"3.35.0", "4"},
			{"3.35.0", "4.1.0"},
			{"3.35.0", "3.35.1"},
		} {
			have, want := tt[0], tt[1]
			if have.AtLeast(want) {
				t.Errorf("is true: %s.AtLeast(%s)", have, want)
			}
		}

		for _, tt := range [][]zdb.ServerVersion{
			{"4.0.0", "4"},
			{"4.1.0", "4"},
			{"4.1", "4"},
			{"4.0.1", "4"},
		} {
			have, want := tt[0], tt[1]
			if !have.AtLeast(want) {
				t.Errorf("is false: %s.AtLeast(%s)", have, want)
			}
		}
	})
}

// TODO: hard to test as it requires using a "fake stderr". Ideally we'd allow
// passing a io.Writer to dump to via zdb.Select(..., zdb.DumpWriter(..)) or
// something.
//
// func BufStderr(t *testing.T) *bytes.Buffer {
// 	buf := new(bytes.Buffer)
// 	stderr = buf
// 	t.Cleanup(func() { stderr = os.Stderr })
// 	return buf
// }
//
// func TestPrepareDump(t *testing.T) {
// 	zdb.RunTest(t, func(t *testing.T, ctx context.Context) {
// 		err := zdb.Exec(ctx, `create table tbl (col1 text, col2 int);`)
// 		if err != nil {
// 			t.Fatal(err)
// 		}
//
// 		// Show just query.
// 		func() {
// 			buf := zdb.BufStderr(t)
//
// 			err = zdb.Exec(ctx, `insert into tbl values (:val, 1), {{:val2 (:val2, 2)}}`, map[string]any{
// 				"val":  "hello",
// 				"val2": "world",
// 			}, zdb.DumpQuery)
// 			if err != nil {
// 				t.Fatal(err)
// 			}
//
// 			zdb.Dump(ctx, buf, `select * from tbl`)
//
// 			out := buf.String()
// 			want := "insert into tbl values ('hello', 1), ('world', 2);\n\ncol1   col2\nhello  1\nworld  2\n\n"
// 			if out != want {
// 				t.Errorf("wrong query\nout:  %q\nwant: %q", out, want)
// 			}
// 		}()
//
// 		// Show query and output.
// 		func() {
// 			buf := zdb.BufStderr(t)
//
// 			err = zdb.Exec(ctx, `select * from tbl where col1 = :val`, map[string]any{
// 				"val": "hello",
// 			}, zdb.DumpResult)
// 			if err != nil {
// 				t.Fatal(err)
// 			}
//
// 			out := buf.String()
// 			want := "col1   col2\nhello  1\n\n"
// 			if out != want {
// 				t.Errorf("wrong query\nout:  %q\nwant: %q", out, want)
// 			}
// 		}()
//
// 		// Show explain
// 		func() {
// 			buf := zdb.BufStderr(t)
//
// 			err = zdb.Exec(ctx, `select * from tbl where col1 = :val`, map[string]any{
// 				"val": "hello",
// 			}, zdb.DumpResult, zdb.DumpExplain)
// 			if err != nil {
// 				t.Fatal(err)
// 			}
//
// 			out := buf.String()
// 			var want string
// 			switch zdb.SQLDialect(ctx) {
// 			case zdb.DialectSQLite:
// 				want = `
// 					[1mEXPLAIN[0m:
// 					  SCAN tbl
// 					  Time: 0.016 ms
// 					[1mRESULT[0m:
// 					  col1   col2
// 					  hello  1`
// 			case zdb.DialectPostgreSQL:
// 				want = `
// 					[1mEXPLAIN[0m:
// 					  Seq Scan on tbl  (cost=0.00..25.88 rows=6 width=36) (actual time=0.005..0.015 rows=1 loops=1)
// 						Filter: (col1 = 'hello'::text)
// 						Rows Removed by Filter: 1
// 					  Planning Time: 0.123 ms
// 					  Execution Time: 0.646 ms
// 					[1mRESULT[0m:
// 					  col1   col2
// 					  hello  1`
// 			case zdb.DialectMariaDB:
// 				want = `
// 					[1mEXPLAIN[0m:
// 					id  select_type  table  type  possible_keys  key   key_len  ref   rows  Extra
// 					1   SIMPLE       tbl    ALL   NULL           NULL  NULL     NULL  2     Using where
// 					[1mRESULT[0m:
// 					col1   col2
// 					hello  1
// 				`
// 			}
//
// 			out, want = prep(ctx, out, want)
//
// 			if d := ztest.Diff(out, want, ztest.DiffNormalizeWhitespace); d != "" {
// 				t.Error(d)
// 			}
// 		}()
// 	})
// }

func prep(ctx context.Context, got, want string) (string, string) {
	re := []string{`([0-9]+.[0-9]+) ms`, `log_test\.go:(\d\d)`}
	if zdb.SQLDialect(ctx) == zdb.DialectPostgreSQL {
		re = append(re, `(?:cost|time)=([0-9.]+)\.\.([0-9.]+) `)
	}

	got = ztest.Replace(got, re...)
	want = ztest.Replace(want, re...)
	return got, want

}

func TestInsertID(t *testing.T) {
	zdb.RunTest(t, func(t *testing.T, ctx context.Context) {
		tbl := `create table test (col_id integer primary key autoincrement, v varchar)`
		if zdb.SQLDialect(ctx) == zdb.DialectPostgreSQL {
			tbl = `create table test (col_id serial primary key, v varchar)`
		}
		if zdb.SQLDialect(ctx) == zdb.DialectMariaDB {
			tbl = `create table test (col_id integer auto_increment, v varchar(255), primary key(col_id))`
		}
		err := zdb.Exec(ctx, tbl)
		if err != nil {
			t.Fatal(err)
		}

		{ // One row
			id, err := zdb.InsertID[int64](ctx, `col_id`, `insert into test (v) values (:val)`, zdb.P{"val": "aa"})
			if err != nil {
				t.Error(err)
			}
			if id != 1 {
				t.Errorf("id is %d, not 1", id)
			}
		}

		{ // Multiple rows
			id, err := zdb.InsertID[int32](ctx, `col_id`, `insert into test (v) values (:val), ('bb')`, zdb.P{"val": "aa"})
			if err != nil {
				t.Error(err)
			}
			if id != 3 {
				t.Errorf("id is %d, not 3\n%s", id, zdb.DumpString(ctx, `select * from test`))
			}
		}

		{
			id, err := zdb.InsertID[int](ctx, `col_id`, `insert into test (v) values (?), (?)`,
				"X", "Y")
			if err != nil {
				t.Error(err)
			}
			if id != 5 {
				t.Errorf("id is %d, not 5\n%s", id, zdb.DumpString(ctx, `select * from test`))
			}
		}

		{ // Invalid SQL
			id, err := zdb.InsertID[int](ctx, `col_id`, `insert into test (no_such_col) values ($1)`)
			if err == nil {
				t.Error("err is nil")
			}
			if id != 0 {
				t.Errorf("id is not 0: %d", id)
			}
		}

		out := "\n" + zdb.DumpString(ctx, `select * from test`)
		want := `
col_id  v
1       aa
2       aa
3       bb
4       X
5       Y
`
		if out != want {
			t.Errorf("\nwant: %v\ngot:  %v", want, out)
		}
	})
}

func TestQuery(t *testing.T) {
	zdb.RunTest(t, func(t *testing.T, ctx context.Context) {
		err := zdb.Exec(ctx, `
			create table tbl (
				s  text,
				i  int,
				t  timestamp,
				n  int null
			);
		`)
		if err != nil {
			t.Fatal(err)
		}
		err = zdb.Exec(ctx, `insert into tbl values
			('Hello', 42,  '2020-06-18', null),
			('Hello', 42,  '2020-06-18', null),
			('Hello', 42,  '2020-06-18', null),
			('Hello', 42,  '2020-06-18', null);
		`)
		if err != nil {
			t.Fatal(err)
		}

		rows, err := zdb.Query(ctx, `select * from tbl`)
		if err != nil {
			t.Fatal(err)
		}

		i := 0
		for rows.Next() {
			switch i {
			case 0:
				var (
					s  string
					i  int
					ti time.Time
					n  *int
				)
				err := rows.Scan(&s, &i, &ti, &n)
				if err != nil {
					t.Fatal(err)
				}
				//fmt.Println("scan", s, i, ti, n)
			case 1:
				var r map[string]any
				err := rows.Scan(&r)
				if err != nil {
					t.Fatal(err)
				}
				//fmt.Println("map", r)
			case 2:
				var r []any
				err := rows.Scan(&r)
				if err != nil {
					t.Fatal(err)
				}
				//fmt.Println("slice", r)
			case 3:
				var r struct {
					S string
					I int
					T time.Time
					N *int
				}
				err := rows.Scan(&r)
				if err != nil {
					t.Fatal(err)
				}
				//fmt.Println("struct", r)
			}
			i++
		}
	})
}

func TestLoad(t *testing.T) {
	zdb.RunTest(t, func(t *testing.T, ctx context.Context) {
		db := zdb.MustGetDB(ctx)

		{
			got, _, err := zdb.Load(db, "select-1")
			if err != nil {
				t.Fatal(err)
			}
			want := "/* select-1 */\nselect * from t where col like :find\n"
			if got != want {
				t.Errorf("\ngot:  %q\nwant: %q", got, want)
			}
		}

		{
			for _, n := range []string{"comment", "comment.sql"} {
				got, _, err := zdb.Load(db, n)
				if err != nil {
					t.Fatal(err)
				}
				want := "/* comment */\nselect 1\n\nfrom x;  -- xx\n"
				if got != want {
					t.Errorf("\ngot:  %q\nwant: %q", got, want)
				}
			}
		}
	}, drivers.TestOptions{Files: testdata.Files})
}

func TestBegin(t *testing.T) {
	zdb.RunTest(t, func(t *testing.T, ctx context.Context) {
		txctx, tx, err := zdb.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}

		err = tx.Rollback()
		if err != nil {
			t.Fatal(err)
		}

		t.Run("nested", func(t *testing.T) {
			txctx2, tx2, err := zdb.Begin(txctx)
			if err != zdb.ErrTransactionStarted {
				t.Fatal(err)
			}
			if tx2 != tx {
				t.Error("tx2 != tx")
			}
			if txctx2 != txctx {
				t.Error("txctx2 != txctx")
			}
		})
	})
}

func TestTX(t *testing.T) {
	zdb.RunTest(t, func(t *testing.T, ctx context.Context) {
		err := zdb.TX(ctx, func(ctx context.Context) error {
			// _, ok := zdb.MustGetDB(ctx).(*zdb.E_zTX)
			// if !ok {
			// 	t.Errorf("not a tx: %T", zdb.MustGetDB(ctx))
			// }

			return nil
		})
		if err != nil {
			t.Fatal(err)
		}

		t.Run("nested", func(t *testing.T) {
			err := zdb.TX(ctx, func(ctx context.Context) error {
				err := zdb.Exec(ctx, `create table test_tx (c text)`)
				if err != nil {
					return err
				}
				err = zdb.Exec(ctx, `insert into test_tx values ('outer')`)
				if err != nil {
					return err
				}

				return zdb.TX(ctx, func(ctx context.Context) error {
					err := zdb.Exec(ctx, `insert into test_tx values ('inner')`)
					return err
				})
			})
			if err != nil {
				t.Fatal(err)
			}

			got := zdb.DumpString(ctx, `select * from test_tx`)
			want := "c\nouter\ninner\n"
			if got != want {
				t.Errorf("\ngot:  %q\nwant: %q", got, want)
			}
		})

		t.Run("nested_inner_error", func(t *testing.T) {
			zdb.Exec(ctx, `create table test_tx2 (c text)`)
			err := zdb.TX(ctx, func(ctx context.Context) error {
				err := zdb.Exec(ctx, `insert into test_tx2 values ('outer')`)
				if err != nil {
					return err
				}

				return zdb.TX(ctx, func(ctx context.Context) error {
					zdb.Exec(ctx, `insert into test_tx2 values ('inner')`)
					return errors.New("oh noes")
				})
			})
			if err == nil {
				t.Fatal("err is nil")
			}

			have := zdb.DumpString(ctx, `select * from test_tx2`)
			want := "c\n"
			if have != want {
				t.Errorf("\nhave: %q\nwant: %q", have, want)
			}
		})

		t.Run("nested_outer_error", func(t *testing.T) {
			zdb.Exec(ctx, `create table test_tx3 (c text)`)

			err := zdb.TX(ctx, func(ctx context.Context) error {
				err := zdb.Exec(ctx, `insert into test_tx3 values ('outer')`)
				if err != nil {
					return err
				}

				err = zdb.TX(ctx, func(ctx context.Context) error {
					zdb.Exec(ctx, `insert into test_tx3 values ('inner')`)
					return nil
				})
				if err != nil {
					return err
				}

				return errors.New("oh noes")
			})
			if !ztest.ErrorContains(err, "oh noes") {
				t.Fatalf("wrong error: %v", err)
			}

			got := zdb.DumpString(ctx, `select * from test_tx3`)
			want := "c\n"
			if got != want {
				t.Errorf("\ngot:  %q\nwant: %q", got, want)
			}
		})
	})
}

func BenchmarkLoad(b *testing.B) {
	db, err := zdb.Connect(context.Background(), zdb.ConnectOptions{
		Connect: "sqlite3+:memory:",
		Create:  true,
		Files:   testdata.Files,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		_, _, _ = zdb.Load(db, "hit_list.GetTotalCount")
	}
}
