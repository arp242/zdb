package zdb_test

import (
	"context"
	"testing"

	"zgo.at/zdb"
	"zgo.at/zstd/ztest"
)

func testTable(ctx context.Context, t *testing.T) {
	t.Helper()
	q := `create table tbl (id serial, str text, "NoTag" text)`
	if zdb.SQLDialect(ctx) == zdb.DialectSQLite {
		q = `create table tbl (id integer primary key autoincrement, str text, "NoTag" text)`
	}
	err := zdb.Exec(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
}

type insertRow struct {
	ID             int    `db:"id,id"`
	Str            string `db:"str"`
	NoTag          string
	NoInsert       string `db:"other,noinsert"`
	Dash           string `db:"-"`
	unExport1      string
	defaultCalled  bool
	validateCalled bool
}

func (insertRow) Table() string                     { return "tbl" }
func (r *insertRow) Defaults(context.Context)       { r.defaultCalled = true }
func (r *insertRow) Validate(context.Context) error { r.validateCalled = true; return nil }

func TestInsert(t *testing.T) {
	zdb.RunTest(t, func(t *testing.T, ctx context.Context) {
		testTable(ctx, t)

		row := insertRow{0, "aaa", "bbb", "xxx", "xxx", "xxx", false, false}

		{ // Insert should work
			err := zdb.Insert(ctx, &row)
			if err != nil {
				t.Fatal(err)
			}
			if row.ID != 1 {
				t.Fatalf("row.ID is not 1: %d", row.ID)
			}
			want := "id  str  NoTag\n1   aaa  bbb\n"
			if have := zdb.DumpString(ctx, "select * from tbl"); have != want {
				t.Fatal(have)
			}
			if !row.defaultCalled {
				t.Fatal("Defaults() not called")
			}
			if !row.validateCalled {
				t.Fatal("Validate() not called")
			}
		}

		{ // Fail if ID not zero value
			err := zdb.Insert(ctx, &row)
			if !ztest.ErrorContains(err, "not zero value") {
				t.Fatal(err)
			}
			if row.ID != 1 {
				t.Fatalf("row.ID is not 1: %d", row.ID)
			}
			want := "id  str  NoTag\n1   aaa  bbb\n"
			if have := zdb.DumpString(ctx, "select * from tbl"); have != want {
				t.Fatal(have)
			}
		}

		{ // Fail on non-ptr
			err := zdb.Insert(ctx, row)
			if !ztest.ErrorContains(err, "not a pointer") {
				t.Fatal(err)
			}
			want := "id  str  NoTag\n1   aaa  bbb\n"
			if have := zdb.DumpString(ctx, "select * from tbl"); have != want {
				t.Fatal(have)
			}
		}
	})
}

type updateRow struct {
	ID             int    `db:"id,id"`
	Str            string `db:"str"`
	NoTag          string
	NoInsert       string `db:"other,noinsert"`
	Dash           string `db:"-"`
	unExport1      string
	defaultCalled  bool
	validateCalled bool
}

func (updateRow) Table() string                     { return "tbl" }
func (r *updateRow) Defaults(context.Context)       { r.defaultCalled = true }
func (r *updateRow) Validate(context.Context) error { r.validateCalled = true; return nil }

type updateReadOnlyRow struct {
	ID    int    `db:"id,id"`
	Str   string `db:"str,readonly"`
	NoTag string
}

func (updateReadOnlyRow) Table() string { return "tbl" }

func TestUpdate(t *testing.T) {
	zdb.RunTest(t, func(t *testing.T, ctx context.Context) {
		testTable(ctx, t)

		{ // ID is zero value
			row := updateRow{0, "aaa", "bbb", "xxx", "xxx", "xxx", false, false}
			err := zdb.Update(ctx, &row, "str")
			if !ztest.ErrorContains(err, "zero value") {
				t.Fatal(err)
			}
		}

		{ // Fail on non-ptr
			row := updateRow{0, "aaa", "bbb", "xxx", "xxx", "xxx", false, false}
			err := zdb.Update(ctx, row, "str")
			if !ztest.ErrorContains(err, "not a pointer") {
				t.Fatal(err)
			}
		}

		row := updateRow{0, "aaa", "bbb", "xxx", "xxx", "xxx", false, false}
		err := zdb.Insert(ctx, &row)
		if err != nil {
			t.Fatal(err)
		}
		row.defaultCalled, row.validateCalled = false, false

		{ // No columns
			err = zdb.Update(ctx, &row)
			if !ztest.ErrorContains(err, "no columns") {
				t.Fatal(err)
			}
		}

		{ // One column
			row.Str = "str changed"
			err = zdb.Update(ctx, &row, "str")
			if err != nil {
				t.Fatal(err)
			}
			want := "id  str          NoTag\n1   str changed  bbb\n"
			if have := zdb.DumpString(ctx, "select * from tbl"); have != want {
				t.Fatal("\n" + have)
			}
			if !row.defaultCalled {
				t.Fatal("Defaults() not called")
			}
			if !row.validateCalled {
				t.Fatal("Validate() not called")
			}
		}

		{ // Two columns
			row.Str = "str changed2"
			row.NoTag = "notag changed"
			err = zdb.Update(ctx, &row, "str", "NoTag")
			if err != nil {
				t.Fatal(err)
			}
			want := "id  str           NoTag\n1   str changed2  notag changed\n"
			if have := zdb.DumpString(ctx, "select * from tbl"); have != want {
				t.Fatal("\n" + have)
			}
		}

		{ // UpdateAll
			row.Str = "all1"
			row.NoTag = "all2"
			err = zdb.Update(ctx, &row, zdb.UpdateAll)
			if err != nil {
				t.Fatal(err)
			}
			want := "id  str   NoTag\n1   all1  all2\n"
			if have := zdb.DumpString(ctx, "select * from tbl"); have != want {
				t.Fatal("\n" + have)
			}
		}

		{ // Readonly
			rowRo := updateReadOnlyRow{ID: row.ID, Str: "ro1", NoTag: "ro2"}
			err = zdb.Update(ctx, &rowRo, zdb.UpdateAll)
			if err != nil {
				t.Fatal(err)
			}
			want := "id  str   NoTag\n1   all1  ro2\n"
			if have := zdb.DumpString(ctx, "select * from tbl"); have != want {
				t.Fatal("\n" + have)
			}
		}

		{ // Updating a column that has ,noinsert
			err = zdb.Update(ctx, &row, "other")
			if !ztest.ErrorContains(err, `column "other" has ,noinsert`) {
				t.Fatal(err)
			}
			want := "id  str   NoTag\n1   all1  ro2\n"
			if have := zdb.DumpString(ctx, "select * from tbl"); have != want {
				t.Fatal("\n" + have)
			}
		}
	})
}
