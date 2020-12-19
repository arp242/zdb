package zdb

import (
	"io/fs"
	"testing"

	"zgo.at/zstd/ztest"
)

// Diff two strings, ignoring whitespace at the start of a line.
//
// This is useful in tests in combination with zdb.Dump():
//
//     got := DumpString(ctx, `select * from factions`)
//     want := `
//         faction_id  name
//         1           Peacekeepers
//         2           Moya`
//     if d := Diff(got, want); d != "" {
//        t.Error(d)
//     }
//
// It normalizes the leading whitespace in want, making "does my database match
// with what's expected?" fairly easy to test.
func Diff(out, want string) string {
	return ztest.Diff(out, want, ztest.DiffNormalizeWhitespace)
}

// TestQueries tests queries in the db/query directory.
//
// for every .sql file you can create a _test.sql file, similar to how Go's
// testing works; the following special comments are recognized:
//
//    -- params     Parameters for the query.
//    -- want       Expected result.
//
// Everything before the first special comment is run as a "setup". The
// "-- params" and "-- want" comments can be repeated for multiple tests.
//
// Example:
//
//    db/query/select-sites.sql:
//       select * from sites where site_id = :site and created_at > :start
//
//    db/query/select-sites_test.sql
//      insert into sites () values (...)
//
//      -- params
//      site_id:    1
//      created_at: 2020-01-01
//
//      -- want
//      1
//
//      -- params
//
//      -- want
func TestQueries(t *testing.T, files fs.FS) {
	t.Helper()
}

type mustDB struct {
	db DB
	t  *testing.T
}

/*
// NewMustDB returns a new database that will call t.Fatal() if a query returns
// an error.
//
// t is optional, and will panic() if not given.
func NewMustDB(db DB, t *testing.T) DB {
	return mustDB{db: db, t: t}
}

func (m mustDB) must(err error) error {
	if err == nil {
		return nil
	}
	if m.t != nil {
		m.t.Helper()
		m.t.Fatal(err)
	}

	panic(err)
}

func (m mustDB) ExecContext(ctx context.Context, query string, params ...interface{}) (sql.Result, error) {
	r, err := m.db.ExecContext(ctx, query, params...)
	return r, m.must(err)
}
func (m mustDB) GetContext(ctx context.Context, dest interface{}, query string, params ...interface{}) error {
	return m.must(m.db.GetContext(ctx, dest, query, params...))
}
func (m mustDB) SelectContext(ctx context.Context, dest interface{}, query string, params ...interface{}) error {
	return m.must(m.db.SelectContext(ctx, dest, query, params...))
}
func (m mustDB) QueryxContext(ctx context.Context, query string, params ...interface{}) (*sqlx.Rows, error) {
	r, err := m.db.QueryxContext(ctx, query, params...)
	return r, m.must(err)
}
func (m mustDB) Unwrap() DB                 { return m.db }
func (m mustDB) Close() error               { return m.db.Close() }
func (m mustDB) Rebind(query string) string { return m.db.Rebind(query) }
func (m mustDB) DriverName() string         { return m.db.DriverName() }
func (m mustDB) BindNamed(query string, arg interface{}) (newquery string, params []interface{}, err error) {
	return m.db.BindNamed(query, arg)
}
*/
