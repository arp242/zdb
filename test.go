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
