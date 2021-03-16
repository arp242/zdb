package zdb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"regexp"
	"strings"
	"testing"
	"text/tabwriter"
	"time"

	"zgo.at/zstd/zbyte"
	"zgo.at/zstd/zdebug"
	"zgo.at/zstd/ztest"
	"zgo.at/zstd/ztime"
)

type DumpArg int

const (
	DumpAll       DumpArg = -1       // Dump all we can.
	DumpLocation  DumpArg = 0b000001 // Show location of Dump call.
	DumpQuery     DumpArg = 0b000010 // Show the query with placeholders substituted.
	DumpExplain   DumpArg = 0b000100 // Show the results of EXPLAIN (or EXPLAIN ANALYZE for PostgreSQL).
	DumpResult    DumpArg = 0b001000 // Show the query result.
	DumpVertical  DumpArg = 0b010000 // Show vertical output instead of horizontal columns.
	dumpFromLogDB DumpArg = 0b100000
)

// Dump the results of a query to a writer in an aligned table. This is a
// convenience function intended mostly for testing/debugging.
//
// Combined with ztest.Diff() it can be an easy way to test the database state.
//
// You can add some special sentinel values in the params to control the output
// (they're not sent as parameters to the DB):
//
//   DumpAll
//   DumpLocation
//   DumpQuery      Show the query with placeholders substituted.
//   DumpExplain    Show the results of EXPLAIN (or EXPLAIN ANALYZE for PostgreSQL).
//   DumpResult     Show the query result (
//   DumpVertical   Show vertical output instead of horizontal columns.
func Dump(ctx context.Context, out io.Writer, query string, params ...interface{}) {
	var showLoc, showQuery, showExplain, showResult, showVertical, fromLogDB bool
	paramsb := params[:0]
	for _, p := range params {
		b, ok := p.(DumpArg)
		if !ok {
			paramsb = append(paramsb, p)
			continue
		}

		if b == DumpAll {
			showLoc, showQuery, showExplain, showResult = true, true, true, true
			continue
		}
		if b&DumpLocation != 0 {
			showLoc = true
		}
		if b&DumpQuery != 0 {
			showQuery = true
		}
		if b&DumpExplain != 0 {
			showExplain = true
		}
		if b&DumpResult != 0 {
			showResult = true
		}
		if b&DumpVertical != 0 {
			showVertical = true
		}
		if b&dumpFromLogDB != 0 {
			fromLogDB = true
		}
	}
	params = paramsb

	if !showLoc && !showQuery && !showExplain && !showResult {
		showResult = true
	}
	var nsections int
	// if showLoc {
	// 	nsections++
	// }
	if showQuery {
		nsections++
	}
	if showExplain {
		nsections++
	}
	if showResult {
		nsections++
	}

	var (
		bold    = func(s string) string { return "\x1b[1m" + s + "\x1b[0m" }
		indent  = func(s string) string { return "  " + strings.ReplaceAll(strings.TrimSpace(s), "\n", "\n  ") }
		section = func(name, s string) {
			r := strings.TrimRight(s, "\n")
			if nsections > 1 {
				r = bold(name) + ":\n" + indent(s)
			}
			if showLoc {
				r = indent(r)
			}
			fmt.Fprintln(out, r)
		}
	)

	if showLoc {
		if fromLogDB {
			fmt.Fprintf(out, "zdb.LogDB: %s\n", bold(zdebug.Loc(5)))
		} else {
			fmt.Fprintf(out, "zdb.Dump: %s\n", bold(zdebug.Loc(4)))
		}
	}

	if showQuery {
		section("QUERY", ApplyParams(query, params...))
	}

	if showExplain {
		var (
			explain []string
			err     error
		)
		switch Driver(ctx) {
		default:
			err = errors.New("zdb.LogDB: unsupported driver for LogExplain " + MustGetDB(ctx).DriverName())
		case DriverPostgreSQL:
			err = Select(ctx, &explain, `explain analyze `+query, params...)
		case DriverMySQL:
			// TODO
		case DriverSQLite:
			var sqe []struct {
				ID, Parent, Notused int
				Detail              string
			}
			t := ztime.Takes(func() {
				err = Select(ctx, &sqe, `explain query plan `+query, params...)
			})
			if len(sqe) > 0 {
				explain = make([]string, len(sqe)+1)
				for i := range sqe {
					explain[i] = sqe[i].Detail
				}
				explain[len(sqe)] = "Time: " + ztime.DurationAs(t.Round(time.Microsecond), time.Millisecond) + " ms"
			}
		}
		if err != nil {
			section("EXPLAIN", err.Error())
		} else {
			section("EXPLAIN", strings.Join(explain, "\n"))
		}
	}

	if showResult {
		buf := new(bytes.Buffer)
		err := func() error {
			rows, err := Query(ctx, query, params...)
			if err != nil {
				return err
			}
			cols, err := rows.Columns()
			if err != nil {
				return err
			}

			t := tabwriter.NewWriter(buf, 4, 4, 2, ' ', 0)
			if showVertical {
				for rows.Next() {
					var row []interface{}
					err := rows.Scan(&row)
					if err != nil {
						return err
					}
					for i, c := range row {
						t.Write([]byte(fmt.Sprintf("%s\t%v\n", cols[i], formatParam(c, false))))
					}
					t.Write([]byte("\n"))
				}
			} else {
				t.Write([]byte(strings.Join(cols, "\t") + "\n"))
				for rows.Next() {
					var row []interface{}
					err := rows.Scan(&row)
					if err != nil {
						return err
					}
					for i, c := range row {
						t.Write([]byte(fmt.Sprintf("%v", formatParam(c, false))))
						if i < len(row)-1 {
							t.Write([]byte("\t"))
						}
					}
					t.Write([]byte("\n"))
				}
			}
			return t.Flush()
		}()
		if err != nil {
			section("RESULT", err.Error())
		} else {
			section("RESULT", buf.String())
		}
	}

	fmt.Fprintln(out)
}

// DumpString is like Dump(), but returns the result as a string.
func DumpString(ctx context.Context, query string, params ...interface{}) string {
	b := new(bytes.Buffer)
	Dump(ctx, b, query, params...)
	return strings.TrimSpace(b.String()) + "\n"
}

// ApplyParams replaces parameter placeholders in query with the values.
//
// This is ONLY for printf-debugging, and NOT for actual usage. Security was NOT
// a consideration when writing this. Parameters in SQL are sent separately over
// the write and are not interpolated, so it's very different.
//
// This supports ? placeholders and $1 placeholders *in order* ($\d+ is simply
// replace with ?).
func ApplyParams(query string, params ...interface{}) string {
	query = regexp.MustCompile(`\$\d+`).ReplaceAllString(query, "?")
	for _, p := range params {
		query = strings.Replace(query, "?", formatParam(p, true), 1)
	}
	query = deIndent(query)
	if !strings.HasSuffix(query, ";") {
		return query + ";"
	}
	return query
}

func formatParam(a interface{}, quoted bool) string {
	if a == nil {
		return "NULL"
	}
	switch aa := a.(type) {
	case *string:
		if aa == nil {
			return "NULL"
		}
		a = *aa
	case *int:
		if aa == nil {
			return "NULL"
		}
		a = *aa
	case *int64:
		if aa == nil {
			return "NULL"
		}
		a = *aa
	case *time.Time:
		if aa == nil {
			return "NULL"
		}
		a = *aa
	}

	switch aa := a.(type) {
	case time.Time:
		// TODO: be a bit smarter about the precision, e.g. a date or time
		// column doesn't need the full date.
		return formatParam(aa.Format("2006-01-02 15:04:05"), quoted)
	case int, int64:
		return fmt.Sprintf("%v", aa)
	case []byte:
		if zbyte.Binary(aa) {
			return fmt.Sprintf("%x", aa)
		} else {
			return formatParam(string(aa), quoted)
		}
	case string:
		if quoted {
			return fmt.Sprintf("'%v'", strings.ReplaceAll(aa, "'", "''"))
		}
		return aa
	default:
		if quoted {
			return fmt.Sprintf("'%v'", aa)
		}
		return fmt.Sprintf("%v", aa)
	}
}

func deIndent(in string) string {
	// Ignore comment at the start for indentation as I often write:
	//     SelectContext(`/* Comment for PostgreSQL logs */
	//             select [..]
	//     `)
	in = strings.TrimLeft(in, "\n\t ")
	comment := 0
	if strings.HasPrefix(in, "/*") {
		comment = strings.Index(in, "*/")
	}

	indent := 0
	for _, c := range strings.TrimLeft(in[comment+2:], "\n") {
		if c != '\t' {
			break
		}
		indent++
	}

	r := ""
	for _, line := range strings.Split(in, "\n") {
		r += strings.Replace(line, "\t", "", indent) + "\n"
	}

	return strings.TrimSpace(r)
}

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

	// TODO
}
