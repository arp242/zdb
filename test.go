package zdb

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/fs"
	"regexp"
	"strings"
	"testing"
	"text/tabwriter"
	"time"

	"zgo.at/zstd/zbyte"
	"zgo.at/zstd/ztest"
)

type DumpArg int

const (
	_ DumpArg = iota
	DumpVertical
	DumpQuery
	DumpExplain
	DumpResult
)

// Dump the results of a query to a writer in an aligned table. This is a
// convenience function intended mostly for testing/debugging.
//
// Combined with ztest.Diff() it can be an easy way to test the database state.
//
// You can add some special sentinel values in the params to control the output
// (they're not sent as parameters to the DB):
//
//   DumpVertical   Show vertical output instead of horizontal columns.
//   DumpQuery      Show the query with placeholders substituted.
//   DumpExplain    Show the results of EXPLAIN (or EXPLAIN ANALYZE for PostgreSQL).
func Dump(ctx context.Context, out io.Writer, query string, params ...interface{}) {
	var showQuery, vertical, explain bool
	paramsb := params[:0]
	for _, p := range params {
		b, ok := p.(DumpArg)
		if !ok {
			paramsb = append(paramsb, p)
			continue
		}
		// TODO: formatting could be better; also merge with explainDB
		// TODO: DumpQuery -> DumpQueryOnly and make "DumpQuery" do query+explain
		switch b {
		case DumpQuery:
			showQuery = true
		case DumpVertical:
			vertical = true
		case DumpExplain:
			explain = true
		}
	}
	params = paramsb

	rows, err := Query(ctx, query, params...)
	if err != nil {
		panic(err)
	}
	cols, err := rows.Columns()
	if err != nil {
		panic(err)
	}

	if showQuery {
		fmt.Fprintln(out, "Query:", ApplyParams(query, params...))
	}

	t := tabwriter.NewWriter(out, 4, 4, 2, ' ', 0)
	if vertical {
		for rows.Next() {
			var row []interface{}
			err := rows.Scan(&row)
			if err != nil {
				panic(err)
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
				panic(err)
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
	t.Flush()

	if explain {
		if PgSQL(ctx) {
			fmt.Fprintln(out, "")
			Dump(ctx, out, "explain analyze "+query, params...)
		} else {
			fmt.Fprintln(out, "\nEXPLAIN:")
			Dump(ctx, out, "explain query plan "+query, params...)
		}
	}
}

// DumpString is like Dump(), but returns the result as a string.
func DumpString(ctx context.Context, query string, params ...interface{}) string {
	b := new(bytes.Buffer)
	Dump(ctx, b, query, params...)
	return b.String()
}

// ApplyParams replaces parameter placeholders in query with the values.
//
// This is ONLY for printf-debugging, and NOT for actual usage. Security was NOT
// a consideration when writing this. Parameters in SQL are sent separately over
// the write and are not interpolated, so it's very different.
//
// This supports ? placeholders and $1 placeholders *in order* ($\d is simply
// replace with ?).
func ApplyParams(query string, params ...interface{}) string {
	query = regexp.MustCompile(`\$\d`).ReplaceAllString(query, "?")
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
}
