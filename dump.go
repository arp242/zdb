package zdb

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"regexp"
	"strings"
	"text/tabwriter"
	"time"

	"zgo.at/zstd/zbyte"
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
// You can add some special sentinel values in the args to control the output
// (they're not sent as parameters to the DB):
//
//   DumpVertical   Show vertical output instead of horizontal columns.
//   DumpQuery      Show the query with placeholders substituted.
//   DumpExplain    Show the results of EXPLAIN (or EXPLAIN ANALYZE for PostgreSQL).
func Dump(ctx context.Context, out io.Writer, query string, args ...interface{}) {
	var showQuery, vertical, explain bool
	argsb := args[:0]
	for _, a := range args {
		b, ok := a.(DumpArg)
		if !ok {
			argsb = append(argsb, a)
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
	args = argsb

	rows, err := Rows(ctx, query, args...)
	if err != nil {
		panic(err)
	}
	cols, err := rows.Columns()
	if err != nil {
		panic(err)
	}

	if showQuery {
		fmt.Fprintln(out, "Query:", ApplyPlaceholders(query, args...))
	}

	t := tabwriter.NewWriter(out, 4, 4, 2, ' ', 0)
	if vertical {
		for rows.Next() {
			row, err := rows.SliceScan()
			if err != nil {
				panic(err)
			}
			for i, c := range row {
				t.Write([]byte(fmt.Sprintf("%s\t%v\n", cols[i], formatArg(c, false))))
			}
			t.Write([]byte("\n"))
		}
	} else {
		t.Write([]byte(strings.Join(cols, "\t") + "\n"))
		for rows.Next() {
			row, err := rows.SliceScan()
			if err != nil {
				panic(err)
			}
			for i, c := range row {
				t.Write([]byte(fmt.Sprintf("%v", formatArg(c, false))))
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
			Dump(ctx, out, "explain analyze "+query, args...)
		} else {
			fmt.Fprintln(out, "\nEXPLAIN:")
			Dump(ctx, out, "explain query plan "+query, args...)
		}
	}
}

// ApplyPlaceholders replaces parameter placeholders in query with the values.
//
// This is ONLY for printf-debugging, and NOT for actual usage. Security was NOT
// a consideration when writing this. Parameters in SQL are sent separately over
// the write and are not interpolated, so it's very different.
//
// This supports ? placeholders and $1 placeholders *in order* ($\d is simply
// replace with ?).
func ApplyPlaceholders(query string, args ...interface{}) string {
	query = regexp.MustCompile(`\$\d`).ReplaceAllString(query, "?")
	for _, a := range args {
		query = strings.Replace(query, "?", formatArg(a, true), 1)
	}
	query = deIndent(query)
	if !strings.HasSuffix(query, ";") {
		return query + ";"
	}
	return query
}

// ListTables lists all tables
func ListTables(ctx context.Context) ([]string, error) {
	query := `select name from sqlite_master where type='table' order by name`
	if PgSQL(ctx) {
		query = `select c.relname as name
			from pg_catalog.pg_class c
			left join pg_catalog.pg_namespace n on n.oid = c.relnamespace
			where
				c.relkind = 'r' and
				n.nspname <> 'pg_catalog' and
				n.nspname <> 'information_schema' and
				n.nspname !~ '^pg_toast' and
				pg_catalog.pg_table_is_visible(c.oid)
			order by name`
	}

	var tables []string
	err := Select(ctx, &tables, query)
	if err != nil {
		return nil, fmt.Errorf("zdb.ListTables: %w", err)
	}
	return tables, nil
}

func formatArg(a interface{}, quoted bool) string {
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
		return formatArg(aa.Format(Date), quoted)
	case int, int64:
		return fmt.Sprintf("%v", aa)
	case []byte:
		if zbyte.Binary(aa) {
			return fmt.Sprintf("%x", aa)
		} else {
			return formatArg(string(aa), quoted)
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

// DumpString is like Dump(), but returns the result as a string.
func DumpString(ctx context.Context, query string, args ...interface{}) string {
	b := new(bytes.Buffer)
	Dump(ctx, b, query, args...)
	return b.String()
}
