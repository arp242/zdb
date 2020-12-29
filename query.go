package zdb

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"

	"github.com/jmoiron/sqlx"
	"github.com/jmoiron/sqlx/reflectx"
	"zgo.at/zstd/zstring"
)

var stderr io.Writer = os.Stderr

// Query creates a new query.
//
// Everything between {{:name ..}} is parsed as a conditional; for example
// {{:foo query}} will only be added if "foo" from arg is true or not a zero
// type.
//
// SQL parameters can be added as :name; sqlx's BindNamed() is used.
//
// Additional dumpArgs can be added to dump the results of the query to stderr
// for testing:
//
//    DumpQuery      Show the query
//    DumpExplain    Show query plain (WILL RUN QUERY TWICE!)
//    DumpResult     Show the query result (WILL RUN QUERY TWICE!)
//    DumpVertical   Show results in "vertical" format.
//
// Running the query twice for a select is usually safe (just slower), but
// running insert, update, or delete twice may cause problems.
func Query(ctx context.Context, query string, arg interface{}, dump ...DumpArg) (string, []interface{}, error) {
	for _, p := range zstring.IndexPairs(query, "{{:", "}}") {
		s := p[0]
		e := p[1]

		name := query[s+3 : e]
		i := strings.IndexAny(name, " \t\n")
		if i == -1 {
			continue
		}
		name = name[:i]

		// This is a bit inefficient, since it duplicates sqlx's NamedMapper
		// logic; still seems plenty fast enough though.
		ok, err := includeConditional(arg, name)
		if err != nil {
			return "", nil, fmt.Errorf("zdb.Query: %w", err)
		}
		if ok {
			query = query[:s] + query[s+i+4:]     // Everything except "{{:word"
			query = query[:e-i-4] + query[e-i-2:] // Everything except "}}"
		} else {
			query = query[:s] + query[e+2:]
		}
	}

	query, args, err := sqlx.Named(query, arg)
	if err != nil {
		return "", nil, fmt.Errorf("zdb.Query: %w", err)
	}
	query, args, err = sqlx.In(query, args...)
	if err != nil {
		return "", nil, fmt.Errorf("zdb.Query: %w", err)
	}
	query = MustGet(ctx).Rebind(query)

	if len(dump) > 0 {
		if len(dump) == 1 && dump[0] == DumpQuery {
			fmt.Fprintln(stderr, ApplyPlaceholders(query, args...))
		} else {
			di := make([]interface{}, len(dump))
			for i := range dump {
				di[i] = dump[i]
			}
			Dump(ctx, stderr, query, append(args, di...)...)
		}
	}

	return query, args, nil
}

// QuerySelect is like Query(), but will also run SelectContext() and scan in to
// desc.
func QuerySelect(ctx context.Context, dest interface{}, query string, arg interface{}, dump ...DumpArg) error {
	query, args, err := Query(ctx, query, arg, dump...)
	if err != nil {
		return err
	}
	return MustGet(ctx).SelectContext(ctx, dest, query, args...)
}

// QueryGet is like Query(), but will also run GetContext() and scan in to
// desc.
func QueryGet(ctx context.Context, dest interface{}, query string, arg interface{}, dump ...DumpArg) error {
	query, args, err := Query(ctx, query, arg, dump...)
	if err != nil {
		return err
	}
	return MustGet(ctx).GetContext(ctx, dest, query, args...)
}

// QueryExec is like Query(), but will also run ExecContext().
func QueryExec(ctx context.Context, query string, arg interface{}, dump ...DumpArg) (sql.Result, error) {
	query, args, err := Query(ctx, query, arg, dump...)
	if err != nil {
		return nil, err
	}
	return MustGet(ctx).ExecContext(ctx, query, args...)
}

func includeConditional(arg interface{}, name string) (bool, error) {
	var (
		cond interface{}
		m    map[string]interface{}
		v    = reflect.ValueOf(arg)
	)
	if v.Type().ConvertibleTo(reflect.TypeOf(m)) {
		m = v.Convert(reflect.TypeOf(m)).Interface().(map[string]interface{})
	}

	if m != nil {
		v, ok := m[name]
		if !ok {
			return false, fmt.Errorf("%q not in map", name)
		}
		cond = v // TODO: defer pointers?
	} else {
		for v = reflect.ValueOf(arg); v.Kind() == reflect.Ptr; {
			v = v.Elem()
		}
		c := reflectx.NewMapperFunc("db", sqlx.NameMapper).FieldByName(v, name)
		if c.Type() == v.Type() { // FieldByName() returns original struct if it's not found.
			return false, fmt.Errorf("%q not in struct", name)
		}
		cond = c.Interface()
	}

	switch c := cond.(type) {
	case bool:
		return c, nil
	case string:
		return len(c) > 0, nil
	case int:
		return c > 0, nil
	case int64:
		return c > 0, nil
	case []string:
		return len(c) > 0, nil
	case []int:
		return len(c) > 0, nil
	case []int64:
		return len(c) > 0, nil
	default:
		return false, fmt.Errorf("unsupported conditional type %T for %q", c, name)
	}
}
