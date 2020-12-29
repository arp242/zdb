package zdb

import (
	"context"
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

/*
TODO: I don't really like how you need to use []interface for positional
parameters; especially if you have just one or two parameters position is often
easier, and with inserts it's pretty ugly:

   p.ID, err = zdb.InsertID(ctx, "user_agent_id", `insert into user_agents
       (ua, isbot, browser_id, system_id) values (?, ?, ?, ?)`,
       []interface{}{shortUA, p.Isbot, p.BrowserID, p.SystemID})

We can't accept ...interface{} because of those DumpArgs and that shizzle,
but maybe just change it anyway even though we lose some type safe type.

This would also allow passing two struct or maps for named arguments, which is
actually kind of nice in some cases

	alwaysAdd := zdb.A{"site": site.ID}
	zdb.Select(ctx, "...", alwaysAdd, zdb.A{"just_this": "..."})

Note: we must be careful to not use between something like time.Time for named
parameters.

Actually, that entire (?, ?, ?, ?) is something that annoyed me for a while;
let's see if we can improve on that, too. For example ?all? or whatnot which
expands to (? * num_of_args).
*/

// Query creates a new query.
//
// Named paramters (:name) are used if arg is a map or struct; positional
// parameters (? or $1) are used if it's a []inerface{}.
//
// Everything between {{:name ..}} is parsed as a conditional; for example
// {{:foo query}} will only be added if "foo" from arg is true or not a zero
// type. Conditionals only work with named parameters.
//
// Additional DumpArgs can be added to dump the results of the query to stderr
// for testing and debugging:
//
//    DumpQuery      Show the query
//    DumpExplain    Show query plain (WILL RUN QUERY TWICE!)
//    DumpResult     Show the query result (WILL RUN QUERY TWICE!)
//    DumpVertical   Show results in vertical format.
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

	// sqlx doesn't deal well with this, but if there are no parameters then it
	// makes little sense having to pass a map or struct.
	if arg == nil {
		arg = struct{}{}
	}

	var (
		args []interface{}
		err  error
	)
	if s, ok := arg.([]interface{}); ok { // Slice: use positional paramters.
		args = s
	} else { // Named parameters
		query, args, err = sqlx.Named(query, arg)
		if err != nil {
			return "", nil, fmt.Errorf("zdb.Query: %w", err)
		}
	}

	query, args, err = sqlx.In(query, args...)
	if err != nil {
		return "", nil, fmt.Errorf("zdb.Query: %w", err)
	}
	query = MustGetDB(ctx).Rebind(query)

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

// Select one or more rows; dest needs to be a pointer to a slice.
//
// Returns nil if there are no rows.
//
// This uses Query(), and all the documentation from there applies here too.
func Select(ctx context.Context, dest interface{}, query string, arg interface{}, dump ...DumpArg) error {
	query, args, err := Query(ctx, query, arg, dump...)
	if err != nil {
		return err
	}
	return MustGetDB(ctx).SelectContext(ctx, dest, query, args...)
}

// Get one row, returning sql.ErrNoRows if there are no rows.
//
// This uses Query(), and all the documentation from there applies here too.
func Get(ctx context.Context, dest interface{}, query string, arg interface{}, dump ...DumpArg) error {
	query, args, err := Query(ctx, query, arg, dump...)
	if err != nil {
		return err
	}
	return MustGetDB(ctx).GetContext(ctx, dest, query, args...)
}

// Exec executes a query without returning the result.
//
// This uses Query(), and all the documentation from there applies here too.
func Exec(ctx context.Context, query string, arg interface{}, dump ...DumpArg) error {
	query, args, err := Query(ctx, query, arg, dump...)
	if err != nil {
		return err
	}
	_, err = MustGetDB(ctx).ExecContext(ctx, query, args...)
	return err
}

// NumRows executes a query and returns the number of affected rows.
//
// This uses Query(), and all the documentation from there applies here too.
func NumRows(ctx context.Context, query string, arg interface{}, dump ...DumpArg) (int64, error) {
	query, args, err := Query(ctx, query, arg, dump...)
	if err != nil {
		return 0, err
	}
	r, err := MustGetDB(ctx).ExecContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	return r.RowsAffected()
}

// InsertID runs a INSERT query and returns the ID column idColumn.
//
// If multiple rows are inserted it will return the ID of the last inserted row.
// This works for both PostgreSQL and SQLite.
//
// This uses Query(), and all the documentation from there applies here too.
func InsertID(ctx context.Context, idColumn, query string, arg interface{}, dump ...DumpArg) (int64, error) {
	query, args, err := Query(ctx, query, arg, dump...)
	if err != nil {
		return 0, err
	}

	if PgSQL(ctx) {
		var id []int64
		err := MustGetDB(ctx).SelectContext(ctx, &id, query+" returning "+idColumn, args...)
		if err != nil {
			return 0, err
		}
		return id[len(id)-1], nil
	}

	r, err := MustGetDB(ctx).ExecContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	return r.LastInsertId()
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
