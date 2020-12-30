package zdb

// TODO: at this point we've abstracted sqlx to such a degree that I'm not
// entirely sure if it's worth keeping, not in the least because it's not *that*
// great of a package, bugs don't get fixed, and we re-implement quite a bit of
// things here.
//
// Maybe just stick in in internal/sqlx and maintain it from there.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/jmoiron/sqlx/reflectx"
	"zgo.at/zstd/zstring"
)

var stderr io.Writer = os.Stderr

// Prepare a query for sendoff to the database.
//
// Named parameters (:name) are used if params contains a map or struct;
// positional parameters (? or $1) are used if it's doesn't. You can add
// multiple structs or maps, but mixing named and positional parameters is not
// allowed.
//
// Everything between {{:name ..}} is parsed as a conditional; for example
// {{:foo query}} will only be added if "foo" from params is true or not a zero
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
func Prepare(ctx context.Context, query string, params ...interface{}) (string, []interface{}, error) {
	merged, named, dumpArgs, err := prepareParams(params)
	if err != nil {
		return "", nil, fmt.Errorf("zdb.Prepare: %w", err)
	}

	if named {
		query, err = replaceConditionals(query, merged)
		if err != nil {
			return "", nil, fmt.Errorf("zdb.Prepare: %w", err)
		}
	}

	qargs, _ := merged.([]interface{})
	if named {
		var err error
		query, qargs, err = sqlx.Named(query, merged)
		if err != nil {
			return "", nil, fmt.Errorf("zdb.Prepare: %w", err)
		}
	}

	query, qargs, err = sqlx.In(query, qargs...)
	if err != nil {
		return "", nil, fmt.Errorf("zdb.Prepare: %w", err)
	}
	query = MustGetDB(ctx).Rebind(query)

	if len(dumpArgs) > 0 {
		if len(dumpArgs) == 1 && dumpArgs[0] == DumpQuery {
			fmt.Fprintln(stderr, ApplyPlaceholders(query, qargs...))
		} else {
			di := make([]interface{}, len(dumpArgs))
			for i := range dumpArgs {
				di[i] = dumpArgs[i]
			}
			Dump(ctx, stderr, query, append(qargs, di...)...)
		}
	}

	return query, qargs, nil
}

// Select one or more rows; dest needs to be a pointer to a slice.
//
// Returns nil if there are no rows.
//
// This uses Prepare(), and all the documentation from there applies here too.
func Select(ctx context.Context, dest interface{}, query string, params ...interface{}) error {
	query, params, err := Prepare(ctx, query, params...)
	if err != nil {
		return err
	}
	return MustGetDB(ctx).SelectContext(ctx, dest, query, params...)
}

// Get one row, returning sql.ErrNoRows if there are no rows.
//
// This uses Prepare(), and all the documentation from there applies here too.
func Get(ctx context.Context, dest interface{}, query string, params ...interface{}) error {
	query, params, err := Prepare(ctx, query, params...)
	if err != nil {
		return err
	}
	return MustGetDB(ctx).GetContext(ctx, dest, query, params...)
}

// Exec executes a query without returning the result.
//
// This uses Prepare(), and all the documentation from there applies here too.
func Exec(ctx context.Context, query string, params ...interface{}) error {
	query, params, err := Prepare(ctx, query, params...)
	if err != nil {
		return err
	}
	_, err = MustGetDB(ctx).ExecContext(ctx, query, params...)
	return err
}

// NumRows executes a query and returns the number of affected rows.
//
// This uses Prepare(), and all the documentation from there applies here too.
func NumRows(ctx context.Context, query string, params ...interface{}) (int64, error) {
	query, params, err := Prepare(ctx, query, params...)
	if err != nil {
		return 0, err
	}
	r, err := MustGetDB(ctx).ExecContext(ctx, query, params...)
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
// This uses Prepare(), and all the documentation from there applies here too.
func InsertID(ctx context.Context, idColumn, query string, params ...interface{}) (int64, error) {
	query, params, err := Prepare(ctx, query, params...)
	if err != nil {
		return 0, err
	}

	if PgSQL(ctx) {
		var id []int64
		err := MustGetDB(ctx).SelectContext(ctx, &id, query+" returning "+idColumn, params...)
		if err != nil {
			return 0, err
		}
		return id[len(id)-1], nil
	}

	r, err := MustGetDB(ctx).ExecContext(ctx, query, params...)
	if err != nil {
		return 0, err
	}
	return r.LastInsertId()
}

// Rows queries the database without immediatly loading the result.
//
// This gives more flexibility over Select(), and won't load the entire result
// in memory and allows fetching the result one row at a time.
//
// TODO: what will this return on error?
//
// TODO: document a bit more, since the various Scan() methods in sqlx are kind
// if hard to distinguish.
//
// TODO: Actually, I don't care too much for sqlx.Rows. See if we can improve on
// that.
//
// This uses Prepare(), and all the documentation from there applies here too.
func Rows(ctx context.Context, query string, params ...interface{}) (*sqlx.Rows, error) {
	query, params, err := Prepare(ctx, query, params...)
	if err != nil {
		return nil, err
	}
	return MustGetDB(ctx).QueryxContext(ctx, query, params...)
}

// Support multiple named parameters by merging the lot in a map.
func prepareParams(params []interface{}) (interface{}, bool, []interface{}, error) {
	if len(params) == 0 {
		return nil, false, nil, nil
	}

	// No need to merge.
	if len(params) == 1 {
		if params[0] == nil {
			return nil, false, nil, nil
		}

		var (
			named = isNamed(typeOfElem(params[0]), params[0])
			a     = params[0]
		)
		if !named {
			a = []interface{}{params[0]}
		}
		return a, named, nil, nil
	}

	var (
		dumpArgs    []interface{}
		mergedPos   []interface{}
		mergedNamed = make(map[string]interface{})
		named       bool
	)
	for _, param := range params {
		if param == nil {
			continue
		}
		if d, ok := param.(DumpArg); ok {
			dumpArgs = append(dumpArgs, d)
			continue
		}

		t := typeOfElem(param)
		switch t.Kind() {
		default:
			mergedPos = append(mergedPos, param)

		case reflect.Map:
			named = true
			var m map[string]interface{}
			if !t.ConvertibleTo(reflect.TypeOf(m)) {
				return nil, false, dumpArgs, fmt.Errorf("unsupported map type: %T", param)
			}

			m = reflect.ValueOf(param).Convert(reflect.TypeOf(m)).Interface().(map[string]interface{})
			for k, v := range m {
				if _, ok := mergedNamed[k]; ok {
					return nil, false, nil, fmt.Errorf("parameter given more than once: %q", k)
				}
				mergedNamed[k] = v
			}

		case reflect.Struct:
			if !isNamed(t, param) {
				mergedPos = append(mergedPos, param)
				continue
			}

			named = true
			m := reflectx.NewMapperFunc("db", sqlx.NameMapper).FieldMap(reflect.ValueOf(param))
			for k, v := range m {
				if _, ok := mergedNamed[k]; ok {
					return nil, false, nil, fmt.Errorf("parameter given more than once: %q", k)
				}
				mergedNamed[k] = v.Interface()
			}
		}
	}

	if named {
		if len(mergedPos) > 0 {
			return nil, false, dumpArgs, errors.New("can't mix named and positional parameters")
		}
		return mergedNamed, named, dumpArgs, nil
	}
	return mergedPos, named, dumpArgs, nil
}

func typeOfElem(i interface{}) reflect.Type {
	//v := reflect.TypeOf(i)
	var t reflect.Type
	for t = reflect.TypeOf(i); t.Kind() == reflect.Ptr; {
		t = t.Elem()
	}
	return t
}

func isNamed(t reflect.Type, a interface{}) bool {
	_, ok := a.(time.Time)
	if ok {
		return false
	}
	_, ok = a.(*time.Time)
	if ok {
		return false
	}

	n := reflect.New(t)
	n.Elem().Set(reflect.ValueOf(a))
	_, ok = n.Interface().(sql.Scanner)
	if ok {
		return false
	}

	return t.Kind() == reflect.Struct || t.Kind() == reflect.Map
}

func replaceConditionals(query string, params ...interface{}) (string, error) {
	for _, p := range zstring.IndexPairs(query, "{{:", "}}") {
		s := p[0]
		e := p[1]

		name := query[s+3 : e]
		i := strings.IndexAny(name, " \t\n")
		if i == -1 {
			continue
		}
		name = name[:i]

		found := false
		for _, param := range params {
			// This is a bit inefficient, since it duplicates sqlx's NamedMapper
			// logic; still seems plenty fast enough though.
			include, has, err := includeConditional(param, name)
			if err != nil {
				return "", err
			}
			if !has {
				continue
			}
			found = true
			if include {
				query = query[:s] + query[s+i+4:]     // Everything except "{{:word"
				query = query[:e-i-4] + query[e-i-2:] // Everything except "}}"
			} else {
				query = query[:s] + query[e+2:]
			}
			if !found {
				return "", fmt.Errorf("found not find %q for conditional", name)
			}
		}
	}
	return query, nil
}

// TODO: we can simplify this a bit if we just always convert struct to map in
// prepareParams.
func includeConditional(param interface{}, name string) (include, has bool, err error) {
	v := reflect.ValueOf(param)
	for v = reflect.ValueOf(param); v.Kind() == reflect.Ptr; {
		v = v.Elem()
	}

	// Map
	var m map[string]interface{}
	if v.Type().ConvertibleTo(reflect.TypeOf(m)) {
		m = v.Convert(reflect.TypeOf(m)).Interface().(map[string]interface{})
	}
	if m != nil {
		v, ok := m[name]
		if !ok {
			return false, false, nil
		}
		include, err := isTruthy(name, v)
		return include, true, err
	}

	// Struct
	if v.Kind() == reflect.Struct {
		c := reflectx.NewMapperFunc("db", sqlx.NameMapper).FieldByName(v, name)
		if c.Type() == v.Type() { // FieldByName() returns original struct if it's not found.
			return false, false, nil
		}
		include, err := isTruthy(name, c.Interface())
		return include, true, err
	}

	return false, false, nil
}

func isTruthy(name string, cond interface{}) (bool, error) {
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
