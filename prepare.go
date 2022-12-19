package zdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"time"

	"zgo.at/zdb/internal/sqlx"
	"zgo.at/zdb/internal/sqlx/reflectx"
	"zgo.at/zstd/zint"
	"zgo.at/zstd/zstring"
)

// "Prepare" a query; unlike e.g. sqlx this is always done, and always done
// automatically. This involves:
//
// 1. Extract the DumpArgs out of params.
// 2. Load from filesystem if the query starts with "load:".
// 3. Replace simple conditionals.
// 4. Bind named parameters ("sqlx.Named()").
// 5. Expand slices to multiple parameters ("sqlx.In()").
// 6. Rebind to use the correct placeholder ("sqlx.Rebind()").
//
// I don't see any good reason to *not* just automatically do it, except to save
// dozens to hundreds of ns per query; that said, we should be a bit smarter
// about this and we can cache some things; don't really need to tokenize the
// same query over and over again.
//
// First I'd like to move internal/sqlx to here.

func prepareImpl(ctx context.Context, db DB, query string, params ...any) (string, []any, error) {
	merged, named, dumpArgs, dumpOut, err := prepareParams(params)
	if err != nil {
		return "", nil, fmt.Errorf("zdb.Prepare: %w", err)
	}

	var isTpl bool
	if strings.HasPrefix(query, "load:") {
		query, isTpl, err = loadImpl(db, query[5:])
		if err != nil {
			return "", nil, fmt.Errorf("zdb.Prepare: %w", err)
		}
	}

	if isTpl {
		q, err := Template(db.SQLDialect(), query, merged)
		if err != nil {
			return "", nil, fmt.Errorf("zdb.Prepare: %w", err)
		}
		query = string(q)
	} else if named {
		query, err = replaceConditionals(query, merged)
		if err != nil {
			return "", nil, fmt.Errorf("zdb.Prepare: %w", err)
		}
	}

	qparams, _ := merged.([]any)
	if named {
		var err error
		query, qparams, err = sqlx.Named(query, merged)
		if err != nil {
			return "", nil, fmt.Errorf("zdb.Prepare: %w", err)
		}
	}

	// Sprintf SQL types and []int slices directly in the query. This solves two
	// cases:
	//
	// - IN (...) with a lot of parameters.
	//   TODO: this one should be optional; when we start using prepared
	//   statements/caching it will cause issues. Actually, maybe we can just
	//   remove it; the reason it's here is because GoatCounter does "path not
	//   in (.. list of filtered paths ..)", which can be quite large, but we
	//   can maybe solve that in some other way (the reason it works like that
	//   now is so that we only need to search the paths table once, instead of
	//   every time for every widget on the dashboard).
	//
	// - The SQL type is useful for generated SQL (i.e. "interval ...") that
	//   shouldn't be escaped.
	var rm []int
	for i := len(qparams) - 1; i >= 0; i-- {
		// These are aliases of []uint8 and []int32; there isn't really any way
		// to detect which is which AFAIK.
		if _, ok := qparams[i].([]byte); ok {
			continue
		}
		if _, ok := qparams[i].([]rune); ok {
			continue
		}

		if s, ok := qparams[i].(SQL); ok {
			query, err = replaceParam(query, i, s)
			if err != nil {
				return "", nil, fmt.Errorf("zdb.Prepare: %w", err)
			}
			rm = append(rm, i)
			continue
		}

		if s, ok := zint.ToIntSlice(qparams[i]); ok {
			query, err = replaceParam(query, i, SQL(zint.Join(s, ", ")))
			if err != nil {
				return "", nil, fmt.Errorf("zdb.Prepare: %w", err)
			}
			rm = append(rm, i)
		}
	}
	for _, i := range rm {
		qparams = append(qparams[:i], qparams[i+1:]...)
	}

	query, qparams, err = sqlx.In(query, qparams...)
	if err != nil {
		return "", nil, fmt.Errorf("zdb.Prepare: %w", err)
	}
	query = Unwrap(db).(interface{ rebind(string) string }).rebind(query)

	if dumpArgs > 0 {
		if dumpOut == nil {
			dumpOut = stderr
		}
		Dump(WithDB(ctx, db), dumpOut, query, append(qparams, dumpArgs)...)
	}

	return query, qparams, nil
}

// Prepare the paramers:
//
//   - Multiple named parameters are merged in a single map.
//   - DumpArgs are removed.
//   - Any io.Writer is removed.
//
// TODO: document that you can pass a io.Writer.
func prepareParams(params []any) (any, bool, DumpArg, io.Writer, error) {
	if len(params) == 0 {
		return nil, false, 0, nil, nil
	}

	var (
		dumpArgs    DumpArg
		dumpOut     io.Writer
		mergedPos   []any
		mergedNamed = make(map[string]any)
		named       bool
	)
	for _, param := range params {
		if param == nil {
			mergedPos = append(mergedPos, param)
			continue
		}
		if d, ok := param.(DumpArg); ok {
			dumpArgs |= d
			continue
		}
		// TODO: maybe restrict this a bit more? What if you're passing a type
		// which satisfies this interface?
		if d, ok := param.(io.Writer); ok {
			dumpOut = d
			continue
		}

		t := typeOfElem(param)

		// If this implements Value() then we never want to merge it with other
		// structs or maps.
		if t.Implements(reflect.TypeOf((*driver.Valuer)(nil)).Elem()) {
			mergedPos = append(mergedPos, param)
			continue
		}

		switch t.Kind() {
		default:
			mergedPos = append(mergedPos, param)

		case reflect.Map:
			var m map[string]any
			if !t.ConvertibleTo(reflect.TypeOf(m)) {
				mergedPos = append(mergedPos, param)
				continue
			}

			named = true
			m = reflect.ValueOf(param).Convert(reflect.TypeOf(m)).Interface().(map[string]any)
			for k, v := range m {
				if _, ok := mergedNamed[k]; ok {
					return nil, false, 0, nil, fmt.Errorf("parameter given more than once: %q", k)
				}
				mergedNamed[k] = v
			}

		case reflect.Struct:
			if !isNamed(t, param) {
				mergedPos = append(mergedPos, param)
				continue
			}

			named = true
			m := reflectx.NewMapper("db", sqlx.NameMapper).FieldMap(reflect.ValueOf(param))
			for k, v := range m {
				if _, ok := mergedNamed[k]; ok {
					return nil, false, 0, nil, fmt.Errorf("parameter given more than once: %q", k)
				}
				mergedNamed[k] = v.Interface()
			}
		}
	}

	if named {
		if len(mergedPos) > 0 {
			return nil, false, dumpArgs, dumpOut, errors.New("can't mix named and positional parameters")
		}
		return mergedNamed, named, dumpArgs, dumpOut, nil
	}
	return mergedPos, named, dumpArgs, dumpOut, nil
}

func typeOfElem(i any) reflect.Type {
	var t reflect.Type
	for t = reflect.TypeOf(i); t.Kind() == reflect.Ptr; {
		t = t.Elem()
	}
	return t
}

func isNamed(t reflect.Type, a any) bool {
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

// TODO: it would be nice if this would deal with whitespace a bit better.
//
// This has two spaces in the resulting SQL:
//
//	"where {{:x x = :x}} order by a" → "where  order by a"
//
// And with newlines it's even worse:
//
//	    where
//		     {{:x x = :x}}
//		   order by a
//	  →
//		   where
//
//		   order by a
func replaceConditionals(query string, params ...any) (string, error) {
	for _, p := range zstring.IndexPairs(query, "{{:", "}}") {
		s := p[0]
		e := p[1]

		name := query[s+3 : e]
		i := strings.IndexAny(name, " \t\n")
		if i == -1 {
			continue
		}
		name = name[:i]

		negate := strings.HasSuffix(name, "!")
		if negate {
			name = name[:len(name)-1]
		}

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
			if negate {
				include = !include
			}
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
func includeConditional(param any, name string) (include, has bool, err error) {
	v := reflect.ValueOf(param)
	for v = reflect.ValueOf(param); v.Kind() == reflect.Ptr; {
		v = v.Elem()
	}

	// Map
	var m map[string]any
	if v.Type().ConvertibleTo(reflect.TypeOf(m)) {
		m = v.Convert(reflect.TypeOf(m)).Interface().(map[string]any)
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
		c := reflectx.NewMapper("db", sqlx.NameMapper).FieldByName(v, name)
		if c.Type() == v.Type() { // FieldByName() returns original struct if it's not found.
			return false, false, nil
		}
		include, err := isTruthy(name, c.Interface())
		return include, true, err
	}

	return false, false, nil
}

func isTruthy(name string, cond any) (bool, error) {
	t := reflect.TypeOf(cond)
	v := reflect.ValueOf(cond)
	switch t.Kind() {
	case reflect.Bool:
		return v.Bool(), nil
	case reflect.String, reflect.Array, reflect.Slice:
		return v.Len() > 0, nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() > 0, nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return v.Uint() > 0, nil
	default:
		switch c := cond.(type) {
		case time.Time:
			return !c.IsZero(), nil
		}
		return false, fmt.Errorf("unsupported conditional type %T for %q", cond, name)
	}
}

func replaceParam(query string, n int, param SQL) (string, error) {
	i := zstring.IndexN(query, "?", uint(n+1))
	if i == -1 {
		return "", fmt.Errorf("not enough parameters")
	}
	return query[:i] + string(param) + query[i+1:], nil
}
