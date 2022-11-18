package sqlx

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"regexp"
	"strconv"

	"zgo.at/zdb/internal/sqltoken"
	"zgo.at/zdb/internal/sqlx/reflectx"
)

var namedParseConfigs = func() []sqltoken.Config {
	configs := make([]sqltoken.Config, PlaceholderAt+1)

	pg := sqltoken.PostgreSQLConfig()
	pg.NoticeColonWord = true
	pg.ColonWordIncludesUnicode = true
	pg.NoticeDollarNumber = false
	configs[PlaceholderDollar] = pg

	ora := sqltoken.OracleConfig()
	ora.ColonWordIncludesUnicode = true
	configs[PlaceholderNamed] = ora

	ssvr := sqltoken.SQLServerConfig()
	ssvr.NoticeColonWord = true
	ssvr.ColonWordIncludesUnicode = true
	ssvr.NoticeAtWord = false
	configs[PlaceholderAt] = ssvr

	mysql := sqltoken.MySQLConfig()
	mysql.NoticeColonWord = true
	mysql.ColonWordIncludesUnicode = true
	mysql.NoticeQuestionMark = false
	configs[PlaceholderQuestion] = mysql
	configs[PlaceholderUnknown] = mysql

	return configs
}()

// Named takes a query using named parameters and an argument and returns a new
// query with a list of args that can be executed by a database.  The return
// value uses the `?` bindvar.
func Named(query string, arg any) (string, []any, error) {
	return bindNamedMapper(PlaceholderQuestion, query, arg, mapper())
}

// NamedQuery binds a named query and then runs Query on the result using the
// provided Ext (sqlx.Tx, sqlx.Db).
//
// It works with both structs and with map[string]any types.
func NamedQuery(ctx context.Context, e Ext, query string, arg any) (*Rows, error) {
	q, args, err := bindNamedMapper(Placeholder(e.DriverName()), query, arg, mapperFor(e))
	if err != nil {
		return nil, err
	}
	return e.QueryxContext(ctx, q, args...)
}

// NamedExec uses BindStruct to get a query executable by the driver and then
// runs Exec on the result.  Returns an error from the binding or the query
// execution itself.
func NamedExec(ctx context.Context, e Ext, query string, arg any) (sql.Result, error) {
	q, args, err := bindNamedMapper(Placeholder(e.DriverName()), query, arg, mapperFor(e))
	if err != nil {
		return nil, err
	}
	return e.ExecContext(ctx, q, args...)
}

func bindNamedMapper(style PlaceholderStyle, query string, arg any, m *reflectx.Mapper) (string, []any, error) {
	t := reflect.TypeOf(arg)
	k := t.Kind()
	switch {
	case k == reflect.Map && t.Key().Kind() == reflect.String:
		m, ok := convertMapStringInterface(arg)
		if !ok {
			return "", nil, fmt.Errorf("sqlx.bindNamedMapper: unsupported map type: %T", arg)
		}
		return bindMap(style, query, m)
	case k == reflect.Array || k == reflect.Slice:
		return bindArray(style, query, arg, m)
	default:
		return bindStruct(style, query, arg, m)
	}
}

// convertMapStringInterface attempts to convert v to map[string]any.
// Unlike v.(map[string]any), this function works on named types that
// are convertible to map[string]any as well.
func convertMapStringInterface(v any) (map[string]any, bool) {
	var m map[string]any
	mtype := reflect.TypeOf(m)
	t := reflect.TypeOf(v)
	if !t.ConvertibleTo(mtype) {
		return nil, false
	}
	return reflect.ValueOf(v).Convert(mtype).Interface().(map[string]any), true

}

// Bind map or struct.
func bindAnyArgs(names []string, arg any, m *reflectx.Mapper) ([]any, error) {
	if maparg, ok := convertMapStringInterface(arg); ok {
		return bindMapArgs(names, maparg)
	}
	return bindArgs(names, arg, m)
}

// Generate a list of interfaces from a given struct type, given a list of names
// to pull out of the struct.
func bindArgs(names []string, arg any, m *reflectx.Mapper) ([]any, error) {
	arglist := make([]any, 0, len(names))

	// grab the indirected value of arg
	v := reflect.ValueOf(arg)
	for v = reflect.ValueOf(arg); v.Kind() == reflect.Ptr; {
		v = v.Elem()
	}

	err := m.TraversalsByNameFunc(v.Type(), names, func(i int, t []int) error {
		if len(t) == 0 {
			return fmt.Errorf("could not find name %s in %#v", names[i], arg)
		}

		val := reflectx.FieldByIndexesReadOnly(v, t)
		arglist = append(arglist, val.Interface())

		return nil
	})

	return arglist, err
}

// like bindArgs, but for maps.
func bindMapArgs(names []string, arg map[string]any) ([]any, error) {
	arglist := make([]any, 0, len(names))

	for _, name := range names {
		val, ok := arg[name]
		if !ok {
			return arglist, fmt.Errorf("could not find name %s in %#v", name, arg)
		}
		arglist = append(arglist, val)
	}
	return arglist, nil
}

// bindStruct binds a named parameter query with fields from a struct argument.
// The rules for binding field names to parameter names follow the same
// conventions as for StructScan, including obeying the `db` struct tags.
func bindStruct(style PlaceholderStyle, query string, arg any, m *reflectx.Mapper) (string, []any, error) {
	bound, names, err := rebindNamed([]byte(query), style)
	if err != nil {
		return "", []any{}, err
	}

	arglist, err := bindAnyArgs(names, arg, m)
	if err != nil {
		return "", []any{}, err
	}

	return bound, arglist, nil
}

var valuesReg = regexp.MustCompile(`\)\s*(?i)VALUES\s*\(`)

func findMatchingClosingBracketIndex(s string) int {
	count := 0
	for i, ch := range s {
		if ch == '(' {
			count++
		}
		if ch == ')' {
			count--
			if count == 0 {
				return i
			}
		}
	}
	return 0
}

func fixBound(bound string, loop int) string {
	loc := valuesReg.FindStringIndex(bound)
	if len(loc) < 2 { // "VALUES (...)" not found
		return bound
	}

	openingBracketIndex := loc[1] - 1
	index := findMatchingClosingBracketIndex(bound[openingBracketIndex:])
	if index == 0 { // must have closing bracket
		return bound
	}
	closingBracketIndex := openingBracketIndex + index + 1

	var buffer bytes.Buffer
	buffer.WriteString(bound[0:closingBracketIndex])
	for i := 0; i < loop-1; i++ {
		buffer.WriteString(",")
		buffer.WriteString(bound[openingBracketIndex:closingBracketIndex])
	}
	buffer.WriteString(bound[closingBracketIndex:])
	return buffer.String()
}

// bindArray binds a named parameter query with fields from an array or slice of
// structs argument.
func bindArray(style PlaceholderStyle, query string, arg any, m *reflectx.Mapper) (string, []any, error) {
	// do the initial binding with PlaceholderQuestion; if style is not
	// question, we can rebind it at the end.
	bound, names, err := rebindNamed([]byte(query), PlaceholderQuestion)
	if err != nil {
		return "", []any{}, err
	}

	arrayValue := reflect.ValueOf(arg)
	arrayLen := arrayValue.Len()
	if arrayLen == 0 {
		return "", []any{}, fmt.Errorf("length of array is 0: %#v", arg)
	}

	arglist := make([]any, 0, len(names)*arrayLen)
	for i := 0; i < arrayLen; i++ {
		elemArglist, err := bindAnyArgs(names, arrayValue.Index(i).Interface(), m)
		if err != nil {
			return "", []any{}, err
		}
		arglist = append(arglist, elemArglist...)
	}
	if arrayLen > 1 {
		bound = fixBound(bound, arrayLen)
	}

	// adjust binding type if we weren't on question
	if style != PlaceholderQuestion {
		bound = Rebind(style, bound)
	}
	return bound, arglist, nil
}

// bindMap binds a named parameter query with a map of arguments.
func bindMap(style PlaceholderStyle, query string, args map[string]any) (string, []any, error) {
	bound, names, err := rebindNamed([]byte(query), style)
	if err != nil {
		return "", []any{}, err
	}

	arglist, err := bindMapArgs(names, args)
	return bound, arglist, err
}

// Rebind a named query to the placeholder style, returning the new query and a
// list of names.
func rebindNamed(qs []byte, style PlaceholderStyle) (string, []string, error) {
	var (
		names   = make([]string, 0, 8)
		rebound = make([]byte, 0, len(qs))
		tokens  = sqltoken.Tokenize(string(qs), namedParseConfigs[style])
	)
	for _, token := range tokens {
		if token.Type != sqltoken.ColonWord {
			rebound = append(rebound, ([]byte)(token.Text)...)
			continue
		}

		names = append(names, token.Text[1:])
		switch style {
		case PlaceholderNamed:
			rebound = append(rebound, token.Text...)
		case PlaceholderQuestion, PlaceholderUnknown:
			rebound = append(rebound, '?')
		case PlaceholderDollar:
			rebound = append(rebound, '$')
			rebound = strconv.AppendInt(rebound, int64(len(names)), 10)
		case PlaceholderAt:
			rebound = append(rebound, "@p"...)
			rebound = strconv.AppendInt(rebound, int64(len(names)), 10)
		}
	}
	return string(rebound), names, nil
}
