package sqlx

// Named Query Support
//
//  - BindMap                bind query bindvars to map/struct args
//	- NamedExec, NamedQuery  named query w/ struct or map
//  - NamedStmt              a pre-compiled named query which is a prepared statement
//
// Internal Interfaces:
//
//  - compileNamedQuery                    rebind a named query, returning a query and list of names
//  - bindArgs, bindMapArgs, bindAnyArgs   given a list of names, return an arglist

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"unicode"

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

// NamedQuery binds a named query and then runs Query on the result using the
// provided Ext (sqlx.Tx, sqlx.Db).
//
// It works with both structs and with map[string]interface{} types.
func NamedQuery(ctx context.Context, e Ext, query string, arg interface{}) (*Rows, error) {
	q, args, err := bindNamedMapper(Placeholder(e.DriverName()), query, arg, mapperFor(e))
	if err != nil {
		return nil, err
	}
	return e.QueryxContext(ctx, q, args...)
}

// NamedExec uses BindStruct to get a query executable by the driver and then
// runs Exec on the result.  Returns an error from the binding or the query
// execution itself.
func NamedExec(ctx context.Context, e Ext, query string, arg interface{}) (sql.Result, error) {
	q, args, err := bindNamedMapper(Placeholder(e.DriverName()), query, arg, mapperFor(e))
	if err != nil {
		return nil, err
	}
	return e.ExecContext(ctx, q, args...)
}

// NamedStmt is a prepared statement that executes named queries. Prepare it how
// you would execute a NamedQuery, but pass in a struct or map when executing.
type NamedStmt struct {
	Params      []string
	QueryString string
	Stmt        *Stmt
}

// Close closes the named statement.
func (n *NamedStmt) Close() error {
	return n.Stmt.Close()
}

// Exec executes a named statement using the struct passed. Any named
// placeholder parameters are replaced with fields from arg.
func (n *NamedStmt) Exec(arg interface{}) (sql.Result, error) {
	args, err := bindAnyArgs(n.Params, arg, n.Stmt.Mapper)
	if err != nil {
		return *new(sql.Result), err
	}
	return n.Stmt.Exec(args...)
}

// Query executes a named statement using the struct argument, returning rows.
// Any named placeholder parameters are replaced with fields from arg.
func (n *NamedStmt) Query(arg interface{}) (*sql.Rows, error) {
	args, err := bindAnyArgs(n.Params, arg, n.Stmt.Mapper)
	if err != nil {
		return nil, err
	}
	return n.Stmt.Query(args...)
}

// QueryRow executes a named statement against the database.  Because sqlx cannot
// create a *sql.Row with an error condition pre-set for binding errors, sqlx
// returns a *sqlx.Row instead.
// Any named placeholder parameters are replaced with fields from arg.
func (n *NamedStmt) QueryRow(arg interface{}) *Row {
	args, err := bindAnyArgs(n.Params, arg, n.Stmt.Mapper)
	if err != nil {
		return &Row{err: err}
	}
	return n.Stmt.QueryRowxContext(context.TODO(), args...)
}

// Queryx using this NamedStmt
// Any named placeholder parameters are replaced with fields from arg.
func (n *NamedStmt) Queryx(arg interface{}) (*Rows, error) {
	r, err := n.Query(arg)
	if err != nil {
		return nil, err
	}
	return &Rows{Rows: r, Mapper: n.Stmt.Mapper, unsafe: isUnsafe(n)}, err
}

// QueryRowx this NamedStmt.  Because of limitations with QueryRow, this is
// an alias for QueryRow.
// Any named placeholder parameters are replaced with fields from arg.
func (n *NamedStmt) QueryRowx(arg interface{}) *Row {
	return n.QueryRow(arg)
}

// Select using this NamedStmt
// Any named placeholder parameters are replaced with fields from arg.
func (n *NamedStmt) Select(dest interface{}, arg interface{}) error {
	rows, err := n.Queryx(arg)
	if err != nil {
		return err
	}
	// if something happens here, we want to make sure the rows are Closed
	defer rows.Close()
	return scanAll(rows, dest, false)
}

// Get using this NamedStmt
// Any named placeholder parameters are replaced with fields from arg.
func (n *NamedStmt) Get(dest interface{}, arg interface{}) error {
	r := n.QueryRowx(arg)
	return r.scanAny(dest, false)
}

// Unsafe creates an unsafe version of the NamedStmt
func (n *NamedStmt) Unsafe() *NamedStmt {
	r := &NamedStmt{Params: n.Params, Stmt: n.Stmt, QueryString: n.QueryString}
	r.Stmt.unsafe = true
	return r
}

// ExecContext executes a named statement using the struct passed.
// Any named placeholder parameters are replaced with fields from arg.
func (n *NamedStmt) ExecContext(ctx context.Context, arg interface{}) (sql.Result, error) {
	args, err := bindAnyArgs(n.Params, arg, n.Stmt.Mapper)
	if err != nil {
		return *new(sql.Result), err
	}
	return n.Stmt.ExecContext(ctx, args...)
}

// QueryContext executes a named statement using the struct argument, returning rows.
// Any named placeholder parameters are replaced with fields from arg.
func (n *NamedStmt) QueryContext(ctx context.Context, arg interface{}) (*sql.Rows, error) {
	args, err := bindAnyArgs(n.Params, arg, n.Stmt.Mapper)
	if err != nil {
		return nil, err
	}
	return n.Stmt.QueryContext(ctx, args...)
}

// QueryRowContext executes a named statement against the database.  Because sqlx cannot
// create a *sql.Row with an error condition pre-set for binding errors, sqlx
// returns a *sqlx.Row instead.
// Any named placeholder parameters are replaced with fields from arg.
func (n *NamedStmt) QueryRowContext(ctx context.Context, arg interface{}) *Row {
	args, err := bindAnyArgs(n.Params, arg, n.Stmt.Mapper)
	if err != nil {
		return &Row{err: err}
	}
	return n.Stmt.QueryRowxContext(ctx, args...)
}

// QueryxContext using this NamedStmt
// Any named placeholder parameters are replaced with fields from arg.
func (n *NamedStmt) QueryxContext(ctx context.Context, arg interface{}) (*Rows, error) {
	r, err := n.QueryContext(ctx, arg)
	if err != nil {
		return nil, err
	}
	return &Rows{Rows: r, Mapper: n.Stmt.Mapper, unsafe: isUnsafe(n)}, err
}

// QueryRowxContext this NamedStmt.  Because of limitations with QueryRow, this is
// an alias for QueryRow.
// Any named placeholder parameters are replaced with fields from arg.
func (n *NamedStmt) QueryRowxContext(ctx context.Context, arg interface{}) *Row {
	return n.QueryRowContext(ctx, arg)
}

// SelectContext using this NamedStmt
// Any named placeholder parameters are replaced with fields from arg.
func (n *NamedStmt) SelectContext(ctx context.Context, dest interface{}, arg interface{}) error {
	rows, err := n.QueryxContext(ctx, arg)
	if err != nil {
		return err
	}
	// if something happens here, we want to make sure the rows are Closed
	defer rows.Close()
	return scanAll(rows, dest, false)
}

// GetContext using this NamedStmt
// Any named placeholder parameters are replaced with fields from arg.
func (n *NamedStmt) GetContext(ctx context.Context, dest interface{}, arg interface{}) error {
	r := n.QueryRowxContext(ctx, arg)
	return r.scanAny(dest, false)
}

// convertMapStringInterface attempts to convert v to map[string]interface{}.
// Unlike v.(map[string]interface{}), this function works on named types that
// are convertible to map[string]interface{} as well.
func convertMapStringInterface(v interface{}) (map[string]interface{}, bool) {
	var m map[string]interface{}
	mtype := reflect.TypeOf(m)
	t := reflect.TypeOf(v)
	if !t.ConvertibleTo(mtype) {
		return nil, false
	}
	return reflect.ValueOf(v).Convert(mtype).Interface().(map[string]interface{}), true

}

func bindAnyArgs(names []string, arg interface{}, m *reflectx.Mapper) ([]interface{}, error) {
	if maparg, ok := convertMapStringInterface(arg); ok {
		return bindMapArgs(names, maparg)
	}
	return bindArgs(names, arg, m)
}

// private interface to generate a list of interfaces from a given struct
// type, given a list of names to pull out of the struct.  Used by public
// BindStruct interface.
func bindArgs(names []string, arg interface{}, m *reflectx.Mapper) ([]interface{}, error) {
	arglist := make([]interface{}, 0, len(names))

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
func bindMapArgs(names []string, arg map[string]interface{}) ([]interface{}, error) {
	arglist := make([]interface{}, 0, len(names))

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
func bindStruct(bindType PlaceholderStyle, query string, arg interface{}, m *reflectx.Mapper) (string, []interface{}, error) {
	bound, names, err := compileNamedQuery([]byte(query), bindType)
	if err != nil {
		return "", []interface{}{}, err
	}

	arglist, err := bindAnyArgs(names, arg, m)
	if err != nil {
		return "", []interface{}{}, err
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
	// defensive guard when "VALUES (...)" not found
	if len(loc) < 2 {
		return bound
	}

	openingBracketIndex := loc[1] - 1
	index := findMatchingClosingBracketIndex(bound[openingBracketIndex:])
	// defensive guard. must have closing bracket
	if index == 0 {
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
func bindArray(bindType PlaceholderStyle, query string, arg interface{}, m *reflectx.Mapper) (string, []interface{}, error) {
	// do the initial binding with PlaceholderQuestion; if bindType is not question, we
	// can rebind it at the end.
	bound, names, err := compileNamedQuery([]byte(query), PlaceholderQuestion)
	if err != nil {
		return "", []interface{}{}, err
	}
	arrayValue := reflect.ValueOf(arg)
	arrayLen := arrayValue.Len()
	if arrayLen == 0 {
		return "", []interface{}{}, fmt.Errorf("length of array is 0: %#v", arg)
	}
	var arglist = make([]interface{}, 0, len(names)*arrayLen)
	for i := 0; i < arrayLen; i++ {
		elemArglist, err := bindAnyArgs(names, arrayValue.Index(i).Interface(), m)
		if err != nil {
			return "", []interface{}{}, err
		}
		arglist = append(arglist, elemArglist...)
	}
	if arrayLen > 1 {
		bound = fixBound(bound, arrayLen)
	}
	// adjust binding type if we weren't on question
	if bindType != PlaceholderQuestion {
		bound = Rebind(bindType, bound)
	}
	return bound, arglist, nil
}

// bindMap binds a named parameter query with a map of arguments.
func bindMap(bindType PlaceholderStyle, query string, args map[string]interface{}) (string, []interface{}, error) {
	bound, names, err := compileNamedQuery([]byte(query), bindType)
	if err != nil {
		return "", []interface{}{}, err
	}

	arglist, err := bindMapArgs(names, args)
	return bound, arglist, err
}

// -- Compilation of Named Queries

// Allow digits and letters in bind params;  additionally runes are
// checked against underscores, meaning that bind params can have be
// alphanumeric with underscores.  Mind the difference between unicode
// digits and numbers, where '5' is a digit but '五' is not.
var allowedBindRunes = []*unicode.RangeTable{unicode.Letter, unicode.Digit}

// FIXME: this function isn't safe for unicode named params, as a failing test
// can testify.  This is not a regression but a failure of the original code
// as well.  It should be modified to range over runes in a string rather than
// bytes, even though this is less convenient and slower.  Hopefully the
// addition of the prepared NamedStmt (which will only do this once) will make
// up for the slightly slower ad-hoc NamedExec/NamedQuery.

// compile a NamedQuery into an unbound query (using the '?' bindvar) and
// a list of names.
func compileNamedQuery(qs []byte, bindType PlaceholderStyle) (query string, names []string, err error) {
	names = make([]string, 0, 10)
	rebound := make([]byte, 0, len(qs))

	currentVar := 1
	tokens := sqltoken.Tokenize(string(qs), namedParseConfigs[bindType])

	for _, token := range tokens {
		if token.Type != sqltoken.ColonWord {
			rebound = append(rebound, ([]byte)(token.Text)...)
			continue
		}
		names = append(names, token.Text[1:])
		switch bindType {
		// oracle only supports named type bind vars even for positional
		case PlaceholderNamed:
			rebound = append(rebound, ([]byte)(token.Text)...)
		case PlaceholderQuestion, PlaceholderUnknown:
			rebound = append(rebound, '?')
		case PlaceholderDollar:
			rebound = append(rebound, '$')
			rebound = strconv.AppendInt(rebound, int64(currentVar), 10)
			currentVar++
		case PlaceholderAt:
			rebound = append(rebound, '@', 'p')
			rebound = strconv.AppendInt(rebound, int64(currentVar), 10)
			currentVar++
		}
	}
	return string(rebound), names, nil
}

// Named takes a query using named parameters and an argument and
// returns a new query with a list of args that can be executed by
// a database.  The return value uses the `?` bindvar.
func Named(query string, arg interface{}) (string, []interface{}, error) {
	return bindNamedMapper(PlaceholderQuestion, query, arg, mapper())
}

func bindNamedMapper(bindType PlaceholderStyle, query string, arg interface{}, m *reflectx.Mapper) (string, []interface{}, error) {
	t := reflect.TypeOf(arg)
	k := t.Kind()
	switch {
	case k == reflect.Map && t.Key().Kind() == reflect.String:
		m, ok := convertMapStringInterface(arg)
		if !ok {
			return "", nil, fmt.Errorf("sqlx.bindNamedMapper: unsupported map type: %T", arg)
		}
		return bindMap(bindType, query, m)
	case k == reflect.Array || k == reflect.Slice:
		return bindArray(bindType, query, arg, m)
	default:
		return bindStruct(bindType, query, arg, m)
	}
}

func prepareNamed(ctx context.Context, p namedPreparer, query string) (*NamedStmt, error) {
	bindType := Placeholder(p.DriverName())
	q, args, err := compileNamedQuery([]byte(query), bindType)
	if err != nil {
		return nil, err
	}
	stmt, err := Preparex(ctx, p, q)
	if err != nil {
		return nil, err
	}
	return &NamedStmt{
		QueryString: q,
		Params:      args,
		Stmt:        stmt,
	}, nil
}