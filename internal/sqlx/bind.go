package sqlx

import (
	"database/sql/driver"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"sync"

	"zgo.at/zdb/internal/sqltoken"
	"zgo.at/zdb/internal/sqlx/reflectx"
)

// PlaceholderStyle controls which placeholders to use parametrized queries.
type PlaceholderStyle uint8

// Placeholders styles we know about.
const (
	PlaceholderUnknown  PlaceholderStyle = iota // Fall back to PlaceholderQuestion
	PlaceholderQuestion                         // ?
	PlaceholderDollar                           // $1, $2
	PlaceholderNamed                            // :arg1, :arg2
	PlaceholderAt                               // @p1, @p2
)

var placeholders sync.Map

func init() {
	for p, drivers := range map[PlaceholderStyle][]string{
		PlaceholderDollar:   []string{"postgres", "pgx", "pq", "pq-timeouts", "cloudsqlpostgres", "ql", "nrpostgres", "cockroach"},
		PlaceholderQuestion: []string{"mysql", "sqlite3", "nrmysql", "nrsqlite3"},
		PlaceholderNamed:    []string{"oci8", "ora", "goracle", "godror", "oracle"},
		PlaceholderAt:       []string{"sqlserver"},
	} {
		for _, d := range drivers {
			PlaceholderRegister(d, p)
		}
	}
}

var rebindConfigs = func() []sqltoken.Config {
	configs := make([]sqltoken.Config, PlaceholderAt+1)

	pg := sqltoken.PostgreSQLConfig()
	pg.NoticeQuestionMark = true
	pg.NoticeDollarNumber = false
	configs[PlaceholderDollar] = pg

	ora := sqltoken.OracleConfig()
	ora.NoticeColonWord = false
	ora.NoticeQuestionMark = true
	configs[PlaceholderNamed] = ora

	ssvr := sqltoken.SQLServerConfig()
	ssvr.NoticeAtWord = false
	ssvr.NoticeQuestionMark = true
	configs[PlaceholderAt] = ssvr

	return configs
}()

// PlaceholderRegister sets the placeholder style for a SQL driver.
func PlaceholderRegister(driver string, style PlaceholderStyle) {
	placeholders.Store(driver, style)
}

// Placeholder returns the placeholder style for a SQL driver.
func Placeholder(driver string) PlaceholderStyle {
	p, ok := placeholders.Load(driver)
	if !ok {
		return PlaceholderUnknown
	}
	return p.(PlaceholderStyle)
}

// Rebind a query from the default placeholder style (PlaceholderQuestion) to
// the target placeholder style.
func Rebind(style PlaceholderStyle, query string) string {
	switch style {
	case PlaceholderQuestion, PlaceholderUnknown:
		return query
	}

	// Add space enough for 10 params before we have to allocate
	var (
		tokens = sqltoken.Tokenize(query, rebindConfigs[style])
		rqb    = make([]byte, 0, len(query)+10)
		j      int
	)
	for _, token := range tokens {
		if token.Type != sqltoken.QuestionMark {
			rqb = append(rqb, ([]byte)(token.Text)...)
			continue
		}
		switch style {
		case PlaceholderDollar:
			rqb = append(rqb, '$')
		case PlaceholderNamed:
			rqb = append(rqb, ':', 'a', 'r', 'g')
		case PlaceholderAt:
			rqb = append(rqb, '@', 'p')
		}
		j++
		rqb = strconv.AppendInt(rqb, int64(j), 10)
	}
	return string(rqb)
}

// In expands slice values in args, returning the modified query string and a
// new arg list that can be executed by a database.
//
// The query should use the "?" placeholder style, and the return value the
// return value also uses the "?" style.
func In(query string, args ...any) (string, []any, error) {
	// TODO: automatically expand slices for a single ?, like zdb already does.
	// Then we can remove/unexport this.

	// argMeta stores reflect.Value and length for slices and the value itself
	// for non-slice arguments
	type argMeta struct {
		v      reflect.Value
		i      any
		length int
	}

	var (
		flatArgsCount int
		anySlices     bool
		stackMeta     [32]argMeta
		meta          []argMeta
	)
	if len(args) <= len(stackMeta) {
		meta = stackMeta[:len(args)]
	} else {
		meta = make([]argMeta, len(args))
	}

	for i, arg := range args {
		if a, ok := arg.(driver.Valuer); ok {
			var err error
			arg, err = a.Value()
			if err != nil {
				return "", nil, err
			}
		}

		if v, ok := asSliceForIn(arg); ok {
			meta[i].length = v.Len()
			meta[i].v = v

			anySlices = true
			flatArgsCount += meta[i].length

			if meta[i].length == 0 {
				return "", nil, errors.New("sqlx.In: empty slice passed to 'in' query")
			}
		} else {
			meta[i].i = arg
			flatArgsCount++
		}
	}

	// don't do any parsing if there aren't any slices;  note that this means
	// some errors that we might have caught below will not be returned.
	if !anySlices {
		return query, args, nil
	}

	newArgs := make([]any, 0, flatArgsCount)

	var buf strings.Builder
	buf.Grow(len(query) + len(", ?")*flatArgsCount)

	var arg, offset int
	for i := strings.IndexByte(query[offset:], '?'); i != -1; i = strings.IndexByte(query[offset:], '?') {
		if arg >= len(meta) {
			// if an argument wasn't passed, lets return an error;  this is
			// not actually how database/sql Exec/Query works, but since we are
			// creating an argument list programmatically, we want to be able
			// to catch these programmer errors earlier.
			return "", nil, errors.New("sqlx.In: more placeholders than arguments")
		}

		argMeta := meta[arg]
		arg++

		// not a slice, continue.
		// our questionmark will either be written before the next expansion
		// of a slice or after the loop when writing the rest of the query
		if argMeta.length == 0 {
			offset = offset + i + 1
			newArgs = append(newArgs, argMeta.i)
			continue
		}

		// write everything up to and including our ? character
		buf.WriteString(query[:offset+i+1])

		for si := 1; si < argMeta.length; si++ {
			buf.WriteString(", ?")
		}

		newArgs = appendReflectSlice(newArgs, argMeta.v, argMeta.length)

		// slice the query and reset the offset. this avoids some bookkeeping for
		// the write after the loop
		query = query[offset+i+1:]
		offset = 0
	}

	buf.WriteString(query)

	if arg < len(meta) {
		return "", nil, errors.New("sqlx.In: more arguments than placeholders")
	}

	return buf.String(), newArgs, nil
}

func asSliceForIn(i any) (reflect.Value, bool) {
	if i == nil {
		return reflect.Value{}, false
	}

	v := reflect.ValueOf(i)
	t := reflectx.Deref(v.Type())

	// Only expand slices
	// TODO: probably also array?
	if t.Kind() != reflect.Slice {
		return reflect.Value{}, false
	}

	// []byte is a driver.Value type so it should not be expanded
	if t == reflect.TypeOf([]byte{}) {
		return reflect.Value{}, false

	}

	return v, true
}

func appendReflectSlice(args []any, v reflect.Value, vlen int) []any {
	switch val := v.Interface().(type) {
	case []any:
		args = append(args, val...)
	case []int:
		for i := range val {
			args = append(args, val[i])
		}
	case []string:
		for i := range val {
			args = append(args, val[i])
		}
	default:
		for si := 0; si < vlen; si++ {
			args = append(args, v.Index(si).Interface())
		}
	}

	return args
}
