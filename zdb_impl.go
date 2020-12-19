package zdb

// This file contains the implementations for the DB interface in zdb.go.
//
// How this works:
//
// - We use sqlx.DB internally, but everything we return is a "zDB" or zTX". For
//   all public appearances, there is no sqlx.
//
// - Most of the actual implementations are in the *Impl() functions (the name
//   avoids some conflicts with keywords, packages, and common variables).
//
// - The top-level Get(), zDB.Get(), and zTX.Get() all call these *Impl()
//   functions.
//
// This is a little bit convoluted, but it solves some issues:
//
// - Some of the added methods (zdb.Prepare()) aren't on the sqlx.DB type, and
//   would prefer a consistent API which is always the same (i.e. zDB.Get()
//   and zdb.Get() are identical).
//
// - sqlx.DB and sqlx.Tx don't share some methods which makes passing around a
//   single DB interface hard (i.e. sqlx.DB has no Commit() and sqlx.Tx has no
//   Begin(). The zdb wrappers shims these methods out, making various things
//   easier.
//
// - Wrapping a sqlx.DB is easy, for example for logging or whatnot, but also
//   making this wrapper work on transaction is hard, because of the above
//   issue.
//
// - For zDB.Load() / zdb.Load() we need a fs.FS, but I don't really like the
//   idea of having to pass that around all the time, and I also don't really want
//   to add it to the context. But we can add it to the zDB.
//
// Perhaps we should just stop using sqlx or fork locally. It's not
// super-maintained anyway.

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/jmoiron/sqlx/reflectx"
	"zgo.at/zstd/zstring"
)

var ctxkey = &struct{ n string }{"zdb"}

type zDB struct {
	db *sqlx.DB
	fs fs.FS
}

func (db zDB) files() fs.FS { return db.fs }

func (db zDB) Prepare(ctx context.Context, query string, params ...interface{}) (string, []interface{}, error) {
	return prepareImpl(ctx, db, query, params...)
}
func (db zDB) Load(ctx context.Context, name string) (string, error) { return loadImpl(ctx, db, name) }
func (db zDB) Exec(ctx context.Context, query string, params ...interface{}) error {
	return execImpl(ctx, db, query, params...)
}
func (db zDB) NumRows(ctx context.Context, query string, params ...interface{}) (int64, error) {
	return numRowsImpl(ctx, db, query, params...)
}
func (db zDB) InsertID(ctx context.Context, idColumn, query string, params ...interface{}) (int64, error) {
	return insertIDImpl(ctx, db, idColumn, query, params...)
}
func (db zDB) Get(ctx context.Context, dest interface{}, query string, params ...interface{}) error {
	return getImpl(ctx, db, dest, query, params...)
}
func (db zDB) Select(ctx context.Context, dest interface{}, query string, params ...interface{}) error {
	return selectImpl(ctx, db, dest, query, params...)
}
func (db zDB) Query(ctx context.Context, query string, params ...interface{}) (*Rows, error) {
	return queryImpl(ctx, db, query, params...)
}
func (db zDB) BindNamed(query string, param interface{}) (newquery string, params []interface{}, err error) {
	return db.db.BindNamed(query, param)
}
func (db zDB) Rebind(query string) string { return db.db.Rebind(query) }
func (db zDB) DriverName() string         { return db.db.DriverName() }
func (db zDB) Close() error               { return db.db.Close() }
func (db zDB) Begin(ctx context.Context, opts ...beginOpt) (context.Context, DB, error) {
	return beginImpl(ctx, &db, opts...)
}
func (db zDB) Commit() error   { return errors.New("cannot commit, as this is not a transaction") }
func (db zDB) Rollback() error { return errors.New("cannot rollback, as this is not a transaction") }
func (db zDB) TX(ctx context.Context, fn func(context.Context) error) error {
	return txImpl(ctx, db, fn)
}

func (db zDB) ExecContext(ctx context.Context, query string, params ...interface{}) (sql.Result, error) {
	return db.db.ExecContext(ctx, query, params...)
}
func (db zDB) GetContext(ctx context.Context, dest interface{}, query string, params ...interface{}) error {
	return db.db.GetContext(ctx, dest, query, params...)
}
func (db zDB) SelectContext(ctx context.Context, dest interface{}, query string, params ...interface{}) error {
	return db.db.SelectContext(ctx, dest, query, params...)
}
func (db zDB) QueryxContext(ctx context.Context, query string, params ...interface{}) (*sqlx.Rows, error) {
	return db.db.QueryxContext(ctx, query, params...)
}

type zTX struct {
	db     *sqlx.Tx
	parent *zDB // Needed for Close() and files()
}

func (db zTX) files() fs.FS { return db.parent.files() }

func (db zTX) Prepare(ctx context.Context, query string, params ...interface{}) (string, []interface{}, error) {
	return prepareImpl(ctx, db, query, params...)
}
func (db zTX) Load(ctx context.Context, name string) (string, error) { return loadImpl(ctx, db, name) }
func (db zTX) Exec(ctx context.Context, query string, params ...interface{}) error {
	return execImpl(ctx, db, query, params...)
}
func (db zTX) NumRows(ctx context.Context, query string, params ...interface{}) (int64, error) {
	return numRowsImpl(ctx, db, query, params...)
}
func (db zTX) InsertID(ctx context.Context, idColumn, query string, params ...interface{}) (int64, error) {
	return insertIDImpl(ctx, db, idColumn, query, params...)
}
func (db zTX) Get(ctx context.Context, dest interface{}, query string, params ...interface{}) error {
	return getImpl(ctx, db, dest, query, params...)
}
func (db zTX) Select(ctx context.Context, dest interface{}, query string, params ...interface{}) error {
	return selectImpl(ctx, db, dest, query, params...)
}
func (db zTX) Query(ctx context.Context, query string, params ...interface{}) (*Rows, error) {
	return queryImpl(ctx, db, query, params...)
}
func (db zTX) BindNamed(query string, param interface{}) (newquery string, params []interface{}, err error) {
	return db.db.BindNamed(query, param)
}
func (db zTX) Rebind(query string) string { return db.db.Rebind(query) }
func (db zTX) DriverName() string         { return db.db.DriverName() }
func (db zTX) Close() error {
	err := db.Rollback() // Not sure if this is actually needed, but can't hurt.
	if err != nil {
		return err
	}
	return db.parent.Close()
}

func (db zTX) Begin(ctx context.Context, opt ...beginOpt) (context.Context, DB, error) {
	return ctx, db, ErrTransactionStarted
}
func (db zTX) Commit() error   { return db.db.Commit() }
func (db zTX) Rollback() error { return db.db.Rollback() }
func (db zTX) TX(ctx context.Context, fn func(context.Context) error) error {
	return ErrTransactionStarted
}

func (db zTX) ExecContext(ctx context.Context, query string, params ...interface{}) (sql.Result, error) {
	return db.db.ExecContext(ctx, query, params...)
}
func (db zTX) GetContext(ctx context.Context, dest interface{}, query string, params ...interface{}) error {
	return db.db.GetContext(ctx, dest, query, params...)
}
func (db zTX) SelectContext(ctx context.Context, dest interface{}, query string, params ...interface{}) error {
	return db.db.SelectContext(ctx, dest, query, params...)
}
func (db zTX) QueryxContext(ctx context.Context, query string, params ...interface{}) (*sqlx.Rows, error) {
	return db.db.QueryxContext(ctx, query, params...)
}

// Actual implementations
// ----------------------

var stderr io.Writer = os.Stderr

func prepareImpl(ctx context.Context, db DB, query string, params ...interface{}) (string, []interface{}, error) {
	merged, named, dumpArgs, err := prepareParams(params)
	if err != nil {
		return "", nil, fmt.Errorf("zdb.Prepare: %w", err)
	}

	if strings.HasPrefix(query, "load:") {
		query, err = Load(ctx, query[5:])
		if err != nil {
			return "", nil, fmt.Errorf("zdb.Prepare: %w", err)
		}
	}

	if named {
		query, err = replaceConditionals(query, merged)
		if err != nil {
			return "", nil, fmt.Errorf("zdb.Prepare: %w", err)
		}
	}

	qparams, _ := merged.([]interface{})
	if named {
		var err error
		query, qparams, err = sqlx.Named(query, merged)
		if err != nil {
			return "", nil, fmt.Errorf("zdb.Prepare: %w", err)
		}
	}

	query, qparams, err = sqlx.In(query, qparams...)
	if err != nil {
		return "", nil, fmt.Errorf("zdb.Prepare: %w", err)
	}
	query = db.Rebind(query)

	if len(dumpArgs) > 0 {
		if len(dumpArgs) == 1 && dumpArgs[0] == DumpQuery {
			fmt.Fprintln(stderr, ApplyParams(query, qparams...))
		} else {
			di := make([]interface{}, len(dumpArgs))
			for i := range dumpArgs {
				di[i] = dumpArgs[i]
			}
			Dump(ctx, stderr, query, append(qparams, di...)...)
		}
	}

	return query, qparams, nil
}

func loadImpl(ctx context.Context, db DB, name string) (string, error) {
	var driver string
	switch PgSQL(ctx) {
	case PgSQL(ctx):
		driver = "postgres"
	case SQLite(ctx):
		driver = "sqlite3"
	default:
		return "", errors.New("zdb.Load: unknown driver")
	}

	files, err := fs.Sub(db.(interface{ files() fs.FS }).files(), "query")
	if err != nil {
		return "", fmt.Errorf("zdb.Load: %s", err)
	}

	q, err := findFile(files, insertDriver(name, driver)...)
	if err != nil {
		return "", fmt.Errorf("zdb.Load: %s", err)
	}

	return "/* " + name + " */\n" + string(bytes.TrimSpace(q)), nil
}

type dbImpl interface {
	ExecContext(ctx context.Context, query string, params ...interface{}) (sql.Result, error)
	GetContext(ctx context.Context, dest interface{}, query string, params ...interface{}) error
	SelectContext(ctx context.Context, dest interface{}, query string, params ...interface{}) error
	QueryxContext(ctx context.Context, query string, params ...interface{}) (*sqlx.Rows, error)
}

func beginImpl(ctx context.Context, db DB, opts ...beginOpt) (context.Context, DB, error) {
	// Could use savepoints, but that's probably more confusing than anything
	// else: almost all of the time you want the outermost transaction to be
	// completed in full or not at all. If you really want savepoints then you
	// can do it manually.
	if tx, ok := Unwrap(db).(*zTX); ok {
		return ctx, tx, ErrTransactionStarted
	}

	var txopt *sql.TxOptions
	for _, o := range opts {
		o(txopt)
	}

	tx, err := db.(*zDB).db.BeginTxx(ctx, txopt)
	if err != nil {
		return nil, nil, fmt.Errorf("zdb.Begin: %w", err)
	}

	ztx := &zTX{db: tx, parent: Unwrap(db).(*zDB)}
	return WithDB(ctx, ztx), ztx, nil
}

func txImpl(ctx context.Context, db DB, fn func(context.Context) error) error {
	txctx, tx, err := db.Begin(ctx)
	if err == ErrTransactionStarted {
		err := fn(txctx)
		if err != nil {
			return fmt.Errorf("zdb.TX fn: %w", err)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("zdb.TX: %w", err)
	}

	defer tx.Rollback()

	err = fn(txctx)
	if err != nil {
		return fmt.Errorf("zdb.TX fn: %w", err)
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("zdb.TX commit: %w", err)
	}
	return nil
}

func execImpl(ctx context.Context, db DB, query string, params ...interface{}) error {
	query, params, err := prepareImpl(ctx, db, query, params...)
	if err != nil {
		return err
	}
	_, err = db.(dbImpl).ExecContext(ctx, query, params...)
	return err
}

func numRowsImpl(ctx context.Context, db DB, query string, params ...interface{}) (int64, error) {
	query, params, err := prepareImpl(ctx, db, query, params...)
	if err != nil {
		return 0, err
	}
	r, err := db.(dbImpl).ExecContext(ctx, query, params...)
	if err != nil {
		return 0, err
	}
	return r.RowsAffected()
}

func insertIDImpl(ctx context.Context, db DB, idColumn, query string, params ...interface{}) (int64, error) {
	query, params, err := prepareImpl(ctx, db, query, params...)
	if err != nil {
		return 0, err
	}

	if PgSQL(ctx) {
		var id []int64
		err := db.(dbImpl).SelectContext(ctx, &id, query+" returning "+idColumn, params...)
		if err != nil {
			return 0, err
		}
		return id[len(id)-1], nil
	}

	r, err := db.(dbImpl).ExecContext(ctx, query, params...)
	if err != nil {
		return 0, err
	}
	return r.LastInsertId()
}

func selectImpl(ctx context.Context, db DB, dest interface{}, query string, params ...interface{}) error {
	query, params, err := prepareImpl(ctx, db, query, params...)
	if err != nil {
		return err
	}
	return db.(dbImpl).SelectContext(ctx, dest, query, params...)
}

func getImpl(ctx context.Context, db DB, dest interface{}, query string, params ...interface{}) error {
	query, params, err := prepareImpl(ctx, db, query, params...)
	if err != nil {
		return err
	}
	return db.(dbImpl).GetContext(ctx, dest, query, params...)
}

func queryImpl(ctx context.Context, db DB, query string, params ...interface{}) (*Rows, error) {
	query, params, err := prepareImpl(ctx, db, query, params...)
	if err != nil {
		return nil, err
	}
	r, err := db.(dbImpl).QueryxContext(ctx, query, params...)
	if err != nil {
		return nil, err
	}
	return &Rows{r}, nil
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

// TODO: it would be nice if this would deal with whitesace a bit better
//
// This has two spaces:
//
//    where {{:x x = :x}} order by a → where  order by a
//
// And with newlines it's even worse:
//
//    where
//		{{:x x = :x}}
//	  order by a
//	  →
//	  where
//
//	  order by a
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