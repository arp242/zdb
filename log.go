package zdb

import (
	"context"
	"database/sql"
	"io"
	"regexp"
	"strings"

	"zgo.at/zdb/internal/sqlx"
)

type logDB struct {
	DB
	out     io.Writer
	logWhat DumpArg
	filter  string
}

// NewLogDB returns a DB wrapper to log queries, query plans, and query results.
//
// If Filter is not an empty string then only queries containing the text are
// logged. Use an empty string to log everything.
//
// If LogQuery is enabled then the query text will be logged, if LogExplain is
// enabled then the query plan will be logged, and if LogResult is enabled then
// the query result will be logged.
//
// Only LogQuery will be set if opts is nil,
//
// WARNING: printing the result means the query will be run twice, which is a
// significant performance impact and DATA MODIFICATION STATEMENTS ARE ALSO RUN
// TWICE. Some SQL engines may also run the query on EXPLAIN (e.g. PostgreSQL
// does).
func NewLogDB(db DB, out io.Writer, logWhat DumpArg, filter string) DB {
	if logWhat == 0 {
		logWhat = DumpQuery
	}
	if logWhat == DumpAll {
		logWhat = DumpQuery | DumpExplain | DumpResult
	}
	return &logDB{DB: db, out: out, logWhat: logWhat | DumpLocation | dumpFromLogDB, filter: filter}
}

func (d logDB) Unwrap() DB { return d.DB }

func (d logDB) Begin(ctx context.Context, opts ...beginOpt) (context.Context, DB, error) {
	ctx, tx, err := d.DB.Begin(ctx, opts...)
	if err != nil {
		return nil, nil, err
	}
	ldb := &logDB{DB: tx, out: d.out, logWhat: d.logWhat, filter: d.filter}
	return WithDB(ctx, ldb), ldb, nil
}

func (d logDB) ExecContext(ctx context.Context, query string, params ...any) (sql.Result, error) {
	defer d.log(ctx, query, params)()
	return d.DB.(dbImpl).ExecContext(ctx, query, params...)
}
func (d logDB) GetContext(ctx context.Context, dest any, query string, params ...any) error {
	defer d.log(ctx, query, params)()
	return d.DB.(dbImpl).GetContext(ctx, dest, query, params...)
}
func (d logDB) SelectContext(ctx context.Context, dest any, query string, params ...any) error {
	defer d.log(ctx, query, params)()
	return d.DB.(dbImpl).SelectContext(ctx, dest, query, params...)
}
func (d logDB) QueryxContext(ctx context.Context, query string, params ...any) (*sqlx.Rows, error) {
	defer d.log(ctx, query, params)()
	return d.DB.(dbImpl).QueryxContext(ctx, query, params...)
}

func (d logDB) log(ctx context.Context, query string, params []any) func() {
	if _, ok := GetDB(ctx); !ok {
		return func() {}
	}

	if d.filter != "" &&
		!strings.Contains(regexp.MustCompile(`\s+`).ReplaceAllString(query, " "), d.filter) {
		return func() {}
	}

	return func() {
		Dump(WithDB(ctx, Unwrap(MustGetDB(ctx))), d.out, query, append(params, d.logWhat)...)
	}
}

// TODO: the reason we need these is because it's implemented like so:
//
//     func (db zDB) Get(ctx context.Context, dest any, query string, params ...any) error {
//       return getImpl(ctx, db, dest, query, params...)
//     }
//
//     func Get(ctx context.Context, dest any, query string, params ...any) error {
//       return getImpl(ctx, MustGetDB(ctx), dest, query, params...)
//     }
//
// The idea was that you could "wrap" a DB with just a few methods. But this
// doesn't work because for zDB.Get() the receiver is always zDB, rather than
// the type that it wraps/embeds, and doesn't call the appropriate "wrapped"
// method.
//
// It works with the package-level zdb.Get() from the context
// because it's not calling the receiver method, but rather top the
// top "logDB".
//
// This fixes it, but having to implement a bunch of boilerplate is exactly what
// I wanted to avoid...
//
// Need to think about a good solution for this. Things will be easier once we
// unify zdb and internal/sqlx, too.

func (db logDB) Exec(ctx context.Context, query string, params ...any) error {
	return execImpl(ctx, db, query, params...)
}
func (db logDB) NumRows(ctx context.Context, query string, params ...any) (int64, error) {
	return numRowsImpl(ctx, db, query, params...)
}
func (db logDB) InsertID(ctx context.Context, idColumn, query string, params ...any) (int64, error) {
	return insertIDImpl(ctx, db, idColumn, query, params...)
}
func (db logDB) Get(ctx context.Context, dest any, query string, params ...any) error {
	return getImpl(ctx, db, dest, query, params...)
}
func (db logDB) Select(ctx context.Context, dest any, query string, params ...any) error {
	return selectImpl(ctx, db, dest, query, params...)
}
func (db logDB) Query(ctx context.Context, query string, params ...any) (*Rows, error) {
	return queryImpl(ctx, db, query, params...)
}
func (db logDB) TX(ctx context.Context, fn func(context.Context) error) error {
	return txImpl(ctx, db, fn)
}
func (db logDB) Rollback() error { return db.DB.Rollback() }
func (db logDB) Commit() error   { return db.DB.Commit() }

func (db metricDB) Exec(ctx context.Context, query string, params ...any) error {
	return execImpl(ctx, db, query, params...)
}
func (db metricDB) NumRows(ctx context.Context, query string, params ...any) (int64, error) {
	return numRowsImpl(ctx, db, query, params...)
}
func (db metricDB) InsertID(ctx context.Context, idColumn, query string, params ...any) (int64, error) {
	return insertIDImpl(ctx, db, idColumn, query, params...)
}
func (db metricDB) Get(ctx context.Context, dest any, query string, params ...any) error {
	return getImpl(ctx, db, dest, query, params...)
}
func (db metricDB) Select(ctx context.Context, dest any, query string, params ...any) error {
	return selectImpl(ctx, db, dest, query, params...)
}
func (db metricDB) Query(ctx context.Context, query string, params ...any) (*Rows, error) {
	return queryImpl(ctx, db, query, params...)
}
func (db metricDB) TX(ctx context.Context, fn func(context.Context) error) error {
	return txImpl(ctx, db, fn)
}
func (db metricDB) Rollback() error { return db.DB.Rollback() }
func (db metricDB) Commit() error   { return db.DB.Commit() }
