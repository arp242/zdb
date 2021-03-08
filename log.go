package zdb

import (
	"context"
	"database/sql"
	"io"
	"regexp"
	"strings"

	"github.com/jmoiron/sqlx"
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
//
//
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

func (d logDB) ExecContext(ctx context.Context, query string, params ...interface{}) (sql.Result, error) {
	defer d.log(ctx, query, params)()
	return d.DB.(dbImpl).ExecContext(ctx, query, params...)
}
func (d logDB) GetContext(ctx context.Context, dest interface{}, query string, params ...interface{}) error {
	defer d.log(ctx, query, params)()
	return d.DB.(dbImpl).GetContext(ctx, dest, query, params...)
}
func (d logDB) SelectContext(ctx context.Context, dest interface{}, query string, params ...interface{}) error {
	defer d.log(ctx, query, params)()
	return d.DB.(dbImpl).SelectContext(ctx, dest, query, params...)
}
func (d logDB) QueryxContext(ctx context.Context, query string, params ...interface{}) (*sqlx.Rows, error) {
	defer d.log(ctx, query, params)()
	return d.DB.(dbImpl).QueryxContext(ctx, query, params...)
}

func (d logDB) log(ctx context.Context, query string, params []interface{}) func() {
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
