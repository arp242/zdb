package zdb

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"regexp"
	"strings"

	"github.com/jmoiron/sqlx"
	"zgo.at/zlog"
)

type explainDB struct {
	db     DB
	out    io.Writer
	filter string
}

// NewExplainDB returns a wrapper DB connection that will print all queries with
// their EXPLAINs to out.
//
// Only queries which contain the string in filter are shown. Use an empty
// string to show everything.
//
// Because EXPLAIN will actually run the queries this is quite a significant
// performance impact. Note that data modification statements are *also* run
// twice!
//
// This only works for PostgreSQL for now.
func NewExplainDB(db DB, out io.Writer, filter string) DB {
	return &explainDB{db: db, out: out, filter: filter}
}

func (d explainDB) explain(ctx context.Context, query string, args []interface{}) func() {
	if _, ok := Get(ctx); !ok {
		return func() {}
	}

	q := ApplyPlaceholders(query, args...)
	if d.filter != "" {
		q := strings.ReplaceAll(q, "\n", " ")
		q = regexp.MustCompile(`\s+`).ReplaceAllString(q, " ")
		if !strings.Contains(q, d.filter) {
			return func() {}
		}
	}

	return func() {
		var (
			db      = MustGet(ctx)
			explain []string
			kw      = "explain "
		)

		if PgSQL(db) {
			kw += "analyze "
		}

		err := db.(*explainDB).db.SelectContext(ctx, &explain, kw+query, args...)
		if err != nil {
			zlog.Error(err)
		}

		for i := range explain {
			explain[i] = "\t" + explain[i]
		}

		fmt.Fprint(d.out, "QUERY:\n\t"+strings.ReplaceAll(q, "\n", "\n\t")+
			"\nEXPLAIN:\n"+strings.Join(explain, "\n")+
			"\n\n")
	}
}

func (d explainDB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	defer d.explain(ctx, query, args)()
	return d.db.ExecContext(ctx, query, args...)
}

func (d explainDB) GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	defer d.explain(ctx, query, args)()
	return d.db.GetContext(ctx, dest, query, args...)
}

func (d explainDB) SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	defer d.explain(ctx, query, args)()
	return d.db.SelectContext(ctx, dest, query, args...)
}

func (d explainDB) QueryRowxContext(ctx context.Context, query string, args ...interface{}) *sqlx.Row {
	defer d.explain(ctx, query, args)()
	return d.db.QueryRowxContext(ctx, query, args...)
}

func (d explainDB) QueryxContext(ctx context.Context, query string, args ...interface{}) (*sqlx.Rows, error) {
	defer d.explain(ctx, query, args)()
	return d.db.QueryxContext(ctx, query, args...)
}

func (d explainDB) Rebind(query string) string { return d.db.Rebind(query) }
func (d explainDB) DriverName() string         { return d.db.DriverName() }
func (d explainDB) BindNamed(query string, arg interface{}) (newquery string, args []interface{}, err error) {
	return d.db.BindNamed(query, arg)
}
