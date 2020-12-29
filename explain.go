package zdb

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"zgo.at/zlog"
)

type explainDB struct {
	db     DBCloser
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
func NewExplainDB(db DBCloser, out io.Writer, filter string) DBCloser {
	return &explainDB{db: db, out: out, filter: filter}
}

func (d explainDB) explain(ctx context.Context, query string, args []interface{}) func() {
	if _, ok := GetDB(ctx); !ok {
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
			db      = MustGetDB(ctx)
			explain []string
			err     error
		)
		if PgSQL(ctx) {
			err = db.(*explainDB).db.SelectContext(ctx, &explain, `explain analyze `+query, args...)
			for i := range explain {
				explain[i] = "\t" + explain[i]
			}
		} else {
			var sqe []struct {
				ID, Parent, Notused int
				Detail              string
			}
			s := time.Now()
			err = db.(*explainDB).db.SelectContext(ctx, &sqe, `explain query plan `+query, args...)
			if len(sqe) > 0 {
				explain = make([]string, len(sqe)+1)
				for i := range sqe {
					explain[i] = "\t" + sqe[i].Detail
				}
				explain[len(sqe)] = "\tTime: " + time.Now().Sub(s).Round(1*time.Millisecond).String()
			}
		}
		if err != nil {
			zlog.Error(err)
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

func (d explainDB) Unwrap() DB                 { return d.db }
func (d explainDB) Close() error               { return d.db.Close() }
func (d explainDB) Rebind(query string) string { return d.db.Rebind(query) }
func (d explainDB) DriverName() string         { return d.db.DriverName() }
func (d explainDB) BindNamed(query string, arg interface{}) (newquery string, args []interface{}, err error) {
	return d.db.BindNamed(query, arg)
}
