package zdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
)

// To wrap a zdb.DB object embed the zdb.DB interface, which contains the parent
// DB connection.
//
// The Unwrap() method is expected to return the parent DB.
//
// Then implement override whatever you want; usually you will want to implement
// the dbImpl interface, which contains the methods that actually interact with
// the database. All the DB metods call this under the hood. This way you don't
// have to wrap all the methods on DB, but just five.
//
// In Begin() you will want to return a new wrapped DB instance with the
// transaction attached.

type explainDB struct {
	DB
	out    io.Writer
	filter string
}

// NewExplainDB returns a DB wrapper that will print all queries with their
// EXPLAINs to out.
//
// Only queries which contain the string in filter are shown. Use an empty
// string to show everything.
//
// Because EXPLAIN will actually run the queries this is quite a significant
// performance impact. Note that data modification statements are *also* run
// twice!
//
// TODO: rename this to logDB, and add an option to either only log queries, log
// + explain, or log + explain + show results, etc.
func NewExplainDB(db DB, out io.Writer, filter string) DB {
	return &explainDB{DB: db, out: out, filter: filter}
}

func (d explainDB) Unwrap() DB { return d.DB }

func (d explainDB) Begin(ctx context.Context, opts ...beginOpt) (context.Context, DB, error) {
	ctx, tx, err := d.DB.Begin(ctx, opts...)
	if err != nil {
		return nil, nil, err
	}
	edb := &explainDB{DB: tx, out: d.out, filter: d.filter}
	return WithDB(ctx, edb), edb, nil
}

func (d explainDB) ExecContext(ctx context.Context, query string, params ...interface{}) (sql.Result, error) {
	defer d.explain(ctx, query, params)()
	return d.DB.(dbImpl).ExecContext(ctx, query, params...)
}
func (d explainDB) GetContext(ctx context.Context, dest interface{}, query string, params ...interface{}) error {
	defer d.explain(ctx, query, params)()
	return d.DB.(dbImpl).GetContext(ctx, dest, query, params...)
}
func (d explainDB) SelectContext(ctx context.Context, dest interface{}, query string, params ...interface{}) error {
	defer d.explain(ctx, query, params)()
	return d.DB.(dbImpl).SelectContext(ctx, dest, query, params...)
}
func (d explainDB) QueryxContext(ctx context.Context, query string, params ...interface{}) (*sqlx.Rows, error) {
	defer d.explain(ctx, query, params)()
	return d.DB.(dbImpl).QueryxContext(ctx, query, params...)
}

func (d explainDB) explain(ctx context.Context, query string, params []interface{}) func() {
	if _, ok := GetDB(ctx); !ok {
		return func() {}
	}

	q := ApplyParams(query, params...)
	if d.filter != "" {
		q := strings.ReplaceAll(q, "\n", " ")
		q = regexp.MustCompile(`\s+`).ReplaceAllString(q, " ")
		if !strings.Contains(q, d.filter) {
			return func() {}
		}
	}

	return func() {
		var (
			db      = Unwrap(MustGetDB(ctx))
			explain []string
			err     error
		)
		switch {
		default:
			err = errors.New("zdb.ExplainDB: unsupported driver: " + d.DB.DriverName())
		case PgSQL(ctx):
			err = db.Select(ctx, &explain, `explain analyze `+query, params...)
			for i := range explain {
				explain[i] = "\t" + explain[i]
			}
		case SQLite(ctx):
			var sqe []struct {
				ID, Parent, Notused int
				Detail              string
			}
			s := time.Now()
			err = db.Select(ctx, &sqe, `explain query plan `+query, params...)
			if len(sqe) > 0 {
				explain = make([]string, len(sqe)+1)
				for i := range sqe {
					explain[i] = "\t" + sqe[i].Detail
				}
				explain[len(sqe)] = "\tTime: " + time.Now().Sub(s).Round(1*time.Millisecond).String()
			}
		}
		if err != nil {
			fmt.Fprint(d.out, "QUERY:\n\t"+strings.ReplaceAll(q, "\n", "\n\t")+
				"\nERROR:\n\t"+err.Error()+"\n\n")
			return
		}

		fmt.Fprint(d.out, "QUERY:\n\t"+strings.ReplaceAll(q, "\n", "\n\t")+
			"\nEXPLAIN:\n"+strings.Join(explain, "\n")+
			"\n\n")
	}
}
