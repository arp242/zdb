package zdb

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/jmoiron/sqlx"
	"zgo.at/zstd/zstring"
)

// Query creates a new query.
//
// Everything between {{..}} is parsed as a conditional; for example {{query}}
// will only be added if the nth conds parameter is true.
//
// SQL parameters can be added as :name; sqlx's BindNamed is used.
func Query(ctx context.Context, query string, arg interface{}, conds ...bool) (string, []interface{}, error) {
	pairs := zstring.IndexPairs(query, "{{", "}}")
	if len(pairs) != len(conds) {
		return "", nil, fmt.Errorf("zdb.Query: len(pairs)=%d != len(conds)=%d", len(pairs), len(conds))
	}

	for i, p := range pairs {
		s := p[0]
		e := p[1]

		if conds[len(conds)-1-i] {
			query = query[:s] + query[s+2:]
			query = query[:e-2] + query[e:]
		} else {
			query = query[:s] + query[e+2:]
		}
	}

	query, args, err := sqlx.Named(query, arg)
	if err != nil {
		return "", nil, fmt.Errorf("zdb.Query: %w", err)
	}

	query, args, err = sqlx.In(query, args...)
	if err != nil {
		return "", nil, fmt.Errorf("zdb.Query: %w", err)
	}
	return MustGet(ctx).Rebind(query), args, nil
}

// QuerySelect is like Query(), but will also run SelectContext() and scan in to
// desc.
func QuerySelect(ctx context.Context, dest interface{}, query string, arg interface{}, conds ...bool) error {
	query, args, err := Query(ctx, query, arg, conds...)
	if err != nil {
		return err
	}
	return MustGet(ctx).SelectContext(ctx, dest, query, args...)
}

// QueryGet is like Query(), but will also run GetContext() and scan in to
// desc.
func QueryGet(ctx context.Context, dest interface{}, query string, arg interface{}, conds ...bool) error {
	query, args, err := Query(ctx, query, arg, conds...)
	if err != nil {
		return err
	}
	return MustGet(ctx).GetContext(ctx, dest, query, args...)
}

// QueryExec is like Query(), but will also run ExecContext().
func QueryExec(ctx context.Context, dest interface{}, query string, arg interface{}, conds ...bool) (sql.Result, error) {
	query, args, err := Query(ctx, query, arg, conds...)
	if err != nil {
		return nil, err
	}
	return MustGet(ctx).ExecContext(ctx, query, args...)
}
