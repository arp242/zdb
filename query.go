package zdb

import (
	"fmt"

	"zgo.at/zstd/zstring"
)

// Query creates a new query.
//
// Everything between {{..}} is parsed as a conditional; for example {{query}}
// will only be added if the nth conds parameter is true.
//
// SQL parameters can be added as :name; sqlx's BindNamed is used.
func Query(db DB, query string, arg interface{}, conds ...bool) (string, []interface{}, error) {
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

	query, args, err := db.BindNamed(query, arg)
	if err != nil {
		return "", nil, fmt.Errorf("zdb.Query: %w", err)
	}
	return query, args, nil
}
