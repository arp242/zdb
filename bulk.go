package zdb

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"

	"zgo.at/zdb/internal/array"
)

// BulkInsert inserts as many rows as possible per query we send to the server.
type BulkInsert struct {
	mu       *sync.Mutex
	rows     uint16
	Limit    uint16
	ctx      context.Context
	table    string
	columns  []string
	insert   biBuilder
	errors   []error
	returned [][]any
}

// NewBulkInsert makes a new BulkInsert builder.
func NewBulkInsert(ctx context.Context, table string, columns []string) (BulkInsert, error) {
	var (
		psql = SQLDialect(ctx) == DialectPostgreSQL
		ins  = newBuilder(table, psql, columns...)
	)
	if psql {
		var types []struct {
			Name string `db:"column_name"`
			Type string `db:"data_type"`
		}
		err := Select(ctx, &types, `select column_name, data_type from information_schema.columns where table_name = $1`, table)
		if err != nil {
			return BulkInsert{}, err
		}

		ins.types = make([]string, len(columns))
		for _, t := range types {
			if i := slices.Index(columns, t.Name); i > -1 {
				switch t.Type {
				case "timestamp without time zone":
					t.Type = "timestamp"
				case "character varying":
					t.Type = "text"
				}
				ins.types[i] = t.Type
			}
		}
	}

	return BulkInsert{
		mu:  new(sync.Mutex),
		ctx: ctx,
		// SQLITE_MAX_VARIABLE_NUMBER: https://www.sqlite.org/limits.html
		Limit:   uint16(32766/len(columns) - 1),
		table:   table,
		columns: columns,
		insert:  ins,
	}, nil
}

// OnConflict sets the "on conflict [..]" part of the query. This needs to
// include the "on conflict" itself.
func (m *BulkInsert) OnConflict(c string) {
	m.insert.conflict = c
}

// Dump adds [zdb.DumpArgs] flags to any query BulkInsert runs.
func (m *BulkInsert) Dump(d DumpArg) {
	m.insert.dump = d
}

// Returning sets a column name in the "returning" part of the query.
//
// The values can be fetched with [Returned].
func (m *BulkInsert) Returning(columns ...string) {
	m.returned = make([][]any, 0, 32)
	m.insert.returning = columns
}

// Returned returns any rows that were returned; only useful of [Returning] was
// set.
//
// This will only return values once, for example:
//
//	Values(...)    // Inserts 3 rows
//	...
//	Returned()     // Return the 3 rows
//	Values(..)     // Inserts 1 row
//	Returned()     // Returns the 1 row
func (m *BulkInsert) Returned() [][]any {
	m.mu.Lock()
	defer func() {
		m.returned = m.returned[:0]
		m.mu.Unlock()
	}()
	return m.returned
}

// Values adds a set of values.
func (m *BulkInsert) Values(values ...any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.rows+1 >= m.Limit {
		m.doInsert()
	}
	m.insert.values(values...)
	m.rows++
}

// Finish the operation, returning any errors.
//
// This can be called more than once, in cases where you want to have some
// fine-grained control over when actual SQL is sent to the server.
func (m *BulkInsert) Finish() error {
	m.mu.Lock()
	if m.rows > 0 {
		m.doInsert()
	}
	m.mu.Unlock()
	return m.Errors()
}

// Errors returns all errors that have been encountered.
func (m BulkInsert) Errors() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.errors) == 0 {
		return nil
	}
	return fmt.Errorf("%d errors:\n%w", len(m.errors), errors.Join(m.errors...))
}

func (m *BulkInsert) doInsert() {
	query, params, err := m.insert.SQL()
	if err != nil {
		m.errors = append(m.errors, err)
		return
	}
	if len(m.insert.returning) > 0 {
		err = Select(m.ctx, &m.returned, query, params...)
	} else {
		err = Exec(m.ctx, query, params...)
	}
	if err != nil {
		m.errors = append(m.errors, err)
	}

	if m.insert.postgres {
		vals := make([][]any, len(m.columns))
		for i := range vals {
			vals[i] = make([]any, 0, 32)
		}
		m.insert.vals = vals
	} else {
		m.insert.vals = make([][]any, 0, 32)
	}

	m.rows = 0
}

type biBuilder struct {
	table     string
	conflict  string
	returning []string
	cols      []string
	types     []string
	vals      [][]any
	dump      DumpArg
	postgres  bool
	err       error
}

func newBuilder(table string, postgres bool, cols ...string) biBuilder {
	var vals [][]any
	if postgres {
		vals = make([][]any, len(cols))
		for i := range vals {
			vals[i] = make([]any, 0, 32)
		}
	} else {
		vals = make([][]any, 0, 32)
	}
	return biBuilder{
		table:    table,
		cols:     cols,
		vals:     vals,
		postgres: postgres,
	}
}

func (b *biBuilder) values(vals ...any) {
	if b.postgres {
		if len(vals) != len(b.cols) {
			b.err = fmt.Errorf("INSERT has more target columns than expressions")
			return
		}
		for i := range vals {
			b.vals[i] = append(b.vals[i], vals[i])
		}
	} else {
		b.vals = append(b.vals, vals)
	}
}

func (b *biBuilder) SQL(vals ...string) (string, []any, error) {
	if b.err != nil {
		return "", nil, b.err
	}

	var (
		s      strings.Builder
		params []any
	)
	s.WriteString(`insert into "`)
	s.WriteString(b.table)
	s.WriteString(`" (`)
	s.WriteString(strings.Join(b.cols, ","))
	s.WriteByte(')')

	// https://boringsql.com/posts/good-bad-arrays/#bulk-loading-with-arrays
	if b.postgres {
		s.WriteString(" select * from unnest(")

		p := make(map[string]any)
		for i, _ := range b.cols {
			ii := "p" + strconv.Itoa(i+1)
			if i > 0 {
				s.WriteString(", ")
			}
			s.WriteString(":")
			s.WriteString(ii)
			s.WriteString("::")
			s.WriteString(b.types[i])
			s.WriteString("[]")

			p[ii] = array.Array(b.vals[i])
		}
		params = []any{p}

		s.WriteString(")")
	} else {
		s.WriteString(" values ")
		params = make([]any, 0, len(b.vals)*len(b.cols)+1)
		for i := range b.vals {
			s.WriteString("(")
			for j := range b.vals[i] {
				s.WriteByte('?')
				if j < len(b.vals[i])-1 {
					s.WriteString(",")
				}
				params = append(params, b.vals[i][j])
			}
			s.WriteString(")")
			if i < len(b.vals)-1 {
				s.WriteString(",")
			}
		}
	}

	if b.dump > 0 {
		params = append(params, b.dump)
	}

	if b.conflict != "" {
		s.WriteRune(' ')
		s.WriteString(b.conflict)
	}

	if len(b.returning) > 0 {
		s.WriteString(" returning ")
		s.WriteString(strings.Join(b.returning, ","))
	}

	return s.String(), params, nil
}
