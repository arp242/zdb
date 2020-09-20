// Package bulk provides helpers for bulk SQL operations.
package bulk

import (
	"context"
	"fmt"
	"strings"

	"zgo.at/zdb"
)

type builder struct {
	table string
	post  string
	cols  []string
	vals  [][]interface{}
}

func newBuilder(table string, cols ...string) builder {
	return builder{table: table, cols: cols, vals: make([][]interface{}, 0, 32)}
}

func (b *builder) values(vals ...interface{}) {
	b.vals = append(b.vals, vals)
}

func (b *builder) SQL(vals ...string) (string, []interface{}) {
	var s strings.Builder
	s.WriteString("insert into ")
	s.WriteString(b.table)
	s.WriteString(" (")

	s.WriteString(strings.Join(b.cols, ","))
	s.WriteString(") values ")

	offset := 0
	var args []interface{}
	for i := range b.vals {
		s.WriteString("(")
		for j := range b.vals[i] {
			offset++
			s.WriteString(fmt.Sprintf("$%d", offset))
			if j < len(b.vals[i])-1 {
				s.WriteString(",")
			}
			args = append(args, b.vals[i][j])
		}
		s.WriteString(")")
		if i < len(b.vals)-1 {
			s.WriteString(",")
		}
	}

	if b.post != "" {
		s.WriteRune(' ')
		s.WriteString(b.post)
	}

	return s.String(), args
}

// Insert as many rows as possible per query we send to the server.
type Insert struct {
	rows    uint16
	limit   uint16
	ctx     context.Context
	table   string
	columns []string
	insert  builder
	errors  []string
}

// NewInsert makes a new Insert builder.
func NewInsert(ctx context.Context, table string, columns []string) Insert {
	return Insert{
		ctx: ctx,
		// SQLITE_MAX_VARIABLE_NUMBER: https://www.sqlite.org/limits.html
		limit:   uint16(999/len(columns) - 1),
		table:   table,
		columns: columns,
		insert:  newBuilder(table, columns...),
	}
}

// OnConflict sets the "on conflict [..]" part of the query. This needs to
// include the "on conflict" itself.
func (m *Insert) OnConflict(c string) {
	m.insert.post = c
}

// Values adds a set of values.
func (m *Insert) Values(values ...interface{}) {
	m.insert.values(values...)
	m.rows++

	if m.rows >= m.limit {
		m.doInsert()
	}
}

// Finish the operation, returning any errors.
func (m *Insert) Finish() error {
	if m.rows > 0 {
		m.doInsert()
	}

	if len(m.errors) == 0 {
		return nil
	}

	return fmt.Errorf("%d errors: %s", len(m.errors), strings.Join(m.errors, "\n"))
}

func (m *Insert) doInsert() {
	query, args := m.insert.SQL()
	_, err := zdb.MustGet(m.ctx).ExecContext(m.ctx, query, args...)
	if err != nil {
		m.errors = append(m.errors, fmt.Sprintf("%v (query=%q) (args=%q)", err, query, args))
	}

	//m.insert = newBuilder(m.table, m.columns...)
	m.insert.vals = make([][]interface{}, 0, 32)
	m.rows = 0
}
