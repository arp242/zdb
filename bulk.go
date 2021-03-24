package zdb

import (
	"context"
	"fmt"
	"strings"
)

// BulkInsert inserts as many rows as possible per query we send to the server.
type BulkInsert struct {
	rows    uint16
	Limit   uint16
	ctx     context.Context
	table   string
	columns []string
	insert  biBuilder
	errors  []string
}

// NewBulkInsert makes a new BulkInsert builder.
func NewBulkInsert(ctx context.Context, table string, columns []string) BulkInsert {
	return BulkInsert{
		ctx: ctx,
		// SQLITE_MAX_VARIABLE_NUMBER: https://www.sqlite.org/limits.html
		Limit:   uint16(32766/len(columns) - 1),
		table:   table,
		columns: columns,
		insert:  newBuilder(table, columns...),
	}
}

// OnConflict sets the "on conflict [..]" part of the query. This needs to
// include the "on conflict" itself.
func (m *BulkInsert) OnConflict(c string) {
	m.insert.post = c
}

// Values adds a set of values.
func (m *BulkInsert) Values(values ...interface{}) {
	m.insert.values(values...)
	m.rows++

	if m.rows >= m.Limit {
		m.doInsert()
	}
}

// Finish the operation, returning any errors.
func (m *BulkInsert) Finish() error {
	if m.rows > 0 {
		m.doInsert()
	}

	if len(m.errors) == 0 {
		return nil
	}
	return fmt.Errorf("%d errors: %s", len(m.errors), strings.Join(m.errors, "\n"))
}

func (m *BulkInsert) doInsert() {
	query, params := m.insert.SQL()
	err := Exec(m.ctx, query, params...)
	if err != nil {
		fmtParams := make([]interface{}, 0, len(params))
		for _, p := range params {
			fmtParams = append(fmtParams, formatParam(p, true))
		}
		m.errors = append(m.errors, fmt.Sprintf("%v (query=%q) (params=%v)", err, query, fmtParams))
	}

	m.insert.vals = make([][]interface{}, 0, 32)
	m.rows = 0
}

type biBuilder struct {
	table string
	post  string
	cols  []string
	vals  [][]interface{}
}

func newBuilder(table string, cols ...string) biBuilder {
	return biBuilder{table: table, cols: cols, vals: make([][]interface{}, 0, 32)}
}

func (b *biBuilder) values(vals ...interface{}) {
	b.vals = append(b.vals, vals)
}

func (b *biBuilder) SQL(vals ...string) (string, []interface{}) {
	var s strings.Builder
	s.WriteString("insert into ")
	s.WriteString(b.table)
	s.WriteString(" (")

	s.WriteString(strings.Join(b.cols, ","))
	s.WriteString(") values ")

	offset := 0
	var params []interface{}
	for i := range b.vals {
		s.WriteString("(")
		for j := range b.vals[i] {
			offset++
			s.WriteString(fmt.Sprintf("$%d", offset))
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

	if b.post != "" {
		s.WriteRune(' ')
		s.WriteString(b.post)
	}

	return s.String(), params
}
