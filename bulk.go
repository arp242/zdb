package zdb

import (
	"context"
	"fmt"
	"strings"
	"sync"
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
	errors   []string
	returned [][]any
}

// NewBulkInsert makes a new BulkInsert builder.
func NewBulkInsert(ctx context.Context, table string, columns []string) BulkInsert {
	return BulkInsert{
		mu:  new(sync.Mutex),
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
	return fmt.Errorf("%d errors: %s", len(m.errors), strings.Join(m.errors, "\n"))
}

func (m *BulkInsert) doInsert() {
	query, params := m.insert.SQL()
	var err error
	if len(m.insert.returning) > 0 {
		err = Select(m.ctx, &m.returned, query, params...)
	} else {
		err = Exec(m.ctx, query, params...)
	}
	if err != nil {
		m.errors = append(m.errors, err.Error())
	}

	m.insert.vals = make([][]any, 0, 32)
	m.rows = 0
}

type biBuilder struct {
	table     string
	conflict  string
	returning []string
	cols      []string
	vals      [][]any
	dump      DumpArg
}

func newBuilder(table string, cols ...string) biBuilder {
	return biBuilder{table: table, cols: cols, vals: make([][]any, 0, 32)}
}

func (b *biBuilder) values(vals ...any) {
	b.vals = append(b.vals, vals)
}

func (b *biBuilder) SQL(vals ...string) (string, []any) {
	var s strings.Builder
	s.WriteString(`insert into "`)
	s.WriteString(b.table)
	s.WriteString(`" (`)

	s.WriteString(strings.Join(b.cols, ","))
	s.WriteString(") values ")

	params := make([]any, 0, len(b.vals)*len(b.cols)+1)
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

	return s.String(), params
}
