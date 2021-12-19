package test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"

	"zgo.at/zdb/drivers"
)

func init() {
	sql.Register("test", testSQLDriver{})
}

func Use() func() {
	save := drivers.Test()
	drivers.RegisterDriver(testDriver{})
	return save
}

type (
	testDriver    struct{}
	testSQLDriver struct{}
	TestConn      struct{}
	TestStmt      struct{ query string }
	TestTx        struct{}
	TestResult    struct{}
	TestRows      struct {
		cols []string
		rows [][]string
	}
)

func (testDriver) Name() string         { return "test" }
func (testDriver) Dialect() string      { return "postgresql" }
func (testDriver) ErrUnique(error) bool { return false }
func (testDriver) Connect(ctx context.Context, connect string, create bool) (*sql.DB, bool, error) {
	db, err := sql.Open("test", "")
	return db, true, err
}

func (testSQLDriver) Open(name string) (driver.Conn, error) {
	return TestConn{}, nil
}

func (TestConn) Prepare(query string) (driver.Stmt, error) { return TestStmt{query: query}, nil }
func (TestConn) Close() error                              { return nil }
func (TestConn) Begin() (driver.Tx, error)                 { return TestTx{}, nil }

func (TestStmt) Close() error                                    { return nil }
func (TestStmt) NumInput() int                                   { return 0 }
func (TestStmt) Exec(args []driver.Value) (driver.Result, error) { return TestResult{}, nil }
func (s TestStmt) Query(args []driver.Value) (driver.Rows, error) {
	// TODO: allow configuring this kind of thing.
	if s.query == "show server_version" {
		return TestRows{
			cols: []string{"server_version"},
			rows: [][]string{{"12.0"}},
		}, nil
	}
	return TestRows{}, nil
}

func (TestTx) Commit() error   { return nil }
func (TestTx) Rollback() error { return nil }

func (TestResult) LastInsertId() (int64, error) { return 0, nil }
func (TestResult) RowsAffected() (int64, error) { return 0, nil }

func (t TestRows) Columns() []string { return t.cols }
func (TestRows) Close() error        { return nil }
func (t TestRows) Next(dest []driver.Value) error {
	if len(t.rows) == 0 {
		return io.EOF
	}
	if len(dest) != len(t.rows[0]) {
		return errors.New("TestRows: different len")
	}

	row := t.rows[0]
	for i := range row {
		dest[i] = row[i]
	}

	t.rows = t.rows[1:]
	return nil
}
