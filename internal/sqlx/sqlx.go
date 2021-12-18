package sqlx

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"reflect"
	"strings"
	"sync"

	"zgo.at/zdb/internal/sqlx/reflectx"
)

// Although the NameMapper is convenient, in practice it should not
// be relied on except for application code.  If you are writing a library
// that uses sqlx, you should be aware that the name mappings you expect
// can be overridden by your user's application.

// NameMapper is used to map column names to struct field names.  By default,
// it uses strings.ToLower to lowercase struct field names.  It can be set
// to whatever you want, but it is encouraged to be set before sqlx is used
// as name-to-field mappings are cached after first use on a type.
var NameMapper = strings.ToLower
var origMapper = reflect.ValueOf(NameMapper)

// Rather than creating on init, this is created when necessary so that
// importers have time to customize the NameMapper.
var mpr *reflectx.Mapper

// mprMu protects mpr.
var mprMu sync.Mutex

// mapper returns a valid mapper using the configured NameMapper func.
func mapper() *reflectx.Mapper {
	mprMu.Lock()
	defer mprMu.Unlock()

	if mpr == nil {
		mpr = reflectx.NewMapperFunc("db", NameMapper)
	} else if origMapper != reflect.ValueOf(NameMapper) {
		// if NameMapper has changed, create a new mapper
		mpr = reflectx.NewMapperFunc("db", NameMapper)
		origMapper = reflect.ValueOf(NameMapper)
	}
	return mpr
}

// isScannable takes the reflect.Type and the actual dest value and returns
// whether or not it's Scannable.  Something is scannable if:
//   * it is not a struct
//   * it implements sql.Scanner
//   * it has no exported fields
func isScannable(t reflect.Type) bool {
	if reflect.PtrTo(t).Implements(_scannerInterface) {
		return true
	}
	if t.Kind() != reflect.Struct {
		return true
	}

	// it's not important that we use the right mapper for this particular object,
	// we're only concerned on how many exported fields this struct has
	return len(mapper().TypeMap(t).Index) == 0
}

// determine if any of our extensions are unsafe
func isUnsafe(i interface{}) bool {
	switch v := i.(type) {
	case Row:
		return v.unsafe
	case *Row:
		return v.unsafe
	case Rows:
		return v.unsafe
	case *Rows:
		return v.unsafe
	case NamedStmt:
		return v.Stmt.unsafe
	case *NamedStmt:
		return v.Stmt.unsafe
	case Stmt:
		return v.unsafe
	case *Stmt:
		return v.unsafe
	case qStmt:
		return v.unsafe
	case *qStmt:
		return v.unsafe
	case DB:
		return v.unsafe
	case *DB:
		return v.unsafe
	case Tx:
		return v.unsafe
	case *Tx:
		return v.unsafe
	case sql.Rows, *sql.Rows:
		return false
	default:
		return false
	}
}

func mapperFor(i interface{}) *reflectx.Mapper {
	switch i := i.(type) {
	case DB:
		return i.Mapper
	case *DB:
		return i.Mapper
	case Tx:
		return i.Mapper
	case *Tx:
		return i.Mapper
	default:
		return mapper()
	}
}

var _scannerInterface = reflect.TypeOf((*sql.Scanner)(nil)).Elem()

// Row is a reimplementation of sql.Row in order to gain access to the underlying
// sql.Rows.Columns() data, necessary for StructScan.
type Row struct {
	err    error
	unsafe bool
	rows   *sql.Rows
	Mapper *reflectx.Mapper
}

// Scan is a fixed implementation of sql.Row.Scan, which does not discard the
// underlying error from the internal rows object if it exists.
func (r *Row) Scan(dest ...interface{}) error {
	if r.err != nil {
		return r.err
	}

	// TODO(bradfitz): for now we need to defensively clone all
	// []byte that the driver returned (not permitting
	// *RawBytes in Rows.Scan), since we're about to close
	// the Rows in our defer, when we return from this function.
	// the contract with the driver.Next(...) interface is that it
	// can return slices into read-only temporary memory that's
	// only valid until the next Scan/Close.  But the TODO is that
	// for a lot of drivers, this copy will be unnecessary.  We
	// should provide an optional interface for drivers to
	// implement to say, "don't worry, the []bytes that I return
	// from Next will not be modified again." (for instance, if
	// they were obtained from the network anyway) But for now we
	// don't care.
	defer r.rows.Close()
	for _, dp := range dest {
		if _, ok := dp.(*sql.RawBytes); ok {
			return errors.New("sql: RawBytes isn't allowed on Row.Scan")
		}
	}

	if !r.rows.Next() {
		if err := r.rows.Err(); err != nil {
			return err
		}
		return sql.ErrNoRows
	}
	err := r.rows.Scan(dest...)
	if err != nil {
		return err
	}
	// Make sure the query can be processed to completion with no errors.
	if err := r.rows.Close(); err != nil {
		return err
	}
	return nil
}

// Columns returns the underlying sql.Rows.Columns(), or the deferred error usually
// returned by Row.Scan()
func (r *Row) Columns() ([]string, error) {
	if r.err != nil {
		return []string{}, r.err
	}
	return r.rows.Columns()
}

// ColumnTypes returns the underlying sql.Rows.ColumnTypes(), or the deferred error
func (r *Row) ColumnTypes() ([]*sql.ColumnType, error) {
	if r.err != nil {
		return []*sql.ColumnType{}, r.err
	}
	return r.rows.ColumnTypes()
}

// Err returns the error encountered while scanning.
func (r *Row) Err() error {
	return r.err
}

// DB is a wrapper around sql.DB which keeps track of the driverName upon Open,
// used mostly to automatically bind named queries using the right bindvars.
type DB struct {
	*sql.DB
	driverName string
	unsafe     bool
	Mapper     *reflectx.Mapper
}

// NewDb returns a new sqlx DB wrapper for a pre-existing *sql.DB.  The
// driverName of the original database is required for named query support.
func NewDb(db *sql.DB, driverName string) *DB {
	return &DB{DB: db, driverName: driverName, Mapper: mapper()}
}

// DriverName returns the driverName passed to the Open function for this DB.
func (db *DB) DriverName() string {
	return db.driverName
}

// Open is the same as sql.Open, but returns an *sqlx.DB instead.
func Open(driverName, dataSourceName string) (*DB, error) {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, err
	}
	return &DB{DB: db, driverName: driverName, Mapper: mapper()}, err
}

// MapperFunc sets a new mapper for this db using the default sqlx struct tag
// and the provided mapper function.
func (db *DB) MapperFunc(mf func(string) string) {
	db.Mapper = reflectx.NewMapperFunc("db", mf)
}

// Rebind transforms a query from QUESTION to the DB driver's bindvar type.
func (db *DB) Rebind(query string) string {
	return Rebind(Placeholder(db.driverName), query)
}

// Unsafe returns a version of DB which will silently succeed to scan when
// columns in the SQL result have no fields in the destination struct.
// sqlx.Stmt and sqlx.Tx which are created from this DB will inherit its
// safety behavior.
func (db *DB) Unsafe() *DB {
	return &DB{DB: db.DB, driverName: db.driverName, unsafe: true, Mapper: db.Mapper}
}

// BindNamed binds a query using the DB driver's bindvar type.
func (db *DB) BindNamed(query string, arg interface{}) (string, []interface{}, error) {
	return bindNamedMapper(Placeholder(db.driverName), query, arg, db.Mapper)
}

// Beginx begins a transaction and returns an *sqlx.Tx instead of an *sql.Tx.
func (db *DB) Beginx() (*Tx, error) {
	tx, err := db.DB.Begin()
	if err != nil {
		return nil, err
	}
	return &Tx{Tx: tx, driverName: db.driverName, unsafe: db.unsafe, Mapper: db.Mapper}, err
}

// Conn is a wrapper around sql.Conn with extra functionality
type Conn struct {
	*sql.Conn
	driverName string
	unsafe     bool
	Mapper     *reflectx.Mapper
}

// Tx is an sqlx wrapper around sql.Tx with extra functionality
type Tx struct {
	*sql.Tx
	driverName string
	unsafe     bool
	Mapper     *reflectx.Mapper
}

// DriverName returns the driverName used by the DB which began this transaction.
func (tx *Tx) DriverName() string {
	return tx.driverName
}

// Rebind a query within a transaction's bindvar type.
func (tx *Tx) Rebind(query string) string {
	return Rebind(Placeholder(tx.driverName), query)
}

// Unsafe returns a version of Tx which will silently succeed to scan when
// columns in the SQL result have no fields in the destination struct.
func (tx *Tx) Unsafe() *Tx {
	return &Tx{Tx: tx.Tx, driverName: tx.driverName, unsafe: true, Mapper: tx.Mapper}
}

// BindNamed binds a query within a transaction's bindvar type.
func (tx *Tx) BindNamed(query string, arg interface{}) (string, []interface{}, error) {
	return bindNamedMapper(Placeholder(tx.driverName), query, arg, tx.Mapper)
}

// Stmt is an sqlx wrapper around sql.Stmt with extra functionality
type Stmt struct {
	*sql.Stmt
	unsafe bool
	Mapper *reflectx.Mapper
}

// Unsafe returns a version of Stmt which will silently succeed to scan when
// columns in the SQL result have no fields in the destination struct.
func (s *Stmt) Unsafe() *Stmt {
	return &Stmt{Stmt: s.Stmt, unsafe: true, Mapper: s.Mapper}
}

// qStmt is an unexposed wrapper which lets you use a Stmt as a Queryer & Execer by
// implementing those interfaces and ignoring the `query` argument.
type qStmt struct{ *Stmt }

func (q *qStmt) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return q.Stmt.Query(args...)
}

func (q *qStmt) Exec(query string, args ...interface{}) (sql.Result, error) {
	return q.Stmt.Exec(args...)
}

// Rows is a wrapper around sql.Rows which caches costly reflect operations
// during a looped StructScan
type Rows struct {
	*sql.Rows
	unsafe bool
	Mapper *reflectx.Mapper

	// these fields cache memory use for a rows during iteration w/ structScan
	started bool
	fields  [][]int
	values  []interface{}
}

// SliceScan using this Rows.
func (r *Rows) SliceScan() ([]interface{}, error) { return SliceScan(r) }

// MapScan using this Rows.
func (r *Rows) MapScan(dest map[string]interface{}) error { return MapScan(r, dest) }

// SliceScan using this Rows.
func (r *Row) SliceScan() ([]interface{}, error) { return SliceScan(r) }

// MapScan using this Rows.
func (r *Row) MapScan(dest map[string]interface{}) error { return MapScan(r, dest) }

// StructScan a single Row into dest.
func (r *Row) StructScan(dest interface{}) error {
	return r.scanAny(dest, true)
}

// ConnectContext to a database and verify with a ping.
func ConnectContext(ctx context.Context, driverName, dataSourceName string) (*DB, error) {
	db, err := Open(driverName, dataSourceName)
	if err != nil {
		return db, err
	}
	err = db.PingContext(ctx)
	return db, err
}

// SelectContext executes a query using the provided Queryer, and StructScans
// each row into dest, which must be a slice.  If the slice elements are
// scannable, then the result set must have only one column.  Otherwise,
// StructScan is used. The *sql.Rows are closed automatically.
// Any placeholder parameters are replaced with supplied args.
func SelectContext(ctx context.Context, q QueryerContext, dest interface{}, query string, args ...interface{}) error {
	rows, err := q.QueryxContext(ctx, query, args...)
	if err != nil {
		return err
	}
	// if something happens here, we want to make sure the rows are Closed
	defer rows.Close()
	return scanAll(rows, dest, false)
}

// PreparexContext prepares a statement.
//
// The provided context is used for the preparation of the statement, not for
// the execution of the statement.
func PreparexContext(ctx context.Context, p PreparerContext, query string) (*Stmt, error) {
	s, err := p.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	return &Stmt{Stmt: s, unsafe: isUnsafe(p), Mapper: mapperFor(p)}, err
}

// GetContext does a QueryRow using the provided Queryer, and scans the
// resulting row to dest.  If dest is scannable, the result must only have one
// column. Otherwise, StructScan is used.  Get will return sql.ErrNoRows like
// row.Scan would. Any placeholder parameters are replaced with supplied args.
// An error is returned if the result set is empty.
func GetContext(ctx context.Context, q QueryerContext, dest interface{}, query string, args ...interface{}) error {
	r := q.QueryRowxContext(ctx, query, args...)
	return r.scanAny(dest, false)
}

// LoadFileContext exec's every statement in a file (as a single call to Exec).
// LoadFileContext may return a nil *sql.Result if errors are encountered
// locating or reading the file at path.  LoadFile reads the entire file into
// memory, so it is not suitable for loading large data dumps, but can be useful
// for initializing schemas or loading indexes.
//
// FIXME: this does not really work with multi-statement files for mattn/go-sqlite3
// or the go-mysql-driver/mysql drivers;  pq seems to be an exception here.  Detecting
// this by requiring something with DriverName() and then attempting to split the
// queries will be difficult to get right, and its current driver-specific behavior
// is deemed at least not complex in its incorrectness.
func LoadFileContext(ctx context.Context, e ExecerContext, path string) (*sql.Result, error) {
	realpath, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	contents, err := ioutil.ReadFile(realpath)
	if err != nil {
		return nil, err
	}
	res, err := e.ExecContext(ctx, string(contents))
	return &res, err
}

// PrepareNamedContext returns an sqlx.NamedStmt
func (db *DB) PrepareNamedContext(ctx context.Context, query string) (*NamedStmt, error) {
	return prepareNamedContext(ctx, db, query)
}

// NamedQuery using this DB.
// Any named placeholder parameters are replaced with fields from arg.
func (db *DB) NamedQuery(ctx context.Context, query string, arg interface{}) (*Rows, error) {
	return NamedQuery(ctx, db, query, arg)
}

// NamedExec using this DB.
// Any named placeholder parameters are replaced with fields from arg.
func (db *DB) NamedExec(ctx context.Context, query string, arg interface{}) (sql.Result, error) {
	return NamedExec(ctx, db, query, arg)
}

// SelectContext using this DB.
// Any placeholder parameters are replaced with supplied args.
func (db *DB) SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	return SelectContext(ctx, db, dest, query, args...)
}

// GetContext using this DB.
// Any placeholder parameters are replaced with supplied args.
// An error is returned if the result set is empty.
func (db *DB) GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	return GetContext(ctx, db, dest, query, args...)
}

// PreparexContext returns an sqlx.Stmt instead of a sql.Stmt.
//
// The provided context is used for the preparation of the statement, not for
// the execution of the statement.
func (db *DB) PreparexContext(ctx context.Context, query string) (*Stmt, error) {
	return PreparexContext(ctx, db, query)
}

// QueryxContext queries the database and returns an *sqlx.Rows.
// Any placeholder parameters are replaced with supplied args.
func (db *DB) QueryxContext(ctx context.Context, query string, args ...interface{}) (*Rows, error) {
	r, err := db.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return &Rows{Rows: r, unsafe: db.unsafe, Mapper: db.Mapper}, err
}

// QueryRowxContext queries the database and returns an *sqlx.Row.
// Any placeholder parameters are replaced with supplied args.
func (db *DB) QueryRowxContext(ctx context.Context, query string, args ...interface{}) *Row {
	rows, err := db.DB.QueryContext(ctx, query, args...)
	return &Row{rows: rows, err: err, unsafe: db.unsafe, Mapper: db.Mapper}
}

// BeginTxx begins a transaction and returns an *sqlx.Tx instead of an
// *sql.Tx.
//
// The provided context is used until the transaction is committed or rolled
// back. If the context is canceled, the sql package will roll back the
// transaction. Tx.Commit will return an error if the context provided to
// BeginxContext is canceled.
func (db *DB) BeginTxx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	tx, err := db.DB.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &Tx{Tx: tx, driverName: db.driverName, unsafe: db.unsafe, Mapper: db.Mapper}, err
}

// Connx returns an *sqlx.Conn instead of an *sql.Conn.
func (db *DB) Connx(ctx context.Context) (*Conn, error) {
	conn, err := db.DB.Conn(ctx)
	if err != nil {
		return nil, err
	}

	return &Conn{Conn: conn, driverName: db.driverName, unsafe: db.unsafe, Mapper: db.Mapper}, nil
}

// BeginTxx begins a transaction and returns an *sqlx.Tx instead of an
// *sql.Tx.
//
// The provided context is used until the transaction is committed or rolled
// back. If the context is canceled, the sql package will roll back the
// transaction. Tx.Commit will return an error if the context provided to
// BeginxContext is canceled.
func (c *Conn) BeginTxx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	tx, err := c.Conn.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &Tx{Tx: tx, driverName: c.driverName, unsafe: c.unsafe, Mapper: c.Mapper}, err
}

// SelectContext using this Conn.
// Any placeholder parameters are replaced with supplied args.
func (c *Conn) SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	return SelectContext(ctx, c, dest, query, args...)
}

// GetContext using this Conn.
// Any placeholder parameters are replaced with supplied args.
// An error is returned if the result set is empty.
func (c *Conn) GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	return GetContext(ctx, c, dest, query, args...)
}

// PreparexContext returns an sqlx.Stmt instead of a sql.Stmt.
//
// The provided context is used for the preparation of the statement, not for
// the execution of the statement.
func (c *Conn) PreparexContext(ctx context.Context, query string) (*Stmt, error) {
	return PreparexContext(ctx, c, query)
}

// QueryxContext queries the database and returns an *sqlx.Rows.
// Any placeholder parameters are replaced with supplied args.
func (c *Conn) QueryxContext(ctx context.Context, query string, args ...interface{}) (*Rows, error) {
	r, err := c.Conn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return &Rows{Rows: r, unsafe: c.unsafe, Mapper: c.Mapper}, err
}

// QueryRowxContext queries the database and returns an *sqlx.Row.
// Any placeholder parameters are replaced with supplied args.
func (c *Conn) QueryRowxContext(ctx context.Context, query string, args ...interface{}) *Row {
	rows, err := c.Conn.QueryContext(ctx, query, args...)
	return &Row{rows: rows, err: err, unsafe: c.unsafe, Mapper: c.Mapper}
}

// Rebind a query within a Conn's bindvar type.
func (c *Conn) Rebind(query string) string {
	return Rebind(Placeholder(c.driverName), query)
}

// StmtxContext returns a version of the prepared statement which runs within a
// transaction. Provided stmt can be either *sql.Stmt or *sqlx.Stmt.
func (tx *Tx) StmtxContext(ctx context.Context, stmt interface{}) *Stmt {
	var s *sql.Stmt
	switch v := stmt.(type) {
	case Stmt:
		s = v.Stmt
	case *Stmt:
		s = v.Stmt
	case *sql.Stmt:
		s = v
	default:
		panic(fmt.Sprintf("non-statement type %v passed to Stmtx", reflect.ValueOf(stmt).Type()))
	}
	return &Stmt{Stmt: tx.StmtContext(ctx, s), Mapper: tx.Mapper}
}

// NamedStmtContext returns a version of the prepared statement which runs
// within a transaction.
func (tx *Tx) NamedStmtContext(ctx context.Context, stmt *NamedStmt) *NamedStmt {
	return &NamedStmt{
		QueryString: stmt.QueryString,
		Params:      stmt.Params,
		Stmt:        tx.StmtxContext(ctx, stmt.Stmt),
	}
}

// PreparexContext returns an sqlx.Stmt instead of a sql.Stmt.
//
// The provided context is used for the preparation of the statement, not for
// the execution of the statement.
func (tx *Tx) PreparexContext(ctx context.Context, query string) (*Stmt, error) {
	return PreparexContext(ctx, tx, query)
}

// PrepareNamedContext returns an sqlx.NamedStmt
func (tx *Tx) PrepareNamedContext(ctx context.Context, query string) (*NamedStmt, error) {
	return prepareNamedContext(ctx, tx, query)
}

// QueryxContext within a transaction and context.
// Any placeholder parameters are replaced with supplied args.
func (tx *Tx) QueryxContext(ctx context.Context, query string, args ...interface{}) (*Rows, error) {
	r, err := tx.Tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return &Rows{Rows: r, unsafe: tx.unsafe, Mapper: tx.Mapper}, err
}

// SelectContext within a transaction and context.
// Any placeholder parameters are replaced with supplied args.
func (tx *Tx) SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	return SelectContext(ctx, tx, dest, query, args...)
}

// GetContext within a transaction and context.
// Any placeholder parameters are replaced with supplied args.
// An error is returned if the result set is empty.
func (tx *Tx) GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	return GetContext(ctx, tx, dest, query, args...)
}

// QueryRowxContext within a transaction and context.
// Any placeholder parameters are replaced with supplied args.
func (tx *Tx) QueryRowxContext(ctx context.Context, query string, args ...interface{}) *Row {
	rows, err := tx.Tx.QueryContext(ctx, query, args...)
	return &Row{rows: rows, err: err, unsafe: tx.unsafe, Mapper: tx.Mapper}
}

// NamedExec using this Tx.
// Any named placeholder parameters are replaced with fields from arg.
func (tx *Tx) NamedExec(ctx context.Context, query string, arg interface{}) (sql.Result, error) {
	return NamedExec(ctx, tx, query, arg)
}

// SelectContext using the prepared statement.
// Any placeholder parameters are replaced with supplied args.
func (s *Stmt) SelectContext(ctx context.Context, dest interface{}, args ...interface{}) error {
	return SelectContext(ctx, &qStmt{s}, dest, "", args...)
}

// GetContext using the prepared statement.
// Any placeholder parameters are replaced with supplied args.
// An error is returned if the result set is empty.
func (s *Stmt) GetContext(ctx context.Context, dest interface{}, args ...interface{}) error {
	return GetContext(ctx, &qStmt{s}, dest, "", args...)
}

// QueryRowxContext using this statement.
// Any placeholder parameters are replaced with supplied args.
func (s *Stmt) QueryRowxContext(ctx context.Context, args ...interface{}) *Row {
	qs := &qStmt{s}
	return qs.QueryRowxContext(ctx, "", args...)
}

// QueryxContext using this statement.
// Any placeholder parameters are replaced with supplied args.
func (s *Stmt) QueryxContext(ctx context.Context, args ...interface{}) (*Rows, error) {
	qs := &qStmt{s}
	return qs.QueryxContext(ctx, "", args...)
}

func (q *qStmt) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return q.Stmt.QueryContext(ctx, args...)
}

func (q *qStmt) QueryxContext(ctx context.Context, query string, args ...interface{}) (*Rows, error) {
	r, err := q.Stmt.QueryContext(ctx, args...)
	if err != nil {
		return nil, err
	}
	return &Rows{Rows: r, unsafe: q.Stmt.unsafe, Mapper: q.Stmt.Mapper}, err
}

func (q *qStmt) QueryRowxContext(ctx context.Context, query string, args ...interface{}) *Row {
	rows, err := q.Stmt.QueryContext(ctx, args...)
	return &Row{rows: rows, err: err, unsafe: q.Stmt.unsafe, Mapper: q.Stmt.Mapper}
}

func (q *qStmt) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return q.Stmt.ExecContext(ctx, args...)
}
