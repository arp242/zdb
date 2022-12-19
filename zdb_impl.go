package zdb

// This file contains the implementations for the DB interface in zdb.go.
//
// How this works:
//
// - We use sqlx.DB internally, but everything we return is a "zDB" or zTX". For
//   users of zdb, there is no sqlx (the sqlx we use is a heavily modified
//   version in internal/, which should eventually go away entirely).
//
// - Most of the actual implementations are in the *Impl() functions (the name
//   avoids some conflicts with keywords, packages, and common variables).
//
// - zDB.Get(), zTX.Get(), and the package-level Get() all call these *Impl()
//   functions.
//
// This is a little bit convoluted, but it solves some issues:
//
// - Some of the added methods (zdb.Prepare()) aren't on the sqlx.DB type, and
//   I would prefer a consistent API which is always the same (i.e. zDB.Get()
//   and zdb.Get() are exactly identical).
//
// - sqlx.DB and sqlx.Tx don't share some methods which makes passing around a
//   single DB interface hard (sqlx.DB has no Commit() and sqlx.Tx has no
//   Begin(). The zdb wrappers shims these methods out, making various things
//   easier.
//
// - Wrapping a sqlx.DB is easy, for example for logging or whatnot, but also
//   making this wrapper work on transaction is harder, because of the above
//   issue.
//
// - For zDB.Load() / zdb.Load() we need a fs.FS, but I don't really like the
//   idea of having to pass that around all the time, and I also don't really want
//   to add it to the context. But we can add it to the zDB.

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"strings"

	"zgo.at/zdb/internal/sqlx"
)

var ctxkey = &struct{ n string }{"zdb"}

type zDB struct {
	db      *sqlx.DB
	dialect Dialect
	queryFS fs.FS
}

func (db zDB) queryFiles() fs.FS              { return db.queryFS }
func (db zDB) rebind(query string) string     { return db.db.Rebind(query) }
func (db zDB) ping(ctx context.Context) error { return db.db.PingContext(ctx) }
func (db zDB) driverName() string             { return db.db.DriverName() }

func (db zDB) DBSQL() *sql.DB                               { return db.db.DB }
func (db zDB) SQLDialect() Dialect                          { return db.dialect }
func (db zDB) Info(ctx context.Context) (ServerInfo, error) { return infoImpl(ctx, db) }
func (db zDB) Close() error                                 { return db.db.Close() }

func (db zDB) Exec(ctx context.Context, query string, params ...any) error {
	return execImpl(ctx, db, query, params...)
}
func (db zDB) NumRows(ctx context.Context, query string, params ...any) (int64, error) {
	return numRowsImpl(ctx, db, query, params...)
}
func (db zDB) InsertID(ctx context.Context, idColumn, query string, params ...any) (int64, error) {
	return insertIDImpl(ctx, db, idColumn, query, params...)
}
func (db zDB) Get(ctx context.Context, dest any, query string, params ...any) error {
	return getImpl(ctx, db, dest, query, params...)
}
func (db zDB) Select(ctx context.Context, dest any, query string, params ...any) error {
	return selectImpl(ctx, db, dest, query, params...)
}
func (db zDB) Query(ctx context.Context, query string, params ...any) (*Rows, error) {
	return queryImpl(ctx, db, query, params...)
}

func (db zDB) TX(ctx context.Context, fn func(context.Context) error) error {
	return txImpl(ctx, db, fn)
}
func (db zDB) Begin(ctx context.Context, opts ...beginOpt) (context.Context, DB, error) {
	return beginImpl(ctx, &db, opts...)
}
func (db zDB) Rollback() error { return errors.New("cannot rollback, as this is not a transaction") }
func (db zDB) Commit() error   { return errors.New("cannot commit, as this is not a transaction") }

func (db zDB) ExecContext(ctx context.Context, query string, params ...any) (sql.Result, error) {
	return db.db.ExecContext(ctx, query, params...)
}
func (db zDB) GetContext(ctx context.Context, dest any, query string, params ...any) error {
	return db.db.GetContext(ctx, dest, query, params...)
}
func (db zDB) SelectContext(ctx context.Context, dest any, query string, params ...any) error {
	return db.db.SelectContext(ctx, dest, query, params...)
}
func (db zDB) QueryxContext(ctx context.Context, query string, params ...any) (*sqlx.Rows, error) {
	return db.db.QueryxContext(ctx, query, params...)
}

type zTX struct {
	db     *sqlx.Tx
	parent *zDB // Needed for Close() and queryFiles()
}

func (db zTX) queryFiles() fs.FS              { return db.parent.queryFiles() }
func (db zTX) rebind(query string) string     { return db.parent.rebind(query) }
func (db zTX) ping(ctx context.Context) error { return db.parent.ping(ctx) }
func (db zTX) driverName() string             { return db.parent.driverName() }

func (db zTX) DBSQL() *sql.DB                               { return db.parent.DBSQL() }
func (db zTX) SQLDialect() Dialect                          { return db.parent.dialect }
func (db zTX) Info(ctx context.Context) (ServerInfo, error) { return db.parent.Info(ctx) }
func (db zTX) Close() error {
	// Not sure if this is actually needed, but can't hurt. Especially for
	// MariaDB with its stupid "autocommit mode" it's probably a good idea to do
	// an explicit rollback.
	err := db.Rollback()
	if err != nil {
		return err
	}
	return db.parent.Close()
}

func (db zTX) Exec(ctx context.Context, query string, params ...any) error {
	return execImpl(ctx, db, query, params...)
}
func (db zTX) NumRows(ctx context.Context, query string, params ...any) (int64, error) {
	return numRowsImpl(ctx, db, query, params...)
}
func (db zTX) InsertID(ctx context.Context, idColumn, query string, params ...any) (int64, error) {
	return insertIDImpl(ctx, db, idColumn, query, params...)
}
func (db zTX) Get(ctx context.Context, dest any, query string, params ...any) error {
	return getImpl(ctx, db, dest, query, params...)
}
func (db zTX) Select(ctx context.Context, dest any, query string, params ...any) error {
	return selectImpl(ctx, db, dest, query, params...)
}
func (db zTX) Query(ctx context.Context, query string, params ...any) (*Rows, error) {
	return queryImpl(ctx, db, query, params...)
}

func (db zTX) TX(ctx context.Context, fn func(context.Context) error) error {
	return ErrTransactionStarted
}
func (db zTX) Begin(ctx context.Context, opt ...beginOpt) (context.Context, DB, error) {
	return ctx, db, ErrTransactionStarted
}
func (db zTX) Rollback() error { return db.db.Rollback() }
func (db zTX) Commit() error   { return db.db.Commit() }

func (db zTX) ExecContext(ctx context.Context, query string, params ...any) (sql.Result, error) {
	return db.db.ExecContext(ctx, query, params...)
}
func (db zTX) GetContext(ctx context.Context, dest any, query string, params ...any) error {
	return db.db.GetContext(ctx, dest, query, params...)
}
func (db zTX) SelectContext(ctx context.Context, dest any, query string, params ...any) error {
	return db.db.SelectContext(ctx, dest, query, params...)
}
func (db zTX) QueryxContext(ctx context.Context, query string, params ...any) (*sqlx.Rows, error) {
	return db.db.QueryxContext(ctx, query, params...)
}

// Actual implementations
// ----------------------

var stderr io.Writer = os.Stderr

type (
	// ServerInfo contains information about the SQL server.
	ServerInfo struct {
		Version    ServerVersion
		DriverName string
		Dialect    Dialect
	}
	// ServerVersion represents a database version.
	ServerVersion string
)

// AtLeast reports if this version is at least version want.
func (v ServerVersion) AtLeast(want ServerVersion) bool { return want <= v }

func infoImpl(ctx context.Context, db DB) (ServerInfo, error) {
	udb := Unwrap(db)
	info := ServerInfo{Dialect: SQLDialect(ctx)}

	p, ok := udb.(interface{ ping(context.Context) error })
	if ok {
		err := p.ping(ctx)
		if err != nil {
			return ServerInfo{}, nil
		}
	}

	if d, ok := udb.(interface{ driverName() string }); ok {
		info.DriverName = d.driverName()
	}

	var (
		v   string
		err error
	)
	switch db.SQLDialect() {
	case DialectSQLite:
		err = Get(ctx, &v, `select sqlite_version()`)
	case DialectMariaDB:
		err = Get(ctx, &v, `select version()`)
		v = strings.TrimSuffix(v, "-MariaDB")
	case DialectPostgreSQL:
		err = Get(ctx, &v, `show server_version`)
	}
	if err != nil {
		return ServerInfo{}, fmt.Errorf("zdb.Info: %w", err)
	}

	info.Version = ServerVersion(v)
	return info, nil
}

// TODO: this could be cached, but if the FS is an os.DirFS then it may have
// changes on the filesystem (being able to change queries w/o recompile is
// nice).
//
// TODO: implement .gotxt support here too? The {{ .. }} from our own
// mini-template syntax will clash though.
func loadImpl(db DB, name string) (string, bool, error) {
	fsys := db.(interface{ queryFiles() fs.FS }).queryFiles()
	if fsys == nil {
		return "", false, errors.New("zdb.Load: Files not set")
	}

	name = strings.TrimSuffix(name, ".gotxt")
	name = strings.TrimSuffix(name, ".sql")
	q, path, err := findFile(fsys, insertDialect(db, name)...)
	if err != nil {
		return "", false, fmt.Errorf("zdb.Load: %w", err)
	}

	var b strings.Builder
	b.WriteString("/* ")
	b.WriteString(name)
	b.WriteString(" */\n")

	// Strip out "-- " comments at the start of lines; don't attempt to strip
	// other comments, as it requires parsing the SQL and this is "good enough"
	// to allow some comments in the SQL files, while also not cluttering the
	// SQL stats/logs with them.
	for _, line := range bytes.Split(bytes.TrimSpace(q), []byte("\n")) {
		if !bytes.HasPrefix(bytes.TrimSpace(line), []byte("--")) {
			b.Write(line)
			b.WriteRune('\n')
		}
	}
	return b.String(), strings.HasSuffix(path, ".gotxt"), nil
}

type dbImpl interface {
	ExecContext(ctx context.Context, query string, params ...any) (sql.Result, error)
	GetContext(ctx context.Context, dest any, query string, params ...any) error
	SelectContext(ctx context.Context, dest any, query string, params ...any) error
	QueryxContext(ctx context.Context, query string, params ...any) (*sqlx.Rows, error)
}

func beginImpl(ctx context.Context, db DB, opts ...beginOpt) (context.Context, DB, error) {
	// Could use savepoints, but that's probably more confusing than anything
	// else: almost all of the time you want the outermost transaction to be
	// completed in full or not at all. If you really want savepoints then you
	// can do it manually.
	if tx, ok := Unwrap(db).(*zTX); ok {
		return ctx, tx, ErrTransactionStarted
	}

	var txopt *sql.TxOptions
	for _, o := range opts {
		o(txopt)
	}

	tx, err := db.(*zDB).db.BeginTxx(ctx, txopt)
	if err != nil {
		return nil, nil, fmt.Errorf("zdb.Begin: %w", err)
	}

	ztx := &zTX{db: tx, parent: Unwrap(db).(*zDB)}
	return WithDB(ctx, ztx), ztx, nil
}

func txImpl(ctx context.Context, db DB, fn func(context.Context) error) error {
	txctx, tx, err := db.Begin(ctx)
	if err == ErrTransactionStarted {
		err := fn(txctx)
		if err != nil {
			return fmt.Errorf("zdb.TX fn: %w", err)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("zdb.TX: %w", err)
	}

	defer tx.Rollback()

	err = fn(txctx)
	if err != nil {
		return fmt.Errorf("zdb.TX fn: %w", err)
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("zdb.TX commit: %w", err)
	}
	return nil
}

func execImpl(ctx context.Context, db DB, query string, params ...any) error {
	query, params, err := prepareImpl(ctx, db, query, params...)
	if err != nil {
		return fmt.Errorf("zdb.Exec: %w", err)
	}
	_, err = db.(dbImpl).ExecContext(ctx, query, params...)
	if err != nil {
		return fmt.Errorf("zdb.Exec: %w", err)
	}
	return nil
}

func numRowsImpl(ctx context.Context, db DB, query string, params ...any) (int64, error) {
	query, params, err := prepareImpl(ctx, db, query, params...)
	if err != nil {
		return 0, fmt.Errorf("zdb.NumRows: %w", err)
	}
	r, err := db.(dbImpl).ExecContext(ctx, query, params...)
	if err != nil {
		return 0, fmt.Errorf("zdb.NumRows: %w", err)
	}
	n, err := r.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("zdb.NumRows: %w", err)
	}
	return n, nil
}

func insertIDImpl(ctx context.Context, db DB, idColumn, query string, params ...any) (int64, error) {
	query, params, err := prepareImpl(ctx, db, query, params...)
	if err != nil {
		return 0, fmt.Errorf("zdb.InsertID: %w", err)
	}

	var id []int64
	err = db.(dbImpl).SelectContext(ctx, &id, query+" returning "+idColumn, params...)
	if err != nil {
		return 0, fmt.Errorf("zdb.InsertID: %w", err)
	}
	return id[len(id)-1], nil
}

func selectImpl(ctx context.Context, db DB, dest any, query string, params ...any) error {
	query, params, err := prepareImpl(ctx, db, query, params...)
	if err != nil {
		return fmt.Errorf("zdb.Select: %w", err)
	}
	err = db.(dbImpl).SelectContext(ctx, dest, query, params...)
	if err != nil {
		return fmt.Errorf("zdb.Select: %w", err)
	}
	return nil
}

func getImpl(ctx context.Context, db DB, dest any, query string, params ...any) error {
	query, params, err := prepareImpl(ctx, db, query, params...)
	if err != nil {
		return fmt.Errorf("zdb.Get: %w", err)
	}
	err = db.(dbImpl).GetContext(ctx, dest, query, params...)
	if err != nil {
		return fmt.Errorf("zdb.Get: %w", err)
	}
	return nil
}

func queryImpl(ctx context.Context, db DB, query string, params ...any) (*Rows, error) {
	query, params, err := prepareImpl(ctx, db, query, params...)
	if err != nil {
		return nil, fmt.Errorf("zdb.Query: %w", err)
	}
	r, err := db.(dbImpl).QueryxContext(ctx, query, params...)
	if err != nil {
		return nil, fmt.Errorf("zdb.Query: %w", err)
	}
	return &Rows{r}, nil
}
