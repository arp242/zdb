package zdb

import (
	"context"
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"zgo.at/utils/sliceutil"
	"zgo.at/zlog"
)

var ctxkey = &struct{ n string }{"d"}

// DB wraps sqlx.DB so we can add transactions and logging.
type DB interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	Rebind(query string) string
	SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error

	// BeginTxx(ctx context.Context, opts *sql.TxOptions) (*sqlx.Tx, error)
	// Rollback() error
	// Commit() error
}

func With(ctx context.Context, db DB) context.Context {
	return context.WithValue(ctx, ctxkey, db)
}

// MustGet gets the DB from the context, panicking if this fails.
func MustGet(ctx context.Context) DB {
	db, ok := ctx.Value(ctxkey).(DB)
	if !ok {
		panic("zdb.MustGet: no ctxkey value")
	}
	return db
}

// Begin a new transaction.
func Begin(ctx context.Context) (context.Context, *sqlx.Tx, error) {
	// TODO: to supported nested transactions we need to wrap it.
	// Also see: https://github.com/heetch/sqalx/blob/master/sqalx.go
	db := MustGet(ctx)
	if tx, ok := db.(*sqlx.Tx); ok {
		return ctx, tx, nil
	}

	tx, err := db.(*sqlx.DB).BeginTxx(ctx, nil)
	return context.WithValue(ctx, ctxkey, tx), tx, err
}

// Connect to database.
func Connect(connect string, pgSQL bool, schema []byte, migrations map[string][]byte, migpath string) (*sqlx.DB, error) {
	var db *sqlx.DB
	if pgSQL {
		var err error
		db, err = sqlx.Connect("postgres", connect)
		if err != nil {
			return nil, errors.Wrap(err, "sqlx.Connect postgres")
		}

		// db.SetConnMaxLifetime()
		db.SetMaxIdleConns(25) // Default 2
		db.SetMaxOpenConns(25) // Default 0
	} else {
		exists := true
		if _, err := os.Stat(connect); os.IsNotExist(err) {
			zlog.Printf("database %q doesn't exist; loading new schema", connect)
			err = os.MkdirAll(filepath.Dir(connect), 0755)
			if err != nil {
				return nil, errors.Wrap(err, "create DB dir")
			}
			exists = false
		}
		var err error
		db, err = sqlx.Connect("sqlite3", connect)
		if err != nil {
			return nil, errors.Wrap(err, "sqlx.Connect sqlite")
		}

		if !exists {
			db.MustExec(string(schema))
			err := runmig(db, migrations, migpath, "all")
			if err != nil {
				return nil, errors.Wrap(err, "migration")
			}
		}
	}

	return db, checkmig(db, migrations, migpath)
}

func runmig(db *sqlx.DB, migrations map[string][]byte, migpath string, which string) error {
	torun := []string{which}
	if which == "all" {
		haveMig, ranMig, err := lsmig(db, migrations, migpath)
		if err != nil {
			return err
		}

		torun = sliceutil.DifferenceString(haveMig, ranMig)
	}

	for _, run := range torun {
		f, err := ioutil.ReadFile(fmt.Sprintf("%s/%s.sql", migpath, run))
		if err != nil {
			return err
		}

		_, err = db.Exec(string(f))
		if err != nil {
			return errors.Wrapf(err, "migrate %s", run)
		}
	}

	return nil
}

func lsmig(db *sqlx.DB, migrations map[string][]byte, migpath string) (haveMig, ranMig []string, err error) {
	// Check migrations.
	if _, err := os.Stat(migpath); os.IsNotExist(err) {
		for k := range migrations {
			haveMig = append(haveMig, k)
		}
	} else {
		haveMig, err = filepath.Glob(migpath + "/*.sql")
		if err != nil {
			return nil, nil, errors.Wrap(err, "glob")
		}
	}
	for i := range haveMig {
		haveMig[i] = filepath.Base(haveMig[i][:len(haveMig[i])-4])
	}
	sort.Strings(haveMig)

	err = db.Select(&ranMig, `select name from version order by name asc`)
	return haveMig, ranMig, errors.Wrap(err, "select version")
}

// Check if there are pending migrations and zlog.Error() if there are.
func checkmig(db *sqlx.DB, migrations map[string][]byte, migpath string) error {
	if migrations == nil && migpath == "" {
		return nil
	}

	haveMig, ranMig, err := lsmig(db, migrations, migpath)
	if err != nil {
		return err
	}

	if d := diff(haveMig, ranMig); len(d) > 0 {
		zlog.Errorf("pending migrations: %q", d)
	}
	if d := diff(ranMig, haveMig); len(d) > 0 {
		zlog.Errorf("migrations in the DB that don't exist: %q", d)
	}
	return nil
}

// diff returns a new slice with elements that are in "set" but not in "others".
func diff(set []string, others ...[]string) []string {
	var out []string
	for _, setItem := range set {
		found := false
		for _, o := range others {
			if sliceutil.InStringSlice(o, setItem) {
				found = true
				break
			}
		}

		if !found {
			out = append(out, setItem)
		}
	}

	return out
}

// Date formatted for SQL.
func Date(t time.Time) string { return t.Format("2006-01-02 15:04:05") }
