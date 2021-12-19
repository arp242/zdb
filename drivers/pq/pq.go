// Package pq provides a zdb driver for PostgreSQL.
//
// This uses https://github.com/lib/pq
//
// This will set the maximum number of open and idle connections to 25 each,
// instead of Go's default of 0 and 2. To change this, you can use:
//
//    db.DBSQL().SetMaxOpenConns(100)
package pq

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os/exec"
	"regexp"

	"github.com/lib/pq"
	"zgo.at/zdb/drivers"
)

func init() {
	drivers.RegisterDriver(driver{})
}

type driver struct{}

func (driver) Name() string    { return "pq" }
func (driver) Dialect() string { return "postgresql" }
func (driver) ErrUnique(err error) bool {
	var pqErr *pq.Error
	return errors.As(err, &pqErr) && pqErr.Code == "23505"
}
func (driver) Connect(ctx context.Context, connect string, create bool) (*sql.DB, bool, error) {
	exists := true
	db, err := sql.Open("postgres", connect)
	if err != nil {
		var (
			dbname string
			pqErr  *pq.Error
		)
		if errors.As(err, &pqErr) && pqErr.Code == "3D000" {
			// TODO: rather ugly way to get database name :-/ pq doesn't expose
			// any way to parse the connection string :-/
			//
			// This would also allow us to get rid of the "createdb" shell
			// command; we can connect with the same credentials to the
			// "postgres" database and try "create database [wantdb]", using
			// "createdb" only as a fallback. But to do this we need more
			// information.
			//
			// https://github.com/jackc/pgx does expose this, and it the
			// "recommended library" according to the pg readme, but this pulls
			// in a slew of 34 dependencies, so meh. There's a few issues about
			// this; might get fixed in v5.
			//
			// pg is in "maintainence mode" with many unmerged PRs; it works,
			// but is on its way out. Should really look into this. Maybe do a
			// "fork" which just fixed a bit of the dependency stuff(?) We don't
			// need all the logadapter stuff, can vendor the jackc/pg* things,
			// and maybe do some other simple automated changes.
			x := regexp.MustCompile(`pq: database "(.+?)" does not exist`).FindStringSubmatch(pqErr.Error())
			if len(x) >= 2 {
				dbname = x[1]
				exists = true
			}
		}

		if create && dbname != "" {
			out, cerr := exec.Command("createdb", dbname).CombinedOutput()
			if cerr != nil {
				return nil, false, fmt.Errorf("pq.Connect: %w: %s", cerr, out)
			}

			db, err = sql.Open("postgres", connect)
			if err != nil {
				return nil, false, fmt.Errorf("pq.Connect: %w", err)
			}

			return db, false, nil
		}

		if dbname != "" {
			return nil, false, &drivers.NotExistError{Driver: "postgres", DB: dbname, Connect: connect}
		}
		return nil, false, fmt.Errorf("pq.Connect: %w", err)
	}

	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(25)

	return db, exists, nil
}
