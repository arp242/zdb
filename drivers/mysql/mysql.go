// Package mysql provides a zdb driver for MySQL and MariaDB.
//
// This uses https://github.com/go-sql-driver/mysql
package mysql

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
	"zgo.at/zdb/drivers"
)

func init() {
	drivers.RegisterDriver(driver{})
}

type driver struct{}

func (driver) Name() string    { return "mysql" }
func (driver) Dialect() string { return "mysql" }
func (driver) ErrUnique(err error) bool {
	return false // TODO
}
func (driver) Connect(ctx context.Context, connect string, create bool) (*sql.DB, bool, error) {
	db, err := sql.Open("mysql", connect)
	if err != nil {
		return nil, false, fmt.Errorf("mysql.Connect: %w", err)
	}

	err = db.PingContext(ctx)
	if err != nil {
		return nil, false, fmt.Errorf("mysql.Connect: %w", err)
	}

	return db, true, nil
}
