//go:build cgo
// +build cgo

package sqlite3_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/mattn/go-sqlite3"
	"zgo.at/zdb"
	sqlite3Driver "zgo.at/zdb/drivers/go-sqlite3"
)

func TestSQLiteHook(t *testing.T) {
	sql.Register("sqlite3-hook1", &sqlite3.SQLiteDriver{
		ConnectHook: func(c *sqlite3.SQLiteConn) error {
			return c.RegisterFunc("hook1", func() string { return "hook1" }, true)
		},
	})
	sql.Register("sqlite3-hook2", &sqlite3.SQLiteDriver{
		ConnectHook: func(c *sqlite3.SQLiteConn) error {
			return c.RegisterFunc("hook2", func() string { return "hook2" }, true)
		},
	})
	sqlite3Driver.DefaultHook(func(c *sqlite3.SQLiteConn) error {
		return c.RegisterFunc("hookdefault", func() string { return "hookdefault" }, true)
	})

	t.Run("hook1", func(t *testing.T) {
		db, err := zdb.Connect(context.Background(), zdb.ConnectOptions{
			Connect: "sqlite3-hook1+:memory:",
		})
		if err != nil {
			t.Fatal(err)
		}
		ctx := zdb.WithDB(context.Background(), db)

		var o string
		err = db.Get(ctx, &o, `select hook1()`)
		if err != nil {
			t.Fatal(err)
		}
		if o != "hook1" {
			t.Error(o)
		}
		info, _ := db.Info(ctx)
		if info.DriverName != "sqlite3" {
			t.Errorf("wrong driver name\nhave: %q\nwant: sqlite3", info.DriverName)
		}
		if db.SQLDialect() != zdb.DialectSQLite {
			t.Errorf("wrong dialect: %q", db.SQLDialect())
		}

		err = db.Get(ctx, &o, `select hookdefault()`)
		if err == nil {
			t.Errorf("select hookdefault() worked: %q", o)
		}
	})

	t.Run("defaultHook", func(t *testing.T) {
		db, err := zdb.Connect(context.Background(), zdb.ConnectOptions{
			Connect: "sqlite3+:memory:",
		})
		if err != nil {
			t.Fatal(err)
		}
		ctx := zdb.WithDB(context.Background(), db)

		var o string
		err = db.Get(ctx, &o, `select hookdefault()`)
		if err != nil {
			t.Fatal(err)
		}
		if o != "hookdefault" {
			t.Error(o)
		}
		info, _ := db.Info(ctx)
		if info.DriverName != "sqlite3" {
			t.Errorf("wrong driver name\nhave: %q\nwant: sqlite3", info.DriverName)
		}
		if db.SQLDialect() != zdb.DialectSQLite {
			t.Errorf("wrong dialect: %q", db.SQLDialect())
		}

		err = db.Get(ctx, &o, `select hook1()`)
		if err == nil {
			t.Errorf("select hook1() worked: %q", o)
		}
	})

	t.Run("hook2", func(t *testing.T) {
		db, err := zdb.Connect(context.Background(), zdb.ConnectOptions{
			Connect: "sqlite/sqlite3-hook2+:memory:",
		})
		if err != nil {
			t.Fatal(err)
		}
		ctx := zdb.WithDB(context.Background(), db)

		var o string
		err = db.Get(ctx, &o, `select hook2()`)
		if err != nil {
			t.Fatal(err)
		}
		if o != "hook2" {
			t.Error(o)
		}

		info, _ := db.Info(ctx)
		if info.DriverName != "sqlite3" {
			t.Errorf("wrong driver name\nhave: %q\nwant: sqlite3", info.DriverName)
		}
		if db.SQLDialect() != zdb.DialectSQLite {
			t.Errorf("wrong dialect: %q", db.SQLDialect())
		}

		err = db.Get(ctx, &o, `select hookdefault()`)
		if err == nil {
			t.Errorf("select hookdefault() worked: %q", o)
		}
	})
}
