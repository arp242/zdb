//go:build cgo
// +build cgo

package zdb

import (
	"context"
	"testing"

	"github.com/mattn/go-sqlite3"
)

func TestSQLiteHook(t *testing.T) {
	hook1 := func(c *sqlite3.SQLiteConn) error {
		return c.RegisterFunc("hook1", func() string { return "hook1" }, true)
	}
	hook2 := func(c *sqlite3.SQLiteConn) error {
		return c.RegisterFunc("hook2", func() string { return "hook2" }, true)
	}

	var driver1, driver2, driver3 ServerInfo
	{
		db, err := Connect(ConnectOptions{
			Connect:    "sqlite://:memory:",
			SQLiteHook: hook1,
		})
		if err != nil {
			t.Fatal(err)
		}
		ctx := WithDB(context.Background(), db)

		var o string
		err = db.Get(ctx, &o, `select hook1()`)
		if err != nil {
			t.Fatal(err)
		}
		if o != "hook1" {
			t.Error(o)
		}
		driver1, _ = db.Info(ctx)
	}

	{
		db, err := Connect(ConnectOptions{
			Connect:    "sqlite://:memory:",
			SQLiteHook: hook2,
		})
		if err != nil {
			t.Fatal(err)
		}
		ctx := WithDB(context.Background(), db)

		var o string
		err = db.Get(ctx, &o, `select hook2()`)
		if err != nil {
			t.Fatal(err)
		}
		if o != "hook2" {
			t.Error(o)
		}
		driver2, _ = db.Info(ctx)
	}

	{
		db, err := Connect(ConnectOptions{
			Connect:    "sqlite://:memory:",
			SQLiteHook: hook1,
		})
		if err != nil {
			t.Fatal(err)
		}
		ctx := WithDB(context.Background(), db)

		var o string
		err = db.Get(ctx, &o, `select hook1()`)
		if err != nil {
			t.Fatal(err)
		}
		if o != "hook1" {
			t.Error(o)
		}
		driver3, _ = db.Info(ctx)
	}

	if driver1.DriverName != driver3.DriverName {
		t.Error()
	}
	if driver2.DriverName == driver1.DriverName {
		t.Error()
	}
}
