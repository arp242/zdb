package sqlite3_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/mattn/go-sqlite3"
	"zgo.at/zdb"
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
		if info.DriverName != "sqlite3-hook1" {
			t.Errorf("wrong driver name: %q", info.DriverName)
		}
		if db.SQLDialect() != zdb.DialectSQLite {
			t.Errorf("wrong dialect: %q", db.SQLDialect())
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
		if info.DriverName != "sqlite3-hook2" {
			t.Errorf("wrong driver name: %q", info.DriverName)
		}
		if db.SQLDialect() != zdb.DialectSQLite {
			t.Errorf("wrong dialect: %q", db.SQLDialect())
		}
	})
}
