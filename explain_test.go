package zdb

import (
	"testing"
)

func TestExplain(t *testing.T) {
	t.Skip("PostgreSQL only for now")

	// db, err := Connect(ConnectOptions{
	// 	Connect:     "sqlite://:memory:",
	// 	ShowExplain: true,
	// })
	// if err != nil {
	// 	t.Fatal(err)
	// }
	// //defer db.Close()

	// ctx := With(context.Background(), db)

	// var i int
	// err = db.GetContext(ctx, &i, `select 1`)
	// if err != nil {
	// 	t.Fatal(err)
	// }
}
