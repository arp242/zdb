//go:build testpgx

package zdb_test

import (
	"os"

	_ "zgo.at/zdb-drivers/pgx"
)

func init() {
	if _, ok := os.LookupEnv("PGHOST"); !ok {
		os.Setenv("PGHOST", "localhost")
	}
	if _, ok := os.LookupEnv("PGDATABASE"); !ok {
		os.Setenv("PGDATABASE", "zdb")
	}
	if _, ok := os.LookupEnv("PGUSER"); !ok {
		os.Setenv("PGUSER", "zdb")
	}
	if _, ok := os.LookupEnv("PGPASSWORD"); !ok {
		os.Setenv("PGPASSWORD", "zdb")
	}
}
