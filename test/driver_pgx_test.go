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
		os.Setenv("PGDATABASE", "zdb_test")
	}

	// PGHOST=localhost PGUSER=goatcounter PGPASSWORD=goatcounter PGDATABASE=goatcounter go test -tags=testpgx ./...
}
