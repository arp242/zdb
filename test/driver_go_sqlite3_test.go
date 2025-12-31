//go:build !testsqlite && !testpq && !testpgx && !testmaria

package zdb_test

import (
	_ "zgo.at/zdb-drivers/go-sqlite3"
)
