//go:build !testpg && !testmaria
// +build !testpg,!testmaria

package zdb_test

import (
	_ "zgo.at/zdb/drivers/go-sqlite3"
)
