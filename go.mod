module zgo.at/zdb

go 1.13

// cgo branch
replace github.com/mattn/go-sqlite3 => github.com/zgoat/go-sqlite3 v1.13.1-0.20200605053529-678cb5b8512e

require (
	github.com/go-sql-driver/mysql v1.5.0 // indirect
	github.com/jmoiron/sqlx v1.2.0
	github.com/lib/pq v1.3.0
	github.com/mattn/go-sqlite3 v2.0.3+incompatible
	zgo.at/zlog v0.0.0-20200404052423-adffcc8acd57
	zgo.at/zstd v0.0.0-20200528080824-83897c2259b4
)
