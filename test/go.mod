module zgo.at/zdb/test

go 1.27

replace zgo.at/zdb => ../

require (
	zgo.at/zdb v0.0.0-20260826151209-9dfc2b0454eb
	zgo.at/zdb-drivers/go-sqlite3 v0.0.0-20260826151742-4e29699c6d49
	zgo.at/zdb-drivers/mariadb v0.0.0-20260826151742-4e29699c6d49
	zgo.at/zdb-drivers/pq v0.0.0-20260826151742-4e29699c6d49
	zgo.at/zstd v0.0.0-20260825170826-0bd1746f8b87
)

require (
	filippo.io/edwards25519 v1.2.0 // indirect
	github.com/go-sql-driver/mysql v1.10.0 // indirect
	github.com/lib/pq v1.12.3 // indirect
	github.com/mattn/go-sqlite3 v1.14.50 // indirect
)
