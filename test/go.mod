module zgo.at/zdb/test

go 1.27

replace zgo.at/zdb => ../

require (
	zgo.at/zdb v0.0.0-20260425145215-e580739daf43
	zgo.at/zdb-drivers/go-sqlite3 v0.0.0-20260826150446-9fb00bfd13f6
	zgo.at/zdb-drivers/mariadb v0.0.0-20260820212400-f9b1e8796b42
	zgo.at/zdb-drivers/pq v0.0.0-20260820212400-f9b1e8796b42
	zgo.at/zstd v0.0.0-20260825170826-0bd1746f8b87
)

require (
	filippo.io/edwards25519 v1.2.0 // indirect
	github.com/go-sql-driver/mysql v1.10.0 // indirect
	github.com/lib/pq v1.12.3 // indirect
	github.com/mattn/go-sqlite3 v1.14.50 // indirect
)
