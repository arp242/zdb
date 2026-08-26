module zgo.at/zdb/test

go 1.27

replace zgo.at/zdb => ../

require (
	zgo.at/zdb v0.0.0-20260425145215-e580739daf43
	zgo.at/zdb-drivers/go-sqlite3 v0.0.0-20260820212400-f9b1e8796b42
	zgo.at/zdb-drivers/mariadb v0.0.0-20260820212400-f9b1e8796b42
	zgo.at/zdb-drivers/pgx v0.0.0-20260820212400-f9b1e8796b42
	zgo.at/zdb-drivers/pq v0.0.0-20260820212400-f9b1e8796b42
	zgo.at/zstd v0.0.0-20260825170826-0bd1746f8b87
)

require (
	filippo.io/edwards25519 v1.2.0 // indirect
	github.com/go-sql-driver/mysql v1.10.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/pgx/v5 v5.10.0 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/lib/pq v1.12.3 // indirect
	github.com/mattn/go-sqlite3 v1.14.50 // indirect
	golang.org/x/sync v0.22.0 // indirect
	golang.org/x/text v0.41.0 // indirect
)
