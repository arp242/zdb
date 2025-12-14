module zgo.at/zdb/test

go 1.25

replace zgo.at/zdb => ../

require (
	zgo.at/zdb v0.0.0-20251214105645-200d82642ba8
	zgo.at/zdb-drivers/go-sqlite3 v0.0.0-20251214132307-3b1fa0323e96
	zgo.at/zdb-drivers/mariadb v0.0.0-20251214132307-3b1fa0323e96
	zgo.at/zdb-drivers/pgx v0.0.0-20251214132307-3b1fa0323e96
	zgo.at/zdb-drivers/pq v0.0.0-20251214132307-3b1fa0323e96
	zgo.at/zstd v0.0.0-20251128053228-ec259dea6715
)

require (
	filippo.io/edwards25519 v1.1.0 // indirect
	github.com/go-sql-driver/mysql v1.9.3 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/pgx/v5 v5.7.6 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/lib/pq v1.10.9 // indirect
	github.com/mattn/go-sqlite3 v1.14.32 // indirect
	golang.org/x/crypto v0.46.0 // indirect
	golang.org/x/sync v0.19.0 // indirect
	golang.org/x/text v0.32.0 // indirect
)
