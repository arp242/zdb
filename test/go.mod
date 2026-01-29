module zgo.at/zdb/test

go 1.25

replace zgo.at/zdb => ../

require (
	zgo.at/zdb v0.0.0-20260129000812-7d42e9c93ed8
	zgo.at/zdb-drivers/go-sqlite3 v0.0.0-20260129135936-fde4e384e5ae
	zgo.at/zdb-drivers/mariadb v0.0.0-20260129135936-fde4e384e5ae
	zgo.at/zdb-drivers/pgx v0.0.0-20260129135936-fde4e384e5ae
	zgo.at/zdb-drivers/pq v0.0.0-20260129135936-fde4e384e5ae
	zgo.at/zstd v0.0.0-20260108115308-04b7db162be2
)

require (
	filippo.io/edwards25519 v1.1.0 // indirect
	github.com/go-sql-driver/mysql v1.9.3 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/pgx/v5 v5.8.0 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/lib/pq v1.11.1 // indirect
	github.com/mattn/go-sqlite3 v1.14.33 // indirect
	golang.org/x/sync v0.19.0 // indirect
	golang.org/x/text v0.32.0 // indirect
)
