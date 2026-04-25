module zgo.at/zdb/test

go 1.26

replace zgo.at/zdb => ../

require (
	zgo.at/zdb v0.0.0-20260303161845-562342f5f7f5
	zgo.at/zdb-drivers/go-sqlite3 v0.0.0-20260423171746-77fa440c58ab
	zgo.at/zdb-drivers/mariadb v0.0.0-20260423171746-77fa440c58ab
	zgo.at/zdb-drivers/pgx v0.0.0-20260423171746-77fa440c58ab
	zgo.at/zdb-drivers/pq v0.0.0-20260423171746-77fa440c58ab
	zgo.at/zstd v0.0.0-20260423150213-85e323702691
)

require (
	filippo.io/edwards25519 v1.2.0 // indirect
	github.com/go-sql-driver/mysql v1.9.3 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/pgx/v5 v5.9.1 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/lib/pq v1.12.3 // indirect
	github.com/mattn/go-sqlite3 v1.14.42 // indirect
	golang.org/x/sync v0.20.0 // indirect
	golang.org/x/text v0.35.0 // indirect
)
