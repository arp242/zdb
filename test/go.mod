module zgo.at/zdb/test

go 1.25

replace zgo.at/zdb => ../

require (
	zgo.at/zdb v0.0.0-20260224134437-9b2b27f2fb77
	zgo.at/zdb-drivers/go-sqlite3 v0.0.0-20260303161431-7b89451cbe2f
	zgo.at/zdb-drivers/mariadb v0.0.0-20260303161431-7b89451cbe2f
	zgo.at/zdb-drivers/pgx v0.0.0-20260303161431-7b89451cbe2f
	zgo.at/zdb-drivers/pq v0.0.0-20260303161431-7b89451cbe2f
	zgo.at/zstd v0.0.0-20260223143114-826b370d029b
)

require (
	filippo.io/edwards25519 v1.2.0 // indirect
	github.com/go-sql-driver/mysql v1.9.3 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/pgx/v5 v5.8.0 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/lib/pq v1.11.2 // indirect
	github.com/mattn/go-sqlite3 v1.14.34 // indirect
	golang.org/x/sync v0.19.0 // indirect
	golang.org/x/text v0.34.0 // indirect
)
