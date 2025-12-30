module zgo.at/zdb/test

go 1.25

replace zgo.at/zdb => ../

require (
	zgo.at/zdb v0.0.0-20251229200430-f9358a291e07
	zgo.at/zdb-drivers/go-sqlite3 v0.0.0-20251229200649-f359db3f2221
	zgo.at/zdb-drivers/mariadb v0.0.0-20251229200649-f359db3f2221
	zgo.at/zdb-drivers/pgx v0.0.0-20251230081904-ff701a2f492b
	zgo.at/zstd v0.0.0-20251128053228-ec259dea6715
)

require (
	filippo.io/edwards25519 v1.1.0 // indirect
	github.com/go-sql-driver/mysql v1.9.3 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/pgx/v5 v5.8.0 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/mattn/go-sqlite3 v1.14.32 // indirect
	golang.org/x/sync v0.19.0 // indirect
	golang.org/x/text v0.32.0 // indirect
)
