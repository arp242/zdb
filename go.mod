module zgo.at/zdb

go 1.13

// https://github.com/jmoiron/sqlx/pull/680
replace github.com/jmoiron/sqlx => github.com/zgoat/sqlx v1.2.1-0.20201228123424-c5cc0d957b92

require (
	// Last tagged release was over 2 years ago.
	github.com/jmoiron/sqlx v1.2.1-0.20201120164427-00c6e74d816a
	github.com/lib/pq v1.9.0
	github.com/mattn/go-sqlite3 v1.14.6
	zgo.at/zlog v0.0.0-20200404052423-adffcc8acd57
	zgo.at/zstd v0.0.0-20201227165557-c822e638e28c
)
