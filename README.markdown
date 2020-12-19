`zdb` allows interacting with SQL databases.

Much of this is built on top of [sqlx][sqlx], but that's mostly an
implementation detail; in regular usage you shouldn't really have to deal with
the `sqlx` (or `database/sql`) package interfaces at all. The API/interface of
zdb is quite different.

**This requires Go 1.16 or newer**. It uses the new `fs` package to load files.

Right now only PostgreSQL and SQLite are supported. Adding MariaDB or other
engines wouldn't be hard, but I don't use it myself so didn't bother adding (and
testing!) it. Just need someone to write a patch 😅

Full reference documentation: https://pkg.go.dev/zgo.at/zdb#pkg-index

[sqlx]: https://github.com/jmoiron/sqlx
