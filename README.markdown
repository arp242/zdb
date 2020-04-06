`zdb` exposes some database helpers; all of this is built on top of
[sqlx](https://github.com/jmoiron/sqlx), but makes certain things a bit easier.

Right now only PostgreSQL and SQLite are supported. Adding MariaDB etc. wouldn't
be hard, but I don't need it myself so didn't bother adding (and testing!) it.
Just need someone to write a patch 😅

---

It exposes a `DB` interface which can be used for both database connections and
transactions; with the function arguments and return values omitted it looks
like:

```go
type DB interface {
    ExecContext() (
    GetContext()
    Rebind()
    SelectContext()
    QueryxContext()
}
```

It doesn't expose everything sqlx has because I find that for >95% of the use
cases, just this is enough. I'm not against adding more methods though, just
report a use case that's hard to solve otherwise.

Use `zdb.Connect()` to connect to a database; It's not *required* to use this
(`sqlx.Connect()` will work fine as well), but it has some handy stuff like
automatic schema creation and migrations. See godoc for the full details on
that.

---

To start a transaction you can use `zdb.TX()`:

```go
func Example(ctx context.Context) {
    err := zdb.TX(ctx, func(ctx context.Context, tx zdb.DB) error {
        _, err := db.ExecContext(..)
        if err != nil {
            return err
        }

        // ... more queries
    })
    if err != nil {
        return fmt.Errorf("zdb.Example: %w", err)
    }
}
```

The transaction will be rolled back if an error is returned, or commited if it
doesn't.

It's assumed that the context has a database value:

```go
db, err := zdb.Connect(...)
ctx = zdb.With(ctx, db)

// And get it again with db := zdb.MustGet(ctx)
```

I know some people are against storing the database on the context like this,
but I don't really see the problem. You don't *need* to store it on the context;
you'll just have to add a call to `zdb.With()`:

```go
func Example(db zdb.DB) {  // or *sqlx.DB
    err := zdb.TX(zdb.With(ctx, db), func(...) {
        ...
    })
}
```

You can also start a transaction with `zdb.Begin()`, but I find the `TX()`
wrapper more useful:

```go
txctx, tx, err := zdb.Begin(ctx)
if err != nil {
    return err
}
defer tx.Rollback()

[ .. do stuff with tx ..]
err := tx.Commit()
if err != nil {
    return err
}
```

Because it just passes around `zdb.DB` you can pass this to functions that
accept `zdb.DB`, so they will operate on the transaction.

Because `zdb.DB` satisfies both the `sqlx.DB` and `sqlx.Tx` structs, you can
pass this around to your functions if they accept `zdb.DB` instead of
`*sqlx.DB`.

---

`zdb.Dump()` and `zdb.DumpString()` are two rather useful helper functions: they
get the output of any SQL query in a table, similar to what you get on the
`sqlite3` or `psql` commandline. This is rather useful for debugging and tests:

```go
want := "" +
    `site    day                            browser  version  count   count_unique  event`+
    `1       2019-08-31 00:00:00 +0000 UTC  Firefox  68.0     1       0             0`

out := zdb.DumpString(ctx, `select * from table`)

if d := ztest.Diff(out, want); d != "" {
    t.Error(d)
}
```

This will `panic()` on errors. Again, it's only intended for debugging and
tests, and omitting error returns makes it a bit smoother to use.
