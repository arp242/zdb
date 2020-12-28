`zdb` exposes some database helpers; all of this is built on top of
[sqlx](https://github.com/jmoiron/sqlx), but makes things a bit easier.

Right now only PostgreSQL and SQLite are supported. Adding MariaDB or other
engines wouldn't be hard, but I don't use it myself so didn't bother adding (and
testing!) it. Just need someone to write a patch 😅

Note: compile with `CGO_ENABLED=0` if you're not using `cgo`, otherwise it will
depend on the go-sqlite3 (which uses cgo).

---

It exposes a `DB` interface which can be used for both database connections and
transactions; with the function arguments and return values omitted it looks
like:

```go
type DB interface {
    ExecContext()
    GetContext()
    QueryRowxContext()
    QueryxContext()
    SelectContext()

    Rebind()
    DriverName()
}
```

It doesn't expose everything sqlx has because I find that this is enough for
>95% of the use. I'm not against adding more methods though, just report a use
case that's hard to solve otherwise.

Use `zdb.Connect()` to connect to a database; It's not *required* to use this
(`sqlx.Connect()` will work fine as well), but it has some handy stuff like
automatic schema creation and migrations. See godoc for the full details on
that.

---

To run queries in a transaction you can use `zdb.TX()`:

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
doesn't. This can be nested.

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
wrapper more useful in most cases:

```go
txctx, tx, err := zdb.Begin(ctx)
if err != nil {
    return err
}
defer tx.Rollback()

// Do stuff with tx...

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

`Query()` is a light-weight query builder; instead of building queries with a
DSL it just includes or omits some parts based on boolean parameters. 

```go
func getData(ctx context.Context, siteID int64, order bool) error {
    query, args, err := zdb.Query(ctx, `
        select * from tbl
        where site_id = :site
        {{:order order by id}}`,
        struct {
            Site int64
            Order bool
        }{siteID, order})
    if err != nil {
        return err
    }

    var data someStruct
    err = zdb.MustGet(ctx).SelectContext(ctx, &data, query, args...)
    if err != nil {
        return err
    }
    fmt.Println(data)
}
```

Parameters are always inserted as named parameters (with `sqlx.Named()`). This
can be a struct or a map.

Every other parameter corresponds to a `{{...}}` "conditional"; if it's `true`
this will be included, if it's false it won't.

Overall I find this is a fairly nice middle ground between writing plain SQL
queries and using a more complex query builder like Squirrel.

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

The `zdb.ApplyPlaceholders()` function will replace `?` and `$1` with the actual
values. This is intended to make copying long-ish queries to the psql CLI for
additional debugging/testing easier. This is **not** intended for any serious
use and is *not* safe against malicious input.

```go
fmt.Println(zdb.ApplyPlaceholders(
    `select * from users where site=$1 and state=$2`,
    1, "active"))

// Output: select * from users where site=1 and state='active'
```

---

The `zdb/bulk` package makes it easier to bulk insert values:


```go
ins := bulk.NewInsert(ctx, "table", []string{"col1", "col2", "col3"})
for _, v := range listOfValues {
    ins.Values(v.Col1, v.Col2, v.Col3)
}

err := ins.Finish()
```

This won't naïvely group everything in one query; after more than 998 parameters
it will construct an SQL query and send it to the server. 998 was chosen because
that's the [default SQLite limit](https://www.sqlite.org/limits.html#max_variable_number).
You get the error(s) back with `Finish()`.

Note this isn't run in a transaction by default; start a transaction yourself if
you want it.

---

Wrap the database with `zdb.NewExplainDB()` to automatically dump the `explain`s
of all queries to a writer:

```go
db, _ := zdb.Connect(...)
explainDB = zdb.NewExplainDB(db, os.Stdout, "")
```

The last parameter is an optional filter:

```go
explainDB = zdb.NewExplainDB(db, os.Stdout, "only_if_query_matches_this_text")
```

---

There's a few types as well:

`Bool` to store text such as "true", "on", "1" as boolean true. This is always
stored as an `int` for best SQL compatibility.

`Ints`, `Floats`, and `Strings` all store a slice as a comma-separated varchar.
If you use just a single database engine which supports arrays or JSON (like
PostgreSQL) then that's probably a better option, but for simpler cases this
makes some things easier.

Note `Strings` does *not* escape commas in existing strings; don't use it for
arbitrary text.
