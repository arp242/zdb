`zdb` provides a nice API to interact with SQL databases in Go.

Features:

- Nice API with debugging and testing features.
- Easier to wrap databases for logging, metrics, etc.
- Templating in queries.
- Run queries from filesystem.
- Deals with some SQL interoperability issues.
- Basic migrations.

PostgreSQL, SQLite, and MariaDB are supported. Oracle MySQL is not supported as
it's lacking some features.

This requires the following versions because it uses some features introduced in
those versions:

- SQLite 3.35 (go-sqlite3 v1.14.8)
- PostgreSQL 12.0
- MariaDB 10.5

To avoid confusion it gives an error when connecting if an older version is
used. This also requires Go 1.16 or newer.

Full reference documentation: https://godocs.io/zgo.at/zdb

Table of contents for this README:

<!-- [Example](#example) -->
- [Usage](#usage)
  - [Connecting](#connecting)
  - [zdb.DB and context](#zdb.db-and-context)
  - [Running queries](#running-queries)
    - [Simple conditionals](#simple-conditionals)
    - [Templates](#templates)
    - [Queries from filesystem](#queries-from-filesystem)
    - [Transactions](#transactions)
- [Schema creation and migrations](#schema-creation-and-migrations)
- [Bulk insert](#bulk-insert)
- [Testing and debugging](#testing-and-debugging)
- [Database wrapping](#database-wrapping)
  - [LogDB](#logdb)
  - [MetricsDB](#metricsDB)

<!--
- [Other stuff](#other-stuff)

Example
-------
TODO: load zdb_example_test.go
-->


Usage
-----
### Connecting
You first need to register a driver similar to how you register a driver for
database/sql; several drivers are available:

- zgo.at/drivers/zdb-pq
- zgo.at/drivers/zdb-mysql
- zgo.at/drivers/zdb-go-sqlite3

Simply importing this package is enough; e.g.:

    import _ "zgo.at/zdb-pq"

`Connect()` opens a new connection, runs migrations, and/or creates a database
if it doesn't exist yet. A basic example:

    db, err := zdb.Connect(zdb.ConnectOptions{
        Connect: "sqlite3+:memory:",
    })
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

`Connect` is a connection string prefixed with either the database engine (e.g.
"postgresql") or the driver name (e.g. "pq")`. Further details on the connection
string depends on the driver.

Schema creation and migrations is covered in [Schema creation and
migrations](#schema-creation-and-migrations) below.

### zdb.DB and context
There are two ways to use zdb:

- The methods on the `zdb.DB` interface.
- The top-level `zdb.*` functions, which accept a context with a `zdb.DB` value
  on it.

Both are exactly identical, and there is no real difference. Personally I think
that using `zdb.Get(ctx, ...)` is a lot easier, but some people are against
storing the database connection on the context as matter of religion – I don't
really see the problem and it makes a number of things easier. You can use
whatever fits your faith.

You can create a context with `zdb.WithDB()`:

    db, _ := zdb.Connect(..)
    ctx := zdb.WithDB(context.Background(), db)
    zdb.Get(ctx, `select 1`)

But as mentioned, you don't *need* to use the context, the following is
identical and the context is just used for cancellation:

    ctx := context.Background()
    db.Get(ctx, `select 1`)

I will mostly use the `zdb.*` functions in this documentation.

You can use `zdb.GetDB()` or `zdb.MustGetDB()` to get the `zdb.DB` back, but
this should rarely be needed.

### Running queries
The query methods are:

    Get(..)             Run a query and get a single row.
    Select(..)          Run a query and get multiple rows.
    Exec(..)            Execute a query without returning the result.
    NumRows(..)         Run a query and return the number of affected rows.
    InsertID(..)        Run a query and return the last insert ID.
    Query(..)           Select multiple rows, but don't immediatly load them.

Most of these work as you would expect, and similar to database/sql and sqlx.
The main difference is that Exec() doesn't return an `sql.Result` and that
`NumRows()` and `InsertID()` exist for this use case.

You can use `?`, `$n`, or named parameters; these are all identical and work on
any database:

    zdb.Exec(ctx, `insert into test (value) values (?)`, "hello")
    zdb.Exec(ctx, `insert into test (value) values ($1)`, "hello")
    zdb.Exec(ctx, `insert into test (value) values (:value)`, zdb.P{"value": "hello"})

`zdb.P` is just a `map[string]any`, except shorter. The "P" is for "Parameters".

You can pass multiple structs and/or maps with named parameters:

    err = zdb.Exec(ctx, `info into test (a, b) (:a, :b)`,
        zdb.P{"A": "hello"},
        struct{B int}{42})

Get(), Select(), and Query() can scan the results in to a struct or map; see the
function documentation for details on the exact rules.

#### Simple conditionals
There is a mini template language for conditionals; this only works if you're
using named parameters:

    var values []string
    err := zdb.Select(ctx, &values, `
        select * from test
        where
            value = :val
            {{:val2 and value not like :val2}}
    `, zdb.P{
        "val":  "hello",
        "val2": "%world",
    })

The text between `{{:param ... }}` will be omitted if `param` is the type's zero
value. End with the parameter name with `!` to invert the match: `{{:param! ...
}}`.

I find this is a fairly nice middle ground between writing plain SQL queries and
using more complex query builder DSLs.

#### Templates
For more complex use cases you can use text/template; this only works for
queries loaded from the filesystem, and the filenames need to end with `.gotxt`.

See the documentation on [Template()] for a list of template functions.

[Template()]: https://godocs.io/zgo.at/zdb#Template

#### Queries from filesystem
Queries are loaded from the filesystem if the query starts with `load:`:

    var values []string
    err := zdb.Select(ctx, &values, `load:find-site`, zdb.P{
        "name": "hello",
    })

The special prefix `load:[filename]` loads a query from the `db/query/`
directory. You can use `zdb.Load()` to only read a query from the filesystem.

Comments in the form of `-- ` at the start of the line *only* are stripped. This
makes queries a bit less noisy in query logs and the like.

This requires the `Files` parameter in ConnectOptions to be set; e.g.

    zdb.Connect(zdb.ConnectOptions{
        Connect: "...",
        Files:   os.DirFS("db"),
    })

You can also use embeded files here.

#### Transactions
`zdb.TX(func(..) { })` runs the function in a transaction:

    err := zdb.TX(ctx, func(ctx context.Context) error {
        err := zdb.Exec(ctx, ..)
        if err != nil {
            return err
        }

        // ... more queries
    })
    if err != nil {
        log.Fatal(err)
    }

The transaction will be rolled back if an error is returned, or committed if it
doesn't. This can be nested, but will start only one transaction and will only
be committed after the outermost transaction finishes.

You can also start a transaction with `zdb.Begin()`, but I find the `TX()`
wrapper more useful in almost all cases:

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

Schema creation and migrations
------------------------------
If `Create` is set in zdb.Connect(), it will try to:

1. Create the database if it does not yet exist.
2. Tun the schema setup file if there are no tables yet and the file exists;
   this requires the `Files` parameter to be set.

The schemas to set up the database should be in `/schema-{dialect}.sql`,
`/schema.gotxt`, or `/schema.sql`. Files are tried in that order. The
`{dialect}` is `sqlite3`, `postgres`, or `mariadb` and only needed if you need
to vary something by SQL engine: otherwise it will just load the generic file.
You can also use templating with .gotxt files, as documented above.

Migrations are loaded from `/migrate/foo-{dialect}.sql`, `/migrate/foo.gotxt`,
or `/migrate/foo.sql`. Migrations are always run in lexical order, so you
probably want to prefix them with the date, and maybe a sequence number for the
day in case you have multiple migrations on the same day (e.g.
`2021-06-18-1-name.sql`). It uses a `version` table to keep track of which
migrations were already run (will be created automatically if it doesn't exist).

This isn't really intended to solve every possible use case for database
migrations, but it should be enough for many use cases, and for more advanced
things you can use one of several dedicated packages.

You can also pass `GoMigrations` in `zdb.Connect()` to run Go code as
migrations. This is sometimes more convenient if you need to do some complex
processing.

It's okay if directories are missing; e.g. no migrate directory simply means
that it won't attempt to run migrations – you don't need to use all features.

Bulk insert
-----------
`BulkInsert` makes it easier to bulk insert values:

    ins := zdb.NewBulkInsert(ctx, "table", []string{"col1", "col2", "col3"})
    for _, v := range listOfValues {
        ins.Values(v.Col1, v.Col2, v.Col3)
    }
    err := ins.Finish()

This won't naïvely group everything in one query; after more than 998 parameters
it will construct an SQL query and send it to the server. 998 was chosen because
that's the [default SQLite limit][maxvar], and you probably won't get much
benefit from larger inserts anyway.

You get the error(s) back with `Finish()`.

Note this isn't run in a transaction by default; start a transaction yourself if
you want it.

[maxvar]: https://www.sqlite.org/limits.html#max_variable_number

Testing and debugging
---------------------
### DumpArgs
zdb has a rather useful facility of "DumpArgs"; you can add it to any query
method and it will "dump" information to stderr. This is really useful for quick
testing/debugging:

    TODO: include somewhwat realistic example here.

### Testing

    RunTest()              Create a temporary database and run tests.
    TestQueries()          Test queries from fs.

    Dump(), DumpString()   Show result of any query.
    ApplyParams()          Apply parameters.

`zdb.Dump()` and `zdb.DumpString()` are two rather useful helper functions: they
get the output of any SQL query in a table, similar to what you get on the
`sqlite3` or `psql` commandline. This is rather useful for debugging and tests:

    want := `
        site    day                            browser  version  count   count_unique  event
        1       2019-08-31 00:00:00 +0000 UTC  Firefox  68.0     1       0             0`

    out := zdb.DumpString(ctx, `select * from table`)

    if d := ztest.Diff(out, want, ztest.DiffNormalizeWhitespace); d != "" {
        t.Error(d)
    }

This will `panic()` on errors. Again, it's only intended for debugging and
tests, and omitting error returns makes it a bit smoother to use.

The `zdb.ApplyParams()` function will replace `?` and `$1` with the actual
values. This is intended to make copying long-ish queries to the `psql` CLI for
additional debugging/testing easier. This is **not** intended for any serious
use and is *not* safe against malicious input.

    fmt.Println(zdb.ApplyParams(
        `select * from users where site=$1 and state=$2`,
        1, "active"))

    // Output: select * from users where site=1 and state='active'

Database wrapping
-----------------
Wrapping a database well with `database/sql` or `sqlx` is a bit tricky since you
need to wrap both the actual database but *also* the transactions.

### LogDB
Wrap the database with `zdb.NewLognDB()` to automatically dump the query, the
results, the explain, or all of them to a writer:

    db, _ := zdb.Connect(...)
    logDB = zdb.NewLogDB(db, os.Stdout, zdb.DumpAll, "")

The last parameter is an optional filter:

    logDB = zdb.NewExplainDB(db, os.Stdout, zdb.DumpAll, "only_if_query_matches_this_text")

**This may run queries twice**.

### MetricsDB
Wrap the database with `zdb.NewMetricsDB()` to record metrics on the execution
of query times. This takes a "recoder", and the `Record()` method is called for
every query invocation.

There is a `MetricsMemory`, which records the metrics in the process memory. You
can implement your own to send metrics to datadog or grafana or whatnot.

        db, _ := zdb.Connect(...)
        metricDB := zdb.NewMetricsDB(db, zdb.NewMetricsMemory(0))


<!--
Other stuff
-----------

zdb.Dialect()
zdb.ErrNoRows()
zdb.ErrUnique()

zdb is mostly driver-agnostic, with the exception of ErrUnique()

TODO
-->
