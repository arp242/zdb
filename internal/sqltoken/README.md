
# sqltoken - break SQL strings into a token array

[![GoDoc](https://godoc.org/github.com/muir/sqltoken?status.png)](https://pkg.go.dev/github.com/muir/sqltoken)
[![Coverage](http://gocover.io/_badge/github.com/muir/sqltoken)](https://gocover.io/github.com/muir/sqltoken)

Install:

	go get github.com/muir/sqltoken

---

## Sqltoken

Sqltoken is a high-performance hand-coded tokenizer for SQL 
strings.  It's high-performance so that it can be used in
situations where it is being run on every query.

The tokenization is somewhat rough: it correctly detects comments
and numbers, but it depending on the situation and the SQL variant,
it may think an indentifier is a literal.

It has support of MySQL/MariaDB/Singlestore, Postgres/CockroachDB, 
Oracle, and SQL server.

The return value is an array of simple tokens:

```go
type Token struct {
	Type TokenType
	Text string
}
```

Concatting the `Text` portions togehter will reconstruct the original input:

```go
TokenizeMySQL(s).String() == s
```

### Use cases

SQL token can be used to strip comments from SQL code.  It can be used to
parse out variables to substitute.

## Development status

This is new code.  It has lots of tests and complete coverage, but feedback
from users would be valuable.

