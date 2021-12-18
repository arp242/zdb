package sqlx

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"testing"

	"zgo.at/zdb/internal/sqlx/reflectx"
)

func TestCompileQuery(t *testing.T) {
	table := []struct {
		Q, R, D, T, N string
		V             []string
	}{
		// basic test for named parameters, invalid char ',' terminating
		{
			Q: `INSERT INTO foo (a,b,c,d) VALUES (:name, :age, :first, :last)`,
			R: `INSERT INTO foo (a,b,c,d) VALUES (?, ?, ?, ?)`,
			D: `INSERT INTO foo (a,b,c,d) VALUES ($1, $2, $3, $4)`,
			T: `INSERT INTO foo (a,b,c,d) VALUES (@p1, @p2, @p3, @p4)`,
			N: `INSERT INTO foo (a,b,c,d) VALUES (:name, :age, :first, :last)`,
			V: []string{"name", "age", "first", "last"},
		},
		// This query tests a named parameter ending the string as well as numbers
		{
			Q: `SELECT * FROM a WHERE first_name=:name1 AND last_name=:name2`,
			R: `SELECT * FROM a WHERE first_name=? AND last_name=?`,
			D: `SELECT * FROM a WHERE first_name=$1 AND last_name=$2`,
			T: `SELECT * FROM a WHERE first_name=@p1 AND last_name=@p2`,
			N: `SELECT * FROM a WHERE first_name=:name1 AND last_name=:name2`,
			V: []string{"name1", "name2"},
		},
		{
			Q: `SELECT "::foo" FROM a WHERE first_name=:name1 AND last_name=:name2`,
			R: `SELECT ":foo" FROM a WHERE first_name=? AND last_name=?`,
			D: `SELECT ":foo" FROM a WHERE first_name=$1 AND last_name=$2`,
			T: `SELECT ":foo" FROM a WHERE first_name=@p1 AND last_name=@p2`,
			N: `SELECT ":foo" FROM a WHERE first_name=:name1 AND last_name=:name2`,
			V: []string{"name1", "name2"},
		},
		{
			Q: `SELECT 'a::b::c' || first_name, '::::ABC::_::' FROM person WHERE first_name=:first_name AND last_name=:last_name`,
			R: `SELECT 'a:b:c' || first_name, '::ABC:_:' FROM person WHERE first_name=? AND last_name=?`,
			D: `SELECT 'a:b:c' || first_name, '::ABC:_:' FROM person WHERE first_name=$1 AND last_name=$2`,
			T: `SELECT 'a:b:c' || first_name, '::ABC:_:' FROM person WHERE first_name=@p1 AND last_name=@p2`,
			N: `SELECT 'a:b:c' || first_name, '::ABC:_:' FROM person WHERE first_name=:first_name AND last_name=:last_name`,
			V: []string{"first_name", "last_name"},
		},
		{
			Q: `SELECT @name := "name", :age, :first, :last`,
			R: `SELECT @name := "name", ?, ?, ?`,
			D: `SELECT @name := "name", $1, $2, $3`,
			N: `SELECT @name := "name", :age, :first, :last`,
			T: `SELECT @name := "name", @p1, @p2, @p3`,
			V: []string{"age", "first", "last"},
		},
		// This unicode awareness test sadly fails, because of our byte-wise
		// worldview. We could certainly iterate by Rune instead, though it's a
		// great deal slower, it's probably the RightWay™.
		// {
		// 	Q: `INSERT INTO foo (a,b,c,d) VALUES (:あ, :b, :キコ, :名前)`,
		// 	R: `INSERT INTO foo (a,b,c,d) VALUES (?, ?, ?, ?)`,
		// 	D: `INSERT INTO foo (a,b,c,d) VALUES ($1, $2, $3, $4)`,
		// 	N: []string{"name", "age", "first", "last"},
		// },
	}

	for _, test := range table {
		qr, names, err := compileNamedQuery([]byte(test.Q), PlaceholderQuestion)
		if err != nil {
			t.Error(err)
		}
		if qr != test.R {
			t.Errorf("expected %s, got %s", test.R, qr)
		}
		if len(names) != len(test.V) {
			t.Errorf("expected %#v, got %#v", test.V, names)
		} else {
			for i, name := range names {
				if name != test.V[i] {
					t.Errorf("expected %dth name to be %s, got %s", i+1, test.V[i], name)
				}
			}
		}
		qd, _, _ := compileNamedQuery([]byte(test.Q), PlaceholderDollar)
		if qd != test.D {
			t.Errorf("\nexpected: `%s`\ngot:      `%s`", test.D, qd)
		}

		qt, _, _ := compileNamedQuery([]byte(test.Q), PlaceholderAt)
		if qt != test.T {
			t.Errorf("\nexpected: `%s`\ngot:      `%s`", test.T, qt)
		}

		qq, _, _ := compileNamedQuery([]byte(test.Q), PlaceholderNamed)
		if qq != test.N {
			t.Errorf("\nexpected: `%s`\ngot:      `%s`\n(len: %d vs %d)", test.N, qq, len(test.N), len(qq))
		}
	}
}

type Test struct{ t *testing.T }

func (t Test) Error(err error, msg ...interface{}) {
	t.t.Helper()
	if err != nil {
		if len(msg) == 0 {
			t.t.Error(err)
		} else {
			t.t.Error(msg...)
		}
	}
}

func (t Test) Errorf(err error, format string, args ...interface{}) {
	t.t.Helper()
	if err != nil {
		t.t.Errorf(format, args...)
	}
}

func TestEscapedColons(t *testing.T) {
	t.Skip("not sure it is possible to support this in general case without an SQL parser")

	var qs = `SELECT * FROM testtable WHERE timeposted BETWEEN (now() AT TIME ZONE 'utc') AND
		(now() AT TIME ZONE 'utc') - interval '01:30:00') AND name = '\'this is a test\'' and id = :id`
	_, _, err := compileNamedQuery([]byte(qs), PlaceholderDollar)
	if err != nil {
		t.Error("Didn't handle colons correctly when inside a string")
	}
}

func TestFixBounds(t *testing.T) {
	table := []struct {
		name, query, expect string
		loop                int
	}{
		{
			name:   `named syntax`,
			query:  `INSERT INTO foo (a,b,c,d) VALUES (:name, :age, :first, :last)`,
			expect: `INSERT INTO foo (a,b,c,d) VALUES (:name, :age, :first, :last),(:name, :age, :first, :last)`,
			loop:   2,
		},
		{
			name:   `mysql syntax`,
			query:  `INSERT INTO foo (a,b,c,d) VALUES (?, ?, ?, ?)`,
			expect: `INSERT INTO foo (a,b,c,d) VALUES (?, ?, ?, ?),(?, ?, ?, ?)`,
			loop:   2,
		},
		{
			name:   `named syntax w/ trailer`,
			query:  `INSERT INTO foo (a,b,c,d) VALUES (:name, :age, :first, :last) ;--`,
			expect: `INSERT INTO foo (a,b,c,d) VALUES (:name, :age, :first, :last),(:name, :age, :first, :last) ;--`,
			loop:   2,
		},
		{
			name:   `mysql syntax w/ trailer`,
			query:  `INSERT INTO foo (a,b,c,d) VALUES (?, ?, ?, ?) ;--`,
			expect: `INSERT INTO foo (a,b,c,d) VALUES (?, ?, ?, ?),(?, ?, ?, ?) ;--`,
			loop:   2,
		},
		{
			name:   `not found test`,
			query:  `INSERT INTO foo (a,b,c,d) (:name, :age, :first, :last)`,
			expect: `INSERT INTO foo (a,b,c,d) (:name, :age, :first, :last)`,
			loop:   2,
		},
		{
			name:   `found twice test`,
			query:  `INSERT INTO foo (a,b,c,d) VALUES (:name, :age, :first, :last) VALUES (:name, :age, :first, :last)`,
			expect: `INSERT INTO foo (a,b,c,d) VALUES (:name, :age, :first, :last),(:name, :age, :first, :last) VALUES (:name, :age, :first, :last)`,
			loop:   2,
		},
		{
			name:   `nospace`,
			query:  `INSERT INTO foo (a,b) VALUES(:a, :b)`,
			expect: `INSERT INTO foo (a,b) VALUES(:a, :b),(:a, :b)`,
			loop:   2,
		},
		{
			name:   `lowercase`,
			query:  `INSERT INTO foo (a,b) values(:a, :b)`,
			expect: `INSERT INTO foo (a,b) values(:a, :b),(:a, :b)`,
			loop:   2,
		},
		{
			name:   `on duplicate key using VALUES`,
			query:  `INSERT INTO foo (a,b) VALUES (:a, :b) ON DUPLICATE KEY UPDATE a=VALUES(a)`,
			expect: `INSERT INTO foo (a,b) VALUES (:a, :b),(:a, :b) ON DUPLICATE KEY UPDATE a=VALUES(a)`,
			loop:   2,
		},
		{
			name:   `single column`,
			query:  `INSERT INTO foo (a) VALUES (:a)`,
			expect: `INSERT INTO foo (a) VALUES (:a),(:a)`,
			loop:   2,
		},
		{
			name:   `call now`,
			query:  `INSERT INTO foo (a, b) VALUES (:a, NOW())`,
			expect: `INSERT INTO foo (a, b) VALUES (:a, NOW()),(:a, NOW())`,
			loop:   2,
		},
		{
			name:   `two level depth function call`,
			query:  `INSERT INTO foo (a, b) VALUES (:a, YEAR(NOW()))`,
			expect: `INSERT INTO foo (a, b) VALUES (:a, YEAR(NOW())),(:a, YEAR(NOW()))`,
			loop:   2,
		},
		{
			name:   `missing closing bracket`,
			query:  `INSERT INTO foo (a, b) VALUES (:a, YEAR(NOW())`,
			expect: `INSERT INTO foo (a, b) VALUES (:a, YEAR(NOW())`,
			loop:   2,
		},
		{
			name:   `table with "values" at the end`,
			query:  `INSERT INTO table_values (a, b) VALUES (:a, :b)`,
			expect: `INSERT INTO table_values (a, b) VALUES (:a, :b),(:a, :b)`,
			loop:   2,
		},
		{
			name: `multiline indented query`,
			query: `INSERT INTO foo (
		a,
		b,
		c,
		d
	) VALUES (
		:name,
		:age,
		:first,
		:last
	)`,
			expect: `INSERT INTO foo (
		a,
		b,
		c,
		d
	) VALUES (
		:name,
		:age,
		:first,
		:last
	),(
		:name,
		:age,
		:first,
		:last
	)`,
			loop: 2,
		},
	}

	for _, tc := range table {
		t.Run(tc.name, func(t *testing.T) {
			res := fixBound(tc.query, tc.loop)
			if res != tc.expect {
				t.Errorf("mismatched results")
			}
		})
	}
}

func TestNamedQueries(t *testing.T) {
	RunWithSchema(defaultSchema, t, func(db *DB, t *testing.T, now string) {
		loadDefaultFixture(db, t)
		test := Test{t}

		var (
			ns  *NamedStmt
			err error
			ctx = context.Background()
		)

		// Check that invalid preparations fail
		_, err = db.PrepareNamedContext(ctx, "SELECT * FROM person WHERE first_name=:first:name")
		if err == nil {
			t.Error("Expected an error with invalid prepared statement.")
		}

		_, err = db.PrepareNamedContext(ctx, "invalid sql")
		if err == nil {
			t.Error("Expected an error with invalid prepared statement.")
		}

		// Check closing works as anticipated
		ns, err = db.PrepareNamedContext(ctx, "SELECT * FROM person WHERE first_name=:first_name")
		test.Error(err)

		err = ns.Close()
		test.Error(err)

		ns, err = db.PrepareNamedContext(ctx, `
			SELECT first_name, last_name, email 
			FROM person WHERE first_name=:first_name AND email=:email`)
		test.Error(err)

		// test Queryx w/ uses Query
		p := Person{FirstName: "Jason", LastName: "Moiron", Email: "jmoiron@jmoiron.net"}

		rows, err := ns.QueryxContext(ctx, p)
		test.Error(err)
		for rows.Next() {
			var p2 Person
			rows.StructScan(&p2)
			if p.FirstName != p2.FirstName {
				t.Errorf("got %s, expected %s", p.FirstName, p2.FirstName)
			}
			if p.LastName != p2.LastName {
				t.Errorf("got %s, expected %s", p.LastName, p2.LastName)
			}
			if p.Email != p2.Email {
				t.Errorf("got %s, expected %s", p.Email, p2.Email)
			}
		}

		// test Select
		people := make([]Person, 0, 5)
		err = ns.SelectContext(ctx, &people, p)
		test.Error(err)

		if len(people) != 1 {
			t.Errorf("got %d results, expected %d", len(people), 1)
		}
		if p.FirstName != people[0].FirstName {
			t.Errorf("got %s, expected %s", p.FirstName, people[0].FirstName)
		}
		if p.LastName != people[0].LastName {
			t.Errorf("got %s, expected %s", p.LastName, people[0].LastName)
		}
		if p.Email != people[0].Email {
			t.Errorf("got %s, expected %s", p.Email, people[0].Email)
		}

		// test struct batch inserts
		sls := []Person{
			{FirstName: "Ardie", LastName: "Savea", Email: "asavea@ab.co.nz"},
			{FirstName: "Sonny Bill", LastName: "Williams", Email: "sbw@ab.co.nz"},
			{FirstName: "Ngani", LastName: "Laumape", Email: "nlaumape@ab.co.nz"},
		}

		insert := fmt.Sprintf(
			"INSERT INTO person (first_name, last_name, email, added_at) VALUES (:first_name, :last_name, :email, %v)\n",
			now,
		)
		_, err = db.NamedExec(ctx, insert, sls)
		test.Error(err)

		// test map batch inserts
		slsMap := []map[string]interface{}{
			{"first_name": "Ardie", "last_name": "Savea", "email": "asavea@ab.co.nz"},
			{"first_name": "Sonny Bill", "last_name": "Williams", "email": "sbw@ab.co.nz"},
			{"first_name": "Ngani", "last_name": "Laumape", "email": "nlaumape@ab.co.nz"},
		}

		_, err = db.NamedExec(ctx, `INSERT INTO person (first_name, last_name, email)
			VALUES (:first_name, :last_name, :email) ;--`, slsMap)
		test.Error(err)

		type A map[string]interface{}

		typedMap := []A{
			{"first_name": "Ardie", "last_name": "Savea", "email": "asavea@ab.co.nz"},
			{"first_name": "Sonny Bill", "last_name": "Williams", "email": "sbw@ab.co.nz"},
			{"first_name": "Ngani", "last_name": "Laumape", "email": "nlaumape@ab.co.nz"},
		}

		_, err = db.NamedExec(ctx, `INSERT INTO person (first_name, last_name, email)
			VALUES (:first_name, :last_name, :email) ;--`, typedMap)
		test.Error(err)

		for _, p := range sls {
			dest := Person{}
			err = db.GetContext(ctx, &dest, db.Rebind("SELECT * FROM person WHERE email=?"), p.Email)
			test.Error(err)
			if dest.Email != p.Email {
				t.Errorf("expected %s, got %s", p.Email, dest.Email)
			}
		}

		// test Exec
		ns, err = db.PrepareNamedContext(ctx, `
			INSERT INTO person (first_name, last_name, email)
			VALUES (:first_name, :last_name, :email)`)
		test.Error(err)

		js := Person{
			FirstName: "Julien",
			LastName:  "Savea",
			Email:     "jsavea@ab.co.nz",
		}
		_, err = ns.ExecContext(ctx, js)
		test.Error(err)

		// Make sure we can pull him out again
		p2 := Person{}
		db.GetContext(ctx, &p2, db.Rebind("SELECT * FROM person WHERE email=?"), js.Email)
		if p2.Email != js.Email {
			t.Errorf("expected %s, got %s", js.Email, p2.Email)
		}

		// test Txn NamedStmts
		tx, err := db.Beginx()
		if err != nil {
			t.Fatal(err)
		}
		txns := tx.NamedStmtContext(ctx, ns)

		// We're going to add Steven in this txn
		sl := Person{
			FirstName: "Steven",
			LastName:  "Luatua",
			Email:     "sluatua@ab.co.nz",
		}

		_, err = txns.ExecContext(ctx, sl)
		test.Error(err)
		// then rollback...
		tx.Rollback()
		// looking for Steven after a rollback should fail
		err = db.GetContext(ctx, &p2, db.Rebind("SELECT * FROM person WHERE email=?"), sl.Email)
		if err != sql.ErrNoRows {
			t.Errorf("expected no rows error, got %v", err)
		}

		// now do the same, but commit
		tx, err = db.Beginx()
		if err != nil {
			t.Fatal(err)
		}
		txns = tx.NamedStmtContext(ctx, ns)
		_, err = txns.ExecContext(ctx, sl)
		test.Error(err)
		tx.Commit()

		// looking for Steven after a Commit should succeed
		err = db.GetContext(ctx, &p2, db.Rebind("SELECT * FROM person WHERE email=?"), sl.Email)
		test.Error(err)
		if p2.Email != sl.Email {
			t.Errorf("expected %s, got %s", sl.Email, p2.Email)
		}

	})
}

func TestNamedQuery(t *testing.T) {
	var schema = Schema{
		create: `
			CREATE TABLE place (
				id integer PRIMARY KEY,
				name text NULL
			);
			CREATE TABLE person (
				first_name text NULL,
				last_name text NULL,
				email text NULL
			);
			CREATE TABLE placeperson (
				first_name text NULL,
				last_name text NULL,
				email text NULL,
				place_id integer NULL
			);
			CREATE TABLE jsperson (
				"FIRST" text NULL,
				last_name text NULL,
				"EMAIL" text NULL
			);`,
		drop: `
			drop table person;
			drop table jsperson;
			drop table place;
			drop table placeperson;
			`,
	}

	RunWithSchema(schema, t, func(db *DB, t *testing.T, now string) {
		type Person struct {
			FirstName sql.NullString `db:"first_name"`
			LastName  sql.NullString `db:"last_name"`
			Email     sql.NullString
		}

		p := Person{
			FirstName: sql.NullString{String: "ben", Valid: true},
			LastName:  sql.NullString{String: "doe", Valid: true},
			Email:     sql.NullString{String: "ben@doe.com", Valid: true},
		}

		q1 := `INSERT INTO person (first_name, last_name, email) VALUES (:first_name, :last_name, :email)`
		_, err := db.NamedExec(context.TODO(), q1, p)
		if err != nil {
			log.Fatal(err)
		}

		p2 := &Person{}
		rows, err := db.NamedQuery(context.TODO(), "SELECT * FROM person WHERE first_name=:first_name", p)
		if err != nil {
			log.Fatal(err)
		}
		for rows.Next() {
			err = rows.StructScan(p2)
			if err != nil {
				t.Error(err)
			}
			if p2.FirstName.String != "ben" {
				t.Error("Expected first name of `ben`, got " + p2.FirstName.String)
			}
			if p2.LastName.String != "doe" {
				t.Error("Expected first name of `doe`, got " + p2.LastName.String)
			}
		}

		// these are tests for #73;  they verify that named queries work if you've
		// changed the db mapper.  This code checks both NamedQuery "ad-hoc" style
		// queries and NamedStmt queries, which use different code paths internally.
		old := *db.Mapper

		type JSONPerson struct {
			FirstName sql.NullString `json:"FIRST"`
			LastName  sql.NullString `json:"last_name"`
			Email     sql.NullString
		}

		jp := JSONPerson{
			FirstName: sql.NullString{String: "ben", Valid: true},
			LastName:  sql.NullString{String: "smith", Valid: true},
			Email:     sql.NullString{String: "ben@smith.com", Valid: true},
		}

		db.Mapper = reflectx.NewMapperFunc("json", strings.ToUpper)

		// prepare queries for case sensitivity to test our ToUpper function.
		// postgres and sqlite accept "", but mysql uses ``;  since Go's multi-line
		// strings are `` we use "" by default and swap out for MySQL
		pdb := func(s string, db *DB) string {
			if db.DriverName() == "mysql" {
				return strings.Replace(s, `"`, "`", -1)
			}
			return s
		}

		q1 = `INSERT INTO jsperson ("FIRST", last_name, "EMAIL") VALUES (:FIRST, :last_name, :EMAIL)`
		_, err = db.NamedExec(context.TODO(), pdb(q1, db), jp)
		if err != nil {
			t.Fatal(err, db.DriverName())
		}

		// Checks that a person pulled out of the db matches the one we put in
		check := func(t *testing.T, rows *Rows) {
			jp = JSONPerson{}
			for rows.Next() {
				err = rows.StructScan(&jp)
				if err != nil {
					t.Error(err)
				}
				if jp.FirstName.String != "ben" {
					t.Errorf("Expected first name of `ben`, got `%s` (%s) ", jp.FirstName.String, db.DriverName())
				}
				if jp.LastName.String != "smith" {
					t.Errorf("Expected LastName of `smith`, got `%s` (%s)", jp.LastName.String, db.DriverName())
				}
				if jp.Email.String != "ben@smith.com" {
					t.Errorf("Expected first name of `doe`, got `%s` (%s)", jp.Email.String, db.DriverName())
				}
			}
		}

		ns, err := db.PrepareNamedContext(context.TODO(), pdb(`
			SELECT * FROM jsperson
			WHERE
				"FIRST"=:FIRST AND
				last_name=:last_name AND
				"EMAIL"=:EMAIL
		`, db))

		if err != nil {
			t.Fatal(err)
		}
		rows, err = ns.QueryxContext(context.TODO(), jp)
		if err != nil {
			t.Fatal(err)
		}

		check(t, rows)

		// Check exactly the same thing, but with db.NamedQuery, which does not go
		// through the PrepareNamed/NamedStmt path.
		rows, err = db.NamedQuery(context.TODO(), pdb(`
			SELECT * FROM jsperson
			WHERE
				"FIRST"=:FIRST AND
				last_name=:last_name AND
				"EMAIL"=:EMAIL
		`, db), jp)
		if err != nil {
			t.Fatal(err)
		}

		check(t, rows)

		db.Mapper = &old

		// Test nested structs
		type Place struct {
			ID   int            `db:"id"`
			Name sql.NullString `db:"name"`
		}
		type PlacePerson struct {
			FirstName sql.NullString `db:"first_name"`
			LastName  sql.NullString `db:"last_name"`
			Email     sql.NullString
			Place     Place `db:"place"`
		}

		pl := Place{
			Name: sql.NullString{String: "myplace", Valid: true},
		}

		pp := PlacePerson{
			FirstName: sql.NullString{String: "ben", Valid: true},
			LastName:  sql.NullString{String: "doe", Valid: true},
			Email:     sql.NullString{String: "ben@doe.com", Valid: true},
		}

		q2 := `INSERT INTO place (id, name) VALUES (1, :name)`
		_, err = db.NamedExec(context.TODO(), q2, pl)
		if err != nil {
			log.Fatal(err)
		}

		id := 1
		pp.Place.ID = id

		q3 := `INSERT INTO placeperson (first_name, last_name, email, place_id) VALUES (:first_name, :last_name, :email, :place.id)`
		_, err = db.NamedExec(context.TODO(), q3, pp)
		if err != nil {
			log.Fatal(err)
		}

		pp2 := &PlacePerson{}
		rows, err = db.NamedQuery(context.TODO(), `
			SELECT
				first_name,
				last_name,
				email,
				place.id AS "place.id",
				place.name AS "place.name"
			FROM placeperson
			INNER JOIN place ON place.id = placeperson.place_id
			WHERE
				place.id=:place.id`, pp)
		if err != nil {
			log.Fatal(err)
		}
		for rows.Next() {
			err = rows.StructScan(pp2)
			if err != nil {
				t.Error(err)
			}
			if pp2.FirstName.String != "ben" {
				t.Error("Expected first name of `ben`, got " + pp2.FirstName.String)
			}
			if pp2.LastName.String != "doe" {
				t.Error("Expected first name of `doe`, got " + pp2.LastName.String)
			}
			if pp2.Place.Name.String != "myplace" {
				t.Error("Expected place name of `myplace`, got " + pp2.Place.Name.String)
			}
			if pp2.Place.ID != pp.Place.ID {
				t.Errorf("Expected place name of %v, got %v", pp.Place.ID, pp2.Place.ID)
			}
		}
	})
}

func TestNamedStruct(t *testing.T) {
	var err error

	q1 := `INSERT INTO foo (a, b, c, d) VALUES (:name, :age, :first, :last)`

	type tt struct {
		Name  string
		Age   int
		First string
		Last  string
	}

	type tt2 struct {
		Field1 string `db:"field_1"`
		Field2 string `db:"field_2"`
	}

	type tt3 struct {
		tt2
		Name string
	}

	am := tt{"Jason Moiron", 30, "Jason", "Moiron"}

	bq, args, _ := bindStruct(PlaceholderQuestion, q1, am, mapper())
	expect := `INSERT INTO foo (a, b, c, d) VALUES (?, ?, ?, ?)`
	if bq != expect {
		t.Errorf("Interpolation of query failed: got `%v`, expected `%v`\n", bq, expect)
	}

	if args[0].(string) != "Jason Moiron" {
		t.Errorf("Expected `Jason Moiron`, got %v\n", args[0])
	}

	if args[1].(int) != 30 {
		t.Errorf("Expected 30, got %v\n", args[1])
	}

	if args[2].(string) != "Jason" {
		t.Errorf("Expected Jason, got %v\n", args[2])
	}

	if args[3].(string) != "Moiron" {
		t.Errorf("Expected Moiron, got %v\n", args[3])
	}

	am2 := tt2{"Hello", "World"}
	bq, args, _ = bindStruct(PlaceholderQuestion, "INSERT INTO foo (a, b) VALUES (:field_2, :field_1)", am2, mapper())
	expect = `INSERT INTO foo (a, b) VALUES (?, ?)`
	if bq != expect {
		t.Errorf("Interpolation of query failed: got `%v`, expected `%v`\n", bq, expect)
	}

	if args[0].(string) != "World" {
		t.Errorf("Expected 'World', got %s\n", args[0].(string))
	}
	if args[1].(string) != "Hello" {
		t.Errorf("Expected 'Hello', got %s\n", args[1].(string))
	}

	am3 := tt3{Name: "Hello!"}
	am3.Field1 = "Hello"
	am3.Field2 = "World"

	bq, args, err = bindStruct(PlaceholderQuestion, "INSERT INTO foo (a, b, c) VALUES (:name, :field_1, :field_2)", am3, mapper())

	if err != nil {
		t.Fatal(err)
	}

	expect = `INSERT INTO foo (a, b, c) VALUES (?, ?, ?)`
	if bq != expect {
		t.Errorf("Interpolation of query failed: got `%v`, expected `%v`\n", bq, expect)
	}

	if args[0].(string) != "Hello!" {
		t.Errorf("Expected 'Hello!', got %s\n", args[0].(string))
	}
	if args[1].(string) != "Hello" {
		t.Errorf("Expected 'Hello', got %s\n", args[1].(string))
	}
	if args[2].(string) != "World" {
		t.Errorf("Expected 'World', got %s\n", args[0].(string))
	}
}

func TestPlaceholderNamedMapper(t *testing.T) {
	type A map[string]interface{}
	m := reflectx.NewMapperFunc("db", NameMapper)
	query, args, err := bindNamedMapper(PlaceholderDollar, `select :x`, A{
		"x": "X!",
	}, m)
	if err != nil {
		t.Fatal(err)
	}

	got := fmt.Sprintf("%s %s", query, args)
	want := `select $1 [X!]`
	if got != want {
		t.Errorf("\ngot:  %q\nwant: %q", got, want)
	}

	_, _, err = bindNamedMapper(PlaceholderDollar, `select :x`, map[string]string{
		"x": "X!",
	}, m)
	if err == nil {
		t.Fatal("err is nil")
	}
	if !strings.Contains(err.Error(), "unsupported map type") {
		t.Errorf("wrong error: %s", err)
	}
}

func BenchmarkNamedStruct(b *testing.B) {
	b.StopTimer()
	q1 := `INSERT INTO foo (a, b, c, d) VALUES (:name, :age, :first, :last)`
	type t struct {
		Name  string
		Age   int
		First string
		Last  string
	}
	am := t{"Jason Moiron", 30, "Jason", "Moiron"}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		bindStruct(PlaceholderDollar, q1, am, mapper())
	}
}

func BenchmarkNamedMap(b *testing.B) {
	b.StopTimer()
	q1 := `INSERT INTO foo (a, b, c, d) VALUES (:name, :age, :first, :last)`
	am := map[string]interface{}{
		"name":  "Jason Moiron",
		"age":   30,
		"first": "Jason",
		"last":  "Moiron",
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		bindMap(PlaceholderDollar, q1, am)
	}
}
