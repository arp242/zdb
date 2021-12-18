// The following environment variables, if set, will be used:
//
//	* SQLX_SQLITE_DSN
//	* SQLX_POSTGRES_DSN
//	* SQLX_MYSQL_DSN
//
// Set any of these variables to 'skip' to skip them.  Note that for MySQL,
// the string '?parseTime=True' will be appended to the DSN if it's not there
// already.
package sqlx

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
)

var _, _ ColScanner = &Row{}, &Rows{}

var (
	TestPostgres = true
	TestSqlite   = true
	TestMysql    = true

	sldb    *DB
	pgdb    *DB
	mysqldb *DB
)

func init() {
	ConnectAll()
}

func ConnectAll() {
	var err error

	pgdsn := os.Getenv("SQLX_POSTGRES_DSN")
	mydsn := os.Getenv("SQLX_MYSQL_DSN")
	sqdsn := os.Getenv("SQLX_SQLITE_DSN")

	TestPostgres = pgdsn != "skip"
	TestMysql = mydsn != "skip"
	TestSqlite = sqdsn != "skip"

	if !strings.Contains(mydsn, "parseTime=true") {
		mydsn += "?parseTime=true"
	}

	if TestPostgres {
		pgdb, err = ConnectContext(context.TODO(), "postgres", pgdsn)
		if err != nil {
			fmt.Printf("Disabling PG tests: %v\n", err)
			TestPostgres = false
		}
	} else {
		fmt.Println("Disabling Postgres tests.")
	}

	if TestMysql {
		mysqldb, err = ConnectContext(context.TODO(), "mysql", mydsn)
		if err != nil {
			fmt.Printf("Disabling MySQL tests: %v\n", err)
			TestMysql = false
		}
	} else {
		fmt.Println("Disabling MySQL tests.")
	}

	if TestSqlite {
		sldb, err = ConnectContext(context.TODO(), "sqlite3", sqdsn)
		if err != nil {
			fmt.Printf("Disabling SQLite: %v", err)
			TestSqlite = false
		}
	} else {
		fmt.Println("Disabling SQLite tests.")
	}
}

type Schema struct {
	create string
	drop   string
}

func (s Schema) Postgres() (string, string, string) {
	return s.create, s.drop, `now()`
}

func (s Schema) MySQL() (string, string, string) {
	return strings.Replace(s.create, `"`, "`", -1), s.drop, `now()`
}

func (s Schema) Sqlite3() (string, string, string) {
	return strings.Replace(s.create, `now()`, `CURRENT_TIMESTAMP`, -1), s.drop, `CURRENT_TIMESTAMP`
}

var defaultSchema = Schema{
	create: `
CREATE TABLE person (
	first_name text,
	last_name text,
	email text,
	added_at timestamp default now()
);

CREATE TABLE place (
	country text,
	city text NULL,
	telcode integer
);

CREATE TABLE capplace (
	"COUNTRY" text,
	"CITY" text NULL,
	"TELCODE" integer
);

CREATE TABLE nullperson (
    first_name text NULL,
    last_name text NULL,
    email text NULL
);

CREATE TABLE employees (
	name text,
	id integer,
	boss_id integer
);

`,
	drop: `
drop table person;
drop table place;
drop table capplace;
drop table nullperson;
drop table employees;
`,
}

type Person struct {
	FirstName string `db:"first_name"`
	LastName  string `db:"last_name"`
	Email     string
	AddedAt   time.Time `db:"added_at"`
}

type Person2 struct {
	FirstName sql.NullString `db:"first_name"`
	LastName  sql.NullString `db:"last_name"`
	Email     sql.NullString
}

type Place struct {
	Country string
	City    sql.NullString
	TelCode int
}

type PlacePtr struct {
	Country string
	City    *string
	TelCode int
}

type PersonPlace struct {
	Person
	Place
}

type PersonPlacePtr struct {
	*Person
	*Place
}

type EmbedConflict struct {
	FirstName string `db:"first_name"`
	Person
}

type SliceMember struct {
	Country   string
	City      sql.NullString
	TelCode   int
	People    []Person `db:"-"`
	Addresses []Place  `db:"-"`
}

// Note that because of field map caching, we need a new type here
// if we've used Place already somewhere in sqlx
type CPlace Place

func MultiExec(ctx context.Context, e ExecerContext, query string) {
	stmts := strings.Split(query, ";\n")
	if len(strings.Trim(stmts[len(stmts)-1], " \n\t\r")) == 0 {
		stmts = stmts[:len(stmts)-1]
	}
	for _, s := range stmts {
		_, err := e.ExecContext(ctx, s)
		if err != nil {
			fmt.Println(err, s)
		}
	}
}

func RunWithSchema(schema Schema, t *testing.T, test func(db *DB, t *testing.T, now string)) {
	runner := func(db *DB, t *testing.T, create, drop, now string) {
		defer func() {
			MultiExec(context.TODO(), db, drop)
		}()

		MultiExec(context.TODO(), db, create)
		test(db, t, now)
	}

	if TestPostgres {
		create, drop, now := schema.Postgres()
		runner(pgdb, t, create, drop, now)
	}
	if TestSqlite {
		create, drop, now := schema.Sqlite3()
		runner(sldb, t, create, drop, now)
	}
	if TestMysql {
		create, drop, now := schema.MySQL()
		runner(mysqldb, t, create, drop, now)
	}
}

func loadDefaultFixture(db *DB, t *testing.T) {
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	exec := func(query string, params ...interface{}) {
		t.Helper()
		_, err := tx.ExecContext(context.TODO(), db.Rebind(query), params...)
		if err != nil {
			t.Fatal(err)
		}
	}

	exec("INSERT INTO person (first_name, last_name, email) VALUES (?, ?, ?)", "Jason", "Moiron", "jmoiron@jmoiron.net")
	exec("INSERT INTO person (first_name, last_name, email) VALUES (?, ?, ?)", "John", "Doe", "johndoeDNE@gmail.net")
	exec("INSERT INTO place (country, city, telcode) VALUES (?, ?, ?)", "United States", "New York", "1")
	exec("INSERT INTO place (country, telcode) VALUES (?, ?)", "Hong Kong", "852")
	exec("INSERT INTO place (country, telcode) VALUES (?, ?)", "Singapore", "65")
	if db.DriverName() == "mysql" {
		exec("INSERT INTO capplace (`COUNTRY`, `TELCODE`) VALUES (?, ?)", "Sarf Efrica", "27")
	} else {
		exec("INSERT INTO capplace (\"COUNTRY\", \"TELCODE\") VALUES (?, ?)", "Sarf Efrica", "27")
	}
	exec("INSERT INTO employees (name, id) VALUES (?, ?)", "Peter", "4444")
	exec("INSERT INTO employees (name, id, boss_id) VALUES (?, ?, ?)", "Joe", "1", "4444")
	exec("INSERT INTO employees (name, id, boss_id) VALUES (?, ?, ?)", "Martin", "2", "4444")

	err = tx.Commit()
	if err != nil {
		t.Fatal(err)
	}
}

// Test a new backwards compatible feature, that missing scan destinations
// will silently scan into sql.RawText rather than failing/panicing
func TestMissingNames(t *testing.T) {
	RunWithSchema(defaultSchema, t, func(db *DB, t *testing.T, now string) {
		loadDefaultFixture(db, t)
		type PersonPlus struct {
			FirstName string `db:"first_name"`
			LastName  string `db:"last_name"`
			Email     string
			//AddedAt time.Time `db:"added_at"`
		}

		// test Select first
		pps := []PersonPlus{}
		// pps lacks added_at destination
		err := db.SelectContext(context.TODO(), &pps, "SELECT * FROM person")
		if err == nil {
			t.Error("Expected missing name from Select to fail, but it did not.")
		}

		// test Get
		pp := PersonPlus{}
		err = db.GetContext(context.TODO(), &pp, "SELECT * FROM person LIMIT 1")
		if err == nil {
			t.Error("Expected missing name Get to fail, but it did not.")
		}

		// test naked StructScan
		pps = []PersonPlus{}
		rows, err := db.Query("SELECT * FROM person LIMIT 1")
		if err != nil {
			t.Fatal(err)
		}
		rows.Next()
		err = StructScan(rows, &pps)
		if err == nil {
			t.Error("Expected missing name in StructScan to fail, but it did not.")
		}
		rows.Close()

		// now try various things with unsafe set.
		db = db.Unsafe()
		pps = []PersonPlus{}
		err = db.SelectContext(context.TODO(), &pps, "SELECT * FROM person")
		if err != nil {
			t.Error(err)
		}

		// test Get
		pp = PersonPlus{}
		err = db.GetContext(context.TODO(), &pp, "SELECT * FROM person LIMIT 1")
		if err != nil {
			t.Error(err)
		}

		// test naked StructScan
		pps = []PersonPlus{}
		rowsx, err := db.QueryxContext(context.TODO(), "SELECT * FROM person LIMIT 1")
		if err != nil {
			t.Fatal(err)
		}
		rowsx.Next()
		err = StructScan(rowsx, &pps)
		if err != nil {
			t.Error(err)
		}
		rowsx.Close()

		// test Named stmt
		if !isUnsafe(db) {
			t.Error("Expected db to be unsafe, but it isn't")
		}
		nstmt, err := db.PrepareNamedContext(context.TODO(), `SELECT * FROM person WHERE first_name != :name`)
		if err != nil {
			t.Fatal(err)
		}
		// its internal stmt should be marked unsafe
		if !nstmt.Stmt.unsafe {
			t.Error("expected NamedStmt to be unsafe but its underlying stmt did not inherit safety")
		}
		pps = []PersonPlus{}
		err = nstmt.SelectContext(context.TODO(), &pps, map[string]interface{}{"name": "Jason"})
		if err != nil {
			t.Fatal(err)
		}
		if len(pps) != 1 {
			t.Errorf("Expected 1 person back, got %d", len(pps))
		}

		// test it with a safe db
		db.unsafe = false
		if isUnsafe(db) {
			t.Error("expected db to be safe but it isn't")
		}
		nstmt, err = db.PrepareNamedContext(context.TODO(), `SELECT * FROM person WHERE first_name != :name`)
		if err != nil {
			t.Fatal(err)
		}
		// it should be safe
		if isUnsafe(nstmt) {
			t.Error("NamedStmt did not inherit safety")
		}
		nstmt.Unsafe()
		if !isUnsafe(nstmt) {
			t.Error("expected newly unsafed NamedStmt to be unsafe")
		}
		pps = []PersonPlus{}
		err = nstmt.SelectContext(context.TODO(), &pps, map[string]interface{}{"name": "Jason"})
		if err != nil {
			t.Fatal(err)
		}
		if len(pps) != 1 {
			t.Errorf("Expected 1 person back, got %d", len(pps))
		}

	})
}

func TestEmbeddedStructs(t *testing.T) {
	type Loop1 struct{ Person }
	type Loop2 struct{ Loop1 }
	type Loop3 struct{ Loop2 }

	RunWithSchema(defaultSchema, t, func(db *DB, t *testing.T, now string) {
		loadDefaultFixture(db, t)
		peopleAndPlaces := []PersonPlace{}
		err := db.SelectContext(context.TODO(),
			&peopleAndPlaces,
			`SELECT person.*, place.* FROM
             person natural join place`)
		if err != nil {
			t.Fatal(err)
		}
		for _, pp := range peopleAndPlaces {
			if len(pp.Person.FirstName) == 0 {
				t.Errorf("Expected non zero lengthed first name.")
			}
			if len(pp.Place.Country) == 0 {
				t.Errorf("Expected non zero lengthed country.")
			}
		}

		// test embedded structs with StructScan
		rows, err := db.QueryxContext(context.TODO(),
			`SELECT person.*, place.* FROM
         person natural join place`)
		if err != nil {
			t.Error(err)
		}

		perp := PersonPlace{}
		rows.Next()
		err = rows.StructScan(&perp)
		if err != nil {
			t.Error(err)
		}

		if len(perp.Person.FirstName) == 0 {
			t.Errorf("Expected non zero lengthed first name.")
		}
		if len(perp.Place.Country) == 0 {
			t.Errorf("Expected non zero lengthed country.")
		}

		rows.Close()

		// test the same for embedded pointer structs
		peopleAndPlacesPtrs := []PersonPlacePtr{}
		err = db.SelectContext(context.TODO(),
			&peopleAndPlacesPtrs,
			`SELECT person.*, place.* FROM
             person natural join place`)
		if err != nil {
			t.Fatal(err)
		}
		for _, pp := range peopleAndPlacesPtrs {
			if len(pp.Person.FirstName) == 0 {
				t.Errorf("Expected non zero lengthed first name.")
			}
			if len(pp.Place.Country) == 0 {
				t.Errorf("Expected non zero lengthed country.")
			}
		}

		// test "deep nesting"
		l3s := []Loop3{}
		err = db.SelectContext(context.TODO(), &l3s, `select * from person`)
		if err != nil {
			t.Fatal(err)
		}
		for _, l3 := range l3s {
			if len(l3.Loop2.Loop1.Person.FirstName) == 0 {
				t.Errorf("Expected non zero lengthed first name.")
			}
		}

		// test "embed conflicts"
		ec := []EmbedConflict{}
		err = db.SelectContext(context.TODO(), &ec, `select * from person`)
		// I'm torn between erroring here or having some kind of working behavior
		// in order to allow for more flexibility in destination structs
		if err != nil {
			t.Errorf("Was not expecting an error on embed conflicts.")
		}
	})
}

func TestJoinQuery(t *testing.T) {
	type Employee struct {
		Name string
		ID   int64
		// BossID is an id into the employee table
		BossID sql.NullInt64 `db:"boss_id"`
	}
	type Boss Employee

	RunWithSchema(defaultSchema, t, func(db *DB, t *testing.T, now string) {
		loadDefaultFixture(db, t)

		var employees []struct {
			Employee
			Boss `db:"boss"`
		}

		err := db.SelectContext(context.TODO(),
			&employees,
			`SELECT employees.*, boss.id "boss.id", boss.name "boss.name" FROM employees
			  JOIN employees AS boss ON employees.boss_id = boss.id`)
		if err != nil {
			t.Fatal(err)
		}

		for _, em := range employees {
			if len(em.Employee.Name) == 0 {
				t.Errorf("Expected non zero lengthed name.")
			}
			if em.Employee.BossID.Int64 != em.Boss.ID {
				t.Errorf("Expected boss ids to match")
			}
		}
	})
}

func TestJoinQueryNamedPointerStructs(t *testing.T) {
	type Employee struct {
		Name string
		ID   int64
		// BossID is an id into the employee table
		BossID sql.NullInt64 `db:"boss_id"`
	}
	type Boss Employee

	RunWithSchema(defaultSchema, t, func(db *DB, t *testing.T, now string) {
		loadDefaultFixture(db, t)

		var employees []struct {
			Emp1  *Employee `db:"emp1"`
			Emp2  *Employee `db:"emp2"`
			*Boss `db:"boss"`
		}

		err := db.SelectContext(context.TODO(),
			&employees,
			`SELECT emp.name "emp1.name", emp.id "emp1.id", emp.boss_id "emp1.boss_id",
			 emp.name "emp2.name", emp.id "emp2.id", emp.boss_id "emp2.boss_id",
			 boss.id "boss.id", boss.name "boss.name" FROM employees AS emp
			  JOIN employees AS boss ON emp.boss_id = boss.id
			  `)
		if err != nil {
			t.Fatal(err)
		}

		for _, em := range employees {
			if len(em.Emp1.Name) == 0 || len(em.Emp2.Name) == 0 {
				t.Errorf("Expected non zero lengthed name.")
			}
			if em.Emp1.BossID.Int64 != em.Boss.ID || em.Emp2.BossID.Int64 != em.Boss.ID {
				t.Errorf("Expected boss ids to match")
			}
		}
	})
}

func TestSelectSliceMapTime(t *testing.T) {
	RunWithSchema(defaultSchema, t, func(db *DB, t *testing.T, now string) {
		loadDefaultFixture(db, t)
		rows, err := db.QueryxContext(context.TODO(), "SELECT * FROM person")
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			_, err := rows.SliceScan()
			if err != nil {
				t.Error(err)
			}
		}

		rows, err = db.QueryxContext(context.TODO(), "SELECT * FROM person")
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			m := map[string]interface{}{}
			err := rows.MapScan(m)
			if err != nil {
				t.Error(err)
			}
		}

	})
}

func TestNilReceiver(t *testing.T) {
	RunWithSchema(defaultSchema, t, func(db *DB, t *testing.T, now string) {
		loadDefaultFixture(db, t)
		var p *Person
		err := db.GetContext(context.TODO(), p, "SELECT * FROM person LIMIT 1")
		if err == nil {
			t.Error("Expected error when getting into nil struct ptr.")
		}
		var pp *[]Person
		err = db.SelectContext(context.TODO(), pp, "SELECT * FROM person")
		if err == nil {
			t.Error("Expected an error when selecting into nil slice ptr.")
		}
	})
}

func TestNilInserts(t *testing.T) {
	var schema = Schema{
		create: `
			CREATE TABLE tt (
				id integer,
				value text NULL DEFAULT NULL
			);`,
		drop: "drop table tt;",
	}

	RunWithSchema(schema, t, func(db *DB, t *testing.T, now string) {
		type TT struct {
			ID    int
			Value *string
		}
		var v, v2 TT
		r := db.Rebind

		_, err := db.ExecContext(context.TODO(), r(`INSERT INTO tt (id) VALUES (1)`))
		if err != nil {
			t.Fatal(err)
		}
		db.GetContext(context.TODO(), &v, r(`SELECT * FROM tt`))
		if v.ID != 1 {
			t.Errorf("Expecting id of 1, got %v", v.ID)
		}
		if v.Value != nil {
			t.Errorf("Expecting NULL to map to nil, got %s", *v.Value)
		}

		v.ID = 2
		// NOTE: this incidentally uncovered a bug which was that named queries with
		// pointer destinations would not work if the passed value here was not addressable,
		// as reflectx.FieldByIndexes attempts to allocate nil pointer receivers for
		// writing.  This was fixed by creating & using the reflectx.FieldByIndexesReadOnly
		// function.  This next line is important as it provides the only coverage for this.
		db.NamedExec(context.TODO(), `INSERT INTO tt (id, value) VALUES (:id, :value)`, v)

		db.GetContext(context.TODO(), &v2, r(`SELECT * FROM tt WHERE id=2`))
		if v.ID != v2.ID {
			t.Errorf("%v != %v", v.ID, v2.ID)
		}
		if v2.Value != nil {
			t.Errorf("Expecting NULL to map to nil, got %s", *v.Value)
		}
	})
}

func TestScanError(t *testing.T) {
	var schema = Schema{
		create: `
			CREATE TABLE kv (
				k text,
				v integer
			);`,
		drop: `drop table kv;`,
	}

	RunWithSchema(schema, t, func(db *DB, t *testing.T, now string) {
		type WrongTypes struct {
			K int
			V string
		}
		_, err := db.ExecContext(context.TODO(), db.Rebind("INSERT INTO kv (k, v) VALUES (?, ?)"), "hi", 1)
		if err != nil {
			t.Error(err)
		}

		rows, err := db.QueryxContext(context.TODO(), "SELECT * FROM kv")
		if err != nil {
			t.Error(err)
		}
		for rows.Next() {
			var wt WrongTypes
			err := rows.StructScan(&wt)
			if err == nil {
				t.Errorf("%s: Scanning wrong types into keys should have errored.", db.DriverName())
			}
		}
	})
}

func TestMultiInsert(t *testing.T) {
	RunWithSchema(defaultSchema, t, func(db *DB, t *testing.T, now string) {
		loadDefaultFixture(db, t)
		q := db.Rebind(`INSERT INTO employees (name, id) VALUES (?, ?), (?, ?);`)
		_, err := db.ExecContext(context.TODO(), q,
			"Name1", 400,
			"name2", 500,
		)
		if err != nil {
			t.Fatal(err)
		}
	})
}

// FIXME: this function is kinda big but it slows things down to be constantly
// loading and reloading the schema..

func TestUsage(t *testing.T) {
	RunWithSchema(defaultSchema, t, func(db *DB, t *testing.T, now string) {
		loadDefaultFixture(db, t)
		slicemembers := []SliceMember{}
		err := db.SelectContext(context.TODO(), &slicemembers, "SELECT * FROM place ORDER BY telcode ASC")
		if err != nil {
			t.Fatal(err)
		}

		people := []Person{}

		err = db.SelectContext(context.TODO(), &people, "SELECT * FROM person ORDER BY first_name ASC")
		if err != nil {
			t.Fatal(err)
		}

		jason, john := people[0], people[1]
		if jason.FirstName != "Jason" {
			t.Errorf("Expecting FirstName of Jason, got %s", jason.FirstName)
		}
		if jason.LastName != "Moiron" {
			t.Errorf("Expecting LastName of Moiron, got %s", jason.LastName)
		}
		if jason.Email != "jmoiron@jmoiron.net" {
			t.Errorf("Expecting Email of jmoiron@jmoiron.net, got %s", jason.Email)
		}
		if john.FirstName != "John" || john.LastName != "Doe" || john.Email != "johndoeDNE@gmail.net" {
			t.Errorf("John Doe's person record not what expected:  Got %v\n", john)
		}

		jason = Person{}
		err = db.GetContext(context.TODO(), &jason, db.Rebind("SELECT * FROM person WHERE first_name=?"), "Jason")

		if err != nil {
			t.Fatal(err)
		}
		if jason.FirstName != "Jason" {
			t.Errorf("Expecting to get back Jason, but got %v\n", jason.FirstName)
		}

		err = db.GetContext(context.TODO(), &jason, db.Rebind("SELECT * FROM person WHERE first_name=?"), "Foobar")
		if err == nil {
			t.Errorf("Expecting an error, got nil\n")
		}
		if err != sql.ErrNoRows {
			t.Errorf("Expected sql.ErrNoRows, got %v\n", err)
		}

		// The following tests check statement reuse, which was actually a problem
		// due to copying being done when creating Stmt's which was eventually removed
		stmt1, err := db.PreparexContext(context.TODO(), db.Rebind("SELECT * FROM person WHERE first_name=?"))
		if err != nil {
			t.Fatal(err)
		}
		jason = Person{}

		row := stmt1.QueryRowxContext(context.TODO(), "DoesNotExist")
		row.Scan(&jason)
		row = stmt1.QueryRowxContext(context.TODO(), "DoesNotExist")
		row.Scan(&jason)

		err = stmt1.GetContext(context.TODO(), &jason, "DoesNotExist User")
		if err == nil {
			t.Error("Expected an error")
		}
		err = stmt1.GetContext(context.TODO(), &jason, "DoesNotExist User 2")
		if err == nil {
			t.Fatal(err)
		}

		stmt2, err := db.PreparexContext(context.TODO(), db.Rebind("SELECT * FROM person WHERE first_name=?"))
		if err != nil {
			t.Fatal(err)
		}
		jason = Person{}
		tx, err := db.Beginx()
		if err != nil {
			t.Fatal(err)
		}
		tstmt2 := tx.StmtxContext(context.TODO(), stmt2)
		row2 := tstmt2.QueryRowxContext(context.TODO(), "Jason")
		err = row2.StructScan(&jason)
		if err != nil {
			t.Error(err)
		}
		tx.Commit()

		places := []*Place{}
		err = db.SelectContext(context.TODO(), &places, "SELECT telcode FROM place ORDER BY telcode ASC")
		if err != nil {
			t.Fatal(err)
		}

		usa, singsing, honkers := places[0], places[1], places[2]

		if usa.TelCode != 1 || honkers.TelCode != 852 || singsing.TelCode != 65 {
			t.Errorf("Expected integer telcodes to work, got %#v", places)
		}

		placesptr := []PlacePtr{}
		err = db.SelectContext(context.TODO(), &placesptr, "SELECT * FROM place ORDER BY telcode ASC")
		if err != nil {
			t.Error(err)
		}
		//fmt.Printf("%#v\n%#v\n%#v\n", placesptr[0], placesptr[1], placesptr[2])

		// if you have null fields and use SELECT *, you must use sql.Null* in your struct
		// this test also verifies that you can use either a []Struct{} or a []*Struct{}
		places2 := []Place{}
		err = db.SelectContext(context.TODO(), &places2, "SELECT * FROM place ORDER BY telcode ASC")
		if err != nil {
			t.Fatal(err)
		}

		usa, singsing, honkers = &places2[0], &places2[1], &places2[2]

		// this should return a type error that &p is not a pointer to a struct slice
		p := Place{}
		err = db.SelectContext(context.TODO(), &p, "SELECT * FROM place ORDER BY telcode ASC")
		if err == nil {
			t.Errorf("Expected an error, argument to select should be a pointer to a struct slice")
		}

		// this should be an error
		pl := []Place{}
		err = db.SelectContext(context.TODO(), pl, "SELECT * FROM place ORDER BY telcode ASC")
		if err == nil {
			t.Errorf("Expected an error, argument to select should be a pointer to a struct slice, not a slice.")
		}

		if usa.TelCode != 1 || honkers.TelCode != 852 || singsing.TelCode != 65 {
			t.Errorf("Expected integer telcodes to work, got %#v", places)
		}

		stmt, err := db.PreparexContext(context.TODO(), db.Rebind("SELECT country, telcode FROM place WHERE telcode > ? ORDER BY telcode ASC"))
		if err != nil {
			t.Error(err)
		}

		places = []*Place{}
		err = stmt.SelectContext(context.TODO(), &places, 10)
		if len(places) != 2 {
			t.Error("Expected 2 places, got 0.")
		}
		if err != nil {
			t.Fatal(err)
		}
		singsing, honkers = places[0], places[1]
		if singsing.TelCode != 65 || honkers.TelCode != 852 {
			t.Errorf("Expected the right telcodes, got %#v", places)
		}

		rows, err := db.QueryxContext(context.TODO(), "SELECT * FROM place")
		if err != nil {
			t.Fatal(err)
		}
		place := Place{}
		for rows.Next() {
			err = rows.StructScan(&place)
			if err != nil {
				t.Fatal(err)
			}
		}

		rows, err = db.QueryxContext(context.TODO(), "SELECT * FROM place")
		if err != nil {
			t.Fatal(err)
		}
		m := map[string]interface{}{}
		for rows.Next() {
			err = rows.MapScan(m)
			if err != nil {
				t.Fatal(err)
			}
			_, ok := m["country"]
			if !ok {
				t.Errorf("Expected key `country` in map but could not find it (%#v)\n", m)
			}
		}

		rows, err = db.QueryxContext(context.TODO(), "SELECT * FROM place")
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			s, err := rows.SliceScan()
			if err != nil {
				t.Error(err)
			}
			if len(s) != 3 {
				t.Errorf("Expected 3 columns in result, got %d\n", len(s))
			}
		}

		// test advanced querying
		// test that NamedExec works with a map as well as a struct
		_, err = db.NamedExec(context.TODO(), "INSERT INTO person (first_name, last_name, email) VALUES (:first, :last, :email)", map[string]interface{}{
			"first": "Bin",
			"last":  "Smuth",
			"email": "bensmith@allblacks.nz",
		})
		if err != nil {
			t.Fatal(err)
		}

		// ensure that if the named param happens right at the end it still works
		// ensure that NamedQuery works with a map[string]interface{}
		rows, err = db.NamedQuery(context.TODO(), "SELECT * FROM person WHERE first_name=:first", map[string]interface{}{"first": "Bin"})
		if err != nil {
			t.Fatal(err)
		}

		ben := &Person{}
		for rows.Next() {
			err = rows.StructScan(ben)
			if err != nil {
				t.Fatal(err)
			}
			if ben.FirstName != "Bin" {
				t.Fatal("Expected first name of `Bin`, got " + ben.FirstName)
			}
			if ben.LastName != "Smuth" {
				t.Fatal("Expected first name of `Smuth`, got " + ben.LastName)
			}
		}

		ben.FirstName = "Ben"
		ben.LastName = "Smith"
		ben.Email = "binsmuth@allblacks.nz"

		// Insert via a named query using the struct
		_, err = db.NamedExec(context.TODO(), "INSERT INTO person (first_name, last_name, email) VALUES (:first_name, :last_name, :email)", ben)

		if err != nil {
			t.Fatal(err)
		}

		rows, err = db.NamedQuery(context.TODO(), "SELECT * FROM person WHERE first_name=:first_name", ben)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			err = rows.StructScan(ben)
			if err != nil {
				t.Fatal(err)
			}
			if ben.FirstName != "Ben" {
				t.Fatal("Expected first name of `Ben`, got " + ben.FirstName)
			}
			if ben.LastName != "Smith" {
				t.Fatal("Expected first name of `Smith`, got " + ben.LastName)
			}
		}
		// ensure that Get does not panic on emppty result set
		person := &Person{}
		err = db.GetContext(context.TODO(), person, "SELECT * FROM person WHERE first_name=$1", "does-not-exist")
		if err == nil {
			t.Fatal("Should have got an error for Get on non-existent row.")
		}

		// lets test prepared statements some more

		stmt, err = db.PreparexContext(context.TODO(), db.Rebind("SELECT * FROM person WHERE first_name=?"))
		if err != nil {
			t.Fatal(err)
		}
		rows, err = stmt.QueryxContext(context.TODO(), "Ben")
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			err = rows.StructScan(ben)
			if err != nil {
				t.Fatal(err)
			}
			if ben.FirstName != "Ben" {
				t.Fatal("Expected first name of `Ben`, got " + ben.FirstName)
			}
			if ben.LastName != "Smith" {
				t.Fatal("Expected first name of `Smith`, got " + ben.LastName)
			}
		}

		john = Person{}
		stmt, err = db.PreparexContext(context.TODO(), db.Rebind("SELECT * FROM person WHERE first_name=?"))
		if err != nil {
			t.Error(err)
		}
		err = stmt.GetContext(context.TODO(), &john, "John")
		if err != nil {
			t.Error(err)
		}

		// test name mapping
		// THIS USED TO WORK BUT WILL NO LONGER WORK.
		db.MapperFunc(strings.ToUpper)
		rsa := CPlace{}
		err = db.GetContext(context.TODO(), &rsa, "SELECT * FROM capplace;")
		if err != nil {
			t.Error(err, "in db:", db.DriverName())
		}
		db.MapperFunc(strings.ToLower)

		// create a copy and change the mapper, then verify the copy behaves
		// differently from the original.
		dbCopy := NewDb(db.DB, db.DriverName())
		dbCopy.MapperFunc(strings.ToUpper)
		err = dbCopy.GetContext(context.TODO(), &rsa, "SELECT * FROM capplace;")
		if err != nil {
			fmt.Println(db.DriverName())
			t.Error(err)
		}

		err = db.GetContext(context.TODO(), &rsa, "SELECT * FROM cappplace;")
		if err == nil {
			t.Error("Expected no error, got ", err)
		}

		// test base type slices
		var sdest []string
		rows, err = db.QueryxContext(context.TODO(), "SELECT email FROM person ORDER BY email ASC;")
		if err != nil {
			t.Error(err)
		}
		err = scanAll(rows, &sdest, false)
		if err != nil {
			t.Error(err)
		}

		// test Get with base types
		var count int
		err = db.GetContext(context.TODO(), &count, "SELECT count(*) FROM person;")
		if err != nil {
			t.Error(err)
		}
		if count != len(sdest) {
			t.Errorf("Expected %d == %d (count(*) vs len(SELECT ..)", count, len(sdest))
		}

		// test Get and Select with time.Time, #84
		var addedAt time.Time
		err = db.GetContext(context.TODO(), &addedAt, "SELECT added_at FROM person LIMIT 1;")
		if err != nil {
			t.Error(err)
		}

		var addedAts []time.Time
		err = db.SelectContext(context.TODO(), &addedAts, "SELECT added_at FROM person;")
		if err != nil {
			t.Error(err)
		}

		// test it on a double pointer
		var pcount *int
		err = db.GetContext(context.TODO(), &pcount, "SELECT count(*) FROM person;")
		if err != nil {
			t.Error(err)
		}
		if *pcount != count {
			t.Errorf("expected %d = %d", *pcount, count)
		}

		// test Select...
		sdest = []string{}
		err = db.SelectContext(context.TODO(), &sdest, "SELECT first_name FROM person ORDER BY first_name ASC;")
		if err != nil {
			t.Error(err)
		}
		expected := []string{"Ben", "Bin", "Jason", "John"}
		for i, got := range sdest {
			if got != expected[i] {
				t.Errorf("Expected %d result to be %s, but got %s", i, expected[i], got)
			}
		}

		var nsdest []sql.NullString
		err = db.SelectContext(context.TODO(), &nsdest, "SELECT city FROM place ORDER BY city ASC")
		if err != nil {
			t.Error(err)
		}
		for _, val := range nsdest {
			if val.Valid && val.String != "New York" {
				t.Errorf("expected single valid result to be `New York`, but got %s", val.String)
			}
		}
	})
}

// tests that sqlx will not panic when the wrong driver is passed because
// of an automatic nil dereference in sqlx.Open(), which was fixed.
func TestDoNotPanicOnConnect(t *testing.T) {
	db, err := ConnectContext(context.TODO(), "bogus", "hehe")
	if err == nil {
		t.Errorf("Should return error when using bogus driverName")
	}
	if db != nil {
		t.Errorf("Should not return the db on a connect failure")
	}
}

// Test for #117, embedded nil maps

type Message struct {
	Text       string      `db:"string"`
	Properties PropertyMap `db:"properties"` // Stored as JSON in the database
}

type PropertyMap map[string]string

// Implement driver.Valuer and sql.Scanner interfaces on PropertyMap
func (p PropertyMap) Value() (driver.Value, error) {
	if len(p) == 0 {
		return nil, nil
	}
	return json.Marshal(p)
}

func (p PropertyMap) Scan(src interface{}) error {
	v := reflect.ValueOf(src)
	if !v.IsValid() || v.CanAddr() && v.IsNil() {
		return nil
	}
	switch ts := src.(type) {
	case []byte:
		return json.Unmarshal(ts, &p)
	case string:
		return json.Unmarshal([]byte(ts), &p)
	default:
		return fmt.Errorf("Could not not decode type %T -> %T", src, p)
	}
}

func TestEmbeddedMaps(t *testing.T) {
	var schema = Schema{
		create: `
			CREATE TABLE message (
				string text,
				properties text
			);`,
		drop: `drop table message;`,
	}

	RunWithSchema(schema, t, func(db *DB, t *testing.T, now string) {
		messages := []Message{
			{"Hello, World", PropertyMap{"one": "1", "two": "2"}},
			{"Thanks, Joy", PropertyMap{"pull": "request"}},
		}
		q1 := `INSERT INTO message (string, properties) VALUES (:string, :properties);`
		for _, m := range messages {
			_, err := db.NamedExec(context.TODO(), q1, m)
			if err != nil {
				t.Fatal(err)
			}
		}
		var count int
		err := db.GetContext(context.TODO(), &count, "SELECT count(*) FROM message")
		if err != nil {
			t.Fatal(err)
		}
		if count != len(messages) {
			t.Fatalf("Expected %d messages in DB, found %d", len(messages), count)
		}

		var m Message
		err = db.GetContext(context.TODO(), &m, "SELECT * FROM message LIMIT 1;")
		if err != nil {
			t.Fatal(err)
		}
		if m.Properties == nil {
			t.Fatal("Expected m.Properties to not be nil, but it was.")
		}
	})
}

func TestIssue197(t *testing.T) {
	// this test actually tests for a bug in database/sql:
	//   https://github.com/golang/go/issues/13905
	// this potentially makes _any_ named type that is an alias for []byte
	// unsafe to use in a lot of different ways (basically, unsafe to hold
	// onto after loading from the database).
	t.Skip()

	type mybyte []byte
	type Var struct{ Raw json.RawMessage }
	type Var2 struct{ Raw []byte }
	type Var3 struct{ Raw mybyte }
	RunWithSchema(defaultSchema, t, func(db *DB, t *testing.T, now string) {
		var err error
		var v, q Var
		if err = db.GetContext(context.TODO(), &v, `SELECT '{"a": "b"}' AS raw`); err != nil {
			t.Fatal(err)
		}
		if err = db.GetContext(context.TODO(), &q, `SELECT 'null' AS raw`); err != nil {
			t.Fatal(err)
		}

		var v2, q2 Var2
		if err = db.GetContext(context.TODO(), &v2, `SELECT '{"a": "b"}' AS raw`); err != nil {
			t.Fatal(err)
		}
		if err = db.GetContext(context.TODO(), &q2, `SELECT 'null' AS raw`); err != nil {
			t.Fatal(err)
		}

		var v3, q3 Var3
		if err = db.QueryRow(`SELECT '{"a": "b"}' AS raw`).Scan(&v3.Raw); err != nil {
			t.Fatal(err)
		}
		if err = db.QueryRow(`SELECT '{"c": "d"}' AS raw`).Scan(&q3.Raw); err != nil {
			t.Fatal(err)
		}
		t.Fail()
	})
}

func TestEmbeddedLiterals(t *testing.T) {
	var schema = Schema{
		create: `
			CREATE TABLE x (
				k text
			);`,
		drop: `drop table x;`,
	}

	RunWithSchema(schema, t, func(db *DB, t *testing.T, now string) {
		type t1 struct {
			K *string
		}
		type t2 struct {
			Inline struct {
				F string
			}
			K *string
		}

		_, err := db.ExecContext(context.TODO(), db.Rebind("INSERT INTO x (k) VALUES (?), (?), (?);"), "one", "two", "three")
		if err != nil {
			t.Fatal(err)
		}

		target := t1{}
		err = db.GetContext(context.TODO(), &target, db.Rebind("SELECT * FROM x WHERE k=?"), "one")
		if err != nil {
			t.Error(err)
		}
		if *target.K != "one" {
			t.Error("Expected target.K to be `one`, got ", target.K)
		}

		target2 := t2{}
		err = db.GetContext(context.TODO(), &target2, db.Rebind("SELECT * FROM x WHERE k=?"), "one")
		if err != nil {
			t.Error(err)
		}
		if *target2.K != "one" {
			t.Errorf("Expected target2.K to be `one`, got `%v`", target2.K)
		}
	})
}

func TestConn(t *testing.T) {
	var schema = Schema{
		create: `
			CREATE TABLE tt_conn (
				id integer,
				value text NULL DEFAULT NULL
			);`,
		drop: "drop table tt_conn;",
	}

	RunWithSchema(schema, t, func(db *DB, t *testing.T, now string) {
		ctx := context.Background()

		conn, err := db.Connx(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		if err != nil {
			t.Fatal(err)
		}

		_, err = conn.ExecContext(ctx, conn.Rebind(`INSERT INTO tt_conn (id, value) VALUES (?, ?), (?, ?)`), 1, "a", 2, "b")
		if err != nil {
			t.Fatal(err)
		}

		type s struct {
			ID    int    `db:"id"`
			Value string `db:"value"`
		}

		v := []s{}

		err = conn.SelectContext(ctx, &v, "SELECT * FROM tt_conn ORDER BY id ASC")
		if err != nil {
			t.Fatal(err)
		}

		if v[0].ID != 1 {
			t.Errorf("Expecting ID of 1, got %d", v[0].ID)
		}

		v1 := s{}
		err = conn.GetContext(ctx, &v1, conn.Rebind("SELECT * FROM tt_conn WHERE id=?"), 1)

		if err != nil {
			t.Fatal(err)
		}
		if v1.ID != 1 {
			t.Errorf("Expecting to get back 1, but got %v\n", v1.ID)
		}

		stmt, err := conn.PreparexContext(ctx, conn.Rebind("SELECT * FROM tt_conn WHERE id=?"))
		if err != nil {
			t.Fatal(err)
		}
		v1 = s{}
		tx, err := conn.BeginTxx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		tstmt := tx.StmtxContext(context.TODO(), stmt)
		row := tstmt.QueryRowxContext(context.TODO(), 1)
		err = row.StructScan(&v1)
		if err != nil {
			t.Error(err)
		}
		tx.Commit()
		if v1.ID != 1 {
			t.Errorf("Expecting to get back 1, but got %v\n", v1.ID)
		}

		rows, err := conn.QueryxContext(ctx, "SELECT * FROM tt_conn")
		if err != nil {
			t.Fatal(err)
		}

		for rows.Next() {
			err = rows.StructScan(&v1)
			if err != nil {
				t.Fatal(err)
			}
		}
	})
}
