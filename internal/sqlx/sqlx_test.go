// The following environment variables, if set, will be used:
//
//   - SQLX_SQLITE_DSN
//   - SQLX_POSTGRES_DSN
//   - SQLX_MYSQL_DSN
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
	"log"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
	"zgo.at/zdb/internal/sqlx/reflectx"
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
	var (
		err   error
		pgdsn = os.Getenv("SQLX_POSTGRES_DSN")
		mydsn = os.Getenv("SQLX_MYSQL_DSN")
		sqdsn = os.Getenv("SQLX_SQLITE_DSN")
	)
	TestPostgres = pgdsn != "skip"
	TestMysql = mydsn != "skip"
	TestSqlite = sqdsn != "skip"

	if !strings.Contains(mydsn, "parseTime=true") {
		mydsn += "?parseTime=true"
	}

	if TestPostgres {
		pgdb, err = Connect(context.TODO(), "postgres", pgdsn)
		if err != nil {
			fmt.Printf("Disabling PG tests: %v\n", err)
			TestPostgres = false
		}
	} else {
		fmt.Println("Disabling Postgres tests.")
	}

	if TestMysql {
		mysqldb, err = Connect(context.TODO(), "mysql", mydsn)
		if err != nil {
			fmt.Printf("Disabling MySQL tests: %v\n", err)
			TestMysql = false
		}
	} else {
		fmt.Println("Disabling MySQL tests.")
	}

	if TestSqlite {
		sldb, err = Connect(context.TODO(), "sqlite3", sqdsn)
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

func RunWithSchema(schema Schema, t *testing.T, test func(db *DB, t *testing.T, now string)) {
	runner := func(db *DB, t *testing.T, create, drop, now string) {
		exec := func(query string) {
			stmts := strings.Split(query, ";\n")
			if len(strings.Trim(stmts[len(stmts)-1], " \n\t\r")) == 0 {
				stmts = stmts[:len(stmts)-1]
			}
			for _, s := range stmts {
				_, err := db.ExecContext(context.TODO(), s)
				if err != nil {
					fmt.Println(err, s)
				}
			}
		}

		defer func() { exec(drop) }()
		exec(create)
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

	exec := func(query string, params ...any) {
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
			m := map[string]any{}
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

		places = []*Place{}
		err = db.SelectContext(context.TODO(), &places,
			db.Rebind("SELECT country, telcode FROM place WHERE telcode > ? ORDER BY telcode ASC"),
			10)
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
		m := map[string]any{}
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
		_, err = db.NamedExec(context.TODO(), "INSERT INTO person (first_name, last_name, email) VALUES (:first, :last, :email)", map[string]any{
			"first": "Bin",
			"last":  "Smuth",
			"email": "bensmith@allblacks.nz",
		})
		if err != nil {
			t.Fatal(err)
		}

		// ensure that if the named param happens right at the end it still works
		// ensure that NamedQuery works with a map[string]any
		rows, err = db.NamedQuery(context.TODO(), "SELECT * FROM person WHERE first_name=:first", map[string]any{"first": "Bin"})
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
		_, err = db.NamedExec(context.TODO(),
			"INSERT INTO person (first_name, last_name, email) VALUES (:first_name, :last_name, :email)",
			ben)

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
	db, err := Connect(context.TODO(), "bogus", "hehe")
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

func (p PropertyMap) Scan(src any) error {
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

func TestNamedQueries(t *testing.T) {
	RunWithSchema(defaultSchema, t, func(db *DB, t *testing.T, now string) {
		loadDefaultFixture(db, t)
		test := Test{t}

		var (
			err error
			ctx = context.Background()
		)

		// Check that invalid preparations fail
		_, err = db.NamedExec(ctx, "SELECT * FROM person WHERE first_name=:first:name", struct{}{})
		if err == nil {
			t.Error("Expected an error with invalid prepared statement.")
		}

		_, err = db.NamedExec(ctx, "invalid sql", struct{}{})
		if err == nil {
			t.Error("Expected an error with invalid prepared statement.")
		}

		_, err = db.NamedExec(ctx, `
 			SELECT first_name, last_name, email
 			FROM person WHERE first_name=:first_name AND email=:email`,
			map[string]any{"first_name": "asd", "email": "def"})
		test.Error(err)

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
		slsMap := []map[string]any{
			{"first_name": "Ardie", "last_name": "Savea", "email": "asavea@ab.co.nz"},
			{"first_name": "Sonny Bill", "last_name": "Williams", "email": "sbw@ab.co.nz"},
			{"first_name": "Ngani", "last_name": "Laumape", "email": "nlaumape@ab.co.nz"},
		}

		_, err = db.NamedExec(ctx, `INSERT INTO person (first_name, last_name, email)
 			VALUES (:first_name, :last_name, :email) ;--`, slsMap)
		test.Error(err)

		type A map[string]any

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
		_, err = db.NamedExec(ctx, `
 			INSERT INTO person (first_name, last_name, email)
 			VALUES (:first_name, :last_name, :email)`,
			map[string]any{"first_name": "Julien", "last_name": "Savea", "email": "jsavea@ab.co.nz"})
		test.Error(err)

		js := Person{
			FirstName: "Julien",
			LastName:  "Savea",
			Email:     "jsavea@ab.co.nz",
		}

		// Make sure we can pull him out again
		p2 := Person{}
		db.GetContext(ctx, &p2, db.Rebind("SELECT * FROM person WHERE email=?"), js.Email)
		if p2.Email != js.Email {
			t.Errorf("expected %s, got %s", js.Email, p2.Email)
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

		db.Mapper = reflectx.NewMapper("json", strings.ToUpper)

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
