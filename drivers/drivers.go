package drivers

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"sync"
	"testing"
)

// NotExistError is returned by a driver when a database doesn't exist and
// Create is false in the connection arguments.
type NotExistError struct {
	Driver  string // Driver name
	DB      string // Database name
	Connect string // Full connect string
}

func (err NotExistError) Error() string {
	if err.Driver == "" {
		return fmt.Sprintf("%s database exists but is empty (from connection string %q)",
			err.Driver, err.Driver+":"+err.Connect)
	}
	return fmt.Sprintf("%s database %q doesn't exist (from connection string %q)",
		err.Driver, err.DB, err.Driver+":"+err.Connect)
}

// Driver for a SQL connection.
type Driver interface {
	// Name of this driver.
	Name() string

	// SQL dialect for the database engine; "sqlite", "postgresql", or "mysql".
	Dialect() string

	// Connect to the database with the given connect string, which has
	// everything before the "+" removed.
	//
	// If create is true, it should attempt to create the database if it doesn't
	// exist.
	Connect(ctx context.Context, connect string, create bool) (*sql.DB, bool, error)

	// ErrUnique reports if this error reports a UNIQUE constraint violation.
	ErrUnique(error) bool

	// Start a new test. This is expected to set up a temporary database which
	// is cleaned at the end.
	StartTest(*testing.T, *TestOptions) context.Context
}

var (
	drivers   = make(map[string]Driver)
	driversMu sync.Mutex
)

// RegisterDriver registers a new Driver.
func RegisterDriver(d Driver) {
	driversMu.Lock()
	defer driversMu.Unlock()

	_, ok := drivers[d.Name()]
	if ok {
		panic(fmt.Sprintf("zdb.RegisterDriver: driver %q is already registered", d.Name()))
	}
	drivers[d.Name()] = d
}

// Drivers returns a list of currently registered drivers.
func Drivers() []Driver {
	driversMu.Lock()
	defer driversMu.Unlock()

	d := make([]Driver, 0, len(drivers))
	for _, v := range drivers {
		d = append(d, v)
	}
	return d
}

// TODO: temporarily.
func Test() func() {
	driversMu.Lock()
	defer driversMu.Unlock()

	save := make(map[string]Driver)
	for k, v := range drivers {
		save[k] = v
	}
	drivers = make(map[string]Driver)
	return func() {
		driversMu.Lock()
		defer driversMu.Unlock()
		drivers = save
	}
}

// TestOptions are options to pass to zdb.Connect() in the StartTest() method.
//
// This needs to be a new type to avoid import cycles.
type TestOptions struct {
	Connect string
	Files   fs.FS
}
