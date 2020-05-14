package zdb

import (
	"database/sql/driver"
	"fmt"
	"strings"

	"zgo.at/utils/floatutil"
	"zgo.at/utils/intutil"
	"zgo.at/utils/stringutil"
)

// Ints stores a slice of []int64 as a comma-separated string.
type Ints []int64

func (l Ints) String() string {
	return intutil.Join(l, ", ")
}

// Value determines what to store in the DB.
func (l Ints) Value() (driver.Value, error) {
	return intutil.Join(l, ","), nil
}

// Scan converts the data from the DB.
func (l *Ints) Scan(v interface{}) error {
	if v == nil {
		return nil
	}

	var err error
	*l, err = intutil.Split(fmt.Sprintf("%s", v), ",")
	return err
}

// MarshalText converts the data to a human readable representation.
func (l Ints) MarshalText() ([]byte, error) {
	v, err := l.Value()
	return []byte(fmt.Sprintf("%s", v)), err
}

// UnmarshalText parses text in to the Go data structure.
func (l *Ints) UnmarshalText(v []byte) error {
	return l.Scan(v)
}

// Floats stores a slice of []float64 as a comma-separated string.
type Floats []float64

func (l Floats) String() string {
	return floatutil.Join(l, ", ")
}

// Value determines what to store in the DB.
func (l Floats) Value() (driver.Value, error) {
	return floatutil.Join(l, ","), nil
}

// Scan converts the data from the DB.
func (l *Floats) Scan(v interface{}) error {
	if v == nil {
		return nil
	}

	var err error
	*l, err = floatutil.Split(fmt.Sprintf("%s", v), ",")
	return err
}

// MarshalText converts the data to a human readable representation.
func (l Floats) MarshalText() ([]byte, error) {
	v, err := l.Value()
	return []byte(fmt.Sprintf("%s", v)), err
}

// UnmarshalText parses text in to the Go data structure.
func (l *Floats) UnmarshalText(v []byte) error {
	return l.Scan(v)
}

// Strings stores a slice of []string as a comma-separated string.
//
// Note this only works for simple strings (e.g. enums), it DOES NOT ESCAPE
// COMMAS, and you will run in to problems if you use it for arbitrary text.
//
// You're probably better off using e.g. arrays in PostgreSQL or JSON in SQLite,
// if you can. This is intended just for simple cross-SQL-engine use cases.
type Strings []string

func (l Strings) String() string {
	return strings.Join(l, ", ")
}

// Value determines what to store in the DB.
func (l Strings) Value() (driver.Value, error) {
	return strings.Join(stringutil.Filter(l, stringutil.FilterEmpty), ","), nil
}

// Scan converts the data from the DB.
func (l *Strings) Scan(v interface{}) error {
	if v == nil {
		return nil
	}
	strs := []string{}
	for _, s := range strings.Split(fmt.Sprintf("%s", v), ",") {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		strs = append(strs, s)
	}
	*l = strs
	return nil
}

// MarshalText converts the data to a human readable representation.
func (l Strings) MarshalText() ([]byte, error) {
	v, err := l.Value()
	return []byte(fmt.Sprintf("%s", v)), err
}

// UnmarshalText parses text in to the Go data structure.
func (l *Strings) UnmarshalText(v []byte) error {
	return l.Scan(v)
}

// Bool converts various types to a boolean.
//
// It's always stored as an integer in the database (the only cross-platform way
// in SQL).
//
// Supported types:
//
//   bool
//   int* and float*     0 or 1
//   []byte and string   "1", "true", "on", "0", "false", "off"
//   nil                 defaults to false
type Bool bool

// Scan converts the data from the DB.
func (b *Bool) Scan(src interface{}) error {
	if b == nil {
		return fmt.Errorf("zdb.Bool: not initialized")
	}

	switch v := src.(type) {
	default:
		return fmt.Errorf("zdb.Bool: unsupported type %T", src)
	case nil:
		*b = false
	case bool:
		*b = Bool(v)
	case int:
		*b = v != 0
	case int8:
		*b = v != 0
	case int16:
		*b = v != 0
	case int32:
		*b = v != 0
	case int64:
		*b = v != 0
	case uint:
		*b = v != 0
	case uint8:
		*b = v != 0
	case uint16:
		*b = v != 0
	case uint32:
		*b = v != 0
	case uint64:
		*b = v != 0
	case float32:
		*b = v != 0
	case float64:
		*b = v != 0

	case []byte, string:
		var text string
		raw, ok := v.([]byte)
		if !ok {
			text = v.(string)
		} else if len(raw) == 1 {
			// Handle the bit(1) column type.
			*b = raw[0] == 1
			return nil
		} else {
			text = string(raw)
		}

		switch strings.TrimSpace(strings.ToLower(text)) {
		case "true", "1", "on":
			*b = true
		case "false", "0", "off":
			*b = false
		default:
			return fmt.Errorf("zdb.Bool: invalid value %q", text)
		}
	}

	return nil
}

// Value converts a bool type into a number to persist it in the database.
func (b Bool) Value() (driver.Value, error) {
	if b {
		return int64(1), nil
	}
	return int64(0), nil
}

// MarshalJSON converts the data to JSON.
func (b Bool) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("%t", b)), nil
}

// UnmarshalJSON converts the data from JSON.
func (b *Bool) UnmarshalJSON(text []byte) error {
	switch string(text) {
	case "true":
		*b = true
		return nil
	case "false":
		*b = false
		return nil
	default:
		return fmt.Errorf("zdb.Bool: unknown value: %s", text)
	}
}

// MarshalText converts the data to a human readable representation.
func (b Bool) MarshalText() ([]byte, error) {
	return []byte(fmt.Sprintf("%t", b)), nil
}

// UnmarshalText parses text in to the Go data structure.
func (b *Bool) UnmarshalText(text []byte) error {
	if b == nil {
		return fmt.Errorf("zdb.Bool: not initialized")
	}

	switch strings.TrimSpace(strings.ToLower(string(text))) {
	case "true", "1", `"true"`:
		*b = true
	case "false", "0", `"false"`:
		*b = false
	default:
		return fmt.Errorf("zdb.Bool: invalid value %q", text)
	}

	return nil
}
