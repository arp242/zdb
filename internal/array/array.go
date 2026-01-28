// Copy from pq

package array

import (
	"bytes"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"fmt"
	"reflect"
	"strconv"
	"time"
)

var (
	typeByteSlice    = reflect.TypeOf([]byte{})
	typeDriverValuer = reflect.TypeOf((*driver.Valuer)(nil)).Elem()
	typeSQLScanner   = reflect.TypeOf((*sql.Scanner)(nil)).Elem()
)

func Array(a any) driver.Valuer {
	// All passed as []any from bulk
	return GenericArray{a}
}

// GenericArray implements the driver.Valuer and sql.Scanner interfaces for
// an array or slice of any dimension.
type GenericArray struct{ A any }

func (GenericArray) evaluateDestination(rt reflect.Type) (reflect.Type, func([]byte, reflect.Value) error, string) {
	var assign func([]byte, reflect.Value) error

	// TODO calculate the assign function for other types
	// TODO repeat this section on the element type of arrays or slices (multidimensional)
	{
		if reflect.PtrTo(rt).Implements(typeSQLScanner) {
			// dest is always addressable because it is an element of a slice.
			assign = func(src []byte, dest reflect.Value) (err error) {
				ss := dest.Addr().Interface().(sql.Scanner)
				if src == nil {
					err = ss.Scan(nil)
				} else {
					err = ss.Scan(src)
				}
				return
			}
			goto FoundType
		}

		assign = func([]byte, reflect.Value) error {
			return fmt.Errorf("pq: scanning to %s is not implemented; only sql.Scanner", rt)
		}
	}

FoundType:

	return rt, assign, ","
}

// Value implements the driver.Valuer interface.
func (a GenericArray) Value() (driver.Value, error) {
	if a.A == nil {
		return nil, nil
	}

	rv := reflect.ValueOf(a.A)
	switch rv.Kind() {
	case reflect.Slice:
		if rv.IsNil() {
			return nil, nil
		}
	case reflect.Array:
	default:
		return nil, fmt.Errorf("pq: Unable to convert %T to array", a.A)
	}

	if n := rv.Len(); n > 0 {
		// There will be at least two curly brackets, N bytes of values,
		// and N-1 bytes of delimiters.
		b := make([]byte, 0, 1+2*n)
		b, err := appendArray(b, rv, n)
		return string(b), err
	}

	return "{}", nil
}

// appendArray appends rv to the buffer, returning the extended buffer and
// the delimiter used between elements.
//
// It panics when n <= 0 or rv's Kind is not reflect.Array nor reflect.Slice.
func appendArray(b []byte, rv reflect.Value, n int) ([]byte, error) {
	var err error
	b = append(b, '{')

	b, err = appendArrayElement(b, rv.Index(0))
	if err != nil {
		return b, err
	}

	for i := 1; i < n; i++ {
		b = append(b, ',')
		b, err = appendArrayElement(b, rv.Index(i))
		if err != nil {
			return b, err
		}
	}

	return append(b, '}'), nil
}

// appendArrayElement appends rv to the buffer, returning the extended buffer
// and the delimiter to use before the next element.
//
// When rv's Kind is neither reflect.Array nor reflect.Slice, it is converted
// using driver.DefaultParameterConverter and the resulting []byte or string
// is double-quoted.
//
// See http://www.postgresql.org/docs/current/static/arrays.html#ARRAYS-IO
func appendArrayElement(b []byte, rv reflect.Value) ([]byte, error) {
	if k := rv.Kind(); k == reflect.Array || k == reflect.Slice {
		if t := rv.Type(); t != typeByteSlice && !t.Implements(typeDriverValuer) {
			if n := rv.Len(); n > 0 {
				return appendArray(b, rv, n)
			}
			return b, nil
		}
	}

	var err error
	var iv any = rv.Interface()

	iv, err = driver.DefaultParameterConverter.ConvertValue(iv)
	if err != nil {
		return b, err
	}

	switch v := iv.(type) {
	case nil:
		return append(b, "NULL"...), nil
	case []byte:
		b = append(b, '"')
		b = append(b, fmt.Sprintf(`\\x%x`, v)...)
		b = append(b, '"')
		return b, nil
	case string:
		return appendArrayQuotedBytes(b, []byte(v)), nil
	}

	b, err = appendValue(b, iv)
	return b, err
}

func appendArrayQuotedBytes(b, v []byte) []byte {
	b = append(b, '"')
	for {
		i := bytes.IndexAny(v, `"\`)
		if i < 0 {
			b = append(b, v...)
			break
		}
		if i > 0 {
			b = append(b, v[:i]...)
		}
		b = append(b, '\\', v[i])
		v = v[i+1:]
	}
	return append(b, '"')
}

func appendValue(b []byte, v driver.Value) ([]byte, error) {
	return append(b, encode(v, 0)...), nil
}

// Parse a bytea value received from the server.  Both "hex" and the legacy
// "escape" format are supported.
func parseBytea(s []byte) (result []byte, err error) {
	if len(s) >= 2 && bytes.Equal(s[:2], []byte("\\x")) {
		// bytea_output = hex
		s = s[2:] // trim off leading "\\x"
		result = make([]byte, hex.DecodedLen(len(s)))
		_, err := hex.Decode(result, s)
		if err != nil {
			return nil, err
		}
	} else {
		// bytea_output = escape
		for len(s) > 0 {
			if s[0] == '\\' {
				// escaped '\\'
				if len(s) >= 2 && s[1] == '\\' {
					result = append(result, '\\')
					s = s[2:]
					continue
				}

				// '\\' followed by an octal number
				if len(s) < 4 {
					return nil, fmt.Errorf("invalid bytea sequence %v", s)
				}
				r, err := strconv.ParseUint(string(s[1:4]), 8, 8)
				if err != nil {
					return nil, fmt.Errorf("could not parse bytea value: %s", err.Error())
				}
				result = append(result, byte(r))
				s = s[4:]
			} else {
				// We hit an unescaped, raw byte.  Try to read in as many as
				// possible in one go.
				i := bytes.IndexByte(s, '\\')
				if i == -1 {
					result = append(result, s...)
					break
				}
				result = append(result, s[:i]...)
				s = s[i:]
			}
		}
	}

	return result, nil
}

const oidBytea = uint32(17)

func encode(x any, oid uint32) []byte {
	switch v := x.(type) {
	case int64:
		return strconv.AppendInt(nil, v, 10)
	case float64:
		return strconv.AppendFloat(nil, v, 'f', -1, 64)
	case []byte:
		if oid == oidBytea {
			return encodeBytea(v)
		}
		return v
	case string:
		if oid == oidBytea {
			return encodeBytea([]byte(v))
		}
		return []byte(v)
	case bool:
		return strconv.AppendBool(nil, v)
	case time.Time:
		return formatTs(v)
	default:
		panic(fmt.Sprintf("pq: encode: unknown type for %T", v))
	}
}

func encodeBytea(v []byte) (result []byte) {
	result = make([]byte, 2+hex.EncodedLen(len(v)))
	result[0] = '\\'
	result[1] = 'x'
	hex.Encode(result[2:], v)
	return result
}

// formatTs formats t into a format postgres understands.
func formatTs(t time.Time) []byte {
	return FormatTimestamp(t)
}

// FormatTimestamp formats t into Postgres' text format for timestamps.
func FormatTimestamp(t time.Time) []byte {
	// Need to send dates before 0001 A.D. with " BC" suffix, instead of the
	// minus sign preferred by Go.
	// Beware, "0000" in ISO is "1 BC", "-0001" is "2 BC" and so on
	bc := false
	if t.Year() <= 0 {
		// flip year sign, and add 1, e.g: "0" will be "1", and "-10" will be "11"
		t = t.AddDate((-t.Year())*2+1, 0, 0)
		bc = true
	}
	b := []byte(t.Format("2006-01-02 15:04:05.999999999Z07:00"))

	_, offset := t.Zone()
	offset %= 60
	if offset != 0 {
		// RFC3339Nano already printed the minus sign
		if offset < 0 {
			offset = -offset
		}

		b = append(b, ':')
		if offset < 10 {
			b = append(b, '0')
		}
		b = strconv.AppendInt(b, int64(offset), 10)
	}

	if bc {
		b = append(b, " BC"...)
	}
	return b
}
