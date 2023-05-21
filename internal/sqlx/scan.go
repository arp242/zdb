package sqlx

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"

	"zgo.at/zdb/internal/sqlx/reflectx"
)

type ErrMissingField struct{ Column, Type string }

func (e ErrMissingField) Error() string {
	return fmt.Sprintf("missing column %q in type %T", e.Column, e.Type)
}

// StructScan is like sql.Rows.Scan, but scans a single Row into a single Struct.
// Use this and iterate over Rows manually when the memory load of Select() might be
// prohibitive.  *Rows.StructScan caches the reflect work of matching up column
// positions to fields to avoid that overhead per scan, which means it is not safe
// to run StructScan on the same Rows instance with different struct types.
func (r *Rows) StructScan(dest any) error {
	v := reflect.ValueOf(dest)

	if v.Kind() != reflect.Ptr {
		return errors.New("must pass a pointer, not a value, to StructScan destination")
	}

	v = v.Elem()

	var missErr error
	if !r.started {
		columns, err := r.Columns()
		if err != nil {
			return err
		}
		m := r.Mapper

		r.fields = m.TraversalsByName(v.Type(), columns)
		if f, err := missingFields(r.fields); err != nil {
			missErr = &ErrMissingField{
				Column: columns[f],
				Type:   fmt.Sprintf("%T", dest),
			}
		}
		r.values = make([]any, len(columns))
		r.started = true
	}

	err := fieldsByTraversal(v, r.fields, r.values, true)
	if err != nil {
		return err
	}
	// scan into the struct field pointers and append to our results
	err = r.Scan(r.values...)
	if err != nil {
		return err
	}
	err = r.Err()
	if err != nil {
		return err
	}

	return missErr
}

func (r *Row) scanAny(dest any, structOnly bool) error {
	if r.err != nil {
		return r.err
	}
	if r.rows == nil {
		r.err = sql.ErrNoRows
		return r.err
	}
	defer r.rows.Close()

	v := reflect.ValueOf(dest)
	if v.Kind() != reflect.Ptr {
		return errors.New("must pass a pointer, not a value, to StructScan destination")
	}
	if v.IsNil() {
		return errors.New("nil pointer passed to StructScan destination")
	}

	base := reflectx.Deref(v.Type())
	scannable := isScannable(base)

	if structOnly && scannable {
		return structOnlyError(base)
	}

	columns, err := r.Columns()
	if err != nil {
		return err
	}

	if scannable && len(columns) > 1 {
		return fmt.Errorf("scannable dest type %s with >1 columns (%d) in result", base.Kind(), len(columns))
	}

	if scannable {
		return r.Scan(dest)
	}

	m := r.Mapper

	var missErr error
	fields := m.TraversalsByName(v.Type(), columns)
	if f, err := missingFields(fields); err != nil {
		missErr = &ErrMissingField{
			Column: columns[f],
			Type:   fmt.Sprintf("%T", dest),
		}

	}
	values := make([]any, len(columns))

	err = fieldsByTraversal(v, fields, values, true)
	if err != nil {
		return err
	}

	// scan into the struct field pointers and append to our results
	err = r.Scan(values...)
	if err != nil {
		return err
	}

	return missErr
}

// SliceScan a row, returning a []any with values similar to MapScan.
// This function is primarily intended for use where the number of columns
// is not known.  Because you can pass an []any directly to Scan,
// it's recommended that you do that as it will not have to allocate new
// slices per row.
func SliceScan(r ColScanner) ([]any, error) {
	// ignore r.started, since we needn't use reflect for anything.
	columns, err := r.Columns()
	if err != nil {
		return []any{}, err
	}

	values := make([]any, len(columns))
	for i := range values {
		values[i] = new(any)
	}

	err = r.Scan(values...)
	if err != nil {
		return values, err
	}

	for i := range columns {
		values[i] = *(values[i].(*any))
	}

	return values, r.Err()
}

// MapScan scans a single Row into the dest map[string]any.
//
// Use this to get results for SQL that might not be under your control (for
// instance, if you're building an interface for an SQL server that executes SQL
// from input).
//
// Please do not use this as a primary interface! This will modify the map sent
// to it in place, so reuse the same map with care.  Columns which occur more
// than once in the result will overwrite each other!
func MapScan(r ColScanner, dest map[string]any) error {
	// ignore r.started, since we needn't use reflect for anything.
	columns, err := r.Columns()
	if err != nil {
		return err
	}

	values := make([]any, len(columns))
	for i := range values {
		values[i] = new(any)
	}

	err = r.Scan(values...)
	if err != nil {
		return err
	}

	for i, column := range columns {
		dest[column] = *(values[i].(*any))
	}

	return r.Err()
}

type rowsi interface {
	Close() error
	Columns() ([]string, error)
	Err() error
	Next() bool
	Scan(...any) error
}

// structOnlyError returns an error appropriate for type when a non-scannable
// struct is expected but something else is given
func structOnlyError(t reflect.Type) error {
	isStruct := t.Kind() == reflect.Struct
	isScanner := reflect.PtrTo(t).Implements(_scannerInterface)
	if !isStruct {
		return fmt.Errorf("expected %s but got %s", reflect.Struct, t.Kind())
	}
	if isScanner {
		return fmt.Errorf("structscan expects a struct dest but the provided struct type %s implements scanner", t.Name())
	}
	return fmt.Errorf("expected a struct, but struct %s has no exported fields", t.Name())
}

// scanAll scans all rows into a destination, which must be a slice of any
// type.  If the destination slice type is a Struct, then StructScan will be
// used on each row.  If the destination is some other kind of base type, then
// each row must only have one column which can scan into that type.  This
// allows you to do something like:
//
//	rows, _ := db.Query("select id from people;")
//	var ids []int
//	scanAll(rows, &ids, false)
//
// and ids will be a list of the id results.  I realize that this is a desirable
// interface to expose to users, but for now it will only be exposed via changes
// to `Get` and `Select`.  The reason that this has been implemented like this is
// this is the only way to not duplicate reflect work in the new API while
// maintaining backwards compatibility.
func scanAll(rows rowsi, dest any, structOnly bool) error {
	var v, vp reflect.Value

	value := reflect.ValueOf(dest)

	// json.Unmarshal returns errors for these
	if value.Kind() != reflect.Ptr {
		return errors.New("must pass a pointer, not a value, to StructScan destination")
	}
	if value.IsNil() {
		return errors.New("nil pointer passed to StructScan destination")
	}
	direct := reflect.Indirect(value)

	slice, err := baseType(value.Type(), reflect.Slice)
	if err != nil {
		return err
	}

	isPtr := slice.Elem().Kind() == reflect.Ptr
	base := reflectx.Deref(slice.Elem())
	scannable := isScannable(base)

	if structOnly && scannable {
		return structOnlyError(base)
	}

	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	// If it's a base type make sure it only has 1 column; if not return an error
	sliceT := direct.Type().Elem()
	if scannable && (sliceT.Kind() == reflect.Slice || sliceT.Kind() == reflect.Map) {
		switch sliceT.Kind() {
		case reflect.Map:
			k, v := direct.Type().Elem().Key(), direct.Type().Elem().Elem()
			if k.Kind() != reflect.String || v.Kind() != reflect.Interface {
				return fmt.Errorf("dest map must by []map[string]any, not []map[%s]%s", k, v)
			}

			for rows.Next() {
				r := make([]any, len(columns))
				for i := range r {
					r[i] = &r[i]
				}
				rows.Scan(r...)
				if err != nil {
					return err
				}
				m := make(map[string]any)
				for i := range columns {
					m[columns[i]] = r[i]
				}
				direct.Set(reflect.Append(direct, reflect.ValueOf(m)))
			}
			return nil
		case reflect.Slice:
			if t := direct.Type().Elem().Elem(); t.Kind() != reflect.Interface {
				return fmt.Errorf("dest slice must by [][]any, not [][]%s", t)
			}
			for rows.Next() {
				r := make([]any, len(columns))
				for i := range r {
					r[i] = &r[i]
				}
				err := rows.Scan(r...)
				if err != nil {
					return err
				}
				direct.Set(reflect.Append(direct, reflect.ValueOf(r)))
			}
			return nil
		default:
			return errors.New("slice must be [][]any or [][]map[string]any")
		}
	}

	if scannable && len(columns) > 1 {
		return fmt.Errorf("non-struct or slice dest type %s with >1 columns (%d)", base.Kind(), len(columns))
	}

	var missErr error
	if !scannable {
		var (
			values []any
			m      *reflectx.Mapper
		)
		switch r := rows.(type) {
		case *Rows:
			m = r.Mapper
		default:
			m = mapper()
		}

		fields := m.TraversalsByName(base, columns)
		f, err := missingFields(fields)
		if err != nil {
			missErr = &ErrMissingField{
				Column: columns[f],
				Type:   fmt.Sprintf("%T", dest),
			}
		}
		values = make([]any, len(columns))

		for rows.Next() {
			// Create a new struct type (which returns PtrTo) and indirect it
			vp = reflect.New(base)
			v = reflect.Indirect(vp)

			err = fieldsByTraversal(v, fields, values, true)
			if err != nil {
				return err
			}

			// scan into the struct field pointers and append to our results
			err = rows.Scan(values...)
			if err != nil {
				return err
			}

			if isPtr {
				direct.Set(reflect.Append(direct, vp))
			} else {
				direct.Set(reflect.Append(direct, v))
			}
		}
	} else {
		for rows.Next() {
			vp = reflect.New(base)
			err = rows.Scan(vp.Interface())
			if err != nil {
				return err
			}
			// append
			if isPtr {
				direct.Set(reflect.Append(direct, vp))
			} else {
				direct.Set(reflect.Append(direct, reflect.Indirect(vp)))
			}
		}
	}

	err = rows.Err()
	if err != nil {
		return err
	}

	return missErr
}

// FIXME: StructScan was the very first bit of API in sqlx, and now unfortunately
// it doesn't really feel like it's named properly.  There is an incongruency
// between this and the way that StructScan (which might better be ScanStruct
// anyway) works on a rows object.

// StructScan all rows from an sql.Rows or an sqlx.Rows into the dest slice.
// StructScan will scan in the entire rows result, so if you do not want to
// allocate structs for the entire result, use Queryx and see sqlx.Rows.StructScan.
// If rows is sqlx.Rows, it will use its mapper, otherwise it will use the default.
func StructScan(rows rowsi, dest any) error {
	return scanAll(rows, dest, true)

}

// reflect helpers

func baseType(t reflect.Type, expected reflect.Kind) (reflect.Type, error) {
	t = reflectx.Deref(t)
	if t.Kind() != expected {
		return nil, fmt.Errorf("expected %s but got %s", expected, t.Kind())
	}
	return t, nil
}

// fieldsByName fills a values interface with fields from the passed value based
// on the traversals in int. If ptrs is true, return addresses instead of
// values.
//
// We write this instead of using FieldsByName to save allocations and map
// lookups when iterating over many rows.  Empty traversals will get an
// interface pointer. Because of the necessity of requesting ptrs or values,
// it's considered a bit too specialized for inclusion in reflectx itself.
func fieldsByTraversal(v reflect.Value, traversals [][]int, values []any, ptrs bool) error {
	v = reflect.Indirect(v)
	if v.Kind() != reflect.Struct {
		return errors.New("argument not a struct")
	}

	for i, traversal := range traversals {
		if len(traversal) == 0 {
			values[i] = new(any)
			continue
		}
		f := reflectx.FieldByIndexes(v, traversal)
		if ptrs {
			values[i] = f.Addr().Interface()
		} else {
			values[i] = f.Interface()
		}
	}
	return nil
}

func missingFields(transversals [][]int) (field int, err error) {
	for i, t := range transversals {
		if len(t) == 0 {
			return i, errors.New("missing field")
		}
	}
	return 0, nil
}
