package zdb

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"

	"zgo.at/zstd/zreflect"
)

type (
	Tabler interface {
		// Table returns the table this row belongs to.
		Table() string
	}
	Defaulter interface {
		// Defaults sets default values for this row.
		Defaults(context.Context)
	}
	Validator interface {
		// Validate this row.
		Validate(context.Context) error
	}
)

func idcol(opts [][]string) (int, error) {
	col := -1
	for i, o := range opts {
		if slices.Contains(o, "id") {
			if col != -1 {
				return -1, errors.New("more than one field with ,id option")
			}
			col = i
		}
	}
	return col, nil
}

// Insert all struct fields of t.
//
// Column names are taken from the db tag. Fields with the db tag set to "-" or
// with the ",noinsert" option will be skipped.
//
// If a field has the ",id" option it will be fetched with a "returning" clause
// and set.
//
// The Default() and Validator() methods will be called if t satisfies the
// [Defaulter] or [Validator] interface.
//
// onConflict can contain an ON CONFLICT clause. This must include the ON
// CONFLICT text itself. For example:
//
//	zdb.Insert(ctx, t, "on conflict (id) do update set data = tbl.data || excluded.data")
func Insert(ctx context.Context, t Tabler, onConflict ...string) error {
	if reflect.TypeOf(t).Kind() != reflect.Ptr {
		return errors.New("zdb.Insert: t is not a pointer")
	}

	if d, ok := t.(Defaulter); ok {
		d.Defaults(ctx)
	}
	if v, ok := t.(Validator); ok {
		err := v.Validate(ctx)
		if err != nil {
			return err
		}
	}

	cols, params, opts := zreflect.Fields(t, "db", "noinsert")

	// Get the ID column, if any
	idCol, err := idcol(opts)
	if err != nil {
		return fmt.Errorf("zdb.Insert: %w", err)
	}
	var (
		idVal     any
		idColName string
	)
	if idCol > -1 {
		id := reflect.ValueOf(t).Elem().Field(idCol)
		if !id.IsZero() {
			return fmt.Errorf(`zdb.Insert: id field %q is not zero value but "%v"`, cols[idCol], id.Interface())
		}

		idVal, idColName = id.Addr().Interface(), cols[idCol]
		params, cols = append(params[:idCol], params[idCol+1:]...), append(cols[:idCol], cols[idCol+1:]...)
	}

	for i := range cols {
		cols[i] = QuoteIdentifier(cols[i])
	}
	q := fmt.Sprintf(`insert into %s (%s) values (?) %s`,
		QuoteIdentifier(t.Table()),
		strings.Join(cols, ", "),
		strings.Join(onConflict, " "))
	if idColName == "" {
		err = Exec(ctx, q, params)
	} else {
		q += " returning " + QuoteIdentifier(idColName)
		err = Get(ctx, idVal, q, params)
	}
	if err != nil {
		return fmt.Errorf("zdb.Insert: %w\n%s", err, q)
	}
	return nil
}

// QuoteIdentifier quotes ident as an SQL identifier.
func QuoteIdentifier(ident string) string {
	var b strings.Builder
	b.Grow(len(ident) + 2)
	b.WriteByte('"')
	for _, c := range ident {
		if c == '"' {
			b.WriteByte('"')
		}
		b.WriteRune(c)
	}
	b.WriteByte('"')
	return b.String()
}

// UpdateAll signals that all columns should be updated.
var UpdateAll = "\x00update\x00all\x00"

// Update all the given columns. The column names should match the name of the
// db tag.
//
// All columns in the struct wil be updated if [UpdateAll] is used as a column.
// Fields with the db tag set to "-" or with the ",noinsert" or ",readonly"
// option will be skipped.
//
// t needs to have a db tag with the ,id option set, which is used in the WHERE.
//
// The Default() and Validator() methods will be called if t satisfies the
// [Defaulter] or [Validator] interface.
func Update(ctx context.Context, t Tabler, columns ...string) error {
	if reflect.TypeOf(t).Kind() != reflect.Ptr {
		return errors.New("zdb.Update: t is not a pointer")
	}
	if len(columns) == 0 {
		return errors.New("zdb.Update: no columns")
	}

	if d, ok := t.(Defaulter); ok {
		d.Defaults(ctx)
	}
	if v, ok := t.(Validator); ok {
		err := v.Validate(ctx)
		if err != nil {
			return err
		}
	}

	var (
		tbl              = t.Table()
		cols, vals, opts = zreflect.Fields(t, "db", "")
	)

	idCol, err := idcol(opts)
	if err != nil {
		return fmt.Errorf("zdb.Update: %w", err)
	}
	if idCol == -1 {
		return errors.New("zdb.Update: no ,id column")
	}
	if reflect.ValueOf(vals[idCol]).IsZero() {
		return errors.New("zdb.Update: ID column is zero value")
	}
	where, whereParam := fmt.Sprintf(`%s = ?`, QuoteIdentifier(cols[idCol])), vals[idCol]

	var (
		updateAll = len(columns) == 1 && columns[0] == UpdateAll
		set       []string
		params    []any
	)
	for i := range cols {
		if i == idCol {
			continue
		}
		if slices.Contains(opts[i], "noinsert") {
			if slices.Contains(columns, cols[i]) {
				return fmt.Errorf("zdb.Update: column %q has ,noinsert", cols[i])
			}
			continue
		}
		if updateAll {
			if !slices.Contains(opts[i], "readonly") {
				set, params = append(set, QuoteIdentifier(cols[i])+` = ?`), append(params, vals[i])
			}
		} else if slices.Contains(columns, cols[i]) {
			set, params = append(set, QuoteIdentifier(cols[i])+` = ?`), append(params, vals[i])
		}
	}

	q := fmt.Sprintf(`update %s set %s where %s`,
		QuoteIdentifier(tbl), strings.Join(set, ", "), where)
	err = Exec(ctx, q, append(params, whereParam)...)
	if err != nil {
		return fmt.Errorf("zdb.Update: %w", err)
	}
	return nil
}
