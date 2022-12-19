package zdb

import (
	"bytes"
	"fmt"
	"reflect"
	"regexp"
	"text/template"
)

// TemplateFuncMap are additional template functions for Template(). Existing
// functions may be overridden.
var TemplateFuncMap template.FuncMap

// Template runs text/template on SQL to make writing compatible schemas a bit
// easier.
//
// Available template functions:
//
//	{{sqlite "str"}}         Include the string only for the given SQL dialect.
//	{{psql   "str"}}
//	{{maria  "str"}}
//
// Column types:
//
//	{{"auto_increment [large]}}     Auto-incrementing column; the optional
//	                                [large] boolean can be used to make a
//	                                bigserial/bigint.
//	{{json}}                        JSON column type (jsonb, json, varchar)
//	{{blob}}                        Binary column type (bytea, blob, binary)
//
// These only produce output for SQLite:
//
//	{{check_timestamp "colname"}}   Check constraint for timestamp
//	{{check_date                    Check constraint for date
//
// You can set additional functions (or override any of the above) by adding
// functions to [TemplateFuncMap].
func Template(dialect Dialect, tpl string, params ...any) ([]byte, error) {
	paramMap := map[string]any{}
	for _, param := range params {
		v := reflect.ValueOf(param)
		if !v.IsValid() {
			return nil, fmt.Errorf("zdb.Template: invalid template parameter type %#v", param)
		}

		for v = reflect.ValueOf(param); v.Kind() == reflect.Ptr; {
			v = v.Elem()
		}

		var m map[string]any
		if v.Type().ConvertibleTo(reflect.TypeOf(m)) {
			m = v.Convert(reflect.TypeOf(m)).Interface().(map[string]any)
		}
		if m != nil {
			for k, v := range m {
				paramMap[k] = v
			}
		}

		// Struct
		if v.Kind() == reflect.Struct {
			t := v.Type()
			for i := 0; i < v.NumField(); i++ {
				paramMap[t.Field(i).Name] = v.Field(i).Interface()
			}
		}
	}

	t, err := template.New("").Funcs(tplFuncs(dialect)).Parse(tpl)
	if err != nil {
		return nil, fmt.Errorf("zdb.Template: %w", err)
	}

	buf := new(bytes.Buffer)
	err = t.Execute(buf, paramMap)
	if err != nil {
		return nil, fmt.Errorf("zdb.Template: %w", err)
	}
	b := regexp.MustCompile(` +\n`).ReplaceAll(buf.Bytes(), []byte("\n"))
	return b, nil
}

func tplFuncs(dialect Dialect) template.FuncMap {
	f := template.FuncMap{
		"sqlite": func(s string) string { return map[Dialect]string{DialectSQLite: s}[dialect] },
		"psql":   func(s string) string { return map[Dialect]string{DialectPostgreSQL: s}[dialect] },
		"maria":  func(s string) string { return map[Dialect]string{DialectMariaDB: s}[dialect] },
		"auto_increment": func(big ...bool) string {
			if len(big) > 0 {
				return map[Dialect]string{
					DialectPostgreSQL: "bigserial      primary key",
					DialectSQLite:     "integer        primary key autoincrement",
					DialectMariaDB:    "bigint         not null auto_increment primary key",
				}[dialect]
			}
			return map[Dialect]string{
				DialectPostgreSQL: "serial         primary key",
				DialectSQLite:     "integer        primary key autoincrement",
				DialectMariaDB:    "integer        not null auto_increment primary key",
			}[dialect]
		},
		"json": func() string {
			return map[Dialect]string{
				DialectPostgreSQL: "jsonb    ",
				DialectSQLite:     "varchar  ",
				DialectMariaDB:    "json     ",
			}[dialect]
		},
		"blob": func() string {
			return map[Dialect]string{
				DialectPostgreSQL: "bytea   ",
				DialectSQLite:     "blob    ",
				DialectMariaDB:    "blob    ",
			}[dialect]
		},
		"check_timestamp": func(col string) string {
			return map[Dialect]string{
				DialectSQLite: "check(" + col + " = strftime('%Y-%m-%d %H:%M:%S', " + col + "))",
			}[dialect]
		},
		"check_date": func(col string) string {
			return map[Dialect]string{
				DialectSQLite: "check(" + col + " = strftime('%Y-%m-%d', " + col + "))",
			}[dialect]
		},

		"jsonb": func() string { // TODO: remove; old alias for json
			return map[Dialect]string{
				DialectPostgreSQL: "jsonb    ",
				DialectSQLite:     "varchar  ",
				DialectMariaDB:    "json     ",
			}[dialect]
		},
		"cluster": func(tbl, idx string) string { // TODO: remove; too obscure
			return map[Dialect]string{
				DialectPostgreSQL: `cluster ` + tbl + ` using "` + idx + `";`,
			}[dialect]
		},
		"replica": func(tbl, idx string) string { // TODO: remove; too obscure
			return map[Dialect]string{
				DialectPostgreSQL: `alter table ` + tbl + ` replica identity using index "` + idx + `";`,
			}[dialect]
		},
	}
	for k, v := range TemplateFuncMap {
		f[k] = v
	}
	return f
}
