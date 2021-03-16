package zdb

import (
	"bytes"
	"fmt"
	"regexp"
	"text/template"
)

// SchemaFuncMap are additional template functions for SchemaTemplate().
// Existing functions may be overridden.
var SchemaFuncMap template.FuncMap

// SchemaTemplate runs text/template on the database schema to make writing
// compatible schemas a bit easier.
func SchemaTemplate(driver DriverType, tpl string) ([]byte, error) {
	t, err := template.New("").Funcs(tplFuncs(driver)).Parse(tpl)
	if err != nil {
		return nil, fmt.Errorf("zdb.SchemaTemplate: %w", err)
	}

	buf := new(bytes.Buffer)
	err = t.Execute(buf, nil)
	if err != nil {
		return nil, fmt.Errorf("zdb.SchemaTemplate: %w", err)
	}
	b := regexp.MustCompile(` +\n`).ReplaceAll(buf.Bytes(), []byte("\n"))
	return b, nil
}

func tplFuncs(driver DriverType) template.FuncMap {
	// TODO: MySQL for many of these.
	f := template.FuncMap{
		"sqlite": func(s string) string { return map[DriverType]string{DriverSQLite: s}[driver] },
		"psql":   func(s string) string { return map[DriverType]string{DriverPostgreSQL: s}[driver] },
		"mysql":  func(s string) string { return map[DriverType]string{DriverMySQL: s}[driver] },
		"auto_increment": func() string {
			return map[DriverType]string{
				DriverPostgreSQL: "serial         primary key",
				DriverSQLite:     "integer        primary key autoincrement",
			}[driver]
		},
		"jsonb": func() string {
			return map[DriverType]string{
				DriverPostgreSQL: "jsonb    ",
				DriverSQLite:     "varchar  ",
			}[driver]
		},
		"blob": func() string {
			return map[DriverType]string{
				DriverPostgreSQL: "bytea   ",
				DriverSQLite:     "blob    ",
			}[driver]
		},
		"check_timestamp": func(col string) string {
			return map[DriverType]string{
				DriverSQLite: "check(" + col + " = strftime('%Y-%m-%d %H:%M:%S', " + col + "))",
			}[driver]
		},
		"check_date": func(col string) string {
			return map[DriverType]string{
				DriverSQLite: "check(" + col + " = strftime('%Y-%m-%d', " + col + "))",
			}[driver]
		},
		"cluster": func(tbl, idx string) string {
			return map[DriverType]string{
				DriverPostgreSQL: `cluster ` + tbl + ` using "` + idx + `";`,
			}[driver]
		},
		"replica": func(tbl, idx string) string {
			return map[DriverType]string{
				DriverPostgreSQL: `alter table ` + tbl + ` replica identity using index "` + idx + `";`,
			}[driver]
		},
	}
	if SchemaFuncMap != nil {
		for k, v := range SchemaFuncMap {
			f[k] = v
		}
	}
	return f
}
