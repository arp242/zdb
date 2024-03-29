package zdb

import (
	"bytes"
	"strings"
	"testing"
)

func TestTemplate(t *testing.T) {
	const testSchema = `
create table x (
	x_id        {{auto_increment}},
	created_at  timestamp      {{check_date "created_at"}}
);
{{sqlite "SQLITE"}}
{{psql "PSQL"}}
`

	tests := []struct {
		driver Dialect
		want   string
	}{
		{DialectSQLite, `
create table x (
	x_id        integer        primary key autoincrement,
	created_at  timestamp      check(created_at = strftime('%Y-%m-%d', created_at))
);
SQLITE
	`},
		{DialectPostgreSQL, `
create table x (
	x_id        serial         primary key,
	created_at  timestamp
);

PSQL
`},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			got, err := Template(tt.driver, testSchema)
			if err != nil {
				t.Fatal(err)
			}
			got = bytes.TrimSpace(got)
			tt.want = strings.TrimSpace(tt.want)
			if string(got) != tt.want {
				t.Errorf("\ngot:\n%s\nwant:\n%s", string(got), tt.want)
			}
		})
	}
}
