package zdb

import (
	"database/sql/driver"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestInts(t *testing.T) {
	t.Run("value", func(t *testing.T) {
		cases := []struct {
			in   Ints
			want string
		}{
			{Ints{}, ""},
			{Ints{}, ""},
			{Ints{4, 5}, "4, 5"},
			{Ints{1, 1}, "1, 1"},
			{Ints{1}, "1"},
			{Ints{1, 0, 2}, "1, 0, 2"},
		}

		for _, tc := range cases {
			t.Run(fmt.Sprintf("%v", tc.in), func(t *testing.T) {
				out, err := tc.in.Value()
				if err != nil {
					t.Fatal(err)
				}
				if out != tc.want {
					t.Errorf("\nout:  %#v\nwant: %#v\n", out, tc.want)
				}
			})
		}
	})

	t.Run("scan", func(t *testing.T) {
		cases := []struct {
			in      string
			want    Ints
			wantErr string
		}{
			{"", Ints(nil), ""},
			{"1", Ints{1}, ""},
			{"4, 5", Ints{4, 5}, ""},
			{"4,   5", Ints{4, 5}, ""},
			{"1, 1", Ints{1, 1}, ""},
			{"1, 0, 2", Ints{1, 0, 2}, ""},
			{"1,0,2", Ints{1, 0, 2}, ""},
			{"1,    0,    2    ", Ints{1, 0, 2}, ""},
			{"1,", Ints{1}, ""},
			{"1,,,,", Ints{1}, ""},
			{",,1,,", Ints{1}, ""},
			{"1,NaN", Ints(nil), "strconv.ParseInt"},
		}

		for _, tc := range cases {
			t.Run(fmt.Sprintf("%v", tc.in), func(t *testing.T) {
				out := Ints{}
				err := out.Scan(tc.in)
				if !errorContains(err, tc.wantErr) {
					t.Errorf("\nout:  %#v\nwant: %#v\n", err, tc.wantErr)
				}
				if !reflect.DeepEqual(out, tc.want) {
					t.Errorf("\nout:  %#v\nwant: %#v\n", out, tc.want)
				}
			})
		}
	})
}

func TestFloats(t *testing.T) {
	t.Run("value", func(t *testing.T) {
		cases := []struct {
			in   Floats
			want string
		}{
			{Floats{}, ""},
			{Floats{}, ""},
			{Floats{4, 5}, "4, 5"},
			{Floats{1, 1}, "1, 1"},
			{Floats{1}, "1"},
			{Floats{1, 0, 2}, "1, 0, 2"},
		}

		for _, tc := range cases {
			t.Run(fmt.Sprintf("%v", tc.in), func(t *testing.T) {
				out, err := tc.in.Value()
				if err != nil {
					t.Fatal(err)
				}
				if out != tc.want {
					t.Errorf("\nout:  %#v\nwant: %#v\n", out, tc.want)
				}
			})
		}
	})

	t.Run("scan", func(t *testing.T) {

		cases := []struct {
			in      string
			want    Floats
			wantErr string
		}{
			{"", Floats(nil), ""},
			{"1", Floats{1}, ""},
			{"4, 5", Floats{4, 5}, ""},
			{"4,   5", Floats{4, 5}, ""},
			{"1, 1", Floats{1, 1}, ""},
			{"1, 0, 2", Floats{1, 0, 2}, ""},
			{"1,0,2", Floats{1, 0, 2}, ""},
			{"1,    0,    2    ", Floats{1, 0, 2}, ""},
			{"1,", Floats{1}, ""},
			{"1,,,,", Floats{1}, ""},
			{",,1,,", Floats{1}, ""},
			{"1,zxc", Floats(nil), "strconv.ParseFloat"},
		}

		for _, tc := range cases {
			t.Run(fmt.Sprintf("%v", tc.in), func(t *testing.T) {
				out := Floats{}
				err := out.Scan(tc.in)
				if !errorContains(err, tc.wantErr) {
					t.Errorf("\nout:  %#v\nwant: %#v\n", err, tc.wantErr)
				}
				if !reflect.DeepEqual(out, tc.want) {
					t.Errorf("\nout:  %#v\nwant: %#v\n", out, tc.want)
				}
			})
		}
	})
}

func TestStrings(t *testing.T) {
	t.Run("value", func(t *testing.T) {
		cases := []struct {
			in   Strings
			want string
		}{
			{Strings{}, ""},
			{Strings{}, ""},
			{Strings{"4", "5"}, "4,5"},
			{Strings{"1", "1"}, "1,1"},
			{Strings{"€"}, "€"},
			{Strings{"1", "", "1"}, "1,1"},
			{Strings{"لوحة المفاتيح العربية", "xx"}, "لوحة المفاتيح العربية,xx"},
		}

		for _, tc := range cases {
			t.Run(fmt.Sprintf("%v", tc.in), func(t *testing.T) {
				out, err := tc.in.Value()
				if err != nil {
					t.Fatal(err)
				}
				if out != tc.want {
					t.Errorf("\nout:  %#v\nwant: %#v\n", out, tc.want)
				}
			})
		}
	})

	t.Run("scan", func(t *testing.T) {
		cases := []struct {
			in      string
			want    Strings
			wantErr string
		}{
			{"", Strings{}, ""},
			{"1", Strings{"1"}, ""},
			{"4, 5", Strings{"4", "5"}, ""},
			{"1, 1", Strings{"1", "1"}, ""},
			{"1,", Strings{"1"}, ""},
			{"1,,,,", Strings{"1"}, ""},
			{",,1,,", Strings{"1"}, ""},
			{"€", Strings{"€"}, ""},
			{"لوحة المفاتيح العربية, xx", Strings{"لوحة المفاتيح العربية", "xx"}, ""},
		}

		for _, tc := range cases {
			t.Run(fmt.Sprintf("%v", tc.in), func(t *testing.T) {
				out := Strings{}
				err := out.Scan(tc.in)
				if !errorContains(err, tc.wantErr) {
					t.Errorf("\nout:  %#v\nwant: %#v\n", err, tc.wantErr)
				}
				if !reflect.DeepEqual(out, tc.want) {
					t.Errorf("\nout:  %#v\nwant: %#v\n", out, tc.want)
				}
			})
		}
	})

	t.Run("marshalText", func(t *testing.T) {
		cases := []struct {
			in      Strings
			want    []byte
			wantErr string
		}{
			{Strings{"x"}, []byte("x"), ""},
			{Strings{"x", "y"}, []byte("x,y"), ""},
		}

		for _, tc := range cases {
			t.Run(fmt.Sprintf("%v", tc.in), func(t *testing.T) {
				out, err := tc.in.MarshalText()
				if !errorContains(err, tc.wantErr) {
					t.Errorf("\nout:  %#v\nwant: %#v\n", err, tc.wantErr)
				}
				if !reflect.DeepEqual(out, tc.want) {
					t.Errorf("\nout:  %q\nwant: %q\n", string(out), string(tc.want))
				}
			})
		}
	})

	t.Run("unmarshalText", func(t *testing.T) {
		cases := []struct {
			in      []byte
			want    Strings
			wantErr string
		}{
			{[]byte("  a  "), Strings{"a"}, ""},
			{[]byte("  a  , asd"), Strings{"a", "asd"}, ""},
		}

		for _, tc := range cases {
			t.Run(fmt.Sprintf("%v", tc.in), func(t *testing.T) {
				var out Strings
				err := out.UnmarshalText(tc.in)
				if !errorContains(err, tc.wantErr) {
					t.Errorf("\nout:  %#v\nwant: %#v\n", err, tc.wantErr)
				}
				if !reflect.DeepEqual(out, tc.want) {
					t.Errorf("\nout:  %#v\nwant: %#v\n", out, tc.want)
				}
			})
		}
	})
}

func TestBool(t *testing.T) {
	t.Run("value", func(t *testing.T) {
		cases := []struct {
			in   Bool
			want driver.Value
		}{
			{false, int64(0)},
			{true, int64(1)},
		}

		for _, tc := range cases {
			t.Run(fmt.Sprintf("%v", tc.in), func(t *testing.T) {
				out, err := tc.in.Value()
				if err != nil {
					t.Fatal(err)
				}
				if out != tc.want {
					t.Errorf("\nout:  %#v\nwant: %#v\n", out, tc.want)
				}
			})
		}
	})

	t.Run("scan", func(t *testing.T) {
		cases := []struct {
			in      interface{}
			want    Bool
			wantErr string
		}{
			{[]byte("true"), true, ""},
			{float64(1.0), true, ""},
			{[]byte{1}, true, ""},
			{int64(1), true, ""},
			{"true", true, ""},
			{true, true, ""},
			{"1", true, ""},

			{[]byte("false"), false, ""},
			{float64(0), false, ""},
			{[]byte{0}, false, ""},
			{int64(0), false, ""},
			{"false", false, ""},
			{false, false, ""},
			{"0", false, ""},
			{nil, false, ""},

			{"not a valid bool", false, "invalid value \"not a valid bool\""},
			{time.Time{}, false, "unsupported type time.Time"},
		}

		for _, tc := range cases {
			t.Run(fmt.Sprintf("%v", tc.in), func(t *testing.T) {
				var out Bool
				err := out.Scan(tc.in)
				if !errorContains(err, tc.wantErr) {
					t.Errorf("\nout:  %#v\nwant: %#v\n", err, tc.wantErr)
				}
				if !reflect.DeepEqual(out, tc.want) {
					t.Errorf("\nout:  %#v\nwant: %#v\n", out, tc.want)
				}
			})
		}
	})

	t.Run("marshalText", func(t *testing.T) {
		cases := []struct {
			in      Bool
			want    []byte
			wantErr string
		}{
			{false, []byte("false"), ""},
			{true, []byte("true"), ""},
		}

		for _, tc := range cases {
			t.Run(fmt.Sprintf("%v", tc.in), func(t *testing.T) {
				out, err := tc.in.MarshalText()
				if !errorContains(err, tc.wantErr) {
					t.Errorf("\nout:  %#v\nwant: %#v\n", err, tc.wantErr)
				}
				if !reflect.DeepEqual(out, tc.want) {
					t.Errorf("\nout:  %#v\nwant: %#v\n", out, tc.want)
				}
			})
		}
	})

	t.Run("unmarshalText", func(t *testing.T) {
		cases := []struct {
			in      []byte
			want    Bool
			wantErr string
		}{
			{[]byte("  true  "), true, ""},
			{[]byte(` "true"`), true, ""},
			{[]byte(`  1 `), true, ""},
			{[]byte("false  "), false, ""},
			{[]byte(`"false" `), false, ""},
			{[]byte(` 0 `), false, ""},
			{[]byte(`not a valid bool`), false, "invalid value \"not a valid bool\""},
		}

		for _, tc := range cases {
			t.Run(fmt.Sprintf("%v", tc.in), func(t *testing.T) {
				var out Bool
				err := out.UnmarshalText(tc.in)
				if !errorContains(err, tc.wantErr) {
					t.Errorf("\nout:  %#v\nwant: %#v\n", err, tc.wantErr)
				}
				if !reflect.DeepEqual(out, tc.want) {
					t.Errorf("\nout:  %#v\nwant: %#v\n", out, tc.want)
				}
			})
		}
	})
}

func errorContains(out error, want string) bool {
	if out == nil {
		return want == ""
	}
	if want == "" {
		return false
	}
	return strings.Contains(out.Error(), want)
}
