package reflectx

import (
	"reflect"
	"runtime"
	"strings"
	"sync"
)

type (
	// A Mapper maps struct fields to names.
	//
	// A Mapper behaves like most marshallers in the standard library, obeying a
	// field tag for name mapping but also providing a basic transform function.
	Mapper struct {
		cache   map[reflect.Type]*StructMap
		tagName string
		mapFunc func(string) string
		mutex   sync.Mutex
	}

	// A StructMap is an index of field metadata for a struct.
	StructMap struct {
		Tree  *FieldInfo
		Index []*FieldInfo
		Paths map[string]*FieldInfo
		Names map[string]*FieldInfo
	}

	// A FieldInfo is metadata for a struct field.
	FieldInfo struct {
		Index    []int
		Path     string
		Field    reflect.StructField
		Zero     reflect.Value
		Name     string
		Options  map[string]string
		Embedded bool
		Children []*FieldInfo
		Parent   *FieldInfo
	}
)

// NewMapper returns a new mapper using the tag as the struct field tag.
//
// The mapFunc is invoked if the tag is empty, taking the field name as its
// argument. If this returns an empty things the field is ignored.
func NewMapper(tagName string, mapFunc func(string) string) *Mapper {
	return &Mapper{
		cache:   make(map[reflect.Type]*StructMap),
		tagName: tagName,
		mapFunc: mapFunc,
	}
}

// TypeMap returns a mapping of field strings to int slices representing the
// traversal down the struct to reach the field.
func (m *Mapper) TypeMap(t reflect.Type) *StructMap {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	mapping, ok := m.cache[t]
	if !ok {
		mapping = getMapping(t, m.tagName, m.mapFunc)
		m.cache[t] = mapping
	}
	return mapping
}

// FieldMap returns the mapper's mapping of field names to reflect values.
// Panics if v's Kind is not Struct, or v is not Indirectable to a struct kind.
//
// zdb only.
func (m *Mapper) FieldMap(v reflect.Value) map[string]reflect.Value {
	v = reflect.Indirect(v)
	mustBe(v, reflect.Struct)

	r := map[string]reflect.Value{}
	tm := m.TypeMap(v.Type())
	for tagName, fi := range tm.Names {
		r[tagName] = FieldByIndexes(v, fi.Index)
	}
	return r
}

// FieldByName returns a field by its mapped name.
//
// Panics if v is not a struct or pointer to a struct.
//
// Returns zero Value if the name is not found. TODO: that's now how it actually
// behaves!
//
// zdb only.
func (m *Mapper) FieldByName(v reflect.Value, name string) reflect.Value {
	v = reflect.Indirect(v)
	mustBe(v, reflect.Struct)

	tm := m.TypeMap(v.Type())
	fi, ok := tm.Names[name]
	if !ok {
		return v
	}
	return FieldByIndexes(v, fi.Index)
}

// TraversalsByName returns a slice of int slices which represent the struct
// traversals for each mapped name.
//
// Panics if t is not a struct or Indirectable to a struct.
//
// Returns empty int slice for each name not found.
func (m *Mapper) TraversalsByName(t reflect.Type, names []string) [][]int {
	r := make([][]int, 0, len(names))
	m.TraversalsByNameFunc(t, names, func(_ int, i []int) error {
		if i == nil {
			r = append(r, []int{})
		} else {
			r = append(r, i)
		}

		return nil
	})
	return r
}

// TraversalsByNameFunc traverses the mapped names and calls fn with the index of
// each name and the struct traversal represented by that name.
//
// Panics if t is not a struct or Indirectable to a struct.
//
// Returns the first error returned by fn or nil.
func (m *Mapper) TraversalsByNameFunc(t reflect.Type, names []string, fn func(int, []int) error) error {
	t = Deref(t)
	mustBe(t, reflect.Struct)
	tm := m.TypeMap(t)
	for i, name := range names {
		fi, ok := tm.Names[name]
		if !ok {
			if err := fn(i, nil); err != nil {
				return err
			}
		} else {
			if err := fn(i, fi.Index); err != nil {
				return err
			}
		}
	}
	return nil
}

// FieldByIndexes returns a value for the field given by the struct traversal
// for the given value.
func FieldByIndexes(v reflect.Value, indexes []int) reflect.Value {
	for _, i := range indexes {
		v = reflect.Indirect(v).Field(i)
		// if this is a pointer and it's nil, allocate a new value and set it
		if v.Kind() == reflect.Ptr && v.IsNil() {
			alloc := reflect.New(Deref(v.Type()))
			v.Set(alloc)
		}
		if v.Kind() == reflect.Map && v.IsNil() {
			v.Set(reflect.MakeMap(v.Type()))
		}
	}
	return v
}

// FieldByIndexesReadOnly returns a value for a particular struct traversal,
// but is not concerned with allocating nil pointers because the value is
// going to be used for reading and not setting.
func FieldByIndexesReadOnly(v reflect.Value, indexes []int) reflect.Value {
	for _, i := range indexes {
		v = reflect.Indirect(v).Field(i)
	}
	return v
}

// Deref is Indirect for reflect.Types
func Deref(t reflect.Type) reflect.Type {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t
}

// -- helpers & utilities --

type kinder interface {
	Kind() reflect.Kind
}

// mustBe checks a value against a kind, panicing with a reflect.ValueError
// if the kind isn't that which is required.
func mustBe(v kinder, expected reflect.Kind) {
	if k := v.Kind(); k != expected {
		pc, _, _, _ := runtime.Caller(1)
		f := runtime.FuncForPC(pc)
		m := "unknown method"
		if f != nil {
			m = f.Name()
		}
		panic(&reflect.ValueError{Method: m, Kind: k})
	}
}

type typeQueue struct {
	t  reflect.Type
	fi *FieldInfo
	pp string // Parent path
}

// A copying append that creates a new slice each time.
func appendCopy(is []int, i int) []int {
	x := make([]int, len(is)+1)
	copy(x, is)
	x[len(x)-1] = i
	return x
}

type mapf func(string) string

// parseName parses the tag and the target name for the given field using
// the tagName (eg 'json' for `json:"foo"` tags), mapFunc for mapping the
// field's name to a target name for mapping the tag to
// a target name.
func parseName(field reflect.StructField, tagName string, mapFunc mapf) (tag, fieldName string) {
	// first, set the fieldName to the field's name
	fieldName = field.Name
	// if a mapFunc is set, use that to override the fieldName
	if mapFunc != nil {
		fieldName = mapFunc(fieldName)
	}

	// if there's no tag to look for, return the field name
	if tagName == "" {
		return "", fieldName
	}

	// if this tag is not set using the normal convention in the tag,
	// then return the fieldname..  this check is done because according
	// to the reflect documentation:
	//    If the tag does not have the conventional format,
	//    the value returned by Get is unspecified.
	// which doesn't sound great.
	if !strings.Contains(string(field.Tag), tagName+":") {
		return "", fieldName
	}

	// at this point we're fairly sure that we have a tag, so lets pull it out
	tag = field.Tag.Get(tagName)

	// finally, split the options from the name
	parts := strings.Split(tag, ",")
	fieldName = parts[0]

	return tag, fieldName
}

// parseOptions parses options out of a tag string, skipping the name
func parseOptions(tag string) map[string]string {
	parts := strings.Split(tag, ",")
	options := make(map[string]string, len(parts))
	if len(parts) > 1 {
		for _, opt := range parts[1:] {
			// short circuit potentially expensive split op
			if strings.Contains(opt, "=") {
				kv := strings.Split(opt, "=")
				options[kv[0]] = kv[1]
				continue
			}
			options[opt] = ""
		}
	}
	return options
}

// getMapping returns a mapping for the t type, using the tagName, mapFunc
// to determine the canonical names of fields.
func getMapping(t reflect.Type, tagName string, mapFunc mapf) *StructMap {
	m := []*FieldInfo{}

	root := &FieldInfo{}
	queue := []typeQueue{}
	queue = append(queue, typeQueue{Deref(t), root, ""})

QueueLoop:
	for len(queue) != 0 {
		// pop the first item off of the queue
		tq := queue[0]
		queue = queue[1:]

		// ignore recursive field
		for p := tq.fi.Parent; p != nil; p = p.Parent {
			if tq.fi.Field.Type == p.Field.Type {
				continue QueueLoop
			}
		}

		nChildren := 0
		if tq.t.Kind() == reflect.Struct {
			nChildren = tq.t.NumField()
		}
		tq.fi.Children = make([]*FieldInfo, nChildren)

		// iterate through all of its fields
		for fieldPos := 0; fieldPos < nChildren; fieldPos++ {

			f := tq.t.Field(fieldPos)

			// parse the tag and the target name using the mapping options for this field
			tag, name := parseName(f, tagName, mapFunc)

			// if the name is "-", disabled via a tag, skip it
			if name == "-" {
				continue
			}

			fi := FieldInfo{
				Field:   f,
				Name:    name,
				Zero:    reflect.New(f.Type).Elem(),
				Options: parseOptions(tag),
			}

			// if the path is empty this path is just the name
			if tq.pp == "" {
				fi.Path = fi.Name
			} else {
				fi.Path = tq.pp + "." + fi.Name
			}

			// skip unexported fields
			if len(f.PkgPath) != 0 && !f.Anonymous {
				continue
			}

			// bfs search of anonymous embedded structs
			if f.Anonymous {
				pp := tq.pp
				if tag != "" {
					pp = fi.Path
				}

				fi.Embedded = true
				fi.Index = appendCopy(tq.fi.Index, fieldPos)
				nChildren := 0
				ft := Deref(f.Type)
				if ft.Kind() == reflect.Struct {
					nChildren = ft.NumField()
				}
				fi.Children = make([]*FieldInfo, nChildren)
				queue = append(queue, typeQueue{Deref(f.Type), &fi, pp})
			} else if fi.Zero.Kind() == reflect.Struct || (fi.Zero.Kind() == reflect.Ptr && fi.Zero.Type().Elem().Kind() == reflect.Struct) {
				fi.Index = appendCopy(tq.fi.Index, fieldPos)
				fi.Children = make([]*FieldInfo, Deref(f.Type).NumField())
				queue = append(queue, typeQueue{Deref(f.Type), &fi, fi.Path})
			}

			fi.Index = appendCopy(tq.fi.Index, fieldPos)
			fi.Parent = tq.fi
			tq.fi.Children[fieldPos] = &fi
			m = append(m, &fi)
		}
	}

	flds := &StructMap{Index: m, Tree: root, Paths: map[string]*FieldInfo{}, Names: map[string]*FieldInfo{}}
	for _, fi := range flds.Index {
		// check if nothing has already been pushed with the same path
		// sometimes you can choose to override a type using embedded struct
		fld, ok := flds.Paths[fi.Path]
		if !ok || fld.Embedded {
			flds.Paths[fi.Path] = fi
			if fi.Name != "" && !fi.Embedded {
				flds.Names[fi.Path] = fi
			}
		}
	}

	return flds
}
