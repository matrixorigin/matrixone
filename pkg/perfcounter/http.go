// Copyright 2023 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package perfcounter

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/util/metric/stats"
	"io"
	"net/http"
	"reflect"
	"sort"
	"sync/atomic"
)

func (c *CounterSet) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	fmt.Fprintf(w, `
  <style>
  table, th, td {
    border: 1px solid;
    border-collapse: collapse;
    font-family: monospace, monospace;
  }
  </style>
  `)

	v := reflect.ValueOf(c)
	format(w, v, v.Type())
}

// loadCounter reads addressable live counters through their original atomic
// receiver. Non-addressable values (for example map-value snapshots) cannot be
// updated in place and retain the formatter's existing value support.
func loadCounter(v reflect.Value) (int64, bool) {
	if v.CanAddr() {
		switch counter := v.Addr().Interface().(type) {
		case *atomic.Int64:
			return counter.Load(), true
		case *stats.Counter:
			return counter.Load(), true
		}
	}
	if v.Type() == reflect.TypeFor[atomic.Int64]() || v.Type() == reflect.TypeFor[stats.Counter]() {
		switch counter := v.Interface().(type) {
		case atomic.Int64:
			return counter.Load(), true
		case stats.Counter:
			return counter.Load(), true
		}
	}
	return 0, false
}

func counterValueIsZero(v reflect.Value) bool {
	if v.CanAddr() {
		if counter, ok := v.Addr().Interface().(*stats.Counter); ok {
			return counter.Load() == 0 && counter.LoadW() == 0
		}
	}
	if value, ok := loadCounter(v); ok {
		return value == 0
	}
	if v.Kind() == reflect.Struct {
		for i := 0; i < v.NumField(); i++ {
			if !counterValueIsZero(v.Field(i)) {
				return false
			}
		}
		return true
	}
	return v.IsZero()
}

func format(w io.Writer, v reflect.Value, t reflect.Type) {
	if value, ok := loadCounter(v); ok {
		fmt.Fprint(w, value)
		return
	}
	// Do not call Interface on a live container: that copies all its atomic
	// fields before a leaf's Load can protect the read.
	switch t.Kind() {
	case reflect.Struct:
		formatStruct(w, v, t)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		fmt.Fprintf(w, "%v", v.Int())
	case reflect.Pointer, reflect.Interface:
		format(w, v.Elem(), t.Elem())
	case reflect.Map:
		formatMap(w, v, t)
	case reflect.String:
		fmt.Fprint(w, v.String())
	default:
		panic(fmt.Sprintf("unknown kind: %v", t.Kind()))
	}
}

func formatStruct(w io.Writer, v reflect.Value, t reflect.Type) {
	fmt.Fprintf(w, "<table>")
	defer fmt.Fprintf(w, "</table>")
	for i, l := 0, t.NumField(); i < l; i++ {
		fieldValue := v.Field(i)
		if counterValueIsZero(fieldValue) {
			continue
		}
		field := t.Field(i)
		fmt.Fprintf(w, "<tr>")
		fmt.Fprintf(w, "<td>%s</td>", field.Name)
		fmt.Fprintf(w, "<td>")
		format(w, fieldValue, fieldValue.Type())
		fmt.Fprintf(w, "</td>")
		fmt.Fprintf(w, "</tr>")
	}
}

func formatMap(w io.Writer, v reflect.Value, t reflect.Type) {
	fmt.Fprintf(w, "<table>")
	defer fmt.Fprintf(w, "</table>")

	if t.Key().Kind() == reflect.String {
		keyValues := v.MapKeys()
		keys := make([]string, 0, v.Len())
		for _, keyValue := range keyValues {
			keys = append(keys, keyValue.String())
		}
		sort.Strings(keys)
		for _, key := range keys {
			fmt.Fprintf(w, "<tr>")
			fmt.Fprint(w, "<td>")
			fmt.Fprint(w, key)
			fmt.Fprint(w, "</td>")
			value := v.MapIndex(reflect.ValueOf(key))
			fmt.Fprint(w, "<td>")
			format(w, value, value.Type())
			fmt.Fprint(w, "</td>")
			fmt.Fprint(w, "</tr>")
		}

	} else {
		iter := v.MapRange()
		for iter.Next() {
			value := iter.Value()
			if counterValueIsZero(value) {
				continue
			}
			key := iter.Key()
			fmt.Fprintf(w, "<tr>")
			fmt.Fprintf(w, "<td>")
			format(w, key, key.Type())
			fmt.Fprintf(w, "</td>")
			fmt.Fprintf(w, "<td>")
			format(w, value, value.Type())
			fmt.Fprintf(w, "</td>")
			fmt.Fprintf(w, "</tr>")
		}
	}
}
