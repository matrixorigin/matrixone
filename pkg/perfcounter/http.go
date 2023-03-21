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

func format(w io.Writer, v reflect.Value, t reflect.Type) {
	switch val := v.Interface().(type) {
	case atomic.Int64:
		fmt.Fprintf(w, "%v", val.Load())
	case stats.Counter:
		fmt.Fprintf(w, "%v", val.Load())

	default:
		switch t.Kind() {
		case reflect.Struct:
			formatStruct(w, v, t)
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			fmt.Fprintf(w, "%v", v.Int())
		case reflect.Ptr, reflect.Interface:
			format(w, v.Elem(), t.Elem())
		case reflect.Map:
			formatMap(w, v, t)
		case reflect.String:
			fmt.Fprint(w, v.String())
		default:
			panic(fmt.Sprintf("unknown kind: %v", t.Kind()))
		}
	}
}

func formatStruct(w io.Writer, v reflect.Value, t reflect.Type) {
	fmt.Fprintf(w, "<table>")
	defer fmt.Fprintf(w, "</table>")
	for i, l := 0, t.NumField(); i < l; i++ {
		fieldValue := v.Field(i)
		if fieldValue.IsZero() {
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
			if value.IsZero() {
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
