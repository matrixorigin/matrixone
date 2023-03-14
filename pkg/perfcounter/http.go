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
	"io"
	"net/http"
	"reflect"
	"sync/atomic"
)

func (c *Counter) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	fmt.Fprintf(w, `
  <style>
  table, th, td {
    border: 1px solid;
    border-collapse: collapse;
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

	default:
		switch t.Kind() {
		case reflect.Struct:
			formatStruct(w, v, t)
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			fmt.Fprintf(w, "%v", v.Int())
		case reflect.Ptr, reflect.Interface:
			format(w, v.Elem(), t.Elem())
		default:
			panic(fmt.Sprintf("unknown kind: %v", t.Kind()))
		}
	}
}

func formatStruct(w io.Writer, v reflect.Value, t reflect.Type) {
	fmt.Fprintf(w, "<table>")
	defer fmt.Fprintf(w, "</table>")
	for i, l := 0, t.NumField(); i < l; i++ {
		field := t.Field(i)
		fmt.Fprintf(w, "<tr>")
		fmt.Fprintf(w, "<td>%s</td>", field.Name)
		fmt.Fprintf(w, "<td>")
		fieldValue := v.Field(i)
		format(w, fieldValue, fieldValue.Type())
		fmt.Fprintf(w, "</td>")
		fmt.Fprintf(w, "</tr>")
	}
}
