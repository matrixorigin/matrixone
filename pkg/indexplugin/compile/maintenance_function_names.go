// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

import (
	"reflect"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// IndexMaintenanceFunctionNames discovers table functions in an already
// parsed statement, including nested SELECTs. It neither binds nor grants
// access; callers compare the names against plugin-owned descriptors.
func IndexMaintenanceFunctionNames(stmt tree.Statement) []string {
	var names []string
	visited := make(map[uintptr]bool)
	var walk func(reflect.Value)
	walk = func(v reflect.Value) {
		if !v.IsValid() {
			return
		}
		switch v.Kind() {
		case reflect.Interface:
			if !v.IsNil() {
				walk(v.Elem())
			}
		case reflect.Pointer:
			if v.IsNil() || visited[v.Pointer()] {
				return
			}
			visited[v.Pointer()] = true
			if v.CanInterface() {
				if tf, ok := v.Interface().(*tree.TableFunction); ok && tf.Func != nil {
					if name, ok := tf.Func.Func.FunctionReference.(*tree.UnresolvedName); ok {
						names = append(names, strings.ToLower(name.ColName()))
					}
				}
			}
			walk(v.Elem())
		case reflect.Struct:
			for i := 0; i < v.NumField(); i++ {
				if v.Type().Field(i).IsExported() {
					walk(v.Field(i))
				}
			}
		case reflect.Slice, reflect.Array:
			for i := 0; i < v.Len(); i++ {
				walk(v.Index(i))
			}
		case reflect.Map:
			iter := v.MapRange()
			for iter.Next() {
				walk(iter.Value())
			}
		}
	}
	walk(reflect.ValueOf(stmt))
	return names
}
