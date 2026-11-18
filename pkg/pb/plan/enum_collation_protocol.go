// Copyright 2026 Matrix Origin
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

package plan

import "reflect"

// RequiresEnumCollationProtocol examines plan and pipeline protobuf owners.
// It is used only below the feature floor; normal current-version execution
// does not walk a plan for this compatibility check.
func RequiresEnumCollationProtocol(owner any) bool {
	seen := make(map[uintptr]struct{})
	var visit func(reflect.Value, int) bool
	visit = func(value reflect.Value, depth int) bool {
		if !value.IsValid() {
			return false
		}
		if depth > 256 {
			return true // An unexpectedly deep owner must fail closed.
		}
		if value.CanInterface() {
			switch v := value.Interface().(type) {
			case Type:
				if v.EnumCollation != 0 {
					return true
				}
			case *TableDef:
				if v != nil && (v.DefaultCharset != 0 || v.EnumUnsupportedDefault != "") {
					return true
				}
			case *Function:
				if v != nil && v.Func != nil && int32(v.Func.Obj>>32) == 306 && len(v.Args) == 4 {
					return true
				}
			}
		}
		switch value.Kind() {
		case reflect.Interface:
			return !value.IsNil() && visit(value.Elem(), depth+1)
		case reflect.Pointer:
			if value.IsNil() {
				return false
			}
			ptr := value.Pointer()
			if _, ok := seen[ptr]; ok {
				return false
			}
			seen[ptr] = struct{}{}
			return visit(value.Elem(), depth+1)
		case reflect.Struct:
			for i := 0; i < value.NumField(); i++ {
				if child := value.Field(i); child.CanInterface() && visit(child, depth+1) {
					return true
				}
			}
		case reflect.Slice, reflect.Array:
			switch value.Type().Elem().Kind() {
			case reflect.Interface, reflect.Pointer, reflect.Struct, reflect.Slice, reflect.Array, reflect.Map:
			default:
				return false
			}
			for i := 0; i < value.Len(); i++ {
				if visit(value.Index(i), depth+1) {
					return true
				}
			}
		case reflect.Map:
			iter := value.MapRange()
			for iter.Next() {
				if visit(iter.Value(), depth+1) {
					return true
				}
			}
		}
		return false
	}
	return visit(reflect.ValueOf(owner), 0)
}
