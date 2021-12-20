// Copyright 2021 Matrix Origin
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

package fz

import (
	"fmt"
	"reflect"

	"github.com/reusee/dscope"
)

type ConfigItems []any

var _ dscope.Reducer = ConfigItems{}

func (_ ConfigItems) Reduce(_ dscope.Scope, vs []reflect.Value) reflect.Value {
	var ret ConfigItems
	names := make(map[string]struct{})
	for _, value := range vs {
		items := value.Interface().(ConfigItems)
		for _, item := range items {
			name := reflect.TypeOf(item).Name()
			if name == "" {
				panic(fmt.Errorf("config item must be named: %T", item))
			}
			if _, ok := names[name]; ok {
				panic(fmt.Errorf("duplicated config: %s", name))
			}
			names[name] = struct{}{}
			ret = append(ret, item)
		}
	}
	return reflect.ValueOf(ret)
}
