// Copyright 2022 Matrix Origin
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

package txnstorage

import "fmt"

type AnyKey struct {
	Value any
}

var _ Ordered[AnyKey] = AnyKey{}

func (a AnyKey) Less(than AnyKey) bool {
	switch v := a.Value.(type) {
	case Text:
		return v.Less(than.Value.(Text))
	case Int:
		return v.Less(than.Value.(Int))
	}
	panic(fmt.Errorf("unknown type: %T", a.Value))
}

type MultiAnyKey []AnyKey

var _ Ordered[MultiAnyKey] = MultiAnyKey{}

func (m MultiAnyKey) Less(than MultiAnyKey) bool {
	for i, key := range m {
		if i >= len(than) {
			return false
		}
		if key.Less(than[i]) {
			return true
		}
	}
	return false
}

type AnyRow struct {
	primaryKey AnyKey
}

func (a AnyRow) PrimaryKey() AnyKey {
	return a.primaryKey
}
