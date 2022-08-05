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

type AnyKey []any

var _ Ordered[AnyKey] = AnyKey{}

func (a AnyKey) Less(than AnyKey) bool {
	for i, key := range a {
		if i >= len(than) {
			return false
		}
		switch key := key.(type) {

		case Text:
			if key.Less(than[i].(Text)) {
				return true
			}

		case Bool:
			if key.Less(than[i].(Bool)) {
				return true
			}

		case Int:
			if key.Less(than[i].(Int)) {
				return true
			}

		case Uint:
			if key.Less(than[i].(Uint)) {
				return true
			}

		case Float:
			if key.Less(than[i].(Float)) {
				return true
			}

		case Bytes:
			if key.Less(than[i].(Bytes)) {
				return true
			}

		default:
			panic(fmt.Errorf("unknown key type: %T", key))
		}
	}
	return false
}

type AnyRow struct {
	primaryKey AnyKey
	attributes map[string]any // attribute id -> value
}

func (a *AnyRow) PrimaryKey() AnyKey {
	return a.primaryKey
}

func NewAnyRow() *AnyRow {
	return &AnyRow{
		attributes: make(map[string]any),
	}
}
