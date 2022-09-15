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

import (
	"fmt"
	"strings"
)

type AnyKey []any

var _ Ordered[AnyKey] = AnyKey{}

func (a AnyKey) Less(than AnyKey) bool {
	if len(a) < len(than) {
		return true
	}
	if len(than) < len(a) {
		return false
	}
	for i, key := range a {
		switch key := key.(type) {

		case Text:
			key2 := than[i].(Text)
			if key.Less(key2) {
				return true
			} else if key2.Less(key) {
				return false
			}

		case Bool:
			key2 := than[i].(Bool)
			if key.Less(key2) {
				return true
			} else if key2.Less(key) {
				return false
			}

		case Int:
			key2 := than[i].(Int)
			if key.Less(key2) {
				return true
			} else if key2.Less(key) {
				return false
			}

		case Uint:
			key2 := than[i].(Uint)
			if key.Less(key2) {
				return true
			} else if key2.Less(key) {
				return false
			}

		case Float:
			key2 := than[i].(Float)
			if key.Less(key2) {
				return true
			} else if key2.Less(key) {
				return false
			}

		case Bytes:
			key2 := than[i].(Bytes)
			if key.Less(key2) {
				return true
			} else if key2.Less(key) {
				return false
			}

		default:
			panic(fmt.Errorf("unknown key type: %T", key))
		}
	}
	// equal
	return false
}

type AnyRow struct {
	primaryKey AnyKey
	indexes    []AnyKey
	attributes map[string]Nullable // attribute id -> nullable value
}

func (a AnyRow) PrimaryKey() AnyKey {
	return a.primaryKey
}

func (a AnyRow) Indexes() []AnyKey {
	return a.indexes
}

func (a *AnyRow) String() string {
	buf := new(strings.Builder)
	buf.WriteString("AnyRow{")
	buf.WriteString(fmt.Sprintf("key: %+v", a.primaryKey))
	for key, value := range a.attributes {
		buf.WriteString(fmt.Sprintf(", %s: %+v", key, value))
	}
	buf.WriteString("}")
	return buf.String()
}

func NewAnyRow(
	indexes []AnyKey,
) *AnyRow {
	return &AnyRow{
		attributes: make(map[string]Nullable),
		indexes:    indexes,
	}
}

type NamedAnyRow struct {
	Row      *AnyRow
	AttrsMap map[string]*AttributeRow
}

var _ NamedRow = new(NamedAnyRow)

func (n *NamedAnyRow) AttrByName(tx *Transaction, name string) (Nullable, error) {
	return n.Row.attributes[n.AttrsMap[name].ID], nil
}
