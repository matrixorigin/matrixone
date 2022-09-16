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

type DataKey struct {
	tableID    string
	primaryKey Tuple
}

func (d DataKey) Less(than DataKey) bool {
	if d.tableID < than.tableID {
		return true
	}
	if d.tableID > than.tableID {
		return false
	}
	return d.primaryKey.Less(than.primaryKey)
}

type DataRow struct {
	key        DataKey
	indexes    []Tuple
	attributes map[string]Nullable // attribute id -> nullable value
}

func (a DataRow) Key() DataKey {
	return a.key
}

func (a DataRow) Indexes() []Tuple {
	return a.indexes
}

func (a *DataRow) String() string {
	buf := new(strings.Builder)
	buf.WriteString("DataRow{")
	buf.WriteString(fmt.Sprintf("key: %+v", a.key))
	for key, value := range a.attributes {
		buf.WriteString(fmt.Sprintf(", %s: %+v", key, value))
	}
	buf.WriteString("}")
	return buf.String()
}

func NewDataRow(
	tableID string,
	indexes []Tuple,
) *DataRow {
	return &DataRow{
		key: DataKey{
			tableID: tableID,
		},
		attributes: make(map[string]Nullable),
		indexes:    indexes,
	}
}

type NamedDataRow struct {
	Row      *DataRow
	AttrsMap map[string]*AttributeRow
}

var _ NamedRow = new(NamedDataRow)

func (n *NamedDataRow) AttrByName(tx *Transaction, name string) (Nullable, error) {
	return n.Row.attributes[n.AttrsMap[name].ID], nil
}
