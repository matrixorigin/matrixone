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

package memorystorage

import (
	"fmt"
	"strings"
)

type NamedRow interface {
	SetHandler(handler *MemHandler)
	AttrByName(tx *Transaction, name string) (Nullable, error)
}

type DataKey struct {
	tableID    ID
	primaryKey Tuple
}

func (d DataKey) Less(than DataKey) bool {
	if d.tableID.Less(than.tableID) {
		return true
	}
	if than.tableID.Less(d.tableID) {
		return false
	}
	return d.primaryKey.Less(than.primaryKey)
}

// use AttributeRow.Order as index
type DataValue = []Nullable

type DataRow struct {
	key     DataKey
	value   DataValue
	indexes []Tuple
}

func (a DataRow) Key() DataKey {
	return a.key
}

func (a DataRow) Value() DataValue {
	return a.value
}

func (a DataRow) Indexes() []Tuple {
	return a.indexes
}

func (a *DataRow) String() string {
	buf := new(strings.Builder)
	buf.WriteString("DataRow{")
	buf.WriteString(fmt.Sprintf("key: %+v", a.key))
	for _, attr := range a.value {
		buf.WriteString(fmt.Sprintf(", %+v", attr))
	}
	buf.WriteString("}")
	return buf.String()
}

func NewDataRow(
	tableID ID,
	indexes []Tuple,
) *DataRow {
	return &DataRow{
		key: DataKey{
			tableID: tableID,
		},
		indexes: indexes,
	}
}

type NamedDataRow struct {
	Value    DataValue
	AttrsMap map[string]*AttributeRow
}

var _ NamedRow = new(NamedDataRow)

func (n *NamedDataRow) AttrByName(tx *Transaction, name string) (Nullable, error) {
	return n.Value[n.AttrsMap[name].Order], nil
}

func (n *NamedDataRow) SetHandler(handler *MemHandler) {
}
