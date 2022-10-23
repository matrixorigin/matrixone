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

	"github.com/matrixorigin/matrixone/pkg/container/batch"
)

type NamedRow interface {
	AttrByName(handler *MemHandler, tx *Transaction, name string) (Nullable, error)
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
	key           DataKey
	value         DataValue
	indexes       []Tuple
	uniqueIndexes []Tuple
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

func (a DataRow) UniqueIndexes() []Tuple {
	return a.uniqueIndexes
}

func (a *DataRow) String() string {
	buf := new(strings.Builder)
	buf.WriteString("DataRow{")
	fmt.Fprintf(buf, "key: %+v", a.key)
	for _, attr := range a.value {
		fmt.Fprintf(buf, ", %+v", attr)
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

func (n *NamedDataRow) AttrByName(handler *MemHandler, tx *Transaction, name string) (Nullable, error) {
	return n.Value[n.AttrsMap[name].Order], nil
}

func appendNamedRowToBatch(
	tx *Transaction,
	handler *MemHandler,
	offset int,
	bat *batch.Batch,
	row NamedRow,
) error {
	for i := offset; i < len(bat.Attrs); i++ {
		name := bat.Attrs[i]
		value, err := row.AttrByName(handler, tx, name)
		if err != nil {
			return err
		}
		value.AppendVector(bat.Vecs[i], handler.mheap)
	}
	return nil
}
