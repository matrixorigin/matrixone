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
	"database/sql"
	"sync"

	"github.com/tidwall/btree"
)

type Table[
	PrimaryKey Ordered[PrimaryKey],
	Attrs Attributes[PrimaryKey],
] struct {
	sync.Mutex
	Rows *btree.Generic[*Row[PrimaryKey, Attrs]]
	//TODO unique index
	//TODO indexes
	//TODO foreign keys
}

type Attributes[PrimaryKey any] interface {
	PrimaryKey() PrimaryKey
}

type Row[
	PrimaryKey Ordered[PrimaryKey],
	Attrs Attributes[PrimaryKey],
] struct {
	PrimaryKey PrimaryKey
	Values     *MVCC[Attrs]
}

type Ordered[To any] interface {
	Less(to To) bool
}

func NewTable[
	PrimaryKey Ordered[PrimaryKey],
	Attrs Attributes[PrimaryKey],
]() *Table[PrimaryKey, Attrs] {
	return &Table[PrimaryKey, Attrs]{
		Rows: btree.NewGeneric(func(a, b *Row[PrimaryKey, Attrs]) bool {
			return a.PrimaryKey.Less(b.PrimaryKey)
		}),
	}
}

func (t *Table[PrimaryKey, Attrs]) Insert(
	tx *Transaction,
	attrs Attrs,
) error {
	t.Lock()
	key := attrs.PrimaryKey()
	row := t.getRow(key)
	t.Unlock()
	if err := row.Values.Insert(tx, tx.CurrentTime, attrs); err != nil {
		return err
	}
	//TODO this is wrong
	// writeTime's logical time should be the statement number
	// but currently the engine does not expose statement numbers
	// for now, just tick on every write operation
	tx.Tick()
	return nil
}

func (t *Table[PrimaryKey, Attrs]) Update(
	tx *Transaction,
	attrs Attrs,
) error {
	t.Lock()
	key := attrs.PrimaryKey()
	row := t.getRow(key)
	t.Unlock()
	if err := row.Values.Update(tx, tx.CurrentTime, attrs); err != nil {
		return err
	}
	tx.Tick()
	return nil
}

func (t *Table[PrimaryKey, Attrs]) Delete(
	tx *Transaction,
	key PrimaryKey,
) error {
	t.Lock()
	row := t.getRow(key)
	t.Unlock()
	if err := row.Values.Delete(tx, tx.CurrentTime); err != nil {
		return err
	}
	tx.Tick()
	return nil
}

func (t *Table[PrimaryKey, Attrs]) Get(
	tx *Transaction,
	key PrimaryKey,
) (
	attrs Attrs,
	err error,
) {
	t.Lock()
	row := t.getRow(key)
	t.Unlock()
	if row == nil {
		err = sql.ErrNoRows
		return
	}
	attrs = *row.Values.Read(tx, tx.CurrentTime)
	return
}

func (t *Table[PrimaryKey, Attrs]) getRow(key PrimaryKey) *Row[PrimaryKey, Attrs] {
	pivot := &Row[PrimaryKey, Attrs]{
		PrimaryKey: key,
	}
	row, ok := t.Rows.Get(pivot)
	if !ok {
		row = pivot
		row.Values = new(MVCC[Attrs])
		t.Rows.Set(row)
	}
	return row
}
