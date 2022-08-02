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
	K Ordered[K],
	R Row[K],
] struct {
	sync.Mutex
	Rows *btree.Generic[*PhysicalRow[K, R]]
	//TODO unique index
	//TODO indexes
	//TODO foreign keys
}

type Row[K any] interface {
	PrimaryKey() K
}

type PhysicalRow[
	K Ordered[K],
	R Row[K],
] struct {
	PrimaryKey K
	Values     *MVCC[R]
}

type Ordered[To any] interface {
	Less(to To) bool
}

func NewTable[
	K Ordered[K],
	R Row[K],
]() *Table[K, R] {
	return &Table[K, R]{
		Rows: btree.NewGeneric(func(a, b *PhysicalRow[K, R]) bool {
			return a.PrimaryKey.Less(b.PrimaryKey)
		}),
	}
}

func (t *Table[K, R]) Insert(
	tx *Transaction,
	row R,
) error {
	t.Lock()
	key := row.PrimaryKey()
	physicalRow := t.getRow(key)
	t.Unlock()
	if err := physicalRow.Values.Insert(tx, tx.CurrentTime, row); err != nil {
		return err
	}
	//TODO this is wrong
	// writeTime's logical time should be the statement number
	// but currently the engine does not expose statement numbers
	// for now, just tick on every write operation
	tx.Tick()
	return nil
}

func (t *Table[K, R]) Update(
	tx *Transaction,
	row R,
) error {
	t.Lock()
	key := row.PrimaryKey()
	physicalRow := t.getRow(key)
	t.Unlock()
	if err := physicalRow.Values.Update(tx, tx.CurrentTime, row); err != nil {
		return err
	}
	tx.Tick()
	return nil
}

func (t *Table[K, R]) Delete(
	tx *Transaction,
	key K,
) error {
	t.Lock()
	physicalRow := t.getRow(key)
	t.Unlock()
	if err := physicalRow.Values.Delete(tx, tx.CurrentTime); err != nil {
		return err
	}
	//TODO cascade delete with foreign keys
	tx.Tick()
	return nil
}

func (t *Table[K, R]) Get(
	tx *Transaction,
	key K,
) (
	row R,
	err error,
) {
	t.Lock()
	physicalRow := t.getRow(key)
	t.Unlock()
	if physicalRow == nil {
		err = sql.ErrNoRows
		return
	}
	row = *physicalRow.Values.Read(tx, tx.CurrentTime)
	return
}

func (t *Table[K, R]) getRow(key K) *PhysicalRow[K, R] {
	pivot := &PhysicalRow[K, R]{
		PrimaryKey: key,
	}
	row, ok := t.Rows.Get(pivot)
	if !ok {
		row = pivot
		row.Values = new(MVCC[R])
		t.Rows.Set(row)
	}
	return row
}
