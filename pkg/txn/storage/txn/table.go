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
	"errors"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/tidwall/btree"
)

type Table[
	K Ordered[K],
	R Row[K],
] struct {
	sync.Mutex
	rows      *btree.Generic[*PhysicalRow[K, R]]
	index     *btree.Generic[IndexEntry[K, R]]
	writeSets map[*Transaction]map[*PhysicalRow[K, R]]struct{}
}

type Row[K any] interface {
	Key() K
	Indexes() []Tuple
}

type NamedRow interface {
	AttrByName(tx *Transaction, name string) (Nullable, error)
}

type PhysicalRow[
	K Ordered[K],
	R Row[K],
] struct {
	Key        K
	LastUpdate *Atomic[time.Time]
	Values     *MVCC[R]
}

type Ordered[To any] interface {
	Less(to To) bool
}

type IndexEntry[K Ordered[K], R Row[K]] struct {
	Index Tuple
	Key   K
	Row   *R
}

func NewTable[
	K Ordered[K],
	R Row[K],
]() *Table[K, R] {
	return &Table[K, R]{
		rows: btree.NewGeneric(func(a, b *PhysicalRow[K, R]) bool {
			return a.Key.Less(b.Key)
		}),
		index: btree.NewGeneric(func(a, b IndexEntry[K, R]) bool {
			if a.Index.Less(b.Index) {
				return true
			}
			if b.Index.Less(a.Index) {
				return false
			}
			return a.Key.Less(b.Key)
		}),
		writeSets: make(map[*Transaction]map[*PhysicalRow[K, R]]struct{}),
	}
}

func (t *Table[K, R]) Insert(
	tx *Transaction,
	row R,
) error {
	key := row.Key()
	t.Lock()
	physicalRow := t.getOrSetRowByKey(key)
	t.Unlock()

	existed, err := physicalRow.Values.ReadVisible(tx.Time, tx)
	if errors.Is(err, sql.ErrNoRows) {
		err = nil
	}
	if err != nil {
		return err
	}
	if existed != nil {
		return moerr.NewPrimaryKeyDuplicated(key)
	}

	if err := physicalRow.Values.Insert(tx.Time, tx, &row); err != nil {
		return err
	}
	physicalRow.LastUpdate.Store(time.Now())
	for _, index := range row.Indexes() {
		t.index.Set(IndexEntry[K, R]{
			Index: index,
			Key:   key,
			Row:   &row,
		})
	}

	t.setCommitter(tx, physicalRow)

	tx.Time.Tick()
	return nil
}

func (t *Table[K, R]) Update(
	tx *Transaction,
	row R,
) error {
	key := row.Key()
	t.Lock()
	physicalRow := t.getOrSetRowByKey(key)
	t.Unlock()

	if err := physicalRow.Values.Update(tx.Time, tx, &row); err != nil {
		return err
	}
	physicalRow.LastUpdate.Store(time.Now())
	for _, index := range row.Indexes() {
		t.index.Set(IndexEntry[K, R]{
			Index: index,
			Key:   key,
			Row:   &row,
		})
	}

	//TODO
	//t.setCommitter(tx, physicalRow)

	tx.Time.Tick()
	return nil
}

func (t *Table[K, R]) Delete(
	tx *Transaction,
	key K,
) error {
	t.Lock()
	physicalRow := t.getRowByKey(key)
	t.Unlock()

	if physicalRow == nil {
		return nil
	}
	if err := physicalRow.Values.Delete(tx.Time, tx); err != nil {
		return err
	}
	physicalRow.LastUpdate.Store(time.Now())

	//TODO
	//t.setCommitter(tx, physicalRow)

	tx.Time.Tick()
	return nil
}

func (t *Table[K, R]) Get(
	tx *Transaction,
	key K,
) (
	row *R,
	err error,
) {
	t.Lock()
	physicalRow := t.getRowByKey(key)
	t.Unlock()
	if physicalRow == nil {
		err = sql.ErrNoRows
		return
	}
	mvccValues := physicalRow.Values
	row, err = mvccValues.Read(tx.Time, tx)
	if err != nil {
		return
	}
	return
}

func (t *Table[K, R]) getRowByKey(key K) *PhysicalRow[K, R] {
	pivot := &PhysicalRow[K, R]{
		Key: key,
	}
	row, _ := t.rows.Get(pivot)
	return row
}

func (t *Table[K, R]) getOrSetRowByKey(key K) *PhysicalRow[K, R] {
	pivot := &PhysicalRow[K, R]{
		Key: key,
	}
	row, ok := t.rows.Get(pivot)
	if !ok {
		row = pivot
		row.Values = new(MVCC[R])
		row.LastUpdate = NewAtomic(time.Now())
		t.rows.Set(row)
	}
	return row
}

func (t *Table[K, R]) Index(tx *Transaction, index Tuple) (keys []K, err error) {
	pivot := IndexEntry[K, R]{
		Index: index,
	}
	iter := t.index.Copy().Iter()
	defer iter.Release()
	for ok := iter.Seek(pivot); ok; ok = iter.Next() {
		item := iter.Item()
		if index.Less(item.Index) {
			break
		}
		if item.Index.Less(index) {
			break
		}
		cur, err := t.Get(tx, item.Key)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				continue
			}
			return nil, err
		}
		if cur == item.Row {
			keys = append(keys, item.Key)
		}
	}
	return
}

func (t *Table[K, R]) IndexRows(tx *Transaction, index Tuple) (rows []*R, err error) {
	pivot := IndexEntry[K, R]{
		Index: index,
	}
	iter := t.index.Copy().Iter()
	defer iter.Release()
	for ok := iter.Seek(pivot); ok; ok = iter.Next() {
		item := iter.Item()
		if index.Less(item.Index) {
			break
		}
		if item.Index.Less(index) {
			break
		}
		cur, err := t.Get(tx, item.Key)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				continue
			}
			return nil, err
		}
		if cur == item.Row {
			rows = append(rows, cur)
		}
	}
	return
}

func (t *Table[K, R]) setCommitter(tx *Transaction, row *PhysicalRow[K, R]) {
	tx.committers[t] = struct{}{}
	t.Lock()
	set, ok := t.writeSets[tx]
	if !ok {
		set = make(map[*PhysicalRow[K, R]]struct{})
		t.writeSets[tx] = set
	}
	set[row] = struct{}{}
	t.Unlock()
}

func (t *Table[K, R]) CommitTx(tx *Transaction) error {
	t.Lock()
	set := t.writeSets[tx]
	t.Unlock()
	defer func() {
		t.Lock()
		delete(t.writeSets, tx)
		t.Unlock()
	}()

	for physicalRow := range set {
		values := physicalRow.Values

		var err error
		values.RLock()
		for i := len(values.Values) - 1; i >= 0; i-- {
			value := values.Values[i]

			if value.Visible(tx.Time, tx.ID) &&
				value.BornTx.ID != tx.ID &&
				value.BornTime.After(tx.BeginTime) {
				err = moerr.NewPrimaryKeyDuplicated(physicalRow.Key)
				break
			}

		}
		values.RUnlock()

		if err != nil {
			return err
		}

	}

	return nil
}

func (t *Table[K, R]) AbortTx(tx *Transaction) {
	t.Lock()
	delete(t.writeSets, tx)
	t.Unlock()
}
