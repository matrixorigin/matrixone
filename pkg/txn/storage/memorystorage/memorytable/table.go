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

package memorytable

import (
	"database/sql"
	"io"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// Table represents a table
type Table[
	K Ordered[K],
	V any,
	R Row[K, V],
] struct {
	id int64
	sync.Mutex
	state   atomic.Pointer[tableState[K, V]]
	history []*history[K, V]
}

var nextTableID = int64(1)

// Row represents a logical row in a table
type Row[K any, V any] interface {
	Key() K
	Value() V
	Indexes() []Tuple
	UniqueIndexes() []Tuple
}

// NewTable creates new table
func NewTable[
	K Ordered[K],
	V any,
	R Row[K, V],
]() *Table[K, V, R] {
	ret := &Table[K, V, R]{
		id: atomic.AddInt64(&nextTableID, 1),
	}
	state := &tableState[K, V]{
		tree: NewBTree[K, V](),
		log:  NewSliceLog[K, V](),
	}
	ret.state.Store(state)
	return ret
}

func (t *Table[K, V, R]) getTransactionTable(
	tx *Transaction,
) (
	txTable *transactionTable,
	err error,
) {
	tx.tables.Lock()
	var ok bool
	txTable, ok = tx.tables.Map[t.id]
	if !ok {
		return t.getTransactionTableSlow(tx)
	}
	tx.tables.Unlock()
	return
}

func (t *Table[K, V, R]) getTransactionTableSlow(
	tx *Transaction,
) (
	txTable *transactionTable,
	err error,
) {
	defer tx.tables.Unlock()

	txTable = &transactionTable{
		table: t,
	}

	t.Lock()
	defer t.Unlock()
	if !tx.BeginTime.IsEmpty() && len(t.history) > 0 {
		// get from history
		i := sort.Search(len(t.history), func(i int) bool {
			t := t.history[i]
			return tx.BeginTime.Equal(t.EndTime) || tx.BeginTime.Less(t.EndTime)
		})
		if i < len(t.history) {
			if i == 0 {
				if tx.BeginTime.Less(t.history[0].EndTime) {
					// too old
					return nil, moerr.NewInternalErrorNoCtx("transaction begin time too old")
				}
			}
			state := t.history[i].EndState
			txTable.state.Store(state.clone())
			txTable.initState = state
		} else {
			// after all history
			state := t.state.Load()
			txTable.state.Store(state.clone())
			txTable.initState = state
		}

	} else {
		// use latest
		state := t.state.Load()
		txTable.state.Store(state.clone())
		txTable.initState = state
	}

	tx.tables.Map[t.id] = txTable

	return
}

// Insert inserts a row to the table
func (t *Table[K, V, R]) Insert(
	tx *Transaction,
	row R,
) (
	err error,
) {
	bat, err := t.NewBatch(tx)
	if err != nil {
		return err
	}
	if err := bat.Insert(row); err != nil {
		return err
	}
	bat.Commit()
	return nil
}

// Update updates a row in the table
func (t *Table[K, V, R]) Update(
	tx *Transaction,
	row R,
) (
	err error,
) {
	bat, err := t.NewBatch(tx)
	if err != nil {
		return err
	}
	if err := bat.Update(row); err != nil {
		return err
	}
	bat.Commit()
	return nil
}

// Delete deletes a row from the table
func (t *Table[K, V, R]) Delete(
	tx *Transaction,
	key K,
) (
	err error,
) {
	bat, err := t.NewBatch(tx)
	if err != nil {
		return err
	}
	if err := bat.Delete(key); err != nil {
		return err
	}
	bat.Commit()
	return nil
}

// Upsert update a row in the table or insert the row to the table if not exists
func (t *Table[K, V, R]) Upsert(
	tx *Transaction,
	row R,
) (
	err error,
) {
	bat, err := t.NewBatch(tx)
	if err != nil {
		return err
	}
	if err := bat.Upsert(row); err != nil {
		return err
	}
	bat.Commit()
	return nil
}

// Get gets the value of a key
func (t *Table[K, V, R]) Get(
	tx *Transaction,
	key K,
) (
	value V,
	err error,
) {
	txTable, err := t.getTransactionTable(tx)
	if err != nil {
		return
	}
	state := txTable.state.Load().(*tableState[K, V])
	node := TreeNode[K, V]{
		KVPair: &KVPair[K, V]{
			Key: key,
		},
	}
	node, ok := state.tree.Get(node)
	if !ok {
		err = sql.ErrNoRows
		return
	}
	value = node.KVPair.Value
	return
}

// NewIndexIter creates new index iter
func (t *Table[K, V, R]) NewIndexIter(tx *Transaction, min Tuple, max Tuple) (Iter[*IndexEntry[K, V]], error) {
	txTable, err := t.getTransactionTable(tx)
	if err != nil {
		return nil, err
	}
	state := txTable.state.Load().(*tableState[K, V])
	iter := state.tree.Copy().Iter()
	return NewBoundedIndexIter(iter, min, max), nil
}

// Index finds entry in table
func (t *Table[K, V, R]) Index(tx *Transaction, index Tuple) (entries []*IndexEntry[K, V], err error) {
	iter, err := t.NewIndexIter(
		tx,
		index,
		index,
	)
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	for ok := iter.First(); ok; ok = iter.Next() {
		entry, err := iter.Read()
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	return
}

func (t *Table[K, V, R]) Dump(w io.Writer) {
	state := t.state.Load()
	state.dump(w)
}
