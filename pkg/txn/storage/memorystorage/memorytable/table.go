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
	"sort"
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/tidwall/btree"
)

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

type Row[K any, V any] interface {
	Key() K
	Value() V
	Indexes() []Tuple
	UniqueIndexes() []Tuple
}

func NewTable[
	K Ordered[K],
	V any,
	R Row[K, V],
]() *Table[K, V, R] {
	ret := &Table[K, V, R]{
		id: atomic.AddInt64(&nextTableID, 1),
	}
	state := &tableState[K, V]{
		serial:  atomic.AddInt64(&nextTableStateSerial, 1),
		rows:    btree.NewBTreeG(compareKVPair[K, V]),
		logs:    btree.NewBTreeG(compareLog[K, V]),
		indexes: btree.NewBTreeG(compareIndexEntry[K, V]),
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
	var ok bool
	txTable, ok = tx.tables[t.id]
	if !ok {

		txTable = &transactionTable{
			table: t,
		}

		t.Lock()
		defer t.Unlock()
		if !tx.BeginTime.IsEmpty() && len(t.history) > 0 {
			// get from history
			i := sort.Search(len(t.history), func(i int) bool {
				t := t.history[i]
				return tx.BeginTime.Equal(t.Before) || tx.BeginTime.Less(t.Before)
			})
			if i < len(t.history) {
				if i == 0 {
					if tx.BeginTime.Less(t.history[0].Before) {
						// too old
						return nil, moerr.NewInternalError("transaction begin time too old")
					}
				}
				state := t.history[i].State.cloneWithoutLogs()
				txTable.state.Store(state)
			} else {
				// after all history
				state := t.state.Load().cloneWithoutLogs()
				txTable.state.Store(state)
			}

		} else {
			// use latest
			state := t.state.Load().cloneWithoutLogs()
			txTable.state.Store(state)
		}

		tx.tables[t.id] = txTable
	}
	return
}

func (t *Table[K, V, R]) Insert(
	tx *Transaction,
	row R,
) (
	err error,
) {
	txTable, err := t.getTransactionTable(tx)
	if err != nil {
		return err
	}
	initState := txTable.state.Load().(*tableState[K, V])

	key := row.Key()
	pair := &KVPair[K, V]{
		Key: key,
	}

	_, ok := initState.rows.Get(pair)
	if ok {
		return moerr.NewDuplicate()
	}

	state := initState.cloneWithLogs()
	defer func() {
		if err == nil {
			if !txTable.state.CompareAndSwap(initState, state) {
				panic("concurrent mutation")
			}
		}
	}()

	pair.ID = atomic.AddInt64(&nextKVPairID, 1)
	pair.Value = row.Value()
	pair.Indexes = row.Indexes()
	state.setPair(pair, nil)

	return nil
}

func (t *Table[K, V, R]) Update(
	tx *Transaction,
	row R,
) (
	err error,
) {
	txTable, err := t.getTransactionTable(tx)
	if err != nil {
		return err
	}
	initState := txTable.state.Load().(*tableState[K, V])
	key := row.Key()
	pair := &KVPair[K, V]{
		Key: key,
	}

	oldPair, ok := initState.rows.Get(pair)
	if !ok {
		return sql.ErrNoRows
	}

	state := initState.cloneWithLogs()
	defer func() {
		if err == nil {
			if !txTable.state.CompareAndSwap(initState, state) {
				panic("concurrent mutation")
			}
		}
	}()

	pair.ID = atomic.AddInt64(&nextKVPairID, 1)
	pair.Value = row.Value()
	pair.Indexes = row.Indexes()
	state.setPair(pair, oldPair)

	return nil
}

func (t *Table[K, V, R]) Delete(
	tx *Transaction,
	key K,
) (
	err error,
) {
	txTable, err := t.getTransactionTable(tx)
	if err != nil {
		return err
	}
	initState := txTable.state.Load().(*tableState[K, V])

	pivot := &KVPair[K, V]{
		Key: key,
	}

	oldPair, ok := initState.rows.Get(pivot)
	if !ok {
		return sql.ErrNoRows
	}

	state := initState.cloneWithLogs()
	defer func() {
		if err == nil {
			if !txTable.state.CompareAndSwap(initState, state) {
				panic("concurrent mutation")
			}
		}
	}()

	state.unsetPair(pivot, oldPair)

	return nil
}

func (t *Table[K, V, R]) Upsert(
	tx *Transaction,
	row R,
) (
	err error,
) {
	txTable, err := t.getTransactionTable(tx)
	if err != nil {
		return err
	}
	initState := txTable.state.Load().(*tableState[K, V])

	key := row.Key()
	pair := &KVPair[K, V]{
		Key: key,
	}

	oldPair, ok := initState.rows.Get(pair)
	if !ok {
		// insert

		state := initState.cloneWithLogs()
		defer func() {
			if err == nil {
				if !txTable.state.CompareAndSwap(initState, state) {
					panic("concurrent mutation")
				}
			}
		}()

		pair.ID = atomic.AddInt64(&nextKVPairID, 1)
		pair.Value = row.Value()
		pair.Indexes = row.Indexes()
		state.setPair(pair, oldPair)

		return nil
	}

	state := initState.cloneWithLogs()
	defer func() {
		if err == nil {
			if !txTable.state.CompareAndSwap(initState, state) {
				panic("concurrent mutation")
			}
		}
	}()

	pair.ID = atomic.AddInt64(&nextKVPairID, 1)
	pair.Value = row.Value()
	pair.Indexes = row.Indexes()
	state.setPair(pair, oldPair)

	return nil
}

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
	pair := &KVPair[K, V]{
		Key: key,
	}
	pair, ok := state.rows.Get(pair)
	if !ok {
		err = sql.ErrNoRows
		return
	}
	value = pair.Value
	return
}
