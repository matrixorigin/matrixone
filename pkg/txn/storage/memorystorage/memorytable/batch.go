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
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// Batch represents a batch of operations that commit atomically
type Batch[
	K Ordered[K],
	V any,
	R Row[K, V],
] struct {
	txTable   *transactionTable
	initState *tableState[K, V]
	state     *tableState[K, V]
	lastErr   error
}

// NewBatch creates a new batch for tx
func (t *Table[K, V, R]) NewBatch(tx *Transaction) (*Batch[K, V, R], error) {
	txTable, err := t.getTransactionTable(tx)
	if err != nil {
		return nil, err
	}
	initState := txTable.state.Load().(*tableState[K, V])
	return &Batch[K, V, R]{
		txTable:   txTable,
		initState: initState,
		state:     initState.clone(),
	}, nil
}

// Commit commits the batch
func (b *Batch[K, V, R]) Commit() {
	if b.lastErr != nil {
		panic("batch is invalid")
	}
	if !b.txTable.state.CompareAndSwap(b.initState, b.state) {
		panic("concurrent mutation")
	}
}

// Insert inserts a row
func (b *Batch[K, V, R]) Insert(row R) (err error) {
	if b.lastErr != nil {
		panic("batch is invalid")
	}
	defer func() {
		if err != nil {
			b.lastErr = err
		}
	}()

	key := row.Key()
	pair := &KVPair[K, V]{
		Key: key,
	}

	_, ok := b.state.tree.Get(TreeNode[K, V]{
		KVPair: pair,
	})
	if ok {
		return moerr.NewDuplicateNoCtx()
	}

	pair.KVValue = new(KVValue[K, V])
	pair.ID = atomic.AddInt64(&nextKVPairID, 1)
	pair.Value = row.Value()
	pair.Indexes = row.Indexes()
	b.state.setPair(pair, nil)

	return nil
}

// Update updates a row
func (b *Batch[K, V, R]) Update(row R) (err error) {
	if b.lastErr != nil {
		panic("batch is invalid")
	}
	defer func() {
		if err != nil {
			b.lastErr = err
		}
	}()

	key := row.Key()
	pair := &KVPair[K, V]{
		Key: key,
	}

	oldNode, ok := b.state.tree.Get(TreeNode[K, V]{
		KVPair: pair,
	})
	if !ok {
		return sql.ErrNoRows
	}

	pair.KVValue = new(KVValue[K, V])
	pair.ID = atomic.AddInt64(&nextKVPairID, 1)
	pair.Value = row.Value()
	pair.Indexes = row.Indexes()
	b.state.setPair(pair, oldNode.KVPair)

	return nil
}

// Delete deletes a row
func (b *Batch[K, V, R]) Delete(key K) (err error) {
	if b.lastErr != nil {
		panic("batch is invalid")
	}
	defer func() {
		if err != nil {
			b.lastErr = err
		}
	}()

	pivot := KVPair[K, V]{
		Key: key,
	}

	oldNode, ok := b.state.tree.Get(TreeNode[K, V]{
		KVPair: &pivot,
	})
	if !ok {
		return sql.ErrNoRows
	}

	b.state.unsetPair(pivot, oldNode.KVPair)

	return nil
}

// Upsert update or insert a row
func (b *Batch[K, V, R]) Upsert(row R) (err error) {
	if b.lastErr != nil {
		panic("batch is invalid")
	}
	defer func() {
		if err != nil {
			b.lastErr = err
		}
	}()

	key := row.Key()
	pair := &KVPair[K, V]{
		Key: key,
	}

	oldNode, ok := b.state.tree.Get(TreeNode[K, V]{
		KVPair: pair,
	})
	if !ok {
		// insert
		pair.KVValue = new(KVValue[K, V])
		pair.ID = atomic.AddInt64(&nextKVPairID, 1)
		pair.Value = row.Value()
		pair.Indexes = row.Indexes()
		b.state.setPair(pair, oldNode.KVPair)
		return nil
	}

	// update
	pair.KVValue = new(KVValue[K, V])
	pair.ID = atomic.AddInt64(&nextKVPairID, 1)
	pair.Value = row.Value()
	pair.Indexes = row.Indexes()
	b.state.setPair(pair, oldNode.KVPair)

	return nil
}

// Get gets the value of key
func (b *Batch[K, V, R]) Get(key K) (value V, err error) {
	if b.lastErr != nil {
		panic("batch is invalid")
	}
	defer func() {
		if err != nil {
			b.lastErr = err
		}
	}()

	node := TreeNode[K, V]{
		KVPair: &KVPair[K, V]{
			Key: key,
		},
	}
	node, ok := b.state.tree.Get(node)
	if !ok {
		err = sql.ErrNoRows
		return
	}
	value = node.KVPair.Value
	return
}
