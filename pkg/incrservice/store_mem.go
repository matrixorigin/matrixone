// Copyright 2023 Matrix Origin
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

package incrservice

import (
	"context"
	"math"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
)

type memStore struct {
	sync.Mutex
	caches      map[uint64][]AutoColumn
	uncommitted map[string]map[uint64][]AutoColumn
}

// NewMemStore new mem store
func NewMemStore() IncrValueStore {
	return &memStore{
		caches:      make(map[uint64][]AutoColumn),
		uncommitted: make(map[string]map[uint64][]AutoColumn),
	}
}

func (s *memStore) Create(
	ctx context.Context,
	tableID uint64,
	cols []AutoColumn,
	txnOp client.TxnOperator) error {
	s.Lock()
	defer s.Unlock()
	m := s.caches
	if txnOp != nil {
		m = make(map[uint64][]AutoColumn)
		s.uncommitted[string(txnOp.Txn().ID)] = m
		txnOp.(client.EventableTxnOperator).AppendEventCallback(
			client.ClosedEvent,
			func(txnMeta txn.TxnMeta) {
				s.Lock()
				defer s.Unlock()
				delete(s.uncommitted, string(txnMeta.ID))
				if txnMeta.Status == txn.TxnStatus_Committed {
					for k, v := range m {
						s.caches[k] = v
					}
				}
			})
	}

	caches := m[tableID]
	for _, col := range cols {
		has := false
		for _, cache := range caches {
			if cache.ColName == col.ColName {
				has = true
				break
			}
		}
		if !has {
			caches = append(caches, col)
		}
	}
	m[tableID] = caches
	return nil
}

func (s *memStore) GetColumns(
	ctx context.Context,
	tableID uint64,
	txnOp client.TxnOperator) ([]AutoColumn, error) {
	s.Lock()
	defer s.Unlock()
	s.Lock()
	defer s.Unlock()
	m := s.caches
	if txnOp != nil {
		m = s.uncommitted[string(txnOp.Txn().ID)]
	}
	return m[tableID], nil
}

func (s *memStore) Allocate(
	ctx context.Context,
	tableID uint64,
	key string,
	count int,
	txnOp client.TxnOperator) (uint64, uint64, error) {
	s.Lock()
	defer s.Unlock()
	m := s.caches
	if txnOp != nil {
		m = s.uncommitted[string(txnOp.Txn().ID)]
	}

	cols, ok := m[tableID]
	if !ok {
		panic("missing incr column record")
	}

	var c *AutoColumn
	for i := range cols {
		if cols[i].ColName == key {
			c = &cols[i]
		}
	}
	if !ok {
		panic("missing incr column record")
	}

	curr := c.Offset
	next := getNext(curr, count, int(c.Step))
	c.Offset = next
	from, to := getNextRange(curr, next, int(c.Step))
	time.Sleep(time.Millisecond * 10)
	return from, to, nil
}

func (s *memStore) UpdateMinValue(
	ctx context.Context,
	tableID uint64,
	col string,
	minValue uint64,
	txnOp client.TxnOperator) error {
	s.Lock()
	defer s.Unlock()
	m := s.caches
	if txnOp != nil {
		m = s.uncommitted[string(txnOp.Txn().ID)]
	}

	cols, ok := m[tableID]
	if !ok {
		panic("missing incr column record")
	}

	var c *AutoColumn
	for i := range cols {
		if cols[i].ColName == col {
			c = &cols[i]
		}
	}
	if !ok {
		panic("missing incr column record")
	}

	if c != nil && c.Offset < minValue {
		c.Offset = minValue
	}
	return nil
}

func (s *memStore) Delete(
	ctx context.Context,
	tableID uint64,
	op client.TxnOperator) error {
	op.(client.EventableTxnOperator).AppendEventCallback(
		client.ClosedEvent,
		func(txnMeta txn.TxnMeta) {
			if txnMeta.Status == txn.TxnStatus_Committed {
				s.Lock()
				defer s.Unlock()
				delete(s.caches, tableID)
			}
		})
	return nil
}

func (s *memStore) Close() {

}

func getNext(curr uint64, count, step int) uint64 {
	n := uint64(count * step)
	diff := math.MaxUint64 - curr
	if diff <= n {
		return math.MaxUint64
	}
	return curr + n
}

// [from, to)
func getNextRange(curr uint64, next uint64, step int) (uint64, uint64) {
	return curr + uint64(step), next + uint64(step)
}
