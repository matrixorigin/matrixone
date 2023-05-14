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
)

type memStore struct {
	sync.Mutex
	caches map[uint64][]AutoColumn
}

// NewMemStore new mem store
func NewMemStore() IncrValueStore {
	return &memStore{
		caches: make(map[uint64][]AutoColumn),
	}
}

func (s *memStore) Create(
	ctx context.Context,
	tableID uint64,
	cols []AutoColumn) error {
	s.Lock()
	defer s.Unlock()
	caches := s.caches[tableID]
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
	s.caches[tableID] = caches
	return nil
}

func (s *memStore) GetCloumns(
	ctx context.Context,
	tableID uint64) ([]AutoColumn, error) {
	s.Lock()
	defer s.Unlock()
	return s.caches[tableID], nil
}

func (s *memStore) Alloc(
	ctx context.Context,
	tableID uint64,
	key string,
	count int) (uint64, uint64, error) {
	s.Lock()
	defer s.Unlock()
	cols, ok := s.caches[tableID]
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
	return from, to, nil
}

func (s *memStore) UpdateMinValue(
	ctx context.Context,
	tableID uint64,
	col string,
	minValue uint64) error {
	s.Lock()
	defer s.Unlock()
	cols, ok := s.caches[tableID]
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
	tableID uint64) error {
	s.Lock()
	defer s.Unlock()
	delete(s.caches, tableID)
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
