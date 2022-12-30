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
	"fmt"
	"io"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/tidwall/btree"
)

// tableState represents a snapshot state of a table
type tableState[
	K Ordered[K],
	V any,
] struct {
	serial  int64
	rows    *btree.BTreeG[*KVPair[K, V]]
	logs    *btree.BTreeG[*log[K, V]]
	indexes *btree.BTreeG[*IndexEntry[K, V]]
}

var nextTableStateSerial = int64(1 << 16)

func (t *tableState[K, V]) cloneWithLogs() *tableState[K, V] {
	ret := &tableState[K, V]{
		serial:  atomic.AddInt64(&nextTableStateSerial, 1),
		rows:    t.rows.Copy(),
		logs:    t.logs.Copy(),
		indexes: t.indexes.Copy(),
	}
	return ret
}

func (t *tableState[K, V]) cloneWithoutLogs() *tableState[K, V] {
	ret := &tableState[K, V]{
		serial:  atomic.AddInt64(&nextTableStateSerial, 1),
		rows:    t.rows.Copy(),
		logs:    btree.NewBTreeG(compareLog[K, V]),
		indexes: t.indexes.Copy(),
	}
	return ret
}

// merge merges two table states
func (t *tableState[K, V]) merge(
	from *tableState[K, V],
) (
	*tableState[K, V],
	error,
) {

	t = t.cloneWithoutLogs()

	iter := from.logs.Copy().Iter()
	defer iter.Release()

	for ok := iter.First(); ok; ok = iter.Next() {

		log := iter.Item()
		key := log.key

		pivot := &KVPair[K, V]{
			Key: key,
		}
		oldPair, _ := t.rows.Get(pivot)

		if log.pair != nil && log.oldPair != nil {
			// update
			if oldPair == nil {
				return nil, moerr.NewTxnWWConflictNoCtx()
			}
			if oldPair.ID != log.oldPair.ID {
				return nil, moerr.NewTxnWWConflictNoCtx()
			}
			pivot.ID = log.pair.ID
			pivot.Value = log.pair.Value
			t.setPair(pivot, oldPair)

		} else if log.pair == nil {
			// delete
			if oldPair == nil {
				return nil, moerr.NewTxnWWConflictNoCtx()
			}
			if oldPair.ID != log.oldPair.ID {
				return nil, moerr.NewTxnWWConflictNoCtx()
			}
			t.unsetPair(pivot, oldPair)

		} else if log.pair != nil && log.oldPair == nil {
			// insert
			if oldPair != nil {
				return nil, moerr.NewTxnWWConflictNoCtx()
			}
			pivot.ID = log.pair.ID
			pivot.Value = log.pair.Value
			t.setPair(pivot, oldPair)
		}

	}

	return t, nil
}

func (s *tableState[K, V]) dump(w io.Writer) {
	fmt.Fprintf(w, "table state, serial %v\n", s.serial)
	{
		iter := s.rows.Copy().Iter()
		for ok := iter.First(); ok; ok = iter.Next() {
			row := iter.Item()
			fmt.Fprintf(w, "\trow %+v\n", row)
		}
	}
	{
		iter := s.logs.Copy().Iter()
		for ok := iter.First(); ok; ok = iter.Next() {
			log := iter.Item()
			fmt.Fprintf(w, "\tlog %+v\n", log)
		}
	}
}

func init() {
	_ = new(tableState[Int, int]).dump // to tame static checks
}

// setPair set a key-value pair
func (s *tableState[K, V]) setPair(pair *KVPair[K, V], oldPair *KVPair[K, V]) {

	if oldPair != nil {
		// remove indexes
		for _, index := range oldPair.Indexes {
			s.indexes.Delete(&IndexEntry[K, V]{
				Index: index,
				Key:   oldPair.Key,
			})
		}
	}

	if pair != nil {
		// set row
		s.rows.Set(pair)

		// add indexes
		for _, index := range pair.Indexes {
			s.indexes.Set(&IndexEntry[K, V]{
				Index: index,
				Key:   pair.Key,
			})
		}

		// add log
		log := &log[K, V]{
			key:     pair.Key,
			serial:  atomic.AddInt64(&nextLogSerial, 1),
			pair:    pair,
			oldPair: oldPair,
		}
		s.logs.Set(log)

	}

}

// unsetPair unsets a key-value pair
func (s *tableState[K, V]) unsetPair(pivot *KVPair[K, V], oldPair *KVPair[K, V]) {

	if oldPair != nil {
		// remove indexes
		for _, index := range oldPair.Indexes {
			s.indexes.Delete(&IndexEntry[K, V]{
				Index: index,
				Key:   oldPair.Key,
			})
		}
	}

	// delete row
	s.rows.Delete(pivot)

	// add log
	s.logs.Set(&log[K, V]{
		key:     pivot.Key,
		serial:  atomic.AddInt64(&nextLogSerial, 1),
		oldPair: oldPair,
	})

}
