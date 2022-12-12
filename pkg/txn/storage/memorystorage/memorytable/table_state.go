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
)

// tableState represents a snapshot state of a table
type tableState[
	K Ordered[K],
	V any,
] struct {
	id    int64
	rows  Rows[K, V]
	log   Log[K, V]
	index Index[K, V]
}

func (t *tableState[K, V]) cloneWithLogs() *tableState[K, V] {
	ret := &tableState[K, V]{
		rows:  t.rows.Copy(),
		log:   t.log.Copy(),
		index: t.index.Copy(),
	}
	return ret
}

func (t *tableState[K, V]) cloneWithoutLogs() *tableState[K, V] {
	ret := &tableState[K, V]{
		rows:  t.rows.Copy(),
		log:   NewBTreeLog[K, V](),
		index: t.index.Copy(),
	}
	return ret
}

// merge merges two table states
func (t *tableState[K, V]) merge(
	from *tableState[K, V],
) (
	*tableState[K, V],
	[]*logEntry[K, V],
	error,
) {

	t = t.cloneWithoutLogs()
	var logs []*logEntry[K, V]

	iter := from.log.Copy().Iter()
	defer iter.Close()

	for ok := iter.First(); ok; ok = iter.Next() {

		log, err := iter.Read()
		if err != nil {
			return nil, nil, err
		}
		logs = append(logs, log)
		key := log.key

		pivot := &KVPair[K, V]{
			Key: key,
		}
		oldPair, _ := t.rows.Get(pivot)

		if log.pair != nil && log.oldPair != nil {
			// update
			if oldPair == nil {
				return nil, nil, moerr.NewTxnWWConflictNoCtx()
			}
			if oldPair.ID != log.oldPair.ID {
				return nil, nil, moerr.NewTxnWWConflictNoCtx()
			}
			pivot.ID = log.pair.ID
			pivot.Value = log.pair.Value
			t.setPair(pivot, oldPair)

		} else if log.pair == nil {
			// delete
			if oldPair == nil {
				return nil, nil, moerr.NewTxnWWConflictNoCtx()
			}
			if oldPair.ID != log.oldPair.ID {
				return nil, nil, moerr.NewTxnWWConflictNoCtx()
			}
			t.unsetPair(pivot, oldPair)

		} else if log.pair != nil && log.oldPair == nil {
			// insert
			if oldPair != nil {
				return nil, nil, moerr.NewTxnWWConflictNoCtx()
			}
			pivot.ID = log.pair.ID
			pivot.Value = log.pair.Value
			t.setPair(pivot, oldPair)
		}

	}

	return t, logs, nil
}

func (s *tableState[K, V]) dump(w io.Writer) {
	fmt.Fprintf(w, "table state, id %v\n", s.id)
	{
		iter := s.rows.Copy().Iter()
		for ok := iter.First(); ok; ok = iter.Next() {
			row, err := iter.Read()
			if err != nil {
				panic(err)
			}
			fmt.Fprintf(w, "\trow %+v\n", row)
		}
	}
	{
		iter := s.log.Copy().Iter()
		for ok := iter.First(); ok; ok = iter.Next() {
			log, err := iter.Read()
			if err != nil {
				panic(err)
			}
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
			s.index.Delete(&IndexEntry[K, V]{
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
			s.index.Set(&IndexEntry[K, V]{
				Index: index,
				Key:   pair.Key,
			})
		}

		// add log
		log := &logEntry[K, V]{
			key:     pair.Key,
			serial:  atomic.AddInt64(&nextLogSerial, 1),
			pair:    pair,
			oldPair: oldPair,
		}
		s.log.Set(log)

	}

}

// unsetPair unsets a key-value pair
func (s *tableState[K, V]) unsetPair(pivot *KVPair[K, V], oldPair *KVPair[K, V]) {

	if oldPair != nil {
		// remove indexes
		for _, index := range oldPair.Indexes {
			s.index.Delete(&IndexEntry[K, V]{
				Index: index,
				Key:   oldPair.Key,
			})
		}
	}

	// delete row
	s.rows.Delete(pivot)

	// add log
	s.log.Set(&logEntry[K, V]{
		key:     pivot.Key,
		serial:  atomic.AddInt64(&nextLogSerial, 1),
		oldPair: oldPair,
	})

}
