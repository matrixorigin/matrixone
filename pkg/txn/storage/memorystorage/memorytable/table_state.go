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
	tree Tree[K, V]
	log  Log[K, V]
}

func (t *tableState[K, V]) clone() *tableState[K, V] {
	ret := &tableState[K, V]{
		tree: t.tree.Copy(),
		// log is not copied
		log: NewSliceLog[K, V](),
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

	t = t.clone()
	var logs []*logEntry[K, V]

	iter := from.log.Iter()
	defer iter.Close()

	for ok := iter.First(); ok; ok = iter.Next() {

		log, err := iter.Read()
		if err != nil {
			return nil, nil, err
		}
		logs = append(logs, log)
		key := log.Key

		pivot := TreeNode[K, V]{
			KVPair: &KVPair[K, V]{
				Key: key,
			},
		}
		oldNode, _ := t.tree.Get(pivot)
		oldPair := oldNode.KVPair

		if log.Pair != nil && log.OldPair != nil {
			// update
			if oldPair == nil {
				return nil, nil, moerr.NewTxnWWConflictNoCtx(0, "")
			}
			if oldPair.ID != log.OldPair.ID {
				return nil, nil, moerr.NewTxnWWConflictNoCtx(0, "")
			}
			t.setPair(log.Pair, oldNode.KVPair)

		} else if log.Pair == nil {
			// delete
			if oldPair == nil {
				return nil, nil, moerr.NewTxnWWConflictNoCtx(0, "")
			}
			if oldPair.ID != log.OldPair.ID {
				return nil, nil, moerr.NewTxnWWConflictNoCtx(0, "")
			}
			t.unsetPair(*pivot.KVPair, oldNode.KVPair)

		} else if log.Pair != nil && log.OldPair == nil {
			// insert
			if oldPair != nil {
				return nil, nil, moerr.NewTxnWWConflictNoCtx(0, "")
			}
			t.setPair(log.Pair, oldNode.KVPair)
		}

	}

	return t, logs, nil
}

func (s *tableState[K, V]) dump(w io.Writer) {
	{
		iter := s.tree.Copy().Iter()
		for ok := iter.First(); ok; ok = iter.Next() {
			node, err := iter.Read()
			if err != nil {
				panic(err)
			}
			if node.KVPair != nil {
				fmt.Fprintf(w, "\trow %+v\n", node.KVPair)
			} else if node.IndexEntry != nil {
				fmt.Fprintf(w, "\tindex %+v\n", node.IndexEntry)
			}
		}
	}
	{
		iter := s.log.Iter()
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
			s.tree.Delete(TreeNode[K, V]{
				IndexEntry: &IndexEntry[K, V]{
					Index: index,
					Key:   oldPair.Key,
				},
			})
		}
	}

	if pair != nil {
		// set row
		s.tree.Set(TreeNode[K, V]{
			KVPair: pair,
		})

		// add indexes
		for _, index := range pair.Indexes {
			entry := &IndexEntry[K, V]{
				Index: index,
				Key:   pair.Key,
			}
			s.tree.Set(TreeNode[K, V]{
				IndexEntry: entry,
			})
		}

		// add log
		log := &logEntry[K, V]{
			Key:     pair.Key,
			Serial:  atomic.AddInt64(&nextLogSerial, 1),
			Pair:    pair,
			OldPair: oldPair,
		}
		s.log.Set(log)

	}

}

// unsetPair unsets a key-value pair
func (s *tableState[K, V]) unsetPair(pivot KVPair[K, V], oldPair *KVPair[K, V]) {

	if oldPair != nil {
		// remove indexes
		for _, index := range oldPair.Indexes {
			s.tree.Delete(TreeNode[K, V]{
				IndexEntry: &IndexEntry[K, V]{
					Index: index,
					Key:   oldPair.Key,
				},
			})
		}
	}

	// delete row
	s.tree.Delete(TreeNode[K, V]{
		KVPair: &pivot,
	})

	// add log
	s.log.Set(&logEntry[K, V]{
		Key:     pivot.Key,
		Serial:  atomic.AddInt64(&nextLogSerial, 1),
		OldPair: oldPair,
	})

}
