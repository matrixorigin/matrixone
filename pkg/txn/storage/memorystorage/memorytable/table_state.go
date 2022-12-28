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
	"bytes"
	"encoding"
	"encoding/gob"
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
	kv    KV[K, V]
	log   Log[K, V]
	index Index[K, V]
}

func (t *tableState[K, V]) clone() *tableState[K, V] {
	ret := &tableState[K, V]{
		kv:    t.kv.Copy(),
		index: t.index.Copy(),
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

		pivot := KVPair[K, V]{
			Key: key,
		}
		oldPair, _ := t.kv.Get(pivot)

		if log.Pair.Valid() && log.OldPair.Valid() {
			// update
			if !oldPair.Valid() {
				return nil, nil, moerr.NewTxnWWConflictNoCtx()
			}
			if oldPair.ID != log.OldPair.ID {
				return nil, nil, moerr.NewTxnWWConflictNoCtx()
			}
			t.setPair(log.Pair, oldPair)

		} else if !log.Pair.Valid() {
			// delete
			if !oldPair.Valid() {
				return nil, nil, moerr.NewTxnWWConflictNoCtx()
			}
			if oldPair.ID != log.OldPair.ID {
				return nil, nil, moerr.NewTxnWWConflictNoCtx()
			}
			t.unsetPair(pivot, oldPair)

		} else if log.Pair.Valid() && !log.OldPair.Valid() {
			// insert
			if oldPair.Valid() {
				return nil, nil, moerr.NewTxnWWConflictNoCtx()
			}
			t.setPair(log.Pair, oldPair)
		}

	}

	return t, logs, nil
}

func (s *tableState[K, V]) dump(w io.Writer) {
	{
		iter := s.kv.Copy().Iter()
		for ok := iter.First(); ok; ok = iter.Next() {
			row, err := iter.Read()
			if err != nil {
				panic(err)
			}
			fmt.Fprintf(w, "\trow %+v\n", row)
		}
	}
	{
		iter := s.index.Copy().Iter()
		for ok := iter.First(); ok; ok = iter.Next() {
			entry, err := iter.Read()
			if err != nil {
				panic(err)
			}
			fmt.Fprintf(w, "\tindex %+v\n", entry)
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
func (s *tableState[K, V]) setPair(pair KVPair[K, V], oldPair KVPair[K, V]) {

	if oldPair.Valid() {
		// remove indexes
		for _, index := range oldPair.Indexes {
			s.index.Delete(&IndexEntry[K, V]{
				Index: index,
				Key:   oldPair.Key,
			})
		}
	}

	if pair.Valid() {
		// set row
		s.kv.Set(pair)

		// add indexes
		for _, index := range pair.Indexes {
			entry := &IndexEntry[K, V]{
				Index: index,
				Key:   pair.Key,
			}
			s.index.Set(entry)
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
func (s *tableState[K, V]) unsetPair(pivot KVPair[K, V], oldPair KVPair[K, V]) {

	if oldPair.Valid() {
		// remove indexes
		for _, index := range oldPair.Indexes {
			s.index.Delete(&IndexEntry[K, V]{
				Index: index,
				Key:   oldPair.Key,
			})
		}
	}

	// delete row
	s.kv.Delete(pivot)

	// add log
	s.log.Set(&logEntry[K, V]{
		Key:     pivot.Key,
		Serial:  atomic.AddInt64(&nextLogSerial, 1),
		OldPair: oldPair,
	})

}

type encodingTableState[
	K Ordered[K],
	V any,
] struct {
	KV    KV[K, V]
	Log   Log[K, V]
	Index Index[K, V]
}

var _ encoding.BinaryMarshaler = new(tableState[Int, int])

func (t *tableState[K, V]) MarshalBinary() ([]byte, error) {
	gobRegister(t.kv)
	gobRegister(t.log)
	gobRegister(t.index)
	buf := new(bytes.Buffer)
	if err := gob.NewEncoder(buf).Encode(encodingTableState[K, V]{
		KV:    t.kv,
		Log:   t.log,
		Index: t.index,
	}); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

var _ encoding.BinaryUnmarshaler = new(tableState[Int, int])

func (t *tableState[K, V]) UnmarshalBinary(data []byte) error {
	var e encodingTableState[K, V]
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&e); err != nil {
		return err
	}
	t.kv = e.KV
	t.log = e.Log
	t.index = e.Index
	return nil
}
