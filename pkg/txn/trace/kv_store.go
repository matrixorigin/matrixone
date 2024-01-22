// Copyright 2020 MatrixOrigin.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package trace

import (
	"bytes"

	"github.com/cockroachdb/pebble"
)

var (
	// forcedSyncKey we use this fixed key to write a dummy record into the KVStore with
	// sync=true to force a sync of the WAL of the KVStore.
	forcedSyncKey = []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
)

type storage struct {
	db *pebble.DB
}

// NewStorage returns a pebble backed kv store.
func NewStorage(
	dir string,
	opts *pebble.Options) (*storage, error) {
	db, err := pebble.Open(dir, opts)
	if err != nil {
		return nil, err
	}

	return &storage{
		db: db,
	}, nil
}

func (s *storage) Close() error {
	return s.db.Close()
}

func (s *storage) Write(
	wb WriteBatch,
	sync bool) error {
	return s.db.Apply(wb.Batch(), toWriteOptions(sync))
}

func (s *storage) Set(
	key, value []byte,
	sync bool) error {
	return s.db.Set(key, value, toWriteOptions(sync))
}

func (s *storage) Get(key []byte) ([]byte, error) {
	value, closer, err := s.db.Get(key)
	if err == pebble.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	if len(value) == 0 {
		return nil, nil
	}
	v := make([]byte, len(value))
	copy(v, value)
	return v, nil
}

func (s *storage) Delete(
	key []byte,
	sync bool) error {
	return s.db.Delete(key, toWriteOptions(sync))
}

func (s *storage) RangeDelete(
	start, end []byte,
	sync bool) error {
	if len(start) == 0 {
		iter := s.db.NewIter(&pebble.IterOptions{})
		defer iter.Close()

		if iter.First() && iter.Valid() {
			if err := iter.Error(); err != nil {
				return err
			}
			fk := iter.Key()
			startKey := make([]byte, len(fk))
			copy(startKey, fk)
			start = startKey
		}
	}

	if len(end) == 0 {
		iter := s.db.NewIter(&pebble.IterOptions{})
		defer iter.Close()

		if iter.Last() && iter.Valid() {
			if err := iter.Error(); err != nil {
				return err
			}
			lk := iter.Key()
			endKey := make([]byte, len(lk)+1)
			copy(endKey, lk)
			endKey[len(lk)] = 0x1
			end = endKey
		}
	}

	// empty db
	if len(start) == 0 && len(end) == 0 {
		return nil
	}

	return s.db.DeleteRange(start, end, toWriteOptions(sync))
}

func (s *storage) Scan(
	start, end []byte,
	handleFunc func(key, value []byte) (bool, error)) error {
	ios := &pebble.IterOptions{}
	if len(start) > 0 {
		ios.LowerBound = start
	}
	if len(end) > 0 {
		ios.UpperBound = end
	}
	iter := s.db.NewIter(ios)
	defer iter.Close()

	iter.First()
	for iter.Valid() {
		err := iter.Error()
		if err != nil {
			return err
		}

		ok, err := handleFunc(iter.Key(), iter.Value())
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		iter.Next()
	}

	return nil
}

func (s *storage) PrefixScan(prefix []byte, handler func(key, value []byte) (bool, error)) error {
	iter := s.db.NewIter(&pebble.IterOptions{LowerBound: prefix})
	defer iter.Close()
	iter.First()
	for iter.Valid() {
		if err := iter.Error(); err != nil {
			return err
		}
		if ok := bytes.HasPrefix(iter.Key(), prefix); !ok {
			break
		}
		ok, err := handler(iter.Key(), iter.Value())
		if err != nil {
			return err
		}

		if !ok {
			break
		}
		iter.Next()
	}
	return nil
}

func (s *storage) Seek(lowerBound []byte) ([]byte, []byte, error) {
	return s.SeekAndLT(lowerBound, nil)
}

func (s *storage) SeekAndLT(lowerBound, upperBound []byte) ([]byte, []byte, error) {
	var key, value []byte
	view := s.db.NewSnapshot()
	defer view.Close()

	iter := view.NewIter(&pebble.IterOptions{LowerBound: lowerBound, UpperBound: upperBound})
	defer iter.Close()

	if iter.First() {
		if err := iter.Error(); err != nil {
			return nil, nil, err
		}
		key = clone(iter.Key())
		value = clone(iter.Value())
	}
	return key, value, nil
}

func (s *storage) SeekLT(upperBound []byte) ([]byte, []byte, error) {
	return s.SeekLTAndGE(upperBound, nil)
}

func (s *storage) SeekLTAndGE(upperBound, lowerBound []byte) ([]byte, []byte, error) {
	var key, value []byte
	view := s.db.NewSnapshot()
	defer view.Close()

	iter := view.NewIter(&pebble.IterOptions{UpperBound: upperBound, LowerBound: lowerBound})
	defer iter.Close()
	iter.SeekLT(upperBound)

	if iter.Last() {
		if err := iter.Error(); err != nil {
			return nil, nil, err
		}
		key = clone(iter.Key())
		value = clone(iter.Value())
	}
	return key, value, nil
}

// Sync persist data to disk
func (s *storage) Sync() error {
	wb := s.db.NewBatch()
	defer wb.Close()
	if err := wb.Set(forcedSyncKey, forcedSyncKey, nil); err != nil {
		return err
	}
	return s.db.Apply(wb, pebble.Sync)
}

// NewWriteBatch create and returns write batch
func (s *storage) NewWriteBatch() WriteBatch {
	return newWriteBatch(s.db.NewBatch())
}

func toWriteOptions(sync bool) *pebble.WriteOptions {
	if sync {
		return pebble.Sync
	}
	return pebble.NoSync
}

func newWriteBatch(batch *pebble.Batch) WriteBatch {
	return &writeBatch{batch: batch}
}

type writeBatch struct {
	batch *pebble.Batch
}

func (wb *writeBatch) Batch() *pebble.Batch {
	return wb.batch
}

func (wb *writeBatch) Delete(key []byte) {
	if err := wb.batch.Delete(key, nil); err != nil {
		panic(err)
	}
}

func (wb *writeBatch) DeleteRange(fk []byte, lk []byte) {
	if err := wb.batch.DeleteRange(fk, lk, nil); err != nil {
		panic(err)
	}
}

func (wb *writeBatch) Set(key []byte, value []byte) {
	if err := wb.batch.Set(key, value, nil); err != nil {
		panic(err)
	}
}

func (wb *writeBatch) Reset() {
	wb.batch.Reset()
}

func (wb *writeBatch) Close() {
	wb.batch.Close()
}

type WriteBatch interface {
	// Set set kv-value to the batch
	Set([]byte, []byte)
	// Delete add delete key to the batch
	Delete([]byte)
	// DeleteRange deletes the keys specified in the range
	DeleteRange([]byte, []byte)
	// Batch returns the underlying batch
	Batch() *pebble.Batch
	// Reset reset the batch
	Reset()
	// Close close the batch
	Close()
}

func clone(src []byte) []byte {
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}
