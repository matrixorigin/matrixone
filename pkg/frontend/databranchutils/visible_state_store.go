// Copyright 2026 Matrix Origin
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

package databranchutils

import (
	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

var errVisibleStateDrainFull = moerr.NewInternalErrorNoCtx("visible-state drain batch is full")

type branchHashmapVisibleStateStore struct {
	hashmap    *branchHashmap
	drainShard int
}

// NewVisibleStateStore creates the spillable raw key/value store used by
// disttae visible-state recovery. The supplied allocator is the same shared
// Data Branch allocator used by the downstream diff hashmaps.
func NewVisibleStateStore(allocator malloc.Allocator) (engine.VisibleStateStore, error) {
	store, err := NewBranchHashmap(
		WithBranchHashmapAllocator(allocator),
		WithBranchHashmapShardCount(minShardCount),
		withBranchHashmapStrictCapacity(),
		withBranchHashmapRawEncodedKeys(),
	)
	if err != nil {
		return nil, err
	}
	return &branchHashmapVisibleStateStore{hashmap: store.(*branchHashmap)}, nil
}

func (s *branchHashmapVisibleStateStore) PutBatch(entries []engine.VisibleStateEntry) error {
	if len(entries) == 0 {
		return nil
	}
	if s == nil || s.hashmap == nil {
		return moerr.NewInternalErrorNoCtx("visible-state store is closed")
	}
	prepared := make([]preparedEntry, len(entries))
	for i := range entries {
		if len(entries[i].Key) == 0 {
			return moerr.NewInternalErrorNoCtx("visible-state store requires a non-empty key")
		}
		prepared[i] = preparedEntry{key: entries[i].Key, value: entries[i].Value}
	}
	return s.hashmap.flushPreparedEntries(
		make([][]int, s.hashmap.shardCount),
		prepared,
	)
}

func (s *branchHashmapVisibleStateStore) Pop(key []byte) ([]byte, bool, error) {
	if s == nil || s.hashmap == nil {
		return nil, false, moerr.NewInternalErrorNoCtx("visible-state store is closed")
	}
	result, err := s.hashmap.PopByEncodedKey(key, true)
	if err != nil {
		return nil, false, err
	}
	if len(result.Rows) == 0 {
		return nil, false, nil
	}
	if len(result.Rows) != 1 {
		return nil, false, moerr.NewInternalErrorNoCtxf(
			"visible-state store found %d rows for one primary key", len(result.Rows),
		)
	}
	return result.Rows[0], true, nil
}

func (s *branchHashmapVisibleStateStore) Drain(
	maxEntries int,
	fn func(key, value []byte) error,
) (int, error) {
	if maxEntries <= 0 || fn == nil {
		return 0, nil
	}
	if s == nil || s.hashmap == nil {
		return 0, moerr.NewInternalErrorNoCtx("visible-state store is closed")
	}
	drained := 0
	for s.drainShard < len(s.hashmap.shards) {
		shard := s.hashmap.shards[s.drainShard]
		if shard == nil {
			s.drainShard++
			continue
		}
		shard.beginIteration()
		cursor := shardCursor{shard: shard}
		err := cursor.ForEach(func(key, _ []byte) error {
			if drained >= maxEntries {
				return errVisibleStateDrainFull
			}
			keyCopy := append([]byte(nil), key...)
			result, popErr := cursor.PopByEncodedKey(keyCopy, true)
			if popErr != nil {
				return popErr
			}
			if len(result.Rows) != 1 {
				return moerr.NewInternalErrorNoCtxf(
					"visible-state store found %d rows while draining one primary key",
					len(result.Rows),
				)
			}
			if callErr := fn(keyCopy, result.Rows[0]); callErr != nil {
				return callErr
			}
			drained++
			return nil
		})
		shard.endIteration()
		if err == errVisibleStateDrainFull {
			return drained, nil
		}
		if err != nil {
			return drained, err
		}
		s.drainShard++
	}
	return drained, nil
}

func (s *branchHashmapVisibleStateStore) Len() int64 {
	if s == nil || s.hashmap == nil {
		return 0
	}
	return s.hashmap.ItemCount()
}

func (s *branchHashmapVisibleStateStore) Close() error {
	if s == nil || s.hashmap == nil {
		return nil
	}
	err := s.hashmap.Close()
	s.hashmap = nil
	return err
}

var _ engine.VisibleStateStore = (*branchHashmapVisibleStateStore)(nil)
