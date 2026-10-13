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
	"encoding/binary"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestVisibleStateStoreSpillsAndDrainsHighCardinality(t *testing.T) {
	allocator := newLimitedAllocator(256)
	store, err := NewVisibleStateStore(allocator)
	require.NoError(t, err)

	const count = 5000
	entries := make([]engine.VisibleStateEntry, count)
	for i := range entries {
		key := make([]byte, 8)
		binary.LittleEndian.PutUint64(key, uint64(i+1))
		value := make([]byte, 48)
		binary.LittleEndian.PutUint64(value, uint64(i+100))
		entries[i] = engine.VisibleStateEntry{Key: key, Value: value}
	}
	require.NoError(t, store.PutBatch(entries))
	require.Equal(t, int64(count), store.Len())

	value, ok, err := store.Pop(entries[123].Key)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(223), binary.LittleEndian.Uint64(value))

	drained := 0
	for store.Len() > 0 {
		n, err := store.Drain(137, func(key, value []byte) error {
			require.Len(t, key, 8)
			require.Len(t, value, 48)
			drained++
			return nil
		})
		require.NoError(t, err)
		require.Positive(t, n)
	}
	require.Equal(t, count-1, drained)
	require.NoError(t, store.Close())
	require.Zero(t, limitedAllocatorUsed(allocator))
}

func TestVisibleStateStoreRejectsUnspillableEntryAtCapacity(t *testing.T) {
	allocator := newLimitedAllocator(32)
	store, err := NewVisibleStateStore(allocator)
	require.NoError(t, err)

	err = store.PutBatch([]engine.VisibleStateEntry{{
		Key: []byte("key"), Value: make([]byte, 128),
	}})
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrMPoolCapacity))
	require.Zero(t, store.Len())
	require.NoError(t, store.Close())
	require.Zero(t, limitedAllocatorUsed(allocator))
}

func TestVisibleStateStoreBoundaryErrors(t *testing.T) {
	allocator := newLimitedAllocator(256)
	store, err := NewVisibleStateStore(allocator)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, store.Close())
		require.Zero(t, limitedAllocatorUsed(allocator))
	})

	require.NoError(t, store.PutBatch(nil))
	value, ok, err := store.Pop([]byte("missing"))
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, value)

	err = store.PutBatch([]engine.VisibleStateEntry{{Value: []byte("value")}})
	require.ErrorContains(t, err, "non-empty key")

	require.NoError(t, store.PutBatch([]engine.VisibleStateEntry{{
		Key: []byte("key"), Value: []byte("value"),
	}}))
	callbackErr := moerr.NewInternalErrorNoCtx("drain callback failed")
	drained, err := store.Drain(1, func(_, _ []byte) error {
		return callbackErr
	})
	require.Zero(t, drained)
	require.Same(t, callbackErr, err)

	drained, err = store.Drain(0, func(_, _ []byte) error {
		require.FailNow(t, "zero-sized drain invoked callback")
		return nil
	})
	require.NoError(t, err)
	require.Zero(t, drained)

	require.NoError(t, store.Close())
	require.Zero(t, limitedAllocatorUsed(allocator))
	require.Zero(t, store.Len())
	require.NoError(t, store.Close())

	err = store.PutBatch([]engine.VisibleStateEntry{{Key: []byte("closed")}})
	require.ErrorContains(t, err, "store is closed")
	_, _, err = store.Pop([]byte("closed"))
	require.ErrorContains(t, err, "store is closed")
	_, err = store.Drain(1, func(_, _ []byte) error { return nil })
	require.ErrorContains(t, err, "store is closed")
}

func limitedAllocatorUsed(allocator *limitedAllocator) uint64 {
	allocator.mu.Lock()
	defer allocator.mu.Unlock()
	return allocator.used
}
