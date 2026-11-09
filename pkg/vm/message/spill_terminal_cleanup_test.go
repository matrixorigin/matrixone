// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package message

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/stretchr/testify/require"
)

func TestSpillFileTerminalConcurrentClose(t *testing.T) {
	fd, err := os.CreateTemp(t.TempDir(), "owned")
	require.NoError(t, err)
	name := fd.Name()
	_, err = fd.Write([]byte{1})
	require.NoError(t, err)
	var removes, finishes atomic.Int32
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	releaseBarrier := func() { releaseOnce.Do(func() { close(release) }) }
	failure := errors.New("terminal unlink failure")
	file := NewReopenableSpillFile(func(context.Context) (*os.File, error) { return fd, nil }, func() error {
		removes.Add(1)
		close(entered)
		<-release
		return failure
	}, 1, 1, func(removed bool) {
		if removed {
			t.Error("failed unlink cannot refund disk")
		}
		finishes.Add(1)
	})
	_, err = file.Open(context.Background())
	require.NoError(t, err)
	var workers sync.WaitGroup
	results := make(chan error, 16)
	for range 16 {
		workers.Add(1)
		go func() { defer workers.Done(); results <- file.Close() }()
	}
	defer func() { releaseBarrier(); workers.Wait() }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("remove barrier not reached")
	}
	_, err = fd.Stat()
	require.ErrorIs(t, err, os.ErrClosed)
	require.Zero(t, finishes.Load(), "completion cannot run before unlink completes")
	require.FileExists(t, name)
	// Release before joining workers, even when an assertion above fails.
	releaseBarrier()
	workers.Wait()
	close(results)
	for err := range results {
		require.ErrorIs(t, err, failure)
	}
	require.EqualValues(t, 1, removes.Load())
	require.EqualValues(t, 1, finishes.Load())
	require.ErrorIs(t, file.Close(), failure)
	require.Nil(t, file.open)
	require.Nil(t, file.remove)
	require.Nil(t, file.finish)
}

func TestSpillPayloadTerminalCleanupDropsAllQueryReferences(t *testing.T) {
	for _, boundary := range []string{"payload", "free", "free-memory"} {
		t.Run(boundary, func(t *testing.T) {
			var removed, abandoned, attempts atomic.Int32
			failure := errors.New("cannot unlink failed file")
			failed := NewReopenableSpillFile(nil, func() error { attempts.Add(1); return failure }, 1, 1,
				func(gone bool) { require.False(t, gone); abandoned.Add(1) })
			gone := NewReopenableSpillFile(nil, func() error { return os.ErrNotExist }, 1, 1,
				func(gone bool) { require.True(t, gone); removed.Add(1) })
			payload := SpillBuildPayload{Files: []*SpillFile{failed, nil, gone}, BudgetRef: new([1024]byte)}
			if boundary == "payload" {
				require.ErrorIs(t, payload.Close(), failure)
				require.NoError(t, payload.Close())
			} else {
				mp := mpool.MustNewZero()
				defer mpool.DeleteMPool(mp)
				jm := NewJoinMap(GroupSels{}, nil, nil, nil, nil, mp)
				jm.IncRef(1)
				require.NoError(t, jm.SetSpillBuildPayload(payload))
				payload = SpillBuildPayload{}
				if boundary == "free" {
					jm.Free()
				} else {
					jm.FreeMemory()
					jm.FreeMemory()
				}
				require.Empty(t, jm.spillPayload.Files)
				require.Nil(t, jm.spillPayload.BudgetRef)
			}
			require.Empty(t, payload.Files)
			require.Nil(t, payload.BudgetRef)
			require.EqualValues(t, 1, removed.Load())
			require.EqualValues(t, 1, abandoned.Load())
			require.EqualValues(t, 1, attempts.Load(), "terminal close must not create a hidden retry protocol")
			require.Nil(t, failed.remove)
			require.Nil(t, failed.finish)
		})
	}
}
