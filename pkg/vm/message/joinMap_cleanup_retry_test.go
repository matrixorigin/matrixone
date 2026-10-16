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
	"github.com/matrixorigin/matrixone/pkg/common/spillcleanup"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/stretchr/testify/require"
)

func TestJoinMapTerminalFreeRetriesAfterFilesystemRecovery(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	fd, err := os.CreateTemp(t.TempDir(), "terminal-spill-*")
	require.NoError(t, err)
	name := fd.Name()
	require.NoError(t, fd.Close())
	var blocked atomic.Bool
	blocked.Store(true)
	var released atomic.Int32
	metadata, reserveErr := spillcleanup.Reserve()
	if reserveErr != nil {
		panic(reserveErr)
	}
	file := NewReopenableSpillFile(
		func(context.Context) (*os.File, error) { return os.Open(name) },
		func() error {
			if blocked.Load() {
				return errors.New("injected remove error")
			}
			return os.Remove(name)
		}, 1, 1, func() { released.Add(1) }, metadata,
	)
	jm := NewJoinMap(GroupSels{}, nil, nil, nil, nil, mp)
	jm.IncRef(1)
	require.NoError(t, jm.SetSpillBuildPayload(SpillBuildPayload{Files: []*SpillFile{file}, BudgetRef: struct{}{}}))
	jm.Free()
	jm = nil
	require.FileExists(t, name)
	require.Zero(t, released.Load())
	blocked.Store(false)
	require.Eventually(t, func() bool { return released.Load() == 1 }, 3*time.Second, 10*time.Millisecond)
	require.NoFileExists(t, name)
}

func newRetryCleanupSpillFile(t *testing.T, failures int32, releases *atomic.Int32) (*SpillFile, string, *atomic.Int32) {
	t.Helper()
	fd, err := os.CreateTemp(t.TempDir(), "join-spill-cleanup-*")
	require.NoError(t, err)
	name := fd.Name()
	_, err = fd.Write([]byte{1})
	require.NoError(t, err)
	require.NoError(t, fd.Close())
	calls := new(atomic.Int32)
	metadata, reserveErr := spillcleanup.Reserve()
	if reserveErr != nil {
		panic(reserveErr)
	}
	file := NewReopenableSpillFile(
		func(context.Context) (*os.File, error) { return os.Open(name) },
		func() error {
			if calls.Add(1) <= failures {
				return errors.New("injected remove failure")
			}
			return os.Remove(name)
		},
		1, 1, func() { releases.Add(1) }, metadata,
	)
	return file, name, calls
}

func TestSpillFileConcurrentCloseRetriesOnlyPendingRemoval(t *testing.T) {
	var releases atomic.Int32
	file, name, calls := newRetryCleanupSpillFile(t, 1, &releases)
	var wg sync.WaitGroup
	var failures atomic.Int32
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := file.Close(); err != nil {
				failures.Add(1)
			}
		}()
	}
	wg.Wait()
	require.Equal(t, int32(1), failures.Load())
	require.Equal(t, int32(2), calls.Load())
	require.Equal(t, int32(1), releases.Load())
	require.NoFileExists(t, name)
}

func TestSpillFileCloseRetriesRemovalBeforeReleasingDisk(t *testing.T) {
	var releases atomic.Int32
	file, name, calls := newRetryCleanupSpillFile(t, 1, &releases)
	require.ErrorContains(t, file.Close(), "injected remove failure")
	require.FileExists(t, name)
	require.Zero(t, releases.Load(), "disk reservation must outlive the physical file")
	require.NoError(t, file.Close())
	require.NoFileExists(t, name)
	require.Equal(t, int32(1), releases.Load())
	require.NoError(t, file.Close())
	require.Equal(t, int32(2), calls.Load())
	require.Equal(t, int32(1), releases.Load())
}

func TestSpillFileCloseReleasesDiskWhenFileAlreadyGone(t *testing.T) {
	var released atomic.Int32
	metadata, reserveErr := spillcleanup.Reserve()
	if reserveErr != nil {
		panic(reserveErr)
	}
	file := NewReopenableSpillFile(nil, func() error { return os.ErrNotExist }, 1, 1, func() { released.Add(1) }, metadata)
	require.NoError(t, file.Close())
	require.NoError(t, file.Close())
	require.Equal(t, int32(1), released.Load())
}

func TestSpillBuildPayloadRetainsOnlyFilesNeedingCleanup(t *testing.T) {
	var releases atomic.Int32
	failed, failedName, failedCalls := newRetryCleanupSpillFile(t, 1, &releases)
	healthy, healthyName, _ := newRetryCleanupSpillFile(t, 0, &releases)
	budgetRef := &struct{}{}
	payload := SpillBuildPayload{Files: []*SpillFile{failed, healthy}, BudgetRef: budgetRef}
	require.ErrorContains(t, payload.Close(), "injected remove failure")
	require.FileExists(t, failedName)
	require.NoFileExists(t, healthyName)
	require.Equal(t, int32(1), releases.Load())
	require.Len(t, payload.Files, 1)
	require.Same(t, failed, payload.Files[0])
	require.Same(t, budgetRef, payload.BudgetRef)
	require.NoError(t, payload.Close())
	require.Empty(t, payload.Files)
	require.Nil(t, payload.BudgetRef)
	require.NoFileExists(t, failedName)
	require.Equal(t, int32(2), failedCalls.Load())
	require.Equal(t, int32(2), releases.Load())
}

func TestJoinMapFreeMemoryRetainsFailedSpillCleanup(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	var releases atomic.Int32
	file, name, calls := newRetryCleanupSpillFile(t, 1, &releases)
	jm := NewJoinMap(GroupSels{}, nil, nil, nil, nil, mp)
	jm.IncRef(1)
	require.NoError(t, jm.SetSpillBuildPayload(SpillBuildPayload{
		Files: []*SpillFile{file}, BudgetRef: struct{}{},
	}))
	jm.FreeMemory()
	require.Empty(t, jm.spillPayload.Files, "terminal cleanup must have an owner independent of the map")
	jm.FreeMemory()
	require.Eventually(t, func() bool { return releases.Load() == 1 }, 3*time.Second, 10*time.Millisecond)
	require.NoFileExists(t, name)
	require.Equal(t, int32(2), calls.Load())
	require.Equal(t, int32(1), releases.Load())
}
