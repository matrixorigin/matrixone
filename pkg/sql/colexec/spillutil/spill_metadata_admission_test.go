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

package spillutil

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/spillcleanup"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type metadataCreateFS struct {
	detachedWriterRemoveFS
	cancel  context.CancelFunc
	creates int
	fail    bool
}

func (fs *metadataCreateFS) CreateFile(ctx context.Context, name string) (*os.File, error) {
	fs.creates++
	if fs.fail {
		return nil, os.ErrPermission
	}
	fd, err := fs.MutableFileService.CreateFile(ctx, name)
	if err == nil && fs.cancel != nil {
		fs.cancel()
	}
	return fd, err
}

func TestJoinSpillMetadataAdmissionAndHandoff(t *testing.T) {
	baseline := spillcleanup.Used()
	var held []*spillcleanup.Reservation
	t.Cleanup(func() {
		for _, token := range held {
			token.Release()
		}
	})
	for spillcleanup.Used() < spillcleanup.Capacity-1 {
		token, err := spillcleanup.Reserve()
		require.NoError(t, err)
		held = append(held, token)
	}
	proc := testutil.NewProcess(t)
	t.Cleanup(proc.Free)
	ledger := process.MustNewExecutionResourceBudget(64<<20, 64<<20)
	t.Cleanup(ledger.Close)
	require.NoError(t, ledger.SetSpillCaps(1, 1))
	baseFS, err := proc.GetSpillFileService()
	require.NoError(t, err)
	fault := &metadataCreateFS{detachedWriterRemoveFS: detachedWriterRemoveFS{MutableFileService: baseFS}}
	fault.blocked.Store(true)
	t.Cleanup(func() {
		fault.blocked.Store(false)
		require.Eventually(t, func() bool { return spillcleanup.Used() == baseline+int64(len(held)) }, 5*time.Second, time.Millisecond)
	})
	originalCtx := proc.Ctx
	for i, mode := range []string{"cancel", "handoff"} {
		generation, err := ledger.OpenGeneration(uint64(i + 1))
		require.NoError(t, err)
		cache := &spillFileServiceCache{}
		cache.once.Do(func() { cache.fs = fault })
		writer := BucketWriter{Name: "metadata-join-" + mode, Budget: generation, spillFS: cache}
		fault.blocked.Store(true)
		fault.cancel = nil
		if mode == "cancel" {
			proc.Ctx, fault.cancel = context.WithCancel(originalCtx)
			defer fault.cancel()
		} else {
			proc.Ctx = originalCtx
		}
		err = writeBucketPayload(proc, []byte{1}, 1, &writer, nil)
		if mode == "cancel" {
			require.ErrorIs(t, err, context.Canceled)
		} else {
			require.NoError(t, err)
		}
		fd, err := baseFS.OpenFile(originalCtx, writer.Name)
		require.NoError(t, err)
		info, err := fd.Stat()
		require.NoError(t, err)
		require.NoError(t, fd.Close())
		if mode == "cancel" {
			require.Zero(t, info.Size())
		} else {
			require.EqualValues(t, 1, info.Size())
		}
		proc.Ctx = originalCtx
		if mode == "handoff" {
			file, err := writer.handOffSpillFile(originalCtx)
			require.NoError(t, err)
			require.NoError(t, writer.Close()) // moved token must not be returned here
			require.Error(t, file.Close())
			file.DeferCleanup()
			file.DeferCleanup()
		} else {
			require.Error(t, writer.Close())
			engine := &SpillEngine{orphanWriters: []BucketWriter{writer}}
			engine.DeferCleanup()
		}
		generation.Close()
		require.EqualValues(t, spillcleanup.Capacity, spillcleanup.Used())
		require.Zero(t, generation.SpillFDUsed())
		// Use a fresh ledger so a disk-byte rejection cannot mask metadata admission.
		other := process.MustNewExecutionResourceBudget(64<<20, 64<<20)
		next, err := other.OpenGeneration(1)
		require.NoError(t, err)
		denied := BucketWriter{Name: "must-not-create", Budget: next, spillFS: cache}
		creates := fault.creates
		require.ErrorIs(t, writeBucketPayload(proc, []byte{1}, 1, &denied, nil), spillcleanup.ErrCapacity)
		require.Equal(t, creates, fault.creates)
		require.Zero(t, next.SpillDiskUsed())
		require.Zero(t, next.SpillFDUsed())
		next.Close()
		other.Close()
		fault.blocked.Store(false)
		require.Eventually(t, func() bool { return spillcleanup.Used() == baseline+int64(len(held)) }, 5*time.Second, time.Millisecond)
		require.Zero(t, ledger.SpillDiskUsed())
		_, err = baseFS.OpenFile(originalCtx, writer.Name)
		require.Error(t, err)
	}
	// No file means no retained admission, even when physical creation fails.
	generation, err := ledger.OpenGeneration(3)
	require.NoError(t, err)
	defer generation.Close()
	cache := &spillFileServiceCache{}
	cache.once.Do(func() { cache.fs = fault })
	fault.fail = true
	writer := BucketWriter{Name: "create-error", Budget: generation, spillFS: cache}
	require.ErrorIs(t, writeBucketPayload(proc, []byte{1}, 1, &writer, nil), os.ErrPermission)
	require.EqualValues(t, baseline+int64(len(held)), spillcleanup.Used())
	require.Zero(t, generation.SpillDiskUsed())
	require.Zero(t, generation.SpillFDUsed())
}
