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
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestSpillEngineDeferredCleanupOutlivesEngine(t *testing.T) {
	for _, owner := range []string{"reader", "queue"} {
		t.Run(owner, func(t *testing.T) {
			fd, err := os.CreateTemp(t.TempDir(), "terminal-engine-*")
			require.NoError(t, err)
			name := fd.Name()
			require.NoError(t, fd.Close())
			var blocked atomic.Bool
			blocked.Store(true)
			var released atomic.Int32
			file := message.NewReopenableSpillFile(
				func(context.Context) (*os.File, error) { return os.Open(name) },
				func() error {
					if blocked.Load() {
						return os.ErrPermission
					}
					return os.Remove(name)
				}, 1, 1, func() { released.Add(1) },
			)
			require.Error(t, file.Close())
			engine := &SpillEngine{}
			if owner == "reader" {
				engine.probeReader.spillFile = file
			} else {
				engine.orphanFiles = []*message.SpillFile{file}
			}
			engine.DeferCleanup()
			require.Nil(t, engine.probeReader.spillFile)
			require.Empty(t, engine.orphanFiles)
			engine = nil
			require.FileExists(t, name)
			require.Zero(t, released.Load())
			blocked.Store(false)
			require.Eventually(t, func() bool { return released.Load() == 1 }, 3*time.Second, 10*time.Millisecond)
			require.NoFileExists(t, name)
		})
	}
}

func TestBucketReaderCloseRetainsHandedOffFileForRetry(t *testing.T) {
	for _, block := range []bool{false, true} {
		proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
		budget := process.MustNewExecutionResourceBudget(1<<20, 1<<20)
		generation, err := budget.OpenGeneration(1)
		require.NoError(t, err)
		writer := BucketWriter{Name: "reader-cleanup-probe", Budget: generation}
		require.NoError(t, writeBucketPayload(proc, []byte("real-spill-data"), 1, &writer, nil))
		baseFS, err := proc.GetSpillFileService()
		require.NoError(t, err)
		fault := &blockedRemoveFS{MutableFileService: baseFS}
		writer.fileFS = fault
		file, err := writer.handOffSpillFile(proc.Ctx)
		require.NoError(t, err)
		defer func() {
			fault.blocked = false
			_ = file.Close()
			generation.Close()
			budget.Close()
			proc.Free()
		}()
		reader := BucketReader{}
		require.NoError(t, reader.ResetForSpillFile(proc, generation, file))
		fault.blocked = block
		reader.Close()
		if block {
			require.Positive(t, generation.SpillDiskUsed())
			physical, openErr := baseFS.OpenFile(context.Background(), writer.Name)
			require.NoError(t, openErr)
			require.NoError(t, physical.Close())
			fault.blocked = false
			reader.Close()
		}
		require.Zero(t, generation.SpillDiskUsed())
		_, err = baseFS.OpenFile(context.Background(), writer.Name)
		require.Error(t, err)
	}
}

func TestBucketReaderRejectedFileRetainsFailedRemoval(t *testing.T) {
	fd, err := os.CreateTemp(t.TempDir(), "rejected-spill-*")
	require.NoError(t, err)
	_, err = fd.Write([]byte{1})
	require.NoError(t, err)
	require.NoError(t, fd.Close())
	name := fd.Name()
	var blocked atomic.Bool
	blocked.Store(true)
	var releases atomic.Int32
	file := message.NewReopenableSpillFile(
		func(context.Context) (*os.File, error) { return os.Open(name) },
		func() error {
			if blocked.Load() {
				return os.ErrPermission
			}
			return os.Remove(name)
		},
		0, 1, func() { releases.Add(1) },
	)
	reader := BucketReader{}
	err = reader.ResetForSpillFile(nil, nil, file)
	require.ErrorContains(t, err, "corrupted spill file row count")
	require.ErrorIs(t, err, os.ErrPermission)
	require.FileExists(t, name)
	require.Zero(t, releases.Load())
	blocked.Store(false)
	require.NoError(t, reader.Close())
	require.NoFileExists(t, name)
	require.Equal(t, int32(1), releases.Load())
}

func TestFinishBucketReportsReaderRemovalFailure(t *testing.T) {
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	budget := process.MustNewExecutionResourceBudget(1<<20, 1<<20)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	defer budget.Close()
	defer generation.Close()
	writer := BucketWriter{Name: "finish-bucket-cleanup", Budget: generation}
	require.NoError(t, writeBucketPayload(proc, []byte("real-spill-data"), 1, &writer, nil))
	baseFS, err := proc.GetSpillFileService()
	require.NoError(t, err)
	fault := &blockedRemoveFS{MutableFileService: baseFS}
	writer.fileFS = fault
	file, err := writer.handOffSpillFile(proc.Ctx)
	require.NoError(t, err)
	engine := &SpillEngine{}
	require.NoError(t, engine.probeReader.ResetForSpillFile(proc, generation, file))
	fault.blocked = true
	require.ErrorContains(t, engine.FinishBucket(), "injected persistent remove error")
	require.Positive(t, generation.SpillDiskUsed())
	fault.blocked = false
	require.NoError(t, engine.FinishBucket())
	require.Zero(t, generation.SpillDiskUsed())
	_, err = baseFS.OpenFile(context.Background(), writer.Name)
	require.Error(t, err)
}
