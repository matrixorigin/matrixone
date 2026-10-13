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
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type transientRemoveFS struct {
	fileservice.MutableFileService
	removeCalls int
}

type blockedRemoveFS struct {
	fileservice.MutableFileService
	blocked bool
}

func (fs *blockedRemoveFS) RemoveFile(ctx context.Context, name string) error {
	if fs.blocked {
		return errors.New("injected persistent remove error")
	}
	return fs.MutableFileService.RemoveFile(ctx, name)
}

func (fs *transientRemoveFS) RemoveFile(ctx context.Context, name string) error {
	fs.removeCalls++
	if fs.removeCalls == 1 {
		return errors.New("injected transient remove error")
	}
	return fs.MutableFileService.RemoveFile(ctx, name)
}

func TestBucketWriterRetriesTransientRemoveFailure(t *testing.T) {
	for _, inject := range []bool{false, true} {
		proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
		budget := process.MustNewExecutionResourceBudget(1<<20, 1<<20)
		generation, err := budget.OpenGeneration(1)
		require.NoError(t, err)
		writer := BucketWriter{Name: "join-spill-remove-retry", Budget: generation}
		require.NoError(t, writeBucketPayload(proc, []byte("real-spill-record"), 1, &writer, nil))
		require.True(t, writer.Created())
		require.Positive(t, generation.SpillDiskUsed())
		baseFS, err := proc.GetSpillFileService()
		require.NoError(t, err)
		var fault *transientRemoveFS
		if inject {
			fault = &transientRemoveFS{MutableFileService: baseFS}
			writer.fileFS = fault
		}
		require.NoError(t, writer.Close())
		require.Zero(t, generation.SpillDiskUsed())
		if inject {
			require.Equal(t, 2, fault.removeCalls)
		}
		_, err = baseFS.OpenFile(context.Background(), writer.Name)
		require.Error(t, err)
		generation.Close()
		budget.Close()
		proc.Free()
	}
}

func TestBucketWriterRetainsFailedRemovalForRetry(t *testing.T) {
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	budget := process.MustNewExecutionResourceBudget(1<<20, 1<<20)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	defer budget.Close()
	defer generation.Close()
	writer := BucketWriter{Name: "join-spill-blocked-removal", Budget: generation}
	require.NoError(t, writeBucketPayload(proc, []byte("real-spill-record"), 1, &writer, nil))
	baseFS, err := proc.GetSpillFileService()
	require.NoError(t, err)
	fault := &blockedRemoveFS{MutableFileService: baseFS, blocked: true}
	writer.fileFS = fault
	require.ErrorContains(t, writer.Close(), "injected persistent remove error")
	require.True(t, writer.Created())
	require.Positive(t, generation.SpillDiskUsed())
	file, err := baseFS.OpenFile(context.Background(), writer.Name)
	require.NoError(t, err)
	require.NoError(t, file.Close())
	fault.blocked = false
	require.NoError(t, writer.Close())
	require.False(t, writer.Created())
	require.Zero(t, generation.SpillDiskUsed())
	_, err = baseFS.OpenFile(context.Background(), writer.Name)
	require.Error(t, err)
}

func TestHandedOffJoinSpillRetainsFailedRemovalForRetry(t *testing.T) {
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	budget := process.MustNewExecutionResourceBudget(1<<20, 1<<20)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	defer budget.Close()
	defer generation.Close()
	w := BucketWriter{Name: "join-spill-handoff-retry", Budget: generation}
	require.NoError(t, writeBucketPayload(proc, []byte("real-spill-record"), 1, &w, nil))
	baseFS, err := proc.GetSpillFileService()
	require.NoError(t, err)
	fault := &blockedRemoveFS{MutableFileService: baseFS, blocked: true}
	w.fileFS = fault
	file, err := w.handOffSpillFile(proc.Ctx)
	require.NoError(t, err)
	require.Positive(t, generation.SpillDiskUsed())
	require.ErrorContains(t, file.Close(), "injected persistent remove error")
	require.Positive(t, generation.SpillDiskUsed())
	physical, err := baseFS.OpenFile(context.Background(), w.Name)
	require.NoError(t, err)
	require.NoError(t, physical.Close())
	fault.blocked = false
	require.NoError(t, file.Close())
	require.Zero(t, generation.SpillDiskUsed())
	_, err = baseFS.OpenFile(context.Background(), w.Name)
	require.Error(t, err)
}

func TestSpillEngineCleanupRetainsFailedBucketRemoval(t *testing.T) {
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	engine := newExactTestSpillEngine(t, SpillEngineConfig{})
	writer := BucketWriter{Name: "queued-join-cleanup", Budget: engine.cfg.Budget}
	require.NoError(t, writeBucketPayload(proc, []byte("real-spill-record"), 1, &writer, nil))
	baseFS, err := proc.GetSpillFileService()
	require.NoError(t, err)
	fault := &blockedRemoveFS{MutableFileService: baseFS, blocked: true}
	writer.fileFS = fault
	file, err := writer.handOffSpillFile(proc.Ctx)
	require.NoError(t, err)
	engine.buckets = []SpillBucket{{BuildFd: file, BuildRows: 1}}
	engine.Cleanup(proc)
	require.Positive(t, engine.cfg.Budget.SpillDiskUsed())
	require.Len(t, engine.orphanFiles, 1)
	physical, err := baseFS.OpenFile(context.Background(), writer.Name)
	require.NoError(t, err)
	require.NoError(t, physical.Close())
	fault.blocked = false
	engine.Cleanup(proc)
	require.Empty(t, engine.orphanFiles)
	require.Zero(t, engine.cfg.Budget.SpillDiskUsed())
	_, err = baseFS.OpenFile(context.Background(), writer.Name)
	require.Error(t, err)
}

func TestScatterProbeRetainsFailedRemovalUntilCleanup(t *testing.T) {
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	baseFS, err := proc.GetSpillFileService()
	require.NoError(t, err)
	fault := &blockedRemoveFS{MutableFileService: baseFS, blocked: true}
	engine := newExactTestSpillEngine(t, SpillEngineConfig{
		NeedsProbeForEmptyBuild: true,
	})
	engine.InitFromSpilledFiles(nil)
	engine.buckets = []SpillBucket{{}}
	engine.scatterCoalesceDisabled = true
	engine.spillFS.once.Do(func() { engine.spillFS.fs = fault })
	input := makeInt32Batch(proc, []int32{1, 2, 3, 4})
	defer input.Clean(proc.Mp())
	upstreamErr := errors.New("injected upstream error")
	calls := 0
	err = engine.ScatterProbeTable(proc, func() (*batch.Batch, error) {
		calls++
		if calls == 1 {
			return input, nil
		}
		return nil, upstreamErr
	}, process.NewAnalyzer(0, false, false, "test"), makeTestEvalKeysFn())
	require.ErrorIs(t, err, upstreamErr)
	require.ErrorContains(t, err, "injected persistent remove error")
	require.Len(t, engine.orphanWriters, 1)
	require.Positive(t, engine.cfg.Budget.SpillDiskUsed())
	fault.blocked = false
	engine.Cleanup(proc)
	require.Empty(t, engine.orphanWriters)
	require.Zero(t, engine.cfg.Budget.SpillDiskUsed())
	require.Zero(t, engine.cfg.Budget.SpillFDUsed())
	for entry, listErr := range baseFS.List(context.Background(), "/") {
		require.NoError(t, listErr)
		t.Errorf("spill file remains after cleanup: %s", entry.Name)
	}
}
