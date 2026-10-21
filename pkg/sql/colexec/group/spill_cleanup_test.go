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

package group

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/arenaskl"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/list"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type terminalRemoveErrorFS struct {
	fileservice.MutableFileService
	blocked  atomic.Bool
	names    []string
	onCreate func()
}

func (fs *terminalRemoveErrorFS) CreateFile(ctx context.Context, name string) (*os.File, error) {
	file, err := fs.MutableFileService.CreateFile(ctx, name)
	if err == nil {
		fs.names = append(fs.names, name)
		if fs.onCreate != nil {
			fs.onCreate()
		}
	}
	return file, err
}

func TestDistinctSortMergeErrorRetainsIntermediateRun(t *testing.T) {
	proc := newCleanupTestProcess(t)
	t.Cleanup(proc.Free)
	baseFS, err := proc.GetSpillFileService()
	require.NoError(t, err)
	budget, err := proc.GetExecutionResourceBudget()
	require.NoError(t, err)
	ctr := container{mp: mpool.MustNewZero(), budget: budget}
	controller := &distinctSpillController{ctr: &ctr, rootName: "merge-cleanup"}
	fault := &terminalRemoveErrorFS{MutableFileService: baseFS}
	var slots [64]*spillBucket
	t.Cleanup(func() {
		fault.blocked.Store(false)
		for _, bucket := range slots {
			_ = ctr.freeSpillBucket(bucket)
		}
		controller.close()
		ctr.free()
		for _, name := range fault.names {
			_ = baseFS.RemoveFile(context.Background(), name)
		}
	})
	newRun := func(name string, malformed bool) *spillBucket {
		bkt := &spillBucket{name: name, cnt: 1}
		require.NoError(t, ctr.openSpillBucket(proc, fault, bkt))
		if malformed {
			require.NoError(t, types.WriteInt32(bkt.writer, 0))
		} else {
			require.NoError(t, writeDistinctSortKey(bkt.writer, []byte("key")))
		}
		require.NoError(t, bkt.flushWriter())
		return bkt
	}
	slots[0] = newRun("merge-slot-0", false)
	slots[1] = newRun("merge-slot-1", true)
	run := newRun("merge-input", false)
	fault.onCreate = func() {
		// First merge succeeds; the second creates an output and then fails
		// decoding slot 1. Removal is unavailable for its intermediate/output.
		if len(fault.names) == 5 {
			fault.blocked.Store(true)
		}
	}
	err = controller.addSortRun(proc, fault, &slots, run)
	require.ErrorContains(t, err, "invalid distinct sort key length")
	require.Len(t, fault.names, 5)
	require.Len(t, ctr.spillCleanupPending, 2)
	require.Positive(t, budget.SpillDiskUsed())
	fault.blocked.Store(false)
	for i, bkt := range slots {
		require.NoError(t, ctr.freeSpillBucket(bkt))
		slots[i] = nil
	}
	ctr.freeSpillBkts()
	require.Empty(t, ctr.spillCleanupPending)
	require.Zero(t, budget.SpillDiskUsed())
	require.Zero(t, budget.SpillFDUsed())
	for _, name := range fault.names {
		_, err := baseFS.OpenFile(context.Background(), name)
		require.Error(t, err, name)
	}
}

// Exercise the discard sites themselves: neither a local variable nor an
// emptied wave may be the last owner of a file whose removal failed.
func TestGroupDiscardedBucketsRetainCleanup(t *testing.T) {
	for _, path := range []string{"reload_done", "reload_error", "wave_rollback", "sort_run_error"} {
		t.Run(path, func(t *testing.T) {
			proc := newCleanupTestProcess(t)
			t.Cleanup(proc.Free)
			baseFS, err := proc.GetSpillFileService()
			require.NoError(t, err)
			budget, err := proc.GetExecutionResourceBudget()
			require.NoError(t, err)
			ctr := container{mp: mpool.MustNewZero(), budget: budget}
			fault := &terminalRemoveErrorFS{MutableFileService: baseFS}
			fault.blocked.Store(true)
			t.Cleanup(func() {
				fault.blocked.Store(false)
				ctr.free()
				// Also remove files if the ownership assertion fails pre-fix.
				for _, name := range fault.names {
					_ = baseFS.RemoveFile(context.Background(), name)
				}
			})
			if path == "sort_run_error" {
				controller := &distinctSpillController{ctr: &ctr, rootName: "cleanup-sort"}
				set := arenaskl.NewSkiplist(arenaskl.NewArena(make([]byte, 4096)), bytes.Compare)
				_, err := controller.flushSortSet(proc, fault, set)
				require.ErrorContains(t, err, "empty distinct sort run")
			} else {
				bucket := &spillBucket{name: "cleanup-" + path, cnt: 1}
				require.NoError(t, ctr.openSpillBucket(proc, fault, bucket))
				if path == "reload_error" {
					_, err = bucket.writer.Write([]byte{1})
					require.NoError(t, err)
				} else {
					require.NoError(t, types.WriteInt64(bucket.writer, 0))
				}
				require.NoError(t, bucket.flushWriter())
				if path == "wave_rollback" {
					wave := [spillNumBuckets]*spillBucket{bucket}
					ctr.freeDistinctWave(&wave)
					require.Nil(t, wave[0])
				} else {
					ctr.spillBkts = list.New[*spillBucket]()
					ctr.spillBkts.PushBack(bucket)
					_, err = ctr.loadSpilledData(proc, process.NewAnalyzer(0, false, false, "cleanup"), nil)
					if path == "reload_error" {
						require.ErrorIs(t, err, io.ErrUnexpectedEOF)
					} else {
						require.ErrorContains(t, err, "injected terminal remove error")
					}
					require.Zero(t, ctr.spillBkts.Len())
				}
				require.Positive(t, budget.SpillDiskUsed())
			}
			require.Len(t, ctr.spillCleanupPending, 1)
			require.Zero(t, budget.SpillFDUsed())
			file, err := baseFS.OpenFile(context.Background(), fault.names[0])
			require.NoError(t, err)
			require.NoError(t, file.Close())
			fault.blocked.Store(false)
			ctr.freeSpillBkts()
			require.Empty(t, ctr.spillCleanupPending)
			require.Zero(t, budget.SpillDiskUsed())
			_, err = baseFS.OpenFile(context.Background(), fault.names[0])
			require.Error(t, err)
		})
	}
}

func TestGroupTeardownRetriesWhileOperatorRetained(t *testing.T) {
	for _, owner := range []string{"group_reset", "group_free", "merge_reset", "merge_free"} {
		t.Run(owner, func(t *testing.T) {
			proc := newCleanupTestProcess(t)
			t.Cleanup(proc.Free)
			baseFS, err := proc.GetSpillFileService()
			require.NoError(t, err)
			budget, err := proc.GetExecutionResourceBudget()
			require.NoError(t, err)
			group, merge := &Group{}, &MergeGroup{}
			ctr := &group.ctr
			if strings.HasPrefix(owner, "merge") {
				ctr = &merge.ctr
			}
			*ctr = container{mp: mpool.MustNewZero(), budget: budget}
			fault := &terminalRemoveErrorFS{MutableFileService: baseFS}
			fault.blocked.Store(true)
			t.Cleanup(func() { fault.blocked.Store(false); ctr.freeSpillBkts() })
			bucket := &spillBucket{name: "retained-" + owner}
			require.NoError(t, ctr.openSpillBucket(proc, fault, bucket))
			_, err = bucket.writer.Write([]byte("retained-spill"))
			require.NoError(t, err)
			require.NoError(t, bucket.flushWriter())
			// Include the controller's late cleanup, not only currentSpillBkt.
			ctr.distinctSpill = &distinctSpillController{ctr: ctr}
			ctr.distinctSpill.root[0] = bucket
			switch owner {
			case "group_reset":
				group.Reset(proc, true, nil)
			case "group_free":
				group.Free(proc, true, nil)
			case "merge_reset":
				merge.Reset(proc, true, nil)
			case "merge_free":
				merge.Free(proc, true, nil)
			}
			require.Empty(t, ctr.spillCleanupPending)
			require.Positive(t, budget.SpillDiskUsed())
			require.Zero(t, budget.SpillFDUsed())
			fault.blocked.Store(false)
			// No Release, re-execution, or another cleanup call drives progress.
			require.Eventually(t, func() bool { return budget.SpillDiskUsed() == 0 }, 5*time.Second, 10*time.Millisecond)
			_, err = baseFS.OpenFile(context.Background(), bucket.name)
			require.Error(t, err)
			runtime.KeepAlive(group)
			runtime.KeepAlive(merge)
		})
	}
}

func newCleanupTestProcess(t *testing.T) *process.Process {
	t.Helper()
	proc := testutil.NewProcess(t)
	ledger := process.MustNewExecutionResourceBudget(64<<20, 64<<20)
	generation, err := ledger.OpenGeneration(1)
	require.NoError(t, err)
	t.Cleanup(generation.Close)
	require.NoError(t, proc.SetExecutionResourceBudgetForTesting(generation))
	return proc
}

func (fs *terminalRemoveErrorFS) RemoveFile(ctx context.Context, name string) error {
	if fs.blocked.Load() {
		return errors.New("injected terminal remove error")
	}
	return fs.MutableFileService.RemoveFile(ctx, name)
}

func TestGroupFreeReleaseRetriesAfterFilesystemRecovery(t *testing.T) {
	for _, distinct := range []bool{false, true} {
		proc := newCleanupTestProcess(t)
		baseFS, err := proc.GetSpillFileService()
		require.NoError(t, err)
		budget, err := proc.GetExecutionResourceBudget()
		require.NoError(t, err)
		fault := &terminalRemoveErrorFS{MutableFileService: baseFS}
		fault.blocked.Store(true)
		group := NewArgument()
		group.ctr = container{mp: mpool.MustNewZero(), budget: budget}
		bucket := &spillBucket{name: "terminal-group-spill"}
		require.NoError(t, group.ctr.openSpillBucket(proc, fault, bucket))
		_, err = bucket.writer.Write([]byte("physical-spill-data"))
		require.NoError(t, err)
		require.NoError(t, bucket.flushWriter())
		if distinct {
			group.ctr.distinctSpill = &distinctSpillController{ctr: &group.ctr}
			group.ctr.distinctSpill.root[0] = bucket
		} else {
			group.ctr.currentSpillBkt = []*spillBucket{bucket}
		}
		group.Free(proc, true, errors.New("terminal query error"))
		group.Release()
		require.Positive(t, budget.SpillDiskUsed())
		require.Zero(t, budget.SpillFDUsed())
		fault.blocked.Store(false)
		require.Eventually(t, func() bool { return budget.SpillDiskUsed() == 0 }, 3*time.Second, 10*time.Millisecond)
		_, err = baseFS.OpenFile(context.Background(), bucket.name)
		require.Error(t, err)
		proc.Free()
	}
}

type oneShotRemoveErrorFS struct {
	fileservice.MutableFileService
	removeCalls int
}

type switchableRemoveErrorFS struct {
	fileservice.MutableFileService
	blocked bool
}

func (fs *switchableRemoveErrorFS) RemoveFile(ctx context.Context, name string) error {
	if fs.blocked {
		return errors.New("injected persistent remove error")
	}
	return fs.MutableFileService.RemoveFile(ctx, name)
}

func (fs *oneShotRemoveErrorFS) RemoveFile(ctx context.Context, name string) error {
	fs.removeCalls++
	if fs.removeCalls == 1 {
		return errors.New("injected transient remove error")
	}
	return fs.MutableFileService.RemoveFile(ctx, name)
}

func TestGroupCleanupRetriesTransientRemoveFailure(t *testing.T) {
	for _, inject := range []bool{false, true} {
		proc := newCleanupTestProcess(t)
		baseFS, err := proc.GetSpillFileService()
		require.NoError(t, err)
		budget, err := proc.GetExecutionResourceBudget()
		require.NoError(t, err)
		ctr := container{mp: proc.Mp(), budget: budget}
		bkt := &spillBucket{name: "whitebox-group-cleanup"}
		var fs fileservice.MutableFileService = baseFS
		var fault *oneShotRemoveErrorFS
		if inject {
			fault = &oneShotRemoveErrorFS{MutableFileService: baseFS}
			fs = fault
		}
		require.NoError(t, ctr.openSpillBucket(proc, fs, bkt))
		_, err = bkt.writer.Write([]byte("physical-spill-data"))
		require.NoError(t, err)
		require.NoError(t, bkt.flushWriter())
		require.Positive(t, budget.SpillDiskUsed())
		ctr.currentSpillBkt = []*spillBucket{bkt}
		ctr.freeSpillBkts()
		require.Zero(t, budget.SpillDiskUsed())
		require.Zero(t, budget.SpillFDUsed())
		require.Nil(t, ctr.currentSpillBkt)
		if inject {
			require.Equal(t, 2, fault.removeCalls)
			_, err = baseFS.OpenFile(context.Background(), bkt.name)
			require.Error(t, err, "transient removal error must not orphan the file")
			ctr.freeSpillBkts()
			require.Equal(t, 2, fault.removeCalls)
		} else {
			_, err = baseFS.OpenFile(context.Background(), bkt.name)
			require.Error(t, err)
		}
		proc.Free()
	}
}

func TestDistinctCleanupRetriesTransientRemoveFailure(t *testing.T) {
	proc := newCleanupTestProcess(t)
	defer proc.Free()
	baseFS, err := proc.GetSpillFileService()
	require.NoError(t, err)
	budget, err := proc.GetExecutionResourceBudget()
	require.NoError(t, err)
	ctr := container{mp: proc.Mp(), budget: budget}
	bkt := &spillBucket{name: "whitebox-distinct-cleanup"}
	fault := &oneShotRemoveErrorFS{MutableFileService: baseFS}
	require.NoError(t, ctr.openSpillBucket(proc, fault, bkt))
	_, err = bkt.writer.Write([]byte("physical-distinct-data"))
	require.NoError(t, err)
	require.NoError(t, bkt.flushWriter())
	require.Positive(t, budget.SpillDiskUsed())
	controller := &distinctSpillController{}
	controller.root[0] = bkt
	controller.close()
	require.Equal(t, 2, fault.removeCalls)
	require.Zero(t, budget.SpillDiskUsed())
	require.Zero(t, budget.SpillFDUsed())
	_, err = baseFS.OpenFile(context.Background(), bkt.name)
	require.Error(t, err)
	controller.close()
	require.Equal(t, 2, fault.removeCalls)
}

func TestGroupRetainsFailedRemovalForLaterCleanup(t *testing.T) {
	for _, distinct := range []bool{false, true} {
		proc := newCleanupTestProcess(t)
		baseFS, err := proc.GetSpillFileService()
		require.NoError(t, err)
		budget, err := proc.GetExecutionResourceBudget()
		require.NoError(t, err)
		ctr := container{mp: proc.Mp(), budget: budget}
		bkt := &spillBucket{name: "persistently-blocked-group-spill"}
		fault := &switchableRemoveErrorFS{MutableFileService: baseFS, blocked: true}
		require.NoError(t, ctr.openSpillBucket(proc, fault, bkt))
		_, err = bkt.writer.Write([]byte("persisted-spill-data"))
		require.NoError(t, err)
		require.NoError(t, bkt.flushWriter())
		if distinct {
			controller := &distinctSpillController{ctr: &ctr}
			controller.root[0] = bkt
			controller.close()
		} else {
			ctr.currentSpillBkt = []*spillBucket{bkt}
			ctr.freeSpillBkts()
		}
		require.Len(t, ctr.spillCleanupPending, 1)
		require.Positive(t, budget.SpillDiskUsed())
		file, err := baseFS.OpenFile(context.Background(), bkt.name)
		require.NoError(t, err)
		require.NoError(t, file.Close())
		fault.blocked = false
		ctr.freeSpillBkts()
		require.Empty(t, ctr.spillCleanupPending)
		require.Zero(t, budget.SpillDiskUsed())
		require.Zero(t, budget.SpillFDUsed())
		_, err = baseFS.OpenFile(context.Background(), bkt.name)
		require.Error(t, err)
		proc.Free()
	}
}
