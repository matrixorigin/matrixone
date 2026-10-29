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
	"path/filepath"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/arenaskl"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/list"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type groupTerminalFS struct {
	*fileservice.LocalETLFS
	blocked  bool
	names    []string
	onCreate func()
	attempts int
}

var groupUnlinkFailure = errors.New("injected group terminal unlink failure")

func (f *groupTerminalFS) CreateFile(ctx context.Context, name string) (*os.File, error) {
	fd, err := f.LocalETLFS.CreateFile(ctx, name)
	if err == nil {
		f.names = append(f.names, name)
		if f.onCreate != nil {
			f.onCreate()
		}
	}
	return fd, err
}
func (f *groupTerminalFS) RemoveFile(ctx context.Context, name string) error {
	f.attempts++
	if f.blocked {
		return groupUnlinkFailure
	}
	return f.LocalETLFS.RemoveFile(ctx, name)
}

func groupTerminalFixture(t *testing.T) (*process.Process, *groupTerminalFS, fileservice.MutableFileService, string) {
	t.Helper()
	base := t.TempDir()
	local, err := fileservice.NewLocalETLFS(defines.LocalFileServiceName, base)
	require.NoError(t, err)
	t.Cleanup(func() { local.Close(context.Background()) })
	fault := &groupTerminalFS{LocalETLFS: local}
	proc := testutil.NewProcess(t, testutil.WithFileService(fault))
	budget := process.MustNewExecutionResourceBudget(64<<20, 64<<20)
	t.Cleanup(budget.Close)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	t.Cleanup(generation.Close)
	require.NoError(t, proc.SetExecutionResourceBudgetForTesting(generation))
	view, err := proc.GetSpillFileService()
	require.NoError(t, err)
	return proc, fault, view, base
}

func TestGroupTerminalDiscardedBuckets(t *testing.T) {
	for _, path := range []string{"reload_done", "reload_error", "wave_rollback", "sort_run_error"} {
		t.Run(path, func(t *testing.T) {
			proc, fault, view, base := groupTerminalFixture(t)
			generation, err := proc.GetExecutionResourceBudget()
			require.NoError(t, err)
			ctr := container{mp: proc.Mp(), budget: generation}
			t.Cleanup(ctr.free)
			fault.blocked = true
			if path == "sort_run_error" {
				controller := &distinctSpillController{ctr: &ctr, rootName: "cleanup-sort"}
				set := arenaskl.NewSkiplist(arenaskl.NewArena(make([]byte, 4096)), bytes.Compare)
				_, err := controller.flushSortSet(proc, view, set)
				require.ErrorContains(t, err, "empty distinct sort run")
				require.ErrorIs(t, err, groupUnlinkFailure)
			} else {
				bucket := &spillBucket{name: "cleanup-" + path, cnt: 1}
				require.NoError(t, ctr.openSpillBucket(proc, view, bucket))
				if path == "reload_error" {
					_, err = bucket.writer.Write([]byte{1})
				} else {
					err = types.WriteInt64(bucket.writer, 0)
				}
				require.NoError(t, err)
				require.NoError(t, bucket.flushWriter())
				if path == "wave_rollback" {
					wave := [spillNumBuckets]*spillBucket{bucket}
					freeDistinctWave(&wave)
					require.Nil(t, wave[0])
				} else {
					ctr.spillBkts = list.New[*spillBucket]()
					ctr.spillBkts.PushBack(bucket)
					_, err = ctr.loadSpilledData(proc, process.NewAnalyzer(0, false, false, "cleanup"), nil)
					if path == "reload_error" {
						require.ErrorIs(t, err, io.ErrUnexpectedEOF)
					} else {
						require.ErrorIs(t, err, groupUnlinkFailure)
					}
					require.Zero(t, ctr.spillBkts.Len())
				}
				require.Nil(t, bucket.diskToken)
				require.Nil(t, bucket.fileFS)
				require.Nil(t, bucket.file)
				require.Positive(t, generation.SpillDiskUsed())
			}
			require.Zero(t, generation.SpillFDUsed())
			require.Len(t, fault.names, 1)
			p, err := fileservice.ParsePath(fault.names[0])
			require.NoError(t, err)
			require.FileExists(t, filepath.Join(base, p.File))
			_, err = view.CreateFile(context.Background(), "next-query")
			require.ErrorIs(t, err, fileservice.ErrSpillRootFenced)
			attempts, disk := fault.attempts, generation.SpillDiskUsed()
			fault.blocked = false
			ctr.freeSpillBkts()
			generation.Close()
			require.Equal(t, attempts, fault.attempts, "terminal cleanup must retain no callback for future retry")
			require.Equal(t, disk, generation.SpillDiskUsed())
		})
	}
}

func TestGroupTerminalEmptyCurrentBucketRemovalFailure(t *testing.T) {
	proc, fault, view, base := groupTerminalFixture(t)
	generation, err := proc.GetExecutionResourceBudget()
	require.NoError(t, err)
	ctr := container{mp: proc.Mp(), budget: generation}
	t.Cleanup(ctr.free)
	bucket := &spillBucket{name: "empty-current"}
	require.NoError(t, ctr.openSpillBucket(proc, view, bucket))
	ctr.currentSpillBkt = []*spillBucket{bucket}
	fault.blocked = true
	_, err = ctr.loadSpilledData(proc, process.NewAnalyzer(0, false, false, "empty-current"), nil)
	require.ErrorIs(t, err, groupUnlinkFailure)
	require.Nil(t, ctr.currentSpillBkt[0], "discarded empty bucket must have no second terminal owner")
	require.Nil(t, bucket.diskToken)
	require.Nil(t, bucket.fileFS)
	require.Nil(t, bucket.file)
	require.Zero(t, generation.SpillFDUsed())
	require.FileExists(t, filepath.Join(base, defines.SpillFileServiceName, "empty-current"))
	_, err = view.CreateFile(context.Background(), "later-query")
	require.ErrorIs(t, err, fileservice.ErrSpillRootFenced)
	attempts := fault.attempts
	ctr.freeSpillBkts()
	require.Equal(t, attempts, fault.attempts, "terminal failure must not schedule a later retry")
}

func TestGroupTerminalDistinctMergeIntermediate(t *testing.T) {
	proc, fault, view, base := groupTerminalFixture(t)
	generation, err := proc.GetExecutionResourceBudget()
	require.NoError(t, err)
	ctr := container{mp: proc.Mp(), budget: generation}
	controller := &distinctSpillController{ctr: &ctr, rootName: "merge-cleanup"}
	var slots [64]*spillBucket
	t.Cleanup(func() {
		for _, bucket := range slots {
			_ = bucket.free()
		}
		controller.close()
		ctr.free()
	})
	newRun := func(name string, malformed bool) *spillBucket {
		b := &spillBucket{name: name, cnt: 1}
		require.NoError(t, ctr.openSpillBucket(proc, view, b))
		if malformed {
			require.NoError(t, types.WriteInt32(b.writer, 0))
		} else {
			require.NoError(t, writeDistinctSortKey(b.writer, []byte("key")))
		}
		require.NoError(t, b.flushWriter())
		return b
	}
	slots[0], slots[1] = newRun("slot-0", false), newRun("slot-1", true)
	run := newRun("input", false)
	fault.onCreate = func() {
		if len(fault.names) == 5 {
			fault.blocked = true
		}
	}
	err = controller.addSortRun(proc, view, &slots, run)
	require.ErrorContains(t, err, "invalid distinct sort key length")
	require.ErrorIs(t, err, groupUnlinkFailure)
	require.Len(t, fault.names, 5)
	for i, b := range slots {
		_ = b.free()
		slots[i] = nil
	}
	require.Zero(t, generation.SpillFDUsed())
	require.Positive(t, generation.SpillDiskUsed())
	for _, name := range fault.names[3:] {
		p, err := fileservice.ParsePath(name)
		require.NoError(t, err)
		require.FileExists(t, filepath.Join(base, p.File), "unreturned intermediate/output files remain conservatively charged")
	}
	_, err = view.CreateFile(context.Background(), "new")
	require.ErrorIs(t, err, fileservice.ErrSpillRootFenced)
}

func TestGroupTerminalTeardownAndPoolRelease(t *testing.T) {
	for _, owner := range []string{"group_reset", "group_free", "merge_reset", "merge_free", "group_release"} {
		t.Run(owner, func(t *testing.T) {
			proc, fault, view, base := groupTerminalFixture(t)
			generation, err := proc.GetExecutionResourceBudget()
			require.NoError(t, err)
			group, merge := &Group{}, &MergeGroup{}
			if owner == "group_release" {
				group = NewArgument()
			}
			ctr := &group.ctr
			if owner == "merge_reset" || owner == "merge_free" {
				ctr = &merge.ctr
			}
			*ctr = container{mp: proc.Mp(), budget: generation}
			bucket := &spillBucket{name: "retained"}
			require.NoError(t, ctr.openSpillBucket(proc, view, bucket))
			_, err = bucket.writer.Write([]byte("retained-spill"))
			require.NoError(t, err)
			require.NoError(t, bucket.flushWriter())
			ctr.distinctSpill = &distinctSpillController{ctr: ctr}
			ctr.distinctSpill.root[0] = bucket
			fault.blocked = true
			switch owner {
			case "group_reset":
				group.Reset(proc, true, nil)
			case "group_free", "group_release":
				group.Free(proc, true, nil)
			case "merge_reset":
				merge.Reset(proc, true, nil)
			case "merge_free":
				merge.Free(proc, true, nil)
			}
			if owner == "group_release" {
				group.Release()
			}
			require.FileExists(t, filepath.Join(base, defines.SpillFileServiceName, "retained"))
			require.Positive(t, generation.SpillDiskUsed())
			require.Zero(t, generation.SpillFDUsed())
			require.Nil(t, bucket.diskToken)
			require.Nil(t, bucket.fileFS)
			require.Equal(t, 1, fault.attempts)
		})
	}
}
