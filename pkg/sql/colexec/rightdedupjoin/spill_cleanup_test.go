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

package rightdedupjoin

import (
	"context"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/spillutil"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type cleanupFailureFS struct {
	fileservice.MutableFileService
	blocked atomic.Bool
	removes atomic.Int32
	// OpenFile is only called during synchronous probe-reader setup. The retry
	// worker only calls RemoveFile, so it never accesses this descriptor field.
	reader *os.File
}

func (fs *cleanupFailureFS) OpenFile(ctx context.Context, name string) (*os.File, error) {
	fd, err := fs.MutableFileService.OpenFile(ctx, name)
	if err == nil {
		fs.reader = fd
	}
	return fd, err
}

func (fs *cleanupFailureFS) RemoveFile(ctx context.Context, name string) error {
	if fs.blocked.Load() {
		return os.ErrPermission
	}
	err := fs.MutableFileService.RemoveFile(ctx, name)
	if err == nil {
		fs.removes.Add(1)
	}
	return err
}

func TestRightDedupSpillCleanupAtReset(t *testing.T) {
	for _, scenario := range []struct {
		name      string
		callToEOF bool
		fail      bool
	}{
		{name: "retained_idle_operator", fail: true},
		{name: "eof_remove_error", callToEOF: true, fail: true},
		{name: "healthy_eof", callToEOF: true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
			budget := process.MustNewExecutionResourceBudget(8<<20, 8<<20)
			t.Cleanup(budget.Close)
			generation, err := budget.OpenGeneration(1)
			require.NoError(t, err)
			t.Cleanup(generation.Close)
			registry, err := mpool.NewAllocationAccountRegistry(1, 4096)
			require.NoError(t, err)
			account, err := registry.OpenWithController(8<<20, generation)
			require.NoError(t, err)
			local, err := fileservice.Get[fileservice.MutableFileService](proc.Base.FileService, defines.LocalFileServiceName)
			require.NoError(t, err)
			fault := &cleanupFailureFS{MutableFileService: local}
			proc.Base.FileService = fault
			arg := &RightDedupJoin{}
			var engine *spillutil.SpillEngine
			t.Cleanup(func() {
				fault.blocked.Store(false)
				arg.Reset(proc, true, nil)
				// Keep failed assertions and negative-control mutations from
				// leaving an unqueued file behind after the test has finished.
				if engine != nil {
					engine.Cleanup(proc)
					engine.DeferCleanup()
				}
				require.Eventually(t, func() bool { return generation.SpillDiskUsed() == 0 }, 5*time.Second, 10*time.Millisecond)
				arg.Free(proc, true, nil)
				require.NoError(t, arg.ClearAllocationAccount(account))
				_, _, err := registry.CompleteTerminal(account)
				require.NoError(t, err)
			})
			require.NoError(t, arg.SetAllocationAccount(account))
			engine, err = spillutil.NewSpillEngine(spillutil.SpillEngineConfig{
				Budget: generation, NeedsProbeForEmptyBuild: true,
			}, account, mpool.AllocationOwnerHashBuild)
			require.NoError(t, err)
			arg.ctr.spillEngine = engine
			arg.OpAnalyzer = process.NewAnalyzer(0, false, false, "cleanup-test")
			// A one-row probe and an empty build bucket are sufficient to open a
			// real spill reader, without an unrelated hashmap or SQL service.
			engine.InitFromSpilledFiles([]*message.SpillFile{nil})
			input := batch.NewWithSize(1)
			t.Cleanup(func() { input.Clean(proc.Mp()) })
			input.Vecs[0] = testutil.MakeInt32Vector([]int32{7}, nil, proc.Mp())
			input.SetRowCount(1)
			sent := false
			require.NoError(t, engine.ScatterProbeTable(proc, func() (*batch.Batch, error) {
				if sent {
					return nil, nil
				}
				sent = true
				return input, nil
			}, arg.OpAnalyzer, func(b *batch.Batch) ([]*vector.Vector, error) { return b.Vecs, nil }))
			input.Clean(proc.Mp())
			jm, result, err := engine.RebuildHashmap(proc, arg.OpAnalyzer)
			require.NoError(t, err)
			require.Equal(t, spillutil.BucketEmptyBuild, result)
			require.Nil(t, jm)
			require.True(t, engine.IsProbing())
			require.NotNil(t, fault.reader)
			name := fault.reader.Name()
			require.FileExists(t, name)
			require.Positive(t, generation.SpillFDUsed())
			diskCharge := generation.SpillDiskUsed()
			require.Positive(t, diskCharge)
			fault.blocked.Store(scenario.fail)
			arg.ctr.state = Probe
			var callErr error
			if scenario.callToEOF {
				callResult, err := arg.Call(proc)
				callErr = err
				if scenario.fail {
					require.ErrorIs(t, callErr, os.ErrPermission)
					require.Same(t, engine, arg.ctr.spillEngine)
					require.FileExists(t, name)
					require.Equal(t, diskCharge, generation.SpillDiskUsed())
				} else {
					require.NoError(t, callErr)
					require.Equal(t, vm.ExecStop, callResult.Status)
				}
			} else {
				decoded, err := engine.NextProbeBatch(proc)
				require.NoError(t, err)
				require.Equal(t, []int32{7}, vector.MustFixedColNoTypeCheck[int32](decoded.Vecs[0]))
			}
			require.Positive(t, account.Snapshot().Used, "reader/decoded allocations must actually be exercised")
			arg.Reset(proc, callErr != nil, callErr)
			require.Nil(t, arg.ctr.spillEngine)
			require.Zero(t, generation.SpillFDUsed())
			require.Zero(t, generation.Used())
			require.Zero(t, account.Snapshot().Used)
			require.Zero(t, proc.Mp().CurrNB())
			_, err = fault.reader.Stat()
			require.ErrorIs(t, err, os.ErrClosed)
			if scenario.fail {
				require.FileExists(t, name)
				require.Equal(t, diskCharge, generation.SpillDiskUsed())
				require.Zero(t, fault.removes.Load())
			}
			fault.blocked.Store(false)
			// No more operator lifecycle calls until the independent worker has
			// removed the real file and retired its disk reservation.
			require.Eventually(t, func() bool { return generation.SpillDiskUsed() == 0 }, 5*time.Second, 10*time.Millisecond)
			require.NoFileExists(t, name)
			require.Equal(t, int32(1), fault.removes.Load())
			arg.Reset(proc, false, nil)
			arg.Free(proc, false, nil)
			require.Zero(t, generation.SpillDiskUsed())
			require.Equal(t, int32(1), fault.removes.Load(), "later teardown must not reclaim the detached file again")
		})
	}
}
