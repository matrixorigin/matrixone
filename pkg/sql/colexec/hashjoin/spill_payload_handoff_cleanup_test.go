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

package hashjoin

import (
	"context"
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/spillutil"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestHashJoinFreeReleaseRetriesAfterFilesystemRecovery(t *testing.T) {
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	fd, err := os.CreateTemp(t.TempDir(), "terminal-hashjoin-*")
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
				return errors.New("injected terminal removal failure")
			}
			return os.Remove(name)
		}, 1, 1, func() { released.Add(1) },
	)
	engine := newAccountedTestSpillEngine(t, spillutil.SpillEngineConfig{})
	engine.InitFromSpilledFiles([]*message.SpillFile{file})
	join := NewArgument()
	join.ctr.spillEngine = engine
	join.Free(proc, true, errors.New("terminal query error"))
	require.True(t, engine.CleanupPending())
	join.Release()
	require.False(t, engine.CleanupPending())
	require.FileExists(t, name)
	require.Zero(t, released.Load())
	blocked.Store(false)
	require.Eventually(t, func() bool { return released.Load() == 1 }, 3*time.Second, 10*time.Millisecond)
	require.NoFileExists(t, name)
}

func TestHashJoinSpillConstructorFailureRetainsCleanup(t *testing.T) {
	for _, failFirstRemove := range []bool{false, true} {
		t.Run(map[bool]string{false: "control", true: "transient-remove-error"}[failFirstRemove], func(t *testing.T) {
			proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
			proc.SetMessageBoard(message.NewMessageBoard())
			defer proc.Free()
			join := &HashJoin{JoinMapTag: 91091, EqConds: make([][]*plan.Expr, 2)}
			generation, registry, account := installHashJoinTestAllocation(t, join)
			defer generation.Close()
			defer func() {
				require.NoError(t, join.ClearAllocationAccount(account))
				_, _, err := registry.CompleteTerminal(account)
				require.NoError(t, err)
			}()
			join.OpAnalyzer = process.NewAnalyzer(0, false, false, "test")
			payloadBudget := process.MustNewExecutionResourceBudget(1<<20, 1<<20)
			payloadGeneration, err := payloadBudget.OpenGeneration(1)
			require.NoError(t, err)
			defer payloadBudget.Close()
			fd, err := os.CreateTemp(t.TempDir(), "spill-handoff-*")
			require.NoError(t, err)
			name := fd.Name()
			_, err = fd.Write([]byte{1})
			require.NoError(t, err)
			require.NoError(t, fd.Close())
			var removeCalls, releases atomic.Int32
			file := message.NewReopenableSpillFile(
				func(context.Context) (*os.File, error) { return os.Open(name) },
				func() error {
					if removeCalls.Add(1) == 1 && failFirstRemove {
						return errors.New("injected remove error")
					}
					return os.Remove(name)
				},
				1, 1, func() { releases.Add(1) },
			)
			defer func() { _ = file.Close() }()
			jm := message.NewJoinMap(message.GroupSels{}, nil, nil, nil, nil, proc.Mp())
			jm.IncRef(1)
			jm.SetRowCount(1)
			require.NoError(t, jm.SetSpillBuildPayload(message.SpillBuildPayload{
				Files: []*message.SpillFile{file}, BudgetRef: payloadGeneration,
			}))
			require.True(t, message.SendJoinMapResult(message.NewJoinMapResult(jm), join.JoinMapTag, false, 0, proc.GetMessageBoard()))
			payloadGeneration.Close() // force NewSpillEngine to reject after the payload was taken
			err = join.build(join.OpAnalyzer, proc)
			require.ErrorIs(t, err, process.ErrExecutionResourceClosed)
			require.NoFileExists(t, name, "failed constructor must not lose cleanup ownership")
			require.Equal(t, int32(1), releases.Load())
			if failFirstRemove {
				require.Equal(t, int32(2), removeCalls.Load())
			} else {
				require.Equal(t, int32(1), removeCalls.Load())
			}
		})
	}
}

func TestHashJoinResetRetriesSpillCleanupOnFree(t *testing.T) {
	for _, failFirstRemove := range []bool{false, true} {
		t.Run(map[bool]string{false: "control", true: "transient-remove-error"}[failFirstRemove], func(t *testing.T) {
			proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
			defer proc.Free()
			ctx, cancel := context.WithCancel(context.Background())
			proc.BuildPipelineContext(ctx)
			cancel() // teardown must still remove files after query cancellation
			fd, err := os.CreateTemp(t.TempDir(), "spill-reset-*")
			require.NoError(t, err)
			name := fd.Name()
			_, err = fd.Write([]byte{1})
			require.NoError(t, err)
			require.NoError(t, fd.Close())
			var removeCalls, releases atomic.Int32
			file := message.NewReopenableSpillFile(
				func(context.Context) (*os.File, error) { return os.Open(name) },
				func() error {
					if removeCalls.Add(1) == 1 && failFirstRemove {
						return errors.New("injected remove error")
					}
					return os.Remove(name)
				},
				1, 1, func() { releases.Add(1) },
			)
			defer func() { _ = file.Close() }()
			engine := newAccountedTestSpillEngine(t, spillutil.SpillEngineConfig{})
			engine.InitFromSpilledFiles([]*message.SpillFile{file})
			join := &HashJoin{ctr: container{spillEngine: engine}}
			join.ctr.cleanBucketBatches(proc) // Reset path
			join.ctr.cleanBucketBatches(proc) // Free path
			require.NoFileExists(t, name)
			require.Nil(t, join.ctr.spillEngine)
			require.Equal(t, int32(1), releases.Load())
			if failFirstRemove {
				require.Equal(t, int32(2), removeCalls.Load())
			}
		})
	}
}

func TestHashJoinPrepareDoesNotOverwritePendingSpillCleanup(t *testing.T) {
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	fd, err := os.CreateTemp(t.TempDir(), "spill-reuse-*")
	require.NoError(t, err)
	name := fd.Name()
	_, err = fd.Write([]byte{1})
	require.NoError(t, err)
	require.NoError(t, fd.Close())
	var removeCalls, releases atomic.Int32
	file := message.NewReopenableSpillFile(
		func(context.Context) (*os.File, error) { return os.Open(name) },
		func() error {
			if removeCalls.Add(1) <= 2 {
				return errors.New("injected remove error")
			}
			return os.Remove(name)
		},
		1, 1, func() { releases.Add(1) },
	)
	defer func() { _ = file.Close() }()
	engine := newAccountedTestSpillEngine(t, spillutil.SpillEngineConfig{})
	engine.InitFromSpilledFiles([]*message.SpillFile{file})
	join := &HashJoin{ctr: container{spillEngine: engine}}
	join.ctr.cleanBucketBatches(proc)
	require.True(t, engine.CleanupPending())
	require.ErrorContains(t, join.Prepare(proc), "previous join spill cleanup is pending")
	require.Same(t, engine, join.ctr.spillEngine)
	require.FileExists(t, name)
	require.ErrorIs(t, join.Prepare(proc), mpool.ErrAllocationAccountInvalid)
	require.Nil(t, join.ctr.spillEngine)
	require.NoFileExists(t, name)
	require.Equal(t, int32(3), removeCalls.Load())
	require.Equal(t, int32(1), releases.Load())
}

func TestHashJoinProbeScatterFailureRetainsSpillCleanup(t *testing.T) {
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	proc.SetMessageBoard(message.NewMessageBoard())
	defer proc.Free()
	join := &HashJoin{JoinMapTag: 91092, EqConds: make([][]*plan.Expr, 2)}
	generation, registry, account := installHashJoinTestAllocation(t, join)
	defer generation.Close()
	defer func() {
		require.NoError(t, join.ClearAllocationAccount(account))
		_, _, err := registry.CompleteTerminal(account)
		require.NoError(t, err)
	}()
	join.OpAnalyzer = process.NewAnalyzer(0, false, false, "test")
	upstreamErr := errors.New("injected probe child error")
	join.AppendChild(&terminalBudgetAdmissionChild{
		MockOperator: colexec.NewMockOperator(), err: upstreamErr,
	})
	fd, err := os.CreateTemp(t.TempDir(), "spill-scatter-*")
	require.NoError(t, err)
	name := fd.Name()
	_, err = fd.Write([]byte{1})
	require.NoError(t, err)
	require.NoError(t, fd.Close())
	var removeCalls, releases atomic.Int32
	file := message.NewReopenableSpillFile(
		func(context.Context) (*os.File, error) { return os.Open(name) },
		func() error {
			if removeCalls.Add(1) == 1 {
				return errors.New("injected remove error")
			}
			return os.Remove(name)
		},
		1, 1, func() { releases.Add(1) },
	)
	defer func() { _ = file.Close() }()
	jm := message.NewJoinMap(message.GroupSels{}, nil, nil, nil, nil, proc.Mp())
	jm.IncRef(1)
	jm.SetRowCount(1)
	require.NoError(t, jm.SetSpillBuildPayload(message.SpillBuildPayload{
		Files: []*message.SpillFile{file}, BudgetRef: generation,
	}))
	require.True(t, message.SendJoinMapResult(message.NewJoinMapResult(jm), join.JoinMapTag, false, 0, proc.GetMessageBoard()))
	err = join.build(join.OpAnalyzer, proc)
	require.ErrorIs(t, err, upstreamErr)
	require.NotNil(t, join.ctr.spillEngine, "failed cleanup still needs an owner")
	require.FileExists(t, name)
	join.ctr.cleanBucketBatches(proc)
	require.Nil(t, join.ctr.spillEngine)
	require.NoFileExists(t, name)
	require.Equal(t, int32(2), removeCalls.Load())
	require.Equal(t, int32(1), releases.Load())
}
