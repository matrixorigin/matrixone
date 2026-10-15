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
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/spillutil"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type spillCleanupErrorChild struct {
	*colexec.MockOperator
	err error
}

func (child *spillCleanupErrorChild) Call(*process.Process) (vm.CallResult, error) {
	return vm.NewCallResult(), child.err
}

func TestRightDedupJoinTeardownRetriesWhileOperatorRetained(t *testing.T) {
	for _, boundary := range []string{"reset", "free"} {
		for _, removeError := range []bool{false, true} {
			name := boundary + "/control"
			if removeError {
				name = boundary + "/remove-error"
			}
			t.Run(name, func(t *testing.T) {
				proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
				t.Cleanup(proc.Free)
				ctx, cancel := context.WithCancel(context.Background())
				proc.BuildPipelineContext(ctx)
				t.Cleanup(cancel)
				budget := process.MustNewExecutionResourceBudget(1<<20, 1<<20)
				t.Cleanup(budget.Close)
				generation, err := budget.OpenGeneration(1)
				require.NoError(t, err)
				t.Cleanup(generation.Close)
				fd, err := os.CreateTemp(t.TempDir(), "cached-rightdedupjoin-*")
				require.NoError(t, err)
				t.Cleanup(func() { _ = fd.Close() })
				path := fd.Name()
				require.NoError(t, fd.Close())
				disk, err := generation.ReserveSpillDisk(1)
				require.NoError(t, err)
				t.Cleanup(func() { disk.Release() })
				var blocked atomic.Bool
				blocked.Store(removeError)
				var removes, releases atomic.Int32
				file := message.NewReopenableSpillFile(
					func(context.Context) (*os.File, error) { return os.Open(path) },
					func() error {
						removes.Add(1)
						if blocked.Load() {
							return errors.New("injected cached operator remove failure")
						}
						return os.Remove(path)
					}, 1, 1, func() { disk.Release(); releases.Add(1) },
				)
				op := &RightDedupJoin{}
				t.Cleanup(func() {
					blocked.Store(false)
					op.Free(proc, false, nil)
					require.NoError(t, file.Close())
				})
				engine, err := spillutil.NewSpillEngine(spillutil.SpillEngineConfig{Budget: generation}, installTestAllocation(t), mpool.AllocationOwnerHashBuild)
				require.NoError(t, err)
				engine.InitFromSpilledFiles([]*message.SpillFile{file})
				op.ctr.spillEngine = engine
				cancel()
				if boundary == "reset" {
					op.Reset(proc, true, context.Canceled)
				} else {
					op.Free(proc, true, context.Canceled)
				}
				generation.Close()
				if removeError {
					require.FileExists(t, path)
					require.EqualValues(t, 1, generation.SpillDiskUsed())
					require.Zero(t, releases.Load())
				}
				// Keep the cached operator idle: no Prepare, Free or Release
				// may supply progress after the filesystem recovers.
				blocked.Store(false)
				require.Eventually(t, func() bool { return releases.Load() == 1 }, 3*time.Second, 10*time.Millisecond)
				require.NoFileExists(t, path)
				require.Zero(t, generation.SpillDiskUsed())
				require.Nil(t, op.ctr.spillEngine)
				require.False(t, engine.CleanupPending())
				calls := removes.Load()
				op.Reset(proc, false, nil)
				op.Free(proc, false, nil)
				require.Equal(t, calls, removes.Load(), "detached cleanup must not run twice")
				require.EqualValues(t, 1, releases.Load())
			})
		}
	}
}

func TestRightDedupJoinSpillFailureKeepsCleanupOwner(t *testing.T) {
	for _, phase := range []string{"missing-account", "constructor", "scatter"} {
		for _, removeError := range []bool{false, true} {
			name := phase + "/control"
			if removeError {
				name = phase + "/remove-error"
			}
			t.Run(name, func(t *testing.T) {
				proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
				defer proc.Free()
				proc.SetMessageBoard(message.NewMessageBoard())
				budget := process.MustNewExecutionResourceBudget(1<<20, 1<<20)
				defer budget.Close()
				generation, err := budget.OpenGeneration(1)
				require.NoError(t, err)
				defer generation.Close()
				require.NoError(t, proc.SetExecutionResourceBudgetForTesting(generation))

				keyType := types.T_int32.ToType()
				op := &RightDedupJoin{
					JoinMapTag: 91903,
					Conditions: [][]*plan.Expr{{newExpr(0, keyType)}, {newExpr(0, keyType)}},
				}
				var account *mpool.AllocationAccount
				if phase != "missing-account" {
					account = installTestAllocation(t, op)
					defer func() {
						require.NoError(t, op.ClearAllocationAccount(account))
						require.Zero(t, account.Snapshot().Used)
					}()
				}
				// Cancelled query contexts must not prevent terminal file removal.
				queryCtx, cancel := context.WithCancel(context.Background())
				proc.BuildPipelineContext(queryCtx)
				defer cancel()
				op.OpAnalyzer = process.NewAnalyzer(0, false, false, "spill-cleanup")
				upstreamErr := errors.New("injected probe failure")
				op.AppendChild(&spillCleanupErrorChild{MockOperator: colexec.NewMockOperator(), err: upstreamErr})

				fd, err := os.CreateTemp(t.TempDir(), "spill-owner-*")
				require.NoError(t, err)
				path := fd.Name()
				defer fd.Close()
				_, err = fd.Write([]byte{1})
				require.NoError(t, err)
				require.NoError(t, fd.Close())
				disk, err := generation.ReserveSpillDisk(1)
				require.NoError(t, err)
				defer disk.Release()
				var blocked atomic.Bool
				blocked.Store(removeError)
				var removeCalls, releases atomic.Int32
				file := message.NewReopenableSpillFile(
					func(context.Context) (*os.File, error) { return os.Open(path) },
					func() error {
						removeCalls.Add(1)
						if blocked.Load() {
							// Constructor/account rejection retries through JoinMap.Free.
							// Scatter failure keeps ownership on the operator until recovery.
							if phase != "scatter" {
								blocked.Store(false)
							}
							return errors.New("injected remove failure")
						}
						return os.Remove(path)
					},
					1, 1, func() { disk.Release(); releases.Add(1) },
				)
				defer func() {
					blocked.Store(false)
					op.Free(proc, true, upstreamErr)
					require.NoError(t, file.Close())
				}()
				jm := message.NewJoinMap(message.GroupSels{}, nil, nil, nil, nil, proc.Mp())
				jm.IncRef(1)
				mapOwned := true
				defer func() {
					if mapOwned {
						jm.Free()
					}
				}()
				jm.SetRowCount(1)
				require.NoError(t, jm.SetSpillBuildPayload(message.SpillBuildPayload{
					Files: []*message.SpillFile{file}, BudgetRef: generation,
				}))
				require.True(t, message.SendJoinMapResult(message.NewJoinMapResult(jm), op.JoinMapTag, false, 0, proc.GetMessageBoard()))
				mapOwned = false
				if phase == "constructor" {
					generation.Close()
				}
				err = op.build(op.OpAnalyzer, proc)
				switch phase {
				case "missing-account":
					require.ErrorIs(t, err, mpool.ErrAllocationAccountInvalid)
				case "constructor":
					require.ErrorIs(t, err, process.ErrExecutionResourceClosed)
				case "scatter":
					require.ErrorIs(t, err, upstreamErr)
				}
				require.Nil(t, op.ctr.mp)
				cancel()
				if phase == "scatter" && removeError {
					engine := op.ctr.spillEngine
					require.NotNil(t, engine)
					require.True(t, engine.CleanupPending())
					require.FileExists(t, path)
					require.EqualValues(t, 1, generation.SpillDiskUsed())
					require.Zero(t, releases.Load())
					// Before Reset the error path still owns the engine; Prepare
					// must not overwrite its pending files.
					require.ErrorContains(t, op.Prepare(proc), "previous join spill cleanup is pending")
					require.Same(t, engine, op.ctr.spillEngine)
					op.Reset(proc, true, err)
					require.Nil(t, op.ctr.spillEngine)
					op.Free(proc, true, err)
					blocked.Store(false)
					require.Eventually(t, func() bool { return releases.Load() == 1 }, 3*time.Second, 10*time.Millisecond)
					require.NoError(t, op.Prepare(proc))
				}
				require.Nil(t, op.ctr.spillEngine)
				require.NoFileExists(t, path)
				require.Zero(t, generation.SpillDiskUsed())
				require.EqualValues(t, 1, releases.Load())
				calls := removeCalls.Load()
				op.Reset(proc, false, nil)
				op.Free(proc, false, nil)
				require.Equal(t, calls, removeCalls.Load(), "successful cleanup must not remove twice")
				require.EqualValues(t, 1, releases.Load())
			})
		}
	}
}
