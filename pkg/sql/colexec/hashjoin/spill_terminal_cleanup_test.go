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
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/spillutil"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

type terminalCleanupChild struct {
	*colexec.MockOperator
	err error
}

func (c *terminalCleanupChild) Call(*process.Process) (vm.CallResult, error) {
	return vm.NewCallResult(), c.err
}

func TestHashJoinTerminalCleanupBoundaries(t *testing.T) {
	for _, phase := range []string{"missing-account", "constructor", "scatter", "reset", "free", "release"} {
		for _, fail := range []bool{false, true} {
			name := phase + "/healthy"
			if fail {
				name = phase + "/failure"
			}
			t.Run(name, func(t *testing.T) {
				proc := testutil.NewProcess(t)
				proc.SetMessageBoard(message.NewMessageBoard())
				budget := process.MustNewExecutionResourceBudget(1<<20, 1<<20)
				t.Cleanup(budget.Close)
				g, err := budget.OpenGeneration(1)
				require.NoError(t, err)
				t.Cleanup(g.Close)
				op := &HashJoin{JoinMapTag: 91903, EqConds: make([][]*plan.Expr, 2)}
				if phase == "release" {
					op = NewArgument()
					t.Cleanup(op.Release)
				}
				if phase != "missing-account" {
					_, registry, account := installHashJoinTestAllocation(t, op)
					t.Cleanup(func() {
						require.NoError(t, op.ClearAllocationAccount(account))
						_, _, err := registry.CompleteTerminal(account)
						require.NoError(t, err)
					})
				}
				t.Cleanup(func() { op.Free(proc, false, nil) })
				fd, err := os.CreateTemp(t.TempDir(), "consumer")
				require.NoError(t, err)
				_, err = fd.Write([]byte{1})
				require.NoError(t, err)
				require.NoError(t, fd.Close())
				disk, err := g.ReserveSpillDisk(1)
				require.NoError(t, err)
				removes, finishes := 0, 0
				unlinkErr := errors.New("injected consumer unlink failure")
				file := message.NewReopenableSpillFile(func(context.Context) (*os.File, error) { return os.Open(fd.Name()) },
					func() error {
						removes++
						if fail {
							return unlinkErr
						}
						return os.Remove(fd.Name())
					}, 1, 1,
					func(gone bool) {
						finishes++
						if gone {
							disk.Release()
						} else {
							disk.Abandon()
						}
					})
				if phase == "reset" || phase == "free" || phase == "release" {
					engine, err := spillutil.NewSpillEngine(spillutil.SpillEngineConfig{Budget: g}, op.allocationAccount, mpool.AllocationOwnerHashBuild)
					require.NoError(t, err)
					engine.InitFromSpilledFiles([]*message.SpillFile{file})
					op.ctr.spillEngine = engine
					ctx, cancel := context.WithCancel(context.Background())
					proc.BuildPipelineContext(ctx)
					cancel()
					if phase == "reset" {
						op.Reset(proc, true, context.Canceled)
					} else {
						op.Free(proc, true, context.Canceled)
					}
					require.Nil(t, op.ctr.spillEngine)
				} else {
					op.OpAnalyzer = process.NewAnalyzer(0, false, false, "terminal")
					upstream := errors.New("injected child error")
					op.AppendChild(&terminalCleanupChild{MockOperator: colexec.NewMockOperator(), err: upstream})
					jm := message.NewJoinMap(message.GroupSels{}, nil, nil, nil, nil, proc.Mp())
					jm.IncRef(1)
					jm.SetRowCount(1)
					require.NoError(t, jm.SetSpillBuildPayload(message.SpillBuildPayload{Files: []*message.SpillFile{file}, BudgetRef: g}))
					require.True(t, message.SendJoinMapResult(message.NewJoinMapResult(jm), op.JoinMapTag, false, 0, proc.GetMessageBoard()))
					if phase == "constructor" {
						g.Close()
					}
					err = op.build(op.OpAnalyzer, proc)
					switch phase {
					case "missing-account":
						require.ErrorIs(t, err, mpool.ErrAllocationAccountInvalid)
					case "constructor":
						require.ErrorIs(t, err, process.ErrExecutionResourceClosed)
					case "scatter":
						require.ErrorIs(t, err, upstream)
					}
					if fail {
						require.ErrorIs(t, err, unlinkErr)
					}
					require.Nil(t, op.ctr.mp)
					require.Nil(t, op.ctr.spillEngine)
					op.Reset(proc, true, err)
					op.Free(proc, true, err)
				}
				g.Close()
				require.Equal(t, 1, removes)
				require.Equal(t, 1, finishes)
				require.Zero(t, g.SpillFDUsed())
				if fail {
					require.FileExists(t, fd.Name())
					require.EqualValues(t, 1, g.SpillDiskUsed())
					require.False(t, disk.Release())
				} else {
					require.NoFileExists(t, fd.Name())
					require.Zero(t, g.SpillDiskUsed())
				}
				_ = file.Close()
				require.Equal(t, 1, removes)
			})
		}
	}
}
