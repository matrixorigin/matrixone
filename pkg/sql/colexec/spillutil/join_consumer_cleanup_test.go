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

package spillutil_test

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dedupjoin"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashjoin"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/rightdedupjoin"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// The filesystem only faults removal. The real HashBuild writer, payload transfer, consumer
// Call and terminal lifecycle run unchanged, on a private resource ledger.
type consumerCleanupFS struct {
	fileservice.MutableFileService
	blocked atomic.Bool
	mu      sync.Mutex
	paths   map[string]string
	removes map[string]int
}

func (fs *consumerCleanupFS) CreateFile(ctx context.Context, name string) (*os.File, error) {
	fd, err := fs.MutableFileService.CreateFile(ctx, name)
	if err == nil {
		fs.mu.Lock()
		fs.paths[name] = fd.Name()
		fs.mu.Unlock()
	}
	return fd, err
}

func (fs *consumerCleanupFS) RemoveFile(ctx context.Context, name string) error {
	if fs.blocked.Load() {
		return os.ErrPermission
	}
	err := fs.MutableFileService.RemoveFile(ctx, name)
	if err == nil {
		fs.mu.Lock()
		fs.removes[name]++
		fs.mu.Unlock()
	}
	return err
}

func (fs *consumerCleanupFS) snapshot() (map[string]string, map[string]int) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	paths := make(map[string]string, len(fs.paths))
	removes := make(map[string]int, len(fs.removes))
	for name, path := range fs.paths {
		paths[name] = path
		removes[name] = fs.removes[name]
	}
	return paths, removes
}

type consumerCleanupInput struct {
	*colexec.MockOperator
	err error
}

func (op *consumerCleanupInput) Call(proc *process.Process) (vm.CallResult, error) {
	if op.err != nil {
		return vm.NewCallResult(), op.err
	}
	return op.MockOperator.Call(proc)
}

type cleanupConsumer interface {
	vm.Operator
	SetAllocationAccount(*mpool.AllocationAccount) error
	ClearAllocationAccount(*mpool.AllocationAccount) error
}

func TestJoinConsumerTerminalCleanup(t *testing.T) {
	typ := types.T_int32.ToType()
	key := &plan.Expr{Typ: plan.Type{Id: int32(types.T_int32)},
		Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}
	conditions := [][]*plan.Expr{{key}, {key}}
	const tag int32 = 7301
	consumers := []struct {
		name string
		new  func() cleanupConsumer
	}{
		{"hashjoin", func() cleanupConsumer {
			return &hashjoin.HashJoin{
				JoinType: plan.Node_INNER, EqConds: conditions,
				LeftTypes: []types.Type{typ}, RightTypes: []types.Type{typ},
				ResultCols: []colexec.ResultPos{{Rel: 0, Pos: 0}},
				JoinMapTag: tag, IsShuffle: true, SpillThreshold: 2,
			}
		}},
		{"dedupjoin", func() cleanupConsumer {
			return &dedupjoin.DedupJoin{
				Conditions: conditions, LeftTypes: []types.Type{typ}, RightTypes: []types.Type{typ},
				Result: []colexec.ResultPos{{Rel: 0, Pos: 0}}, OnDuplicateAction: plan.Node_IGNORE,
				JoinMapTag: tag, IsShuffle: true, SpillThreshold: 2,
			}
		}},
		{"rightdedupjoin", func() cleanupConsumer {
			return &rightdedupjoin.RightDedupJoin{
				Conditions: conditions, LeftTypes: []types.Type{typ}, RightTypes: []types.Type{typ},
				Result: []colexec.ResultPos{{Rel: 0, Pos: 0}}, OnDuplicateAction: plan.Node_FAIL,
				JoinMapTag: tag, IsShuffle: true, SpillThreshold: 2,
			}
		}},
	}
	for _, consumer := range consumers {
		for _, scenario := range []struct {
			name         string
			scatterError bool
			removeError  bool
		}{
			{"scatter_remove_error", true, true},
			{"scatter_healthy_removal", true, false},
			{"direct_free_remove_error", false, true},
		} {
			t.Run(consumer.name+"/"+scenario.name, func(t *testing.T) {
				proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
				proc.SetMessageBoard(message.NewMessageBoard())
				txnOp := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
				txnOp.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
				proc.Base.TxnOperator = txnOp
				budget := process.MustNewExecutionResourceBudget(8<<20, 8<<20)
				t.Cleanup(budget.Close)
				generation, err := budget.OpenGeneration(1)
				require.NoError(t, err)
				t.Cleanup(generation.Close)
				require.NoError(t, proc.SetExecutionResourceBudgetForTesting(generation))
				registry, err := mpool.NewAllocationAccountRegistry(1, 4096)
				require.NoError(t, err)
				account, err := registry.OpenWithController(8<<20, generation)
				require.NoError(t, err)
				local, err := fileservice.Get[fileservice.MutableFileService](proc.Base.FileService, defines.LocalFileServiceName)
				require.NoError(t, err)
				fault := &consumerCleanupFS{MutableFileService: local, paths: make(map[string]string), removes: make(map[string]int)}
				proc.Base.FileService = fault
				buildBat := batch.NewWithSize(1)
				buildBat.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 2}, nil, proc.Mp())
				buildBat.SetRowCount(2)
				probeBat := batch.NewWithSize(1)
				probeBat.Vecs[0] = testutil.MakeInt32Vector([]int32{1}, nil, proc.Mp())
				probeBat.SetRowCount(1)
				build := &hashbuild.HashBuild{
					NeedHashMap: true, NeedBatches: true, Conditions: conditions[1],
					IsShuffle: true, SpillThreshold: 2, JoinMapTag: tag, JoinMapRefCnt: 1,
					RuntimeFilterSpec: &plan.RuntimeFilterSpec{Tag: tag + 9000},
				}
				build.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{buildBat}))
				arg := consumer.new()
				input := &consumerCleanupInput{MockOperator: colexec.NewMockOperator().WithBatchs([]*batch.Batch{probeBat})}
				upstreamErr := errors.New("injected upstream scatter failure")
				if scenario.scatterError {
					input.err = upstreamErr
				}
				arg.GetOperatorBase().AppendChild(input)
				require.NoError(t, build.SetAllocationAccount(account))
				require.NoError(t, arg.SetAllocationAccount(account))
				t.Cleanup(func() {
					fault.blocked.Store(false)
					arg.Reset(proc, true, nil)
					arg.Free(proc, true, nil)
					build.Free(proc, true, nil)
					buildBat.Clean(proc.Mp())
					probeBat.Clean(proc.Mp())
					// Failed negative controls must not leave physical files. This
					// private ledger is never shared with another test or a CN.
					fault.mu.Lock()
					for _, path := range fault.paths {
						_ = os.Remove(path)
					}
					fault.mu.Unlock()
				})
				require.NoError(t, build.Prepare(proc))
				require.NoError(t, arg.Prepare(proc))
				_, err = vm.Exec(build, proc)
				require.NoError(t, err)
				require.Positive(t, build.OpAnalyzer.GetOpStats().ExtraStats["HashBuildSpillStarts"])
				require.Positive(t, generation.SpillDiskUsed())
				fault.blocked.Store(scenario.removeError)
				for i := 0; i < 4; i++ {
					_, err = vm.Exec(arg, proc)
					if err != nil {
						break
					}
				}
				if scenario.scatterError {
					require.ErrorIs(t, err, upstreamErr)
				} else {
					require.ErrorIs(t, err, os.ErrPermission)
				}
				if scenario.removeError {
					require.Positive(t, generation.SpillDiskUsed())
				}
				// Free without Reset must transfer ownership too. Scatter already
				// discarded its engine, so later lifecycle calls cannot rescue it.
				arg.Free(proc, true, err)
				build.Free(proc, true, err)
				buildBat.Clean(proc.Mp())
				probeBat.Clean(proc.Mp())
				require.Zero(t, generation.SpillFDUsed())
				require.Zero(t, generation.Used())
				require.Zero(t, account.Snapshot().Used)
				require.Zero(t, proc.Mp().CurrNB())
				if scenario.removeError {
					require.Positive(t, generation.SpillDiskUsed(), "failed removal must retain the charge")
					paths, removes := fault.snapshot()
					require.NotEmpty(t, paths)
					for name, path := range paths {
						_, statErr := os.Stat(path)
						require.NoError(t, statErr, "failed removal must retain the file")
						require.Zero(t, removes[name])
					}
				} else {
					require.Zero(t, generation.SpillDiskUsed(), "healthy cleanup is synchronous")
				}
				fault.blocked.Store(false)
				// No Reset, Free or subsequent execution is allowed to drive recovery.
				require.Eventually(t, func() bool { return generation.SpillDiskUsed() == 0 }, 5*time.Second, 10*time.Millisecond)
				paths, removes := fault.snapshot()
				require.NotEmpty(t, paths)
				for name, path := range paths {
					_, statErr := os.Stat(path)
					require.ErrorIs(t, statErr, os.ErrNotExist)
					require.Equal(t, 1, removes[name], "remove exactly once: %s", name)
				}
				arg.Reset(proc, true, err)
				arg.Free(proc, true, err)
				require.NoError(t, arg.ClearAllocationAccount(account))
				require.NoError(t, build.ClearAllocationAccount(account))
				_, _, err = registry.CompleteTerminal(account)
				require.NoError(t, err)
			})
		}
	}
}
