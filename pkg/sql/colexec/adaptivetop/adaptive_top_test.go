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

package adaptivetop

import (
	"bytes"
	"context"
	"errors"
	"math"
	"os"
	"strconv"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/internal/materialized"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type adaptiveFixture struct {
	op      *AdaptiveTop
	child   *merge.Merge
	proc    *process.Process
	account *mpool.AllocationAccount
	starts  []int
	waits   []int
}

func newAdaptiveFixture(t *testing.T, limit uint64) *adaptiveFixture {
	t.Helper()
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	t.Cleanup(func() {
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	})
	proc.Reg.MergeReceivers = []*process.WaitRegister{{}, {}, {}}
	for _, reg := range proc.Reg.MergeReceivers {
		reg.ResetForReuse(4, 1)
	}
	registry, err := mpool.NewAllocationAccountRegistry(1, 1<<16)
	require.NoError(t, err)
	account, err := registry.Open(math.MaxInt64)
	require.NoError(t, err)
	op := NewArgument()
	op.LimitExpr = plan2.MakePlan2Uint64ConstExprWithType(limit)
	op.Branches = 3
	child := merge.NewArgument().WithPartial(0, 0)
	op.AppendChild(child)
	f := &adaptiveFixture{op: op, child: child, proc: proc, account: account}
	t.Cleanup(func() {
		defer op.Release()
		defer child.Release()
		child.Reset(proc, false, nil)
		op.Free(proc, false, nil)
		child.Free(proc, false, nil)
		require.NoError(t, op.ClearAllocationAccount(account))
		snapshot, first, err := registry.CompleteTerminal(account)
		require.NoError(t, err)
		require.True(t, first)
		require.Zero(t, snapshot.Used)
		for _, owner := range snapshot.Owners {
			require.Equal(t, mpool.AllocationOwnerTop, owner.Owner)
			require.Zero(t, owner.Current)
		}
	})
	require.NoError(t, op.SetAllocationAccount(account))
	return f
}

func (f *adaptiveFixture) send(t *testing.T, branch int, values ...int64) {
	t.Helper()
	bat := batch.NewWithSize(1)
	transferred := false
	defer func() {
		if !transferred {
			bat.Clean(f.proc.Mp())
		}
	}()
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(bat.Vecs[0], values, nil, f.proc.Mp()))
	bat.SetRowCount(len(values))
	require.True(t, process.SendPipelineSignalWithContext(
		f.proc.Ctx, f.proc.Reg.MergeReceivers[branch],
		process.NewPipelineSignalToDirectly(bat, nil, f.proc.Mp())))
	transferred = true
}

func (f *adaptiveFixture) terminal(t *testing.T, branch int, err error) {
	t.Helper()
	signal := process.NewEndSignal()
	if err != nil {
		signal = process.NewErrorSignal(err)
	}
	require.True(t, process.SendPipelineSignalWithContext(
		context.Background(), f.proc.Reg.MergeReceivers[branch], signal))
}

func (f *adaptiveFixture) install(t *testing.T, candidates [][][]int64) {
	t.Helper()
	f.op.SetBranchStarter(func(branch int) error {
		// 上一个候选必须完成清理，且未发布内容。
		require.Len(t, f.waits, branch)
		require.Nil(t, f.op.ctr.output)
		f.starts = append(f.starts, branch)
		for _, values := range candidates[branch] {
			f.send(t, branch, values...)
		}
		f.terminal(t, branch, nil)
		return nil
	})
	f.op.SetBranchWaiter(func(branch int, done func(error)) error {
		require.Len(t, f.starts, branch+1)
		require.Nil(t, f.op.ctr.output)
		f.waits = append(f.waits, branch)
		done(nil)
		return nil
	})
}

func (f *adaptiveFixture) read(t *testing.T) (values []int64) {
	t.Helper()
	for {
		result, err := vm.Exec(f.op, f.proc)
		require.NoError(t, err)
		if result.Status == vm.ExecWaiting {
			waitAdaptiveResult(t, result)
			continue
		}
		if result.Batch != nil {
			values = append(values, vector.MustFixedColWithTypeCheck[int64](result.Batch.Vecs[0])...)
		}
		if result.Status == vm.ExecStop {
			return values
		}
	}
}

// waitAdaptiveResult drives the readiness callback exposed by the event-driven
// VM boundary. Tests that assert a terminal error must continue through every
// intermediate quantum; a single vm.Exec call is no longer guaranteed to
// consume a whole child stream.
func waitAdaptiveResult(t *testing.T, result vm.CallResult) {
	t.Helper()
	require.Equal(t, vm.ExecWaiting, result.Status)
	require.NotNil(t, result.OnReady)
	ready := make(chan struct{}, 1)
	require.NoError(t, result.OnReady(func() { ready <- struct{}{} }))
	<-ready
}

func runAdaptiveUntilTerminal(t *testing.T, f *adaptiveFixture) (vm.CallResult, error) {
	t.Helper()
	for {
		result, err := vm.Exec(f.op, f.proc)
		if err != nil || result.Status == vm.ExecStop {
			return result, err
		}
		if result.Status == vm.ExecWaiting {
			waitAdaptiveResult(t, result)
		}
	}
}

func readAdaptiveBatch(t *testing.T, f *adaptiveFixture) vm.CallResult {
	t.Helper()
	for {
		result, err := vm.Exec(f.op, f.proc)
		require.NoError(t, err)
		if result.Status == vm.ExecWaiting {
			waitAdaptiveResult(t, result)
			continue
		}
		if result.Batch != nil {
			return result
		}
		require.NotEqual(t, vm.ExecStop, result.Status, "adaptive top stopped before producing a batch")
	}
}

func TestAdaptiveTopSelectsOnlyOneCompleteCandidate(t *testing.T) {
	for _, tc := range []struct {
		name       string
		limit      uint64
		candidates [][][]int64
		want       []int64
		starts     []int
	}{
		{"post full", 3, [][][]int64{{{1}, {2, 3}}, {{11}}, {{21}}}, []int64{1, 2, 3}, []int{0}},
		{"post partial", 3, [][][]int64{{{1}}, {{11}, {12, 13}}, {{21}}}, []int64{11, 12, 13}, []int{0, 1}},
		{"pre partial", 3, [][][]int64{{{1}}, {{11, 12}}, {{21, 22, 23}}}, []int64{21, 22, 23}, []int{0, 1, 2}},
		{"force genuinely short", 3, [][][]int64{{}, {}, {{21, 22}}}, []int64{21, 22}, []int{0, 1, 2}},
		{"force genuinely empty", 3, [][][]int64{{}, {}, {}}, nil, []int{0, 1, 2}},
		{"empty batch", 3, [][][]int64{{{}, {1, 2, 3}}, {}, {}}, []int64{1, 2, 3}, []int{0}},
		{"zero limit", 0, [][][]int64{{}, {}, {}}, nil, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newAdaptiveFixture(t, tc.limit)
			f.install(t, tc.candidates)
			require.NoError(t, vm.Prepare(f.op, f.proc))
			require.Equal(t, tc.want, f.read(t))
			require.Equal(t, tc.starts, f.starts)
			require.Equal(t, f.starts, f.waits)
			require.Empty(t, f.read(t), "重复 Call 不能重启或重发")
		})
	}
}

func TestAdaptiveTopReportsRetainedMemory(t *testing.T) {
	f := newAdaptiveFixture(t, 3)
	f.install(t, [][][]int64{{{1, 2, 3}}, {}, {}})
	require.NoError(t, vm.Prepare(f.op, f.proc))
	require.Equal(t, []int64{1, 2, 3}, f.read(t))
	stats := f.op.OpAnalyzer.GetOpStats()
	require.Positive(t, stats.MemorySize)
	require.Zero(t, stats.SpillSize)
	require.Zero(t, stats.SpillRows)
}

func TestAdaptiveTopRejectsCandidateErrorsWithoutPublication(t *testing.T) {
	for _, where := range []string{"start", "after partial data", "after full data", "completion", "page overflow"} {
		t.Run(where, func(t *testing.T) {
			f := newAdaptiveFixture(t, 2)
			wantErr := errors.New(where)
			starts, waits := 0, 0
			f.op.SetBranchStarter(func(branch int) error {
				starts++
				require.Zero(t, branch)
				switch where {
				case "start":
					f.terminal(t, branch, wantErr)
					return wantErr
				case "after partial data":
					f.send(t, branch, 1)
					f.terminal(t, branch, wantErr)
				case "after full data":
					f.send(t, branch, 1, 2)
					f.terminal(t, branch, wantErr)
				case "completion":
					f.send(t, branch, 1, 2)
					f.terminal(t, branch, nil)
				case "page overflow":
					f.send(t, branch, 1, 2, 3)
					f.terminal(t, branch, nil)
				}
				return nil
			})
			f.op.SetBranchWaiter(func(branch int, _ func(error)) error { waits++; return wantErr })
			require.NoError(t, vm.Prepare(f.op, f.proc))
			result, err := runAdaptiveUntilTerminal(t, f)
			if where == "page overflow" {
				require.ErrorContains(t, err, "exceeded its final page limit")
			} else {
				require.ErrorIs(t, err, wantErr)
			}
			require.Nil(t, result.Batch)
			require.Nil(t, f.op.ctr.source)
			require.Zero(t, f.account.Snapshot().Used)
			require.Equal(t, 1, starts)
			if where == "completion" {
				require.Equal(t, 1, waits)
			} else {
				require.Zero(t, waits)
			}
		})
	}
}

func TestAdaptiveTopCancellationBeforePublish(t *testing.T) {
	for _, phase := range []string{"before start", "after completion", "during replay"} {
		t.Run(phase, func(t *testing.T) {
			f := newAdaptiveFixture(t, 2)
			ctx, cancel := context.WithCancelCause(f.proc.Ctx)
			t.Cleanup(func() { cancel(nil) })
			f.proc.Ctx = ctx
			f.install(t, [][][]int64{{{1}, {2}}, {}, {}})
			wantErr := errors.New("client canceled")
			if phase == "after completion" {
				f.op.SetBranchWaiter(func(_ int, done func(error)) error { cancel(wantErr); done(nil); return nil })
			}
			require.NoError(t, vm.Prepare(f.op, f.proc))
			if phase == "during replay" {
				result := readAdaptiveBatch(t, f)
				require.Equal(t, 1, result.Batch.RowCount())
				cancel(wantErr)
			} else if phase == "before start" {
				cancel(wantErr)
			}
			// 直接覆盖算子的取消入口；完成回调可能先返回一个等待量子，
			// 因此通过 VM 驱动它直到取消被观察到。
			var result vm.CallResult
			var err error
			if phase == "after completion" {
				result, err = runAdaptiveUntilTerminal(t, f)
			} else {
				result, err = f.op.Call(f.proc)
			}
			require.ErrorIs(t, err, wantErr)
			require.Nil(t, result.Batch)
			require.Zero(t, f.account.Snapshot().Used)
			if phase == "before start" {
				require.Empty(t, f.starts)
			} else {
				require.Equal(t, []int{0}, f.starts)
			}
		})
	}
}

func TestAdaptiveTopPreparedLimitReuse(t *testing.T) {
	f := newAdaptiveFixture(t, 0)
	param := &plan.Expr{Typ: plan.Type{Id: int32(types.T_varchar)}, Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: 0}}}
	var err error
	f.op.LimitExpr, err = plan2.MakePlan2AssignmentCastExpr(f.proc.Ctx, param, plan.Type{Id: int32(types.T_uint64)})
	require.NoError(t, err)
	for _, limit := range []uint64{0, 2, 1, 0, 2} {
		func() {
			params := vector.NewVec(types.T_varchar.ToType())
			defer params.Free(f.proc.Mp())
			require.NoError(t, vector.AppendBytes(params, []byte(strconv.FormatUint(limit, 10)), false, f.proc.Mp()))
			f.proc.SetPrepareParams(params)
			defer f.proc.SetPrepareParams(nil)
			f.starts, f.waits = nil, nil
			for _, reg := range f.proc.Reg.MergeReceivers {
				reg.ResetForReuse(4, 1)
			}
			f.install(t, [][][]int64{{{1}}, {{11, 12}}, {}})
			require.NoError(t, vm.Prepare(f.op, f.proc))
			require.Len(t, f.read(t), int(limit))
			if limit == 0 {
				require.Empty(t, f.starts)
			} else if limit == 1 {
				require.Equal(t, []int{0}, f.starts)
			} else {
				require.Equal(t, []int{0, 1}, f.starts)
			}
			f.child.Reset(f.proc, false, nil)
			f.op.Reset(f.proc, false, nil)
			require.Nil(t, f.op.startBranch)
			require.Nil(t, f.op.waitBranch)
		}()
	}
}

func TestAdaptiveTopMissingLifecycleAndAccount(t *testing.T) {
	for _, missing := range []string{"starter", "waiter", "account"} {
		t.Run(missing, func(t *testing.T) {
			f := newAdaptiveFixture(t, 1)
			f.install(t, [][][]int64{{{1}}, {}, {}})
			switch missing {
			case "starter":
				f.op.ClearBranchStarter()
			case "waiter":
				f.op.ClearBranchWaiter()
			case "account":
				require.NoError(t, f.op.ClearAllocationAccount(f.account))
			}
			require.NoError(t, vm.Prepare(f.op, f.proc))
			result, err := vm.Exec(f.op, f.proc)
			require.Error(t, err)
			require.Nil(t, result.Batch)
			require.Empty(t, f.starts)
		})
	}
}

func TestAdaptiveTopRetainedBatchBoundFailsClosed(t *testing.T) {
	f := newAdaptiveFixture(t, 4097)
	f.proc.Reg.MergeReceivers[0].ResetForReuse(4098, 1)
	f.op.SetBranchStarter(func(branch int) error {
		require.Zero(t, branch)
		for i := range 4097 {
			f.send(t, branch, int64(i))
		}
		f.terminal(t, branch, nil)
		return nil
	})
	f.op.SetBranchWaiter(func(_ int, done func(error)) error {
		t.Error("不应发布缺少 spill 配置的候选")
		done(nil)
		return nil
	})
	require.NoError(t, vm.Prepare(f.op, f.proc))
	result, err := runAdaptiveUntilTerminal(t, f)
	require.ErrorContains(t, err, "spill is unavailable")
	require.Nil(t, result.Batch)
	require.Zero(t, f.account.Snapshot().Used)
}

func TestAdaptiveTopSpillSelectionAndDiscard(t *testing.T) {
	for _, fallback := range []bool{false, true} {
		t.Run(strconv.FormatBool(fallback), func(t *testing.T) {
			limit := uint64(4097)
			if fallback {
				limit++
			}
			f := newAdaptiveFixture(t, limit)
			f.proc.Reg.MergeReceivers[0].ResetForReuse(4098, 1)
			budget, err := f.proc.GetExecutionResourceBudget()
			require.NoError(t, err)
			dir := t.TempDir()
			f.op.SpillConfig = materialized.SpillConfig{
				FileFactory: func(name string) (*os.File, error) {
					file, err := os.CreateTemp(dir, name)
					if err == nil {
						err = os.Remove(file.Name())
					}
					return file, err
				},
				Budget: materialized.SpillBudget{
					ReserveMemory: func(n uint64) (materialized.Reservation, error) { return budget.ReserveTransientMemory(n) },
					ReserveDisk:   func(n uint64) (materialized.GrowingReservation, error) { return budget.ReserveSpillDisk(n) },
					ReserveFD:     func(n uint64) (materialized.Reservation, error) { return budget.ReserveSpillFD(n) },
				},
			}
			f.op.SetBranchStarter(func(branch int) error {
				f.starts = append(f.starts, branch)
				if branch == 0 {
					for i := range 4097 {
						f.send(t, branch, int64(i))
					}
				} else {
					require.Equal(t, 1, branch)
					require.Zero(t, budget.SpillFDUsed(), "被拒候选的文件应先关闭")
					require.Zero(t, budget.SpillDiskUsed())
					require.Zero(t, f.account.Snapshot().Used)
					values := make([]int64, limit)
					for i := range values {
						values[i] = int64(i) + 10000
					}
					f.send(t, branch, values...)
				}
				f.terminal(t, branch, nil)
				return nil
			})
			f.op.SetBranchWaiter(func(branch int, done func(error)) error {
				if branch == 0 {
					require.Positive(t, budget.SpillDiskUsed(), "必须真实跨过 batch 保留上限")
					require.EqualValues(t, 1, budget.SpillFDUsed())
				}
				done(nil)
				return nil
			})
			require.NoError(t, vm.Prepare(f.op, f.proc))
			values := f.read(t)
			require.Len(t, values, int(limit))
			stats := f.op.OpAnalyzer.GetOpStats()
			require.Positive(t, stats.SpillSize)
			require.Positive(t, stats.SpillRows)
			for i, value := range values {
				want := int64(i)
				if fallback {
					want += 10000
				}
				require.Equal(t, want, value)
			}
			require.Zero(t, budget.SpillFDUsed())
			require.Zero(t, budget.SpillDiskUsed())
			require.Zero(t, budget.Snapshot().Used)
			require.Zero(t, f.account.Snapshot().Used)
		})
	}
}

func TestAdaptiveTopPipelineCleanupAfterVMCancellation(t *testing.T) {
	f := newAdaptiveFixture(t, 2)
	ctx, cancel := context.WithCancel(f.proc.Ctx)
	defer cancel()
	f.proc.Ctx = ctx
	f.install(t, [][][]int64{{{1}, {2}}, {}, {}})
	require.NoError(t, vm.Prepare(f.op, f.proc))
	result := readAdaptiveBatch(t, f)
	require.NotNil(t, result.Batch)
	cancel()
	_, err := runAdaptiveUntilTerminal(t, f)
	require.ErrorIs(t, err, context.Canceled)
	// VM 在 Call 前取消，spool 由 pipeline 的常规 Reset 释放。
	f.child.Reset(f.proc, true, err)
	f.op.Reset(f.proc, true, err)
	require.Zero(t, f.account.Snapshot().Used)
}

func TestAdaptiveTopOperatorContract(t *testing.T) {
	f := newAdaptiveFixture(t, 1)
	var buf bytes.Buffer
	f.op.String(&buf)
	require.Equal(t, opName, buf.String())
	require.Equal(t, opName, f.op.TypeName())
	require.Equal(t, vm.AdaptiveTop, f.op.OpType())
	require.Same(t, &f.op.OperatorBase, f.op.GetOperatorBase())
	require.True(t, f.op.DeferFirstBranch())
	bat := batch.EmptyForConstFoldBatch
	got, err := f.op.ExecProjection(f.proc, bat)
	require.NoError(t, err)
	require.Same(t, bat, got)
	require.ErrorIs(t, f.op.SetAllocationAccount(nil), mpool.ErrAllocationAccountInvalid)
	require.NoError(t, f.op.SetAllocationAccount(f.account))
	require.ErrorIs(t, f.op.SetAllocationAccount(&mpool.AllocationAccount{}), mpool.ErrAllocationAccountMismatch)
	require.ErrorIs(t, f.op.ClearAllocationAccount(&mpool.AllocationAccount{}), mpool.ErrAllocationAccountMismatch)
	f.install(t, [][][]int64{{{1}}, {}, {}})
	require.NoError(t, vm.Prepare(f.op, f.proc))
	_, err = vm.Exec(f.op, f.proc)
	require.NoError(t, err)
	require.ErrorIs(t, f.op.ClearAllocationAccount(f.account), mpool.ErrAllocationAccountInvariant)
	require.Error(t, f.op.Prepare(f.proc))
}
