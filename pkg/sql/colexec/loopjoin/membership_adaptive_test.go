// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package loopjoin

import (
	"context"
	"strconv"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func membershipSwitchInput() ([]int32, [][2]bool) {
	left, want := make([]int32, 35), make([][2]bool, 35)
	for i := range left {
		left[i] = int32(i + 2)
		want[i] = [2]bool{false, true}
	}
	left[32], want[32] = 1, [2]bool{true, false}
	left[33], left[34] = 1000, 0
	return left, want
}

func TestMarkMembershipRealTransitionAndGenerationAccounting(t *testing.T) {
	proc, join, build := newMembershipOperators(t, mpool.MustNewZero())
	left, want := membershipSwitchInput()
	membershipInputs(t, proc, join, build, left, []int32{1, 0}, []uint64{34}, []uint64{1})
	spy := &membershipEvalSpy{ExpressionExecutor: join.ctr.expr}
	join.ctr.expr = spy
	require.Zero(t, join.OpAnalyzer.GetOpStats().MemorySize)
	res, err := vm.Exec(join, proc)
	require.NoError(t, err)
	require.Equal(t, 35, res.Batch.RowCount())
	for row, expected := range want {
		if row != 34 {
			require.Equal(t, left[row], vector.GetFixedAtNoTypeCheck[int32](res.Batch.Vecs[0], row))
		}
		v := res.Batch.Vecs[1]
		require.Equal(t, expected, [2]bool{vector.GetFixedAtNoTypeCheck[bool](v, row), v.GetNulls().Contains(uint64(row))})
	}
	require.True(t, res.Batch.Vecs[0].GetNulls().Contains(34))
	require.Equal(t, 32, spy.calls, "only the 32 real evaluator probes precede the indexed tail")
	require.Equal(t, int64(32), join.ctr.mp.Size())
	require.Equal(t, join.ctr.mp.Size(), join.OpAnalyzer.GetOpStats().MemorySize)
	res, err = vm.Exec(join, proc)
	require.NoError(t, err)
	require.Nil(t, res.Batch)
	require.Equal(t, int64(32), join.OpAnalyzer.GetOpStats().MemorySize)
	join.ctr.expr = spy.ExpressionExecutor
	oldMap, nativeBeforeReset := join.ctr.mp, proc.Mp().CurrNB()
	join.Reset(proc, false, nil)
	require.Zero(t, join.ctr.markWork)
	require.Zero(t, oldMap.Size())
	require.LessOrEqual(t, proc.Mp().CurrNB(), nativeBeforeReset-32, "the index is freed; existing evaluator buffers may be retained for reuse")
	require.Equal(t, int64(32), join.OpAnalyzer.GetOpStats().MemorySize, "Reset preserves collected historical allocation")
	build.Reset(proc, false, nil)
	join.GetChildren(0).Free(proc, false, nil)
	build.GetChildren(0).Free(proc, false, nil)
	proc.SetMessageBoard(proc.GetMessageBoard().Reset())
	left, want = make([]int32, 34), make([][2]bool, 34)
	left[33], want[33] = 2, [2]bool{true, false}
	membershipInputs(t, proc, join, build, left, []int32{2}, nil, nil)
	require.Zero(t, join.OpAnalyzer.GetOpStats().MemorySize, "Prepare resets the next generation")
	require.Equal(t, want, membershipResults(t, proc, join))
	require.Equal(t, int64(16), join.OpAnalyzer.GetOpStats().MemorySize)
	join.Free(proc, false, nil)
	require.Zero(t, proc.Mp().CurrNB())
	require.Equal(t, int64(16), join.OpAnalyzer.GetOpStats().MemorySize, "Free must not erase historical allocation")
}

func TestMarkMembershipConstRHSDoesNotEarnWork(t *testing.T) {
	proc, join, _ := newMembershipOperators(t, mpool.MustNewZero())
	left := make([]int32, 33)
	for i := range left {
		left[i] = 2
	}
	join.GetChildren(0).(*colexec.MockOperator).WithBatchs([]*batch.Batch{makeInt32LoopJoinBatch(proc.Mp(), left)})
	require.NoError(t, join.Prepare(proc))
	vec, err := vector.NewConstFixed(types.T_int32.ToType(), int32(1), 256, proc.Mp())
	require.NoError(t, err)
	unpublished := batch.NewWithSize(1)
	unpublished.Vecs[0] = vec
	t.Cleanup(func() {
		if unpublished != nil {
			unpublished.Clean(proc.Mp())
		}
	})
	unpublished.SetRowCount(256)
	jm := message.NewJoinMap(message.GroupSels{}, nil, nil, nil, []*batch.Batch{unpublished}, proc.Mp())
	jm.SetRowCount(256)
	jm.IncRef(1)
	message.SendMessage(message.JoinMapMsg{JoinMapPtr: jm, Tag: 1}, proc.GetMessageBoard())
	unpublished = nil
	spy := &membershipEvalSpy{ExpressionExecutor: join.ctr.expr}
	join.ctr.expr = spy
	require.Equal(t, make([][2]bool, 33), membershipResults(t, proc, join))
	require.Equal(t, 33, spy.calls)
	require.Zero(t, join.ctr.mp.Size())
	require.Zero(t, join.OpAnalyzer.GetOpStats().MemorySize)
}

func TestMarkMembershipConstProbeEarnsRealWork(t *testing.T) {
	proc, join, build := newMembershipOperators(t, mpool.MustNewZero())
	membershipInputs(t, proc, join, build, make([]int32, 33), []int32{1, 0}, nil, []uint64{1})
	left := join.GetChildren(0).(*colexec.MockOperator).GetBatchs()[0]
	constant, err := vector.NewConstFixed(types.T_int32.ToType(), int32(2), 33, proc.Mp())
	require.NoError(t, err)
	left.Vecs[0].Free(proc.Mp())
	left.Vecs[0] = constant
	spy := &membershipEvalSpy{ExpressionExecutor: join.ctr.expr}
	join.ctr.expr = spy
	want := make([][2]bool, 33)
	for i := range want {
		want[i] = [2]bool{false, true}
	}
	require.Equal(t, want, membershipResults(t, proc, join))
	require.Equal(t, 32, spy.calls)
	require.Equal(t, int64(32), join.OpAnalyzer.GetOpStats().MemorySize)
}

func TestMarkMembershipSharedConsumersAccountOnce(t *testing.T) {
	proc, first, build := newMembershipOperators(t, mpool.MustNewZero())
	build.JoinMapRefCnt = 2
	left := make([]int32, 35)
	for i := range left {
		left[i] = 2
	}
	membershipInputs(t, proc, first, build, left, []int32{1, 0}, nil, []uint64{1})
	child := proc.NewContextChildProc(0)
	t.Cleanup(func() { child.Cancel(nil) })
	second := &LoopJoin{NonEqCond: membershipCondition(t, types.T_int32.ToType()), JoinType: plan.Node_MARK, JoinMapTag: 1,
		LeftTypes: first.LeftTypes, RightTypes: first.RightTypes, ResultCols: first.ResultCols}
	second.AppendChild(colexec.NewMockOperator())
	t.Cleanup(func() { second.Free(child, false, nil); second.GetChildren(0).Free(child, false, nil) })
	second.GetChildren(0).(*colexec.MockOperator).WithBatchs([]*batch.Batch{makeInt32LoopJoinBatch(proc.Mp(), left)})
	require.NoError(t, second.Prepare(child))
	joins := []*LoopJoin{first, second}
	procs := []*process.Process{proc, child}
	spies := make([]*membershipEvalSpy, 2)
	for i, j := range joins {
		spies[i] = &membershipEvalSpy{ExpressionExecutor: j.ctr.expr}
		j.ctr.expr = spies[i]
	}
	var wg sync.WaitGroup
	rows, unknowns := make([]int, 2), make([]int, 2)
	errs := make([]error, 2)
	start := make(chan struct{})
	for i := range joins {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for {
				r, e := vm.Exec(joins[i], procs[i])
				if e != nil {
					errs[i] = e
					return
				}
				if r.Batch == nil {
					return
				}
				rows[i] += r.Batch.RowCount()
				for row := range r.Batch.RowCount() {
					if r.Batch.Vecs[1].GetNulls().Contains(uint64(row)) {
						unknowns[i]++
					}
				}
			}
		}()
	}
	close(start)
	wg.Wait()
	for i := range joins {
		require.NoError(t, errs[i])
		require.Equal(t, 35, rows[i])
		require.Equal(t, 35, unknowns[i])
	}
	require.Same(t, first.ctr.mp, second.ctr.mp)
	size := first.ctr.mp.Size()
	require.Equal(t, int64(32), size)
	require.Equal(t, size, first.OpAnalyzer.GetOpStats().MemorySize+second.OpAnalyzer.GetOpStats().MemorySize)
	chargedBuilders := 0
	for _, join := range joins {
		if bytes := join.OpAnalyzer.GetOpStats().MemorySize; bytes != 0 {
			require.Equal(t, size, bytes)
			chargedBuilders++
		}
	}
	require.Equal(t, 1, chargedBuilders)
	require.GreaterOrEqual(t, spies[0].calls+spies[1].calls, 32)
	require.LessOrEqual(t, spies[0].calls+spies[1].calls, 33)
	jm := first.ctr.mp
	first.Free(proc, false, nil)
	require.Equal(t, size, jm.Size(), "a live sibling retains the shared index")
	second.Free(child, false, nil)
	require.Zero(t, jm.Size())
	require.Equal(t, size, first.OpAnalyzer.GetOpStats().MemorySize+second.OpAnalyzer.GetOpStats().MemorySize)
}

func TestMarkMembershipFinalProbeDoesNotBuild(t *testing.T) {
	for _, n := range []int{0, 1, 32, 33} {
		t.Run(strconv.Itoa(n), func(t *testing.T) {
			proc, join, build := newMembershipOperators(t, mpool.MustNewZero())
			left, want := make([]int32, n), make([][2]bool, n)
			for i := range left {
				left[i], want[i] = 2, [2]bool{false, true}
			}
			membershipInputs(t, proc, join, build, left, []int32{1, 0}, nil, []uint64{1})
			spy := &membershipEvalSpy{ExpressionExecutor: join.ctr.expr}
			join.ctr.expr = spy
			got := membershipResults(t, proc, join)
			require.Len(t, got, n)
			for i := range got {
				require.Equal(t, want[i], got[i])
			}
			require.Equal(t, min(n, 32), spy.calls)
			if n <= 32 {
				require.Zero(t, join.ctr.mp.Size(), "never construct for an absent future probe")
				require.Zero(t, join.OpAnalyzer.GetOpStats().MemorySize)
			} else {
				require.Equal(t, int64(32), join.OpAnalyzer.GetOpStats().MemorySize)
			}
		})
	}
}

func TestMarkMembershipWorkAcrossInputBatches(t *testing.T) {
	proc, join, build := newMembershipOperators(t, mpool.MustNewZero())
	left, want := membershipSwitchInput()
	membershipInputs(t, proc, join, build, left[:16], []int32{1, 0}, nil, []uint64{1})
	tail := makeInt32LoopJoinBatch(proc.Mp(), left[16:])
	join.GetChildren(0).(*colexec.MockOperator).WithBatchs([]*batch.Batch{tail})
	tail.Vecs[0].GetNulls().Add(18)
	spy := &membershipEvalSpy{ExpressionExecutor: join.ctr.expr}
	join.ctr.expr = spy
	require.Equal(t, want, membershipResults(t, proc, join))
	require.Equal(t, 32, spy.calls)
	require.Equal(t, int64(32), join.OpAnalyzer.GetOpStats().MemorySize)
}

func TestMarkMembershipAccountingSurvivesLaterOutputError(t *testing.T) {
	mp, err := mpool.NewMPool("mark-output-after-publication", 1<<20, mpool.NoFixed)
	require.NoError(t, err)
	proc, join, build := newMembershipOperators(t, mp)
	left, _ := membershipSwitchInput()
	membershipInputs(t, proc, join, build, left, []int32{1, 0}, []uint64{34}, []uint64{1})
	join.GetChildren(0).(*colexec.MockOperator).WithBatchs([]*batch.Batch{makeInt32LoopJoinBatch(mp, []int32{1})})
	spy := &membershipEvalSpy{ExpressionExecutor: join.ctr.expr}
	join.ctr.expr = spy
	res, err := vm.Exec(join, proc)
	require.NoError(t, err)
	require.Equal(t, 35, res.Batch.RowCount())
	require.Equal(t, int64(32), join.OpAnalyzer.GetOpStats().MemorySize)
	join.ctr.resBat.Vecs[1].Free(mp)
	join.ctr.resBat.Vecs[1] = vector.NewOffHeapVecWithType(types.T_bool.ToType())
	blocker, err := mp.Alloc(int(mp.Cap()-mp.CurrNB()), true)
	require.NoError(t, err)
	t.Cleanup(func() { mp.Free(blocker) })
	res, err = vm.Exec(join, proc)
	require.ErrorContains(t, err, "mpool out of space")
	require.Nil(t, res.Batch)
	require.Equal(t, int64(32), join.ctr.mp.Size())
	require.Equal(t, int64(32), join.OpAnalyzer.GetOpStats().MemorySize)
	require.Equal(t, 32, spy.calls, "mandatory output failure cannot restart the evaluator")
}

type membershipPublishedContext struct {
	context.Context
	published func() bool
	cancel    context.CancelFunc
}

func (ctx *membershipPublishedContext) Err() error {
	if ctx.published() {
		ctx.cancel()
	}
	return ctx.Context.Err()
}

func TestMarkMembershipPublishedAllocationSurvivesCancellation(t *testing.T) {
	proc, join, build := newMembershipOperators(t, mpool.MustNewZero())
	left, _ := membershipSwitchInput()
	membershipInputs(t, proc, join, build, left, []int32{1, 0}, []uint64{34}, []uint64{1})
	base, cancel := context.WithCancel(proc.Ctx)
	defer cancel()
	proc.Ctx = &membershipPublishedContext{Context: base, cancel: cancel, published: func() bool { return join.ctr.mp != nil && join.ctr.mp.Size() != 0 }}
	_, err := vm.Exec(join, proc)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, int64(32), join.ctr.mp.Size())
	require.Equal(t, int64(32), join.OpAnalyzer.GetOpStats().MemorySize)
	join.Free(proc, false, nil)
	require.Zero(t, proc.Mp().CurrNB())
	require.Equal(t, int64(32), join.OpAnalyzer.GetOpStats().MemorySize)
}
