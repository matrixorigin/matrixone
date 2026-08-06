// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package loopjoin

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	Rows          = 10     // default rows
	BenchmarkRows = 100000 // default rows for benchmark
)

// add unit tests for cases
type joinTestCase struct {
	arg         *LoopJoin
	flgs        []bool // flgs[i] == true: nullable
	types       []types.Type
	proc        *process.Process
	cancel      context.CancelFunc
	barg        *hashbuild.HashBuild
	resultBatch *batch.Batch
}

type loopJoinTestAllocationOwner interface {
	SetAllocationAccount(*mpool.AllocationAccount) error
}

func installLoopJoinTestAllocation(
	t testing.TB,
	owners ...loopJoinTestAllocationOwner,
) *mpool.AllocationAccount {
	t.Helper()
	registry, err := mpool.NewAllocationAccountRegistry(1, 4_096)
	require.NoError(t, err)
	account, err := registry.Open(1 << 60)
	require.NoError(t, err)
	for _, owner := range owners {
		require.NoError(t, owner.SetAllocationAccount(account))
	}
	return account
}

func TestLoopJoinResultBatchUsesAllocationAccount(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	arg := &LoopJoin{
		ResultCols: []colexec.ResultPos{{Rel: 0, Pos: 0}},
		LeftTypes:  []types.Type{types.T_int64.ToType()},
	}
	account := installLoopJoinTestAllocation(t, arg)
	require.NoError(t, arg.resetResultBat())
	require.Same(t, arg.resultAllocation, arg.ctr.resBat.Vecs[0].AllocationAccountSelection())
	require.NoError(t, vector.AppendFixed(arg.ctr.resBat.Vecs[0], int64(1), false, proc.Mp()))
	used := account.Snapshot().Used
	require.Positive(t, used)
	require.NoError(t, arg.resetResultBat())
	require.Equal(t, used, account.Snapshot().Used)

	arg.Reset(proc, false, nil)
	require.Nil(t, arg.ctr.resBat)
	require.Zero(t, account.Snapshot().Used)
	require.NoError(t, arg.ClearAllocationAccount(account))
}

func TestLoopJoinResultBatchHonorsAllocationCapacity(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	registry, err := mpool.NewAllocationAccountRegistry(1, 16)
	require.NoError(t, err)
	account, err := registry.Open(1)
	require.NoError(t, err)
	arg := &LoopJoin{
		ResultCols: []colexec.ResultPos{{Rel: 0, Pos: 0}},
		LeftTypes:  []types.Type{types.T_int64.ToType()},
	}
	require.NoError(t, arg.SetAllocationAccount(account))
	require.NoError(t, arg.resetResultBat())
	err = vector.AppendFixed(arg.ctr.resBat.Vecs[0], int64(1), false, proc.Mp())
	require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
	require.Zero(t, account.Snapshot().Used)
	arg.Reset(proc, false, nil)
	require.NoError(t, arg.ClearAllocationAccount(account))
}

var (
	tag int32
)

func makeTestCases(t *testing.T) []joinTestCase {
	return []joinTestCase{
		newTestCase(t, []bool{false}, []types.Type{types.T_int32.ToType()}, []colexec.ResultPos{colexec.NewResultPos(0, 0), colexec.NewResultPos(1, 0)}),
	}
}

func TestString(t *testing.T) {
	buf := new(bytes.Buffer)
	for _, tc := range makeTestCases(t) {
		tc.arg.String(buf)
	}
}

func TestResetRebuildsExpressionForNextAllocationGeneration(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	join := &LoopJoin{
		NonEqCond: &plan.Expr{
			Typ: plan.Type{Id: int32(types.T_bool)},
			Expr: &plan.Expr_Lit{Lit: &plan.Literal{
				Value: &plan.Literal_Bval{Bval: true},
			}},
		},
	}
	registry, err := mpool.NewAllocationAccountRegistry(1, 8)
	require.NoError(t, err)

	for range 2 {
		account, openErr := registry.Open(1 << 20)
		require.NoError(t, openErr)
		require.NoError(t, join.SetAllocationAccount(account))
		require.NoError(t, join.Prepare(proc))
		require.NotNil(t, join.ctr.expr)

		join.Reset(proc, false, nil)
		require.Nil(t, join.ctr.expr)
		require.NoError(t, join.ClearAllocationAccount(account))
		terminal, _, terminalErr := registry.CompleteTerminal(account)
		require.NoError(t, terminalErr)
		require.Equal(t, mpool.AllocationAccountTerminalValid, terminal.State)
	}
}

func TestJoin(t *testing.T) {
	for _, tc := range makeTestCases(t) {

		resetChildren(tc.arg, tc.proc.Mp())
		resetHashBuildChildren(tc.barg, tc.proc.Mp())
		err := tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		err = tc.barg.Prepare(tc.proc)
		require.NoError(t, err)

		res, err := vm.Exec(tc.barg, tc.proc)
		require.NoError(t, err)
		require.Equal(t, res.Batch == nil, true)
		res, err = vm.Exec(tc.arg, tc.proc)
		require.NoError(t, err)
		require.Equal(t, res.Batch.RowCount(), tc.resultBatch.RowCount())
		require.Equal(t, len(res.Batch.Vecs), len(tc.resultBatch.Vecs))
		for i := range res.Batch.Vecs {
			vec1 := res.Batch.Vecs[i]
			vec2 := tc.resultBatch.Vecs[i]
			require.Equal(t, vec1.GetType().Oid, vec2.GetType().Oid)
			require.Equal(t, bytes.Compare(vec1.GetArea(), vec2.GetArea()), 0)
			require.Equal(t, bytes.Compare(vec1.UnsafeGetRawData(), vec2.UnsafeGetRawData()), 0)
		}

		tc.arg.Reset(tc.proc, false, nil)
		tc.barg.Reset(tc.proc, false, nil)

		resetChildren(tc.arg, tc.proc.Mp())
		resetHashBuildChildren(tc.barg, tc.proc.Mp())
		tc.proc.GetMessageBoard().Reset()
		err = tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		err = tc.barg.Prepare(tc.proc)
		require.NoError(t, err)

		res, err = vm.Exec(tc.barg, tc.proc)
		require.NoError(t, err)
		require.Equal(t, res.Batch == nil, true)
		res, err = vm.Exec(tc.arg, tc.proc)
		require.NoError(t, err)
		require.Equal(t, res.Batch.RowCount(), tc.resultBatch.RowCount())
		require.Equal(t, len(res.Batch.Vecs), len(tc.resultBatch.Vecs))
		for i := range res.Batch.Vecs {
			vec1 := res.Batch.Vecs[i]
			vec2 := tc.resultBatch.Vecs[i]
			require.Equal(t, vec1.GetType().Oid, vec2.GetType().Oid)
			require.Equal(t, bytes.Compare(vec1.GetArea(), vec2.GetArea()), 0)
			require.Equal(t, bytes.Compare(vec1.UnsafeGetRawData(), vec2.UnsafeGetRawData()), 0)
		}

		tc.arg.Reset(tc.proc, false, nil)
		tc.barg.Reset(tc.proc, false, nil)

		tc.arg.Free(tc.proc, false, nil)
		tc.barg.Free(tc.proc, false, nil)
		tc.proc.Free()
		require.Equal(t, int64(0), tc.proc.Mp().CurrNB())
	}
}

func TestLoopJoinPassesRecursiveMarker(t *testing.T) {
	tc := newTestCase(t, []bool{false}, []types.Type{types.T_int32.ToType()}, []colexec.ResultPos{
		colexec.NewResultPos(0, 0),
		colexec.NewResultPos(1, 0),
	})
	defer func() {
		tc.arg.Free(tc.proc, false, nil)
		tc.barg.Free(tc.proc, false, nil)
		tc.proc.Free()
		tc.cancel()
	}()
	marker := colexec.MakeMockBatchs(tc.proc.Mp())
	marker.SetLast()
	resetChildrenWithBatch(tc.arg, marker)
	resetHashBuildChildren(tc.barg, tc.proc.Mp())
	require.NoError(t, tc.arg.Prepare(tc.proc))
	require.NoError(t, tc.barg.Prepare(tc.proc))

	res, err := vm.Exec(tc.barg, tc.proc)
	require.NoError(t, err)
	require.Nil(t, res.Batch)
	res, err = vm.Exec(tc.arg, tc.proc)
	require.NoError(t, err)
	require.Same(t, marker, res.Batch)
	require.True(t, res.Batch.Last())
}

type recursiveLoopJoinProbe struct {
	*colexec.MockOperator
}

func (source *recursiveLoopJoinProbe) OpType() vm.OpType {
	return vm.MergeRecursive
}

func TestLoopJoinPassesRecursiveMarkerWithEmptyBuild(t *testing.T) {
	tc := newTestCase(t, []bool{false}, []types.Type{types.T_int32.ToType()}, []colexec.ResultPos{
		colexec.NewResultPos(0, 0),
		colexec.NewResultPos(1, 0),
	})
	marker := colexec.MakeMockBatchs(tc.proc.Mp())
	marker.SetLast()
	probeCalls := 0
	probe := &recursiveLoopJoinProbe{MockOperator: colexec.NewMockOperator().
		WithBatchs([]*batch.Batch{colexec.MakeMockBatchs(tc.proc.Mp()), marker}).
		WithBatchCallback(func(int) { probeCalls++ })}
	tc.arg.Children = nil
	tc.arg.AppendChild(probe)
	resetHashBuildChildrenWithBatch(tc.barg, batch.EmptyBatch)
	defer func() {
		tc.arg.Free(tc.proc, false, nil)
		tc.barg.Free(tc.proc, false, nil)
		probe.Free(tc.proc, false, nil)
		tc.proc.Free()
		tc.cancel()
	}()

	require.NoError(t, tc.arg.Prepare(tc.proc))
	require.NoError(t, tc.barg.Prepare(tc.proc))
	res, err := vm.Exec(tc.barg, tc.proc)
	require.NoError(t, err)
	require.Nil(t, res.Batch)
	res, err = vm.Exec(tc.arg, tc.proc)
	require.NoError(t, err)
	require.Same(t, marker, res.Batch)
	require.Equal(t, 2, probeCalls)
}

func TestLoopJoinPrepareRecomputesRecursiveProbeForFastPath(t *testing.T) {
	tc := newTestCase(t, []bool{false}, []types.Type{types.T_int32.ToType()}, []colexec.ResultPos{
		colexec.NewResultPos(0, 0),
		colexec.NewResultPos(1, 0),
	})
	probeCalls := 0
	probe := colexec.NewMockOperator().
		WithBatchs([]*batch.Batch{colexec.MakeMockBatchs(tc.proc.Mp())}).
		WithBatchCallback(func(int) { probeCalls++ })
	tc.arg.Children = nil
	tc.arg.AppendChild(probe)
	// Model a reused operator whose previous generation had a recursive probe.
	tc.arg.recursiveProbe = true
	resetHashBuildChildrenWithBatch(tc.barg, batch.EmptyBatch)
	defer func() {
		tc.arg.Free(tc.proc, false, nil)
		tc.barg.Free(tc.proc, false, nil)
		probe.Free(tc.proc, false, nil)
		tc.proc.Free()
		tc.cancel()
	}()

	require.NoError(t, tc.arg.Prepare(tc.proc))
	require.False(t, tc.arg.recursiveProbe)
	require.NoError(t, tc.barg.Prepare(tc.proc))
	res, err := vm.Exec(tc.barg, tc.proc)
	require.NoError(t, err)
	require.Nil(t, res.Batch)
	res, err = vm.Exec(tc.arg, tc.proc)
	require.NoError(t, err)
	require.Nil(t, res.Batch)
	require.Zero(t, probeCalls)
}

func TestLoopJoinResetAfterEmptyProbe(t *testing.T) {
	tc := newTestCase(t, []bool{false}, []types.Type{types.T_int32.ToType()}, []colexec.ResultPos{
		colexec.NewResultPos(0, 0),
		colexec.NewResultPos(1, 0),
	})
	tc.arg.JoinType = plan.Node_LEFT

	resetChildrenWithBatch(tc.arg, colexec.MakeMockBatchs(tc.proc.Mp()))
	resetHashBuildChildrenWithBatch(tc.barg, batch.EmptyBatch)
	err := tc.arg.Prepare(tc.proc)
	require.NoError(t, err)
	err = tc.barg.Prepare(tc.proc)
	require.NoError(t, err)

	res, err := vm.Exec(tc.barg, tc.proc)
	require.NoError(t, err)
	require.Nil(t, res.Batch)
	res, err = vm.Exec(tc.arg, tc.proc)
	require.NoError(t, err)
	require.NotNil(t, res.Batch)
	require.True(t, res.Batch.Vecs[1].IsConst())

	tc.arg.Reset(tc.proc, false, nil)
	tc.barg.Reset(tc.proc, false, nil)

	resetChildrenWithBatch(tc.arg, colexec.MakeMockBatchs(tc.proc.Mp()))
	resetHashBuildChildrenWithBatch(tc.barg, colexec.MakeMockBatchs(tc.proc.Mp()))
	tc.proc.GetMessageBoard().Reset()
	err = tc.arg.Prepare(tc.proc)
	require.NoError(t, err)
	err = tc.barg.Prepare(tc.proc)
	require.NoError(t, err)

	res, err = vm.Exec(tc.barg, tc.proc)
	require.NoError(t, err)
	require.Nil(t, res.Batch)
	res, err = vm.Exec(tc.arg, tc.proc)
	require.NoError(t, err)
	require.NotNil(t, res.Batch)
	require.False(t, res.Batch.Vecs[1].IsConst())
	require.Equal(t, 2, res.Batch.Vecs[1].Length())

	tc.arg.Reset(tc.proc, false, nil)
	tc.barg.Reset(tc.proc, false, nil)
	tc.arg.Free(tc.proc, false, nil)
	tc.barg.Free(tc.proc, false, nil)
	tc.proc.Free()
	require.Equal(t, int64(0), tc.proc.Mp().CurrNB())
}

func TestLoopJoinConstNullAfterNonEmptyProbe(t *testing.T) {
	tc := newTestCase(t, []bool{false}, []types.Type{types.T_int32.ToType()}, []colexec.ResultPos{
		colexec.NewResultPos(0, 0),
		colexec.NewResultPos(1, 0),
	})
	tc.arg.JoinType = plan.Node_LEFT

	resetChildrenWithBatch(tc.arg, colexec.MakeMockBatchs(tc.proc.Mp()))
	resetHashBuildChildrenWithBatch(tc.barg, colexec.MakeMockBatchs(tc.proc.Mp()))
	err := tc.arg.Prepare(tc.proc)
	require.NoError(t, err)
	err = tc.barg.Prepare(tc.proc)
	require.NoError(t, err)

	res, err := vm.Exec(tc.barg, tc.proc)
	require.NoError(t, err)
	require.Nil(t, res.Batch)
	res, err = vm.Exec(tc.arg, tc.proc)
	require.NoError(t, err)
	require.NotNil(t, res.Batch)
	require.False(t, res.Batch.Vecs[1].IsConst())

	tc.arg.Reset(tc.proc, false, nil)
	tc.barg.Reset(tc.proc, false, nil)

	resetChildrenWithBatch(tc.arg, colexec.MakeMockBatchs(tc.proc.Mp()))
	resetHashBuildChildrenWithBatch(tc.barg, batch.EmptyBatch)
	tc.proc.GetMessageBoard().Reset()
	err = tc.arg.Prepare(tc.proc)
	require.NoError(t, err)
	err = tc.barg.Prepare(tc.proc)
	require.NoError(t, err)

	res, err = vm.Exec(tc.barg, tc.proc)
	require.NoError(t, err)
	require.Nil(t, res.Batch)
	res, err = vm.Exec(tc.arg, tc.proc)
	require.NoError(t, err)
	require.NotNil(t, res.Batch)
	require.True(t, res.Batch.Vecs[1].IsConstNull())
	require.Equal(t, 2, res.Batch.Vecs[1].Length())

	tc.arg.Reset(tc.proc, false, nil)
	tc.barg.Reset(tc.proc, false, nil)
	tc.arg.Free(tc.proc, false, nil)
	tc.barg.Free(tc.proc, false, nil)
	tc.proc.Free()
	require.Equal(t, int64(0), tc.proc.Mp().CurrNB())
}

func TestLoopJoinSingleRejectsMultipleRows(t *testing.T) {
	for _, withCondition := range []bool{true, false} {
		name := "without condition"
		if withCondition {
			name = "with condition"
		}
		t.Run(name, func(t *testing.T) {
			tc := newTestCase(t,
				[]bool{false},
				[]types.Type{types.T_int32.ToType()},
				[]colexec.ResultPos{colexec.NewResultPos(0, 0)})
			tc.arg.JoinType = plan.Node_SINGLE
			if !withCondition {
				tc.arg.NonEqCond = nil
			}

			resetChildrenWithBatch(tc.arg, makeInt32LoopJoinBatch(tc.proc.Mp(), []int32{1}))
			resetHashBuildChildrenWithBatch(tc.barg, makeInt32LoopJoinBatch(tc.proc.Mp(), []int32{1, 1}))
			require.NoError(t, tc.arg.Prepare(tc.proc))
			require.NoError(t, tc.barg.Prepare(tc.proc))

			_, err := vm.Exec(tc.barg, tc.proc)
			require.NoError(t, err)
			_, err = vm.Exec(tc.arg, tc.proc)
			require.Error(t, err)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrSubqueryNo1Row))

			tc.arg.Reset(tc.proc, true, err)
			tc.barg.Reset(tc.proc, true, err)
			tc.arg.Free(tc.proc, true, err)
			tc.barg.Free(tc.proc, true, err)
			tc.proc.Free()
			require.Equal(t, int64(0), tc.proc.Mp().CurrNB())
		})
	}
}

func TestLoopJoinFinalizeResetsAfterPreviousEmptyProbe(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	proc.SetMessageBoard(message.NewMessageBoard())

	int32Type := types.T_int32.ToType()

	tag++
	join := &LoopJoin{
		ResultCols: []colexec.ResultPos{colexec.NewResultPos(0, 0), colexec.NewResultPos(1, 0)},
		LeftTypes:  []types.Type{int32Type},
		RightTypes: []types.Type{int32Type},
		JoinType:   plan.Node_OUTER,
		OperatorBase: vm.OperatorBase{
			OperatorInfo: vm.OperatorInfo{
				Idx: 1,
			},
		},
		JoinMapTag: tag,
	}
	build := &hashbuild.HashBuild{
		NeedBatches:   true,
		JoinMapTag:    tag,
		JoinMapRefCnt: 1,
		OperatorBase: vm.OperatorBase{
			OperatorInfo: vm.OperatorInfo{
				Idx: 0,
			},
		},
	}
	installLoopJoinTestAllocation(t, join, build)

	resetChildrenWithBatch(join, makeInt32LoopJoinBatch(proc.Mp(), []int32{7}))
	resetHashBuildChildrenWithBatch(build, batch.EmptyBatch)
	require.NoError(t, join.Prepare(proc))
	require.NoError(t, build.Prepare(proc))

	res, err := vm.Exec(build, proc)
	require.NoError(t, err)
	require.Nil(t, res.Batch)
	res, err = vm.Exec(join, proc)
	require.NoError(t, err)
	require.NotNil(t, res.Batch)
	require.True(t, res.Batch.Vecs[1].IsConst())

	join.Reset(proc, false, nil)
	build.Reset(proc, false, nil)

	resetChildrenWithBatch(join, batch.EmptyBatch)
	resetHashBuildChildrenWithBatch(build, makeInt32LoopJoinBatch(proc.Mp(), []int32{10, 20}))
	proc.GetMessageBoard().Reset()
	require.NoError(t, join.Prepare(proc))
	require.NoError(t, build.Prepare(proc))

	res, err = vm.Exec(build, proc)
	require.NoError(t, err)
	require.Nil(t, res.Batch)
	res, err = vm.Exec(join, proc)
	require.NoError(t, err)
	require.NotNil(t, res.Batch)
	require.Equal(t, 2, res.Batch.RowCount())
	require.False(t, res.Batch.Vecs[1].IsConst())
	require.Equal(t, []int32{10, 20}, vector.MustFixedColNoTypeCheck[int32](res.Batch.Vecs[1])[:2])
	require.True(t, res.Batch.Vecs[0].GetNulls().Contains(0))
	require.True(t, res.Batch.Vecs[0].GetNulls().Contains(1))

	join.Reset(proc, false, nil)
	build.Reset(proc, false, nil)
	join.Free(proc, false, nil)
	build.Free(proc, false, nil)
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestMarkJoinEmitsOneRowPerProbeRowAcrossBuildBatches(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	proc.SetMessageBoard(message.NewMessageBoard())

	int32Type := types.T_int32.ToType()
	fr, err := function.GetFunctionByName(context.Background(), "=", []types.Type{int32Type, int32Type})
	require.NoError(t, err)
	fid := fr.GetEncodedOverloadID()
	cond := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_bool)},
		Expr: &plan.Expr_F{F: &plan.Function{
			Args: []*plan.Expr{
				{
					Typ: plan.Type{Id: int32(types.T_int32)},
					Expr: &plan.Expr_Col{Col: &plan.ColRef{
						RelPos: 0,
						ColPos: 0,
					}},
				},
				{
					Typ: plan.Type{Id: int32(types.T_int32)},
					Expr: &plan.Expr_Col{Col: &plan.ColRef{
						RelPos: 1,
						ColPos: 0,
					}},
				},
			},
			Func: &plan.ObjectRef{Obj: fid, ObjName: "="},
		}},
	}

	tag++
	join := &LoopJoin{
		NonEqCond:  cond,
		ResultCols: []colexec.ResultPos{colexec.NewResultPos(0, 0), colexec.NewResultPos(-1, 0)},
		LeftTypes:  []types.Type{int32Type},
		RightTypes: []types.Type{int32Type},
		JoinType:   plan.Node_MARK,
		OperatorBase: vm.OperatorBase{
			OperatorInfo: vm.OperatorInfo{
				Idx: 1,
			},
		},
		JoinMapTag: tag,
	}
	join.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{
		makeInt32LoopJoinBatch(proc.Mp(), []int32{1, 4}),
	}))

	build := &hashbuild.HashBuild{
		NeedBatches:   true,
		JoinMapTag:    tag,
		JoinMapRefCnt: 1,
		OperatorBase: vm.OperatorBase{
			OperatorInfo: vm.OperatorInfo{
				Idx: 0,
			},
		},
	}
	installLoopJoinTestAllocation(t, join, build)
	build.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{
		makeInt32LoopJoinBatch(proc.Mp(), []int32{1}),
		makeInt32LoopJoinBatch(proc.Mp(), []int32{1}),
	}))

	require.NoError(t, join.Prepare(proc))
	require.NoError(t, build.Prepare(proc))

	res, err := vm.Exec(build, proc)
	require.NoError(t, err)
	require.Nil(t, res.Batch)

	res, err = vm.Exec(join, proc)
	require.NoError(t, err)
	require.NotNil(t, res.Batch)
	require.Equal(t, 2, res.Batch.RowCount())
	require.Equal(t, []int32{1, 4}, vector.MustFixedColNoTypeCheck[int32](res.Batch.Vecs[0])[:2])
	require.Equal(t, []bool{true, false}, vector.MustFixedColNoTypeCheck[bool](res.Batch.Vecs[1])[:2])
	require.False(t, res.Batch.Vecs[1].GetNulls().Contains(0))
	require.False(t, res.Batch.Vecs[1].GetNulls().Contains(1))

	join.Free(proc, false, nil)
	build.Free(proc, false, nil)
	proc.Free()
}

func TestMarkJoinResumesAfterDefaultBatchSize(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	proc.SetMessageBoard(message.NewMessageBoard())

	int32Type := types.T_int32.ToType()
	fr, err := function.GetFunctionByName(context.Background(), "=", []types.Type{int32Type, int32Type})
	require.NoError(t, err)
	fid := fr.GetEncodedOverloadID()
	cond := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_bool)},
		Expr: &plan.Expr_F{F: &plan.Function{
			Args: []*plan.Expr{
				{
					Typ: plan.Type{Id: int32(types.T_int32)},
					Expr: &plan.Expr_Col{Col: &plan.ColRef{
						RelPos: 0,
						ColPos: 0,
					}},
				},
				{
					Typ: plan.Type{Id: int32(types.T_int32)},
					Expr: &plan.Expr_Col{Col: &plan.ColRef{
						RelPos: 1,
						ColPos: 0,
					}},
				},
			},
			Func: &plan.ObjectRef{Obj: fid, ObjName: "="},
		}},
	}

	tag++
	join := &LoopJoin{
		NonEqCond:  cond,
		ResultCols: []colexec.ResultPos{colexec.NewResultPos(0, 0), colexec.NewResultPos(-1, 0)},
		LeftTypes:  []types.Type{int32Type},
		RightTypes: []types.Type{int32Type},
		JoinType:   plan.Node_MARK,
		OperatorBase: vm.OperatorBase{
			OperatorInfo: vm.OperatorInfo{
				Idx: 1,
			},
		},
		JoinMapTag: tag,
	}
	leftVals := make([]int32, colexec.DefaultBatchSize+1)
	for i := range leftVals {
		leftVals[i] = int32(i)
	}
	join.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{
		makeInt32LoopJoinBatch(proc.Mp(), leftVals),
	}))

	build := &hashbuild.HashBuild{
		NeedBatches:   true,
		JoinMapTag:    tag,
		JoinMapRefCnt: 1,
		OperatorBase: vm.OperatorBase{
			OperatorInfo: vm.OperatorInfo{
				Idx: 0,
			},
		},
	}
	installLoopJoinTestAllocation(t, join, build)
	build.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{
		makeInt32LoopJoinBatch(proc.Mp(), []int32{-1}),
	}))

	require.NoError(t, join.Prepare(proc))
	require.NoError(t, build.Prepare(proc))

	res, err := vm.Exec(build, proc)
	require.NoError(t, err)
	require.Nil(t, res.Batch)

	res, err = vm.Exec(join, proc)
	require.NoError(t, err)
	require.NotNil(t, res.Batch)
	require.Equal(t, colexec.DefaultBatchSize, res.Batch.RowCount())
	require.Equal(t, int32(0), vector.MustFixedColNoTypeCheck[int32](res.Batch.Vecs[0])[0])
	require.Equal(t, int32(colexec.DefaultBatchSize-1), vector.MustFixedColNoTypeCheck[int32](res.Batch.Vecs[0])[colexec.DefaultBatchSize-1])

	res, err = vm.Exec(join, proc)
	require.NoError(t, err)
	require.NotNil(t, res.Batch)
	require.Equal(t, 1, res.Batch.RowCount())
	require.Equal(t, int32(colexec.DefaultBatchSize), vector.MustFixedColNoTypeCheck[int32](res.Batch.Vecs[0])[0])
	require.Equal(t, []bool{false}, vector.MustFixedColNoTypeCheck[bool](res.Batch.Vecs[1])[:1])
	require.False(t, res.Batch.Vecs[1].GetNulls().Contains(0))

	join.Free(proc, false, nil)
	build.Free(proc, false, nil)
	proc.Free()
}

func TestLoopJoinNoCondSplitsLargeBuildBatch(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())

	const extraRows = 17
	buildValues := make([]string, colexec.DefaultBatchSize+extraRows)
	for i := range buildValues {
		buildValues[i] = strings.Repeat("x", 128)
	}
	buildBat := makeVarcharLoopJoinBatch(proc.Mp(), buildValues)
	joinMap := message.NewJoinMap(
		message.GroupSels{}, nil, nil, nil, []*batch.Batch{buildBat}, proc.Mp())
	joinMap.IncRef(1)

	join := &LoopJoin{
		ResultCols: []colexec.ResultPos{
			colexec.NewResultPos(0, 0),
			colexec.NewResultPos(1, 0),
		},
		LeftTypes:  []types.Type{types.T_varchar.ToType()},
		RightTypes: []types.Type{types.T_varchar.ToType()},
		JoinType:   plan.Node_LEFT,
	}
	installLoopJoinTestAllocation(t, join)
	join.ctr.state = Probe
	join.ctr.mp = joinMap
	join.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{
		makeVarcharLoopJoinBatch(proc.Mp(), []string{"probe"}),
	}))
	require.NoError(t, join.Prepare(proc))

	result, err := join.Call(proc)
	require.NoError(t, err)
	require.NotNil(t, result.Batch)
	require.Equal(t, colexec.DefaultBatchSize, result.Batch.RowCount())
	require.Equal(t, "probe", result.Batch.Vecs[0].GetStringAt(0))
	require.Equal(t, buildValues[0], result.Batch.Vecs[1].GetStringAt(0))

	result, err = join.Call(proc)
	require.NoError(t, err)
	require.NotNil(t, result.Batch)
	require.Equal(t, extraRows, result.Batch.RowCount())
	require.Equal(t, "probe", result.Batch.Vecs[0].GetStringAt(extraRows-1))
	require.Equal(t, buildValues[len(buildValues)-1], result.Batch.Vecs[1].GetStringAt(extraRows-1))

	result, err = join.Call(proc)
	require.NoError(t, err)
	require.Nil(t, result.Batch)

	join.Free(proc, false, nil)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestLoopJoinNonEqCondSplitsLargeBuildBatch(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())

	const extraRows = colexec.DefaultBatchSize
	buildValues := make([]int32, colexec.DefaultBatchSize+extraRows)
	for i := range buildValues {
		buildValues[i] = 7
	}
	buildBat := makeInt32LoopJoinBatch(proc.Mp(), buildValues)
	joinMap := message.NewJoinMap(
		message.GroupSels{}, nil, nil, nil, []*batch.Batch{buildBat}, proc.Mp())
	joinMap.IncRef(1)

	int32Type := types.T_int32.ToType()
	fr, err := function.GetFunctionByName(context.Background(), "=", []types.Type{int32Type, int32Type})
	require.NoError(t, err)
	join := &LoopJoin{
		NonEqCond: &plan.Expr{
			Typ: plan.Type{Id: int32(types.T_bool)},
			Expr: &plan.Expr_F{F: &plan.Function{
				Args: []*plan.Expr{
					{Typ: plan.Type{Id: int32(types.T_int32)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: 0}}},
					{Typ: plan.Type{Id: int32(types.T_int32)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 1, ColPos: 0}}},
				},
				Func: &plan.ObjectRef{Obj: fr.GetEncodedOverloadID(), ObjName: "="},
			}},
		},
		ResultCols: []colexec.ResultPos{
			colexec.NewResultPos(0, 0),
			colexec.NewResultPos(1, 0),
		},
		LeftTypes:  []types.Type{int32Type},
		RightTypes: []types.Type{int32Type},
		JoinType:   plan.Node_INNER,
	}
	installLoopJoinTestAllocation(t, join)
	join.ctr.state = Probe
	join.ctr.mp = joinMap
	join.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{
		makeInt32LoopJoinBatch(proc.Mp(), []int32{7}),
	}))
	require.NoError(t, join.Prepare(proc))

	result, err := join.Call(proc)
	require.NoError(t, err)
	require.NotNil(t, result.Batch)
	require.Equal(t, colexec.DefaultBatchSize, result.Batch.RowCount())

	result, err = join.Call(proc)
	require.NoError(t, err)
	require.NotNil(t, result.Batch)
	require.Equal(t, extraRows, result.Batch.RowCount())

	result, err = join.Call(proc)
	require.NoError(t, err)
	require.Nil(t, result.Batch)

	join.Free(proc, false, nil)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestLoopJoinNoCondSplitsWideRowsByBytes(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())

	const byteLimit = 1024
	buildValues := make([]string, 20)
	for i := range buildValues {
		buildValues[i] = strings.Repeat("x", 128)
	}
	// A row larger than the byte budget must still make progress as a
	// one-row batch; subsequent rows are packed within the budget.
	buildValues[0] = strings.Repeat("y", byteLimit*2)
	buildBat := makeVarcharLoopJoinBatch(proc.Mp(), buildValues)
	joinMap := message.NewJoinMap(
		message.GroupSels{}, nil, nil, nil, []*batch.Batch{buildBat}, proc.Mp())
	joinMap.IncRef(1)

	join := &LoopJoin{
		ResultCols: []colexec.ResultPos{
			colexec.NewResultPos(0, 0),
			colexec.NewResultPos(1, 0),
		},
		LeftTypes:  []types.Type{types.T_varchar.ToType()},
		RightTypes: []types.Type{types.T_varchar.ToType()},
		JoinType:   plan.Node_LEFT,
	}
	installLoopJoinTestAllocation(t, join)
	join.ctr.state = Probe
	join.ctr.mp = joinMap
	join.ctr.resultBatchByteLimit = byteLimit
	join.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{
		makeVarcharLoopJoinBatch(proc.Mp(), []string{"probe"}),
	}))
	require.NoError(t, join.Prepare(proc))

	totalRows := 0
	batchCount := 0
	for {
		result, err := join.Call(proc)
		require.NoError(t, err)
		if result.Batch == nil {
			break
		}
		require.Positive(t, result.Batch.RowCount())
		if result.Batch.Size() > byteLimit {
			require.Equal(t, 1, result.Batch.RowCount())
		} else {
			require.LessOrEqual(t, result.Batch.Size(), byteLimit)
		}
		totalRows += result.Batch.RowCount()
		batchCount++
	}
	require.Equal(t, len(buildValues), totalRows)
	require.Greater(t, batchCount, 1)

	join.Free(proc, false, nil)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestLoopJoinNonEqCondSplitsWideRowsByBytes(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())

	const byteLimit = 1024
	value := strings.Repeat("x", 128)
	buildValues := make([]string, 20)
	for i := range buildValues {
		buildValues[i] = value
	}
	buildBat := makeVarcharLoopJoinBatch(proc.Mp(), buildValues)
	joinMap := message.NewJoinMap(
		message.GroupSels{}, nil, nil, nil, []*batch.Batch{buildBat}, proc.Mp())
	joinMap.IncRef(1)

	varcharType := types.T_varchar.ToType()
	fr, err := function.GetFunctionByName(context.Background(), "=", []types.Type{varcharType, varcharType})
	require.NoError(t, err)
	join := &LoopJoin{
		NonEqCond: &plan.Expr{
			Typ: plan.Type{Id: int32(types.T_bool)},
			Expr: &plan.Expr_F{F: &plan.Function{
				Args: []*plan.Expr{
					{Typ: plan.Type{Id: int32(types.T_varchar)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: 0}}},
					{Typ: plan.Type{Id: int32(types.T_varchar)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 1, ColPos: 0}}},
				},
				Func: &plan.ObjectRef{Obj: fr.GetEncodedOverloadID(), ObjName: "="},
			}},
		},
		ResultCols: []colexec.ResultPos{
			colexec.NewResultPos(0, 0),
			colexec.NewResultPos(1, 0),
		},
		LeftTypes:  []types.Type{varcharType},
		RightTypes: []types.Type{varcharType},
		JoinType:   plan.Node_INNER,
	}
	installLoopJoinTestAllocation(t, join)
	join.ctr.state = Probe
	join.ctr.mp = joinMap
	join.ctr.resultBatchByteLimit = byteLimit
	join.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{
		makeVarcharLoopJoinBatch(proc.Mp(), []string{value}),
	}))
	require.NoError(t, join.Prepare(proc))

	totalRows := 0
	batchCount := 0
	for {
		result, err := join.Call(proc)
		require.NoError(t, err)
		if result.Batch == nil {
			break
		}
		require.Positive(t, result.Batch.RowCount())
		require.LessOrEqual(t, result.Batch.Size(), byteLimit)
		totalRows += result.Batch.RowCount()
		batchCount++
	}
	require.Equal(t, len(buildValues), totalRows)
	require.Greater(t, batchCount, 1)

	join.Free(proc, false, nil)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestLoopJoinEmptyBuildSplitsWideRowsByBytes(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())

	const byteLimit = 1024
	probeValues := make([]string, 20)
	for i := range probeValues {
		probeValues[i] = strings.Repeat("x", 128)
	}
	join := &LoopJoin{
		ResultCols: []colexec.ResultPos{
			colexec.NewResultPos(0, 0),
			colexec.NewResultPos(1, 0),
		},
		LeftTypes:  []types.Type{types.T_varchar.ToType()},
		RightTypes: []types.Type{types.T_varchar.ToType()},
		JoinType:   plan.Node_LEFT,
	}
	installLoopJoinTestAllocation(t, join)
	join.ctr.state = Probe
	join.ctr.resultBatchByteLimit = byteLimit
	join.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{
		makeVarcharLoopJoinBatch(proc.Mp(), probeValues),
	}))
	require.NoError(t, join.Prepare(proc))

	totalRows := 0
	batchCount := 0
	for {
		result, err := join.Call(proc)
		require.NoError(t, err)
		if result.Batch == nil {
			break
		}
		require.Positive(t, result.Batch.RowCount())
		require.LessOrEqual(t, result.Batch.Size(), byteLimit)
		require.True(t, result.Batch.Vecs[1].IsConstNull())
		totalRows += result.Batch.RowCount()
		batchCount++
	}
	require.Equal(t, len(probeValues), totalRows)
	require.Greater(t, batchCount, 1)

	join.Free(proc, false, nil)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func makeInt32LoopJoinBatch(mp *mpool.MPool, vals []int32) *batch.Batch {
	bat := batch.New([]string{"id"})
	bat.Vecs[0] = testutil.MakeInt32Vector(vals, nil, mp)
	bat.SetRowCount(len(vals))
	return bat
}

func makeVarcharLoopJoinBatch(mp *mpool.MPool, vals []string) *batch.Batch {
	bat := batch.New([]string{"value"})
	bat.Vecs[0] = testutil.MakeVarcharVector(vals, nil, mp)
	bat.SetRowCount(len(vals))
	return bat
}

/*
	func BenchmarkJoin(b *testing.B) {
		for i := 0; i < b.N; i++ {
			tcs = []joinTestCase{
				newTestCase([]bool{false}, []types.Type{types.T_int8.ToType()}, []colexec.ResultPos{colexec.NewResultPos(0, 0), colexec.NewResultPos(1, 0)}),
				newTestCase([]bool{true}, []types.Type{types.T_int8.ToType()}, []colexec.ResultPos{colexec.NewResultPos(0, 0), colexec.NewResultPos(1, 0)}),
			}
			t := new(testing.T)
			for _, tc := range tcs {
				bats := hashBuild(t, tc)
				err := tc.arg.Prepare(tc.proc)
				require.NoError(t, err)
				tc.proc.Reg.MergeReceivers[0].Ch <- testutil.NewRegMsg(newBatch(tc.types, tc.proc, Rows))
				tc.proc.Reg.MergeReceivers[0].Ch <- testutil.NewRegMsg(batch.EmptyBatch)
				tc.proc.Reg.MergeReceivers[0].Ch <- testutil.NewRegMsg(newBatch(tc.types, tc.proc, Rows))
				tc.proc.Reg.MergeReceivers[0].Ch <- testutil.NewRegMsg(newBatch(tc.types, tc.proc, Rows))
				tc.proc.Reg.MergeReceivers[0].Ch <- testutil.NewRegMsg(newBatch(tc.types, tc.proc, Rows))
				tc.proc.Reg.MergeReceivers[0].Ch <- nil
				tc.proc.Reg.MergeReceivers[1].Ch <- testutil.NewRegMsg(bats[1])
				for {
					ok, err := tc.arg.Call(tc.proc)
					if ok.Status == vm.ExecStop || err != nil {
						break
					}
				}
			}
		}
	}
*/

func newTestCase(t *testing.T, flgs []bool, ts []types.Type, rp []colexec.ResultPos) joinTestCase {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	proc.SetMessageBoard(message.NewMessageBoard())
	ctx, cancel := context.WithCancel(context.Background())
	fr, _ := function.GetFunctionByName(ctx, "=", ts)
	fid := fr.GetEncodedOverloadID()
	args := make([]*plan.Expr, 0, 2)
	args = append(args, &plan.Expr{
		Typ: plan.Type{
			Id: int32(ts[0].Oid),
		},
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: 0,
				ColPos: 0,
			},
		},
	})
	args = append(args, &plan.Expr{
		Typ: plan.Type{
			Id: int32(ts[0].Oid),
		},
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: 1,
				ColPos: 0,
			},
		},
	})
	cond := &plan.Expr{
		Typ: plan.Type{
			Id: int32(types.T_bool),
		},
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Args: args,
				Func: &plan.ObjectRef{Obj: fid, ObjName: "="},
			},
		},
	}
	resultBatch := batch.NewWithSize(len(rp))
	resultBatch.SetRowCount(2)
	for i := range rp {
		bat := colexec.MakeMockBatchs(proc.Mp())
		resultBatch.Vecs[i] = bat.Vecs[rp[i].Pos]
	}
	tag++
	testCase := joinTestCase{
		types:  ts,
		flgs:   flgs,
		proc:   proc,
		cancel: cancel,
		arg: &LoopJoin{
			NonEqCond:  cond,
			ResultCols: rp,
			RightTypes: []types.Type{types.T_int32.ToType()},
			OperatorBase: vm.OperatorBase{
				OperatorInfo: vm.OperatorInfo{
					Idx:     1,
					IsFirst: false,
					IsLast:  false,
				},
			},
			JoinMapTag: tag,
		},
		barg: &hashbuild.HashBuild{
			NeedBatches: true,
			OperatorBase: vm.OperatorBase{
				OperatorInfo: vm.OperatorInfo{
					Idx:     0,
					IsFirst: false,
					IsLast:  false,
				},
			},
			JoinMapTag:    tag,
			JoinMapRefCnt: 1,
		},
		resultBatch: resultBatch,
	}
	installLoopJoinTestAllocation(t, testCase.arg, testCase.barg)
	return testCase
}

func resetChildren(arg *LoopJoin, m *mpool.MPool) {
	resetChildrenWithBatch(arg, colexec.MakeMockBatchs(m))
}

func resetChildrenWithBatch(arg *LoopJoin, bat *batch.Batch) {
	op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat})
	arg.Children = nil
	arg.AppendChild(op)
}

func resetHashBuildChildren(arg *hashbuild.HashBuild, m *mpool.MPool) {
	resetHashBuildChildrenWithBatch(arg, colexec.MakeMockBatchs(m))
}

func resetHashBuildChildrenWithBatch(arg *hashbuild.HashBuild, bat *batch.Batch) {
	op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat})
	arg.Children = nil
	arg.AppendChild(op)
}
