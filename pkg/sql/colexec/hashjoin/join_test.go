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

package hashjoin

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
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
	"github.com/stretchr/testify/require"
)

const (
	Rows          = 10     // default rows
	BenchmarkRows = 100000 // default rows for benchmark
)

// add unit tests for cases
type joinTestCase struct {
	arg         *HashJoin
	flgs        []bool // flgs[i] == true: nullable
	types       []types.Type
	proc        *process.Process
	cancel      context.CancelFunc
	barg        *hashbuild.HashBuild
	resultBatch *batch.Batch
}

func TestHashJoinPrepareFailureCanRetry(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	typ := types.T_int32.ToType()
	valid := newExpr(0, typ)
	invalid := &plan.Expr{Typ: plan.Type{Id: int32(types.T_int32)}}
	arg := &HashJoin{
		EqConds:   [][]*plan.Expr{{valid}, {valid}},
		NonEqCond: invalid,
	}
	installTestAllocation(t, arg)

	require.Error(t, arg.Prepare(proc))
	require.Nil(t, arg.ctr.eqCondVecs)
	require.Nil(t, arg.ctr.eqCondExecs)
	require.Nil(t, arg.ctr.nonEqCondExec)

	arg.NonEqCond = valid
	require.NoError(t, arg.Prepare(proc))
	require.Len(t, arg.ctr.eqCondExecs, 1)
	require.NotNil(t, arg.ctr.nonEqCondExec)
	arg.Free(proc, false, nil)
	proc.Free()
}

func TestHashMarkJoinRejectsResidualCondition(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	arg := NewArgument()
	arg.JoinType = plan.Node_MARK
	arg.NonEqCond = newExpr(0, types.T_int32.ToType())

	require.ErrorContains(t, arg.Prepare(proc), "hash MARK join does not support residual conditions")

	arg.Release()
	proc.Free()
}

func TestHashMarkJoinRejectsInvalidOperatorContracts(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	key := newExpr(0, types.T_int32.ToType())

	tests := []struct {
		name string
		arg  *HashJoin
	}{
		{
			name: "missing keys",
			arg:  &HashJoin{JoinType: plan.Node_MARK},
		},
		{
			name: "mismatched key counts",
			arg: &HashJoin{
				JoinType: plan.Node_MARK,
				EqConds:  [][]*plan.Expr{{key}, {key, key}},
			},
		},
		{
			name: "nullable composite keys",
			arg: &HashJoin{
				JoinType: plan.Node_MARK,
				EqConds:  [][]*plan.Expr{{key, key}, {key, key}},
			},
		},
		{
			name: "build-side result column",
			arg: &HashJoin{
				JoinType:   plan.Node_MARK,
				EqConds:    [][]*plan.Expr{{key}, {key}},
				ResultCols: []colexec.ResultPos{colexec.NewResultPos(1, 0)},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Error(t, tt.arg.Prepare(proc))
		})
	}
}

var (
	tag int32
)

func makeTestCases(t *testing.T) []joinTestCase {
	return []joinTestCase{
		newTestCase(t, []bool{false}, []types.Type{types.T_int32.ToType()}, []colexec.ResultPos{colexec.NewResultPos(0, 0)},
			[][]*plan.Expr{
				{
					newExpr(0, types.T_int32.ToType()),
				},
				{
					newExpr(0, types.T_int32.ToType()),
				},
			}),
		newTestCase(t, []bool{true}, []types.Type{types.T_int32.ToType()}, []colexec.ResultPos{colexec.NewResultPos(0, 0), colexec.NewResultPos(1, 0)},
			[][]*plan.Expr{
				{
					newExpr(0, types.T_int32.ToType()),
				},
				{
					newExpr(0, types.T_int32.ToType()),
				},
			}),
	}
}

func TestString(t *testing.T) {
	buf := new(bytes.Buffer)
	for _, tc := range makeTestCases(t) {
		tc.arg.String(buf)
	}

	for _, test := range []struct {
		joinType plan.Node_JoinType
		want     string
	}{
		{joinType: plan.Node_ASOF, want: ": asof join "},
		{joinType: plan.Node_ASOF_LEFT, want: ": asof left join "},
	} {
		buf.Reset()
		arg := NewArgument()
		arg.JoinType = test.joinType
		arg.String(buf)
		require.Contains(t, buf.String(), test.want)
		arg.Release()
	}
}

func TestAsofPhysicalContractValidation(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	arg := NewArgument()
	arg.JoinType = plan.Node_ASOF
	require.ErrorContains(t, arg.Prepare(proc), "invalid ASOF join physical contract")
	arg.Release()

	for _, typ := range []types.T{types.T_date, types.T_datetime, types.T_timestamp, types.T_time} {
		require.True(t, isAsofTemporalType(typ))
	}
	require.False(t, isAsofTemporalType(types.T_int64))
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

func TestHashJoinCountOnlyCollapsesDuplicateMatches(t *testing.T) {
	typ := types.T_int32.ToType()
	conditions := [][]*plan.Expr{
		{newExpr(0, typ)},
		{newExpr(0, typ)},
	}
	tc := newTestCase(t, []bool{false}, []types.Type{typ}, nil, conditions)
	tc.arg.JoinType = plan.Node_INNER
	tc.arg.NonEqCond = nil
	tc.arg.EmitCompressedRowCount = true
	tc.barg.NeedBatches = false

	const duplicateRows = colexec.DefaultBatchSize*2 + 17
	buildValues := make([]int32, duplicateRows+7)
	for i := range duplicateRows {
		buildValues[i] = 1
	}
	for i := duplicateRows; i < len(buildValues); i++ {
		buildValues[i] = 2
	}
	build := makeInt32Batch(tc.proc, buildValues)
	probe := makeInt32Batch(tc.proc, []int32{1, 2, 3})
	resetHashBuildChildrenWithBatch(tc.barg, build)
	resetChildrenWithBatch(tc.arg, probe)

	defer func() {
		tc.arg.Free(tc.proc, false, nil)
		tc.barg.Free(tc.proc, false, nil)
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
	require.NotNil(t, res.Batch)
	require.Empty(t, res.Batch.Vecs)
	require.Equal(t, duplicateRows+7, res.Batch.RowCount())

	res, err = vm.Exec(tc.arg, tc.proc)
	require.NoError(t, err)
	require.Nil(t, res.Batch)
}

func TestHashJoinCountOnlyRequiresLoadedMap(t *testing.T) {
	hashJoin := &HashJoin{
		JoinType:               plan.Node_INNER,
		EmitCompressedRowCount: true,
	}
	require.False(t, hashJoin.canEmitMatchCountOnly())
}

func TestHashJoinEmptyProjectionWithoutCountContractStaysBoundedAndCancelable(t *testing.T) {
	typ := types.T_int32.ToType()
	conditions := [][]*plan.Expr{{newExpr(0, typ)}, {newExpr(0, typ)}}
	tc := newTestCase(t, []bool{false}, []types.Type{typ}, nil, conditions)
	tc.arg.JoinType = plan.Node_INNER
	tc.arg.NonEqCond = nil
	tc.barg.NeedBatches = false

	const duplicateRows = colexec.DefaultBatchSize*2 + 17
	buildValues := make([]int32, duplicateRows)
	for i := range buildValues {
		buildValues[i] = 1
	}
	resetHashBuildChildrenWithBatch(tc.barg, makeInt32Batch(tc.proc, buildValues))
	resetChildrenWithBatch(tc.arg, makeInt32Batch(tc.proc, []int32{1}))

	defer func() {
		tc.arg.Free(tc.proc, false, nil)
		tc.barg.Free(tc.proc, false, nil)
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
	require.NotNil(t, res.Batch)
	require.Empty(t, res.Batch.Vecs)
	require.Equal(t, colexec.DefaultBatchSize, res.Batch.RowCount())

	ctx, cancel := context.WithCancel(tc.proc.Ctx)
	tc.proc.Ctx = ctx
	cancel()
	_, err = vm.Exec(tc.arg, tc.proc)
	require.ErrorIs(t, err, context.Canceled)
}

type recursiveHashJoinProbe struct {
	*colexec.MockOperator
}

func (source *recursiveHashJoinProbe) OpType() vm.OpType {
	return vm.MergeRecursive
}

func TestHashJoinPassesRecursiveMarkerWithEmptyBuild(t *testing.T) {
	typ := types.T_int32.ToType()
	conditions := [][]*plan.Expr{{newExpr(0, typ)}, {newExpr(0, typ)}}
	tc := newTestCase(t, []bool{false}, []types.Type{typ}, []colexec.ResultPos{
		colexec.NewResultPos(0, 0),
	}, conditions)
	marker := colexec.MakeMockBatchs(tc.proc.Mp())
	marker.SetLast()
	probeCalls := 0
	probe := &recursiveHashJoinProbe{MockOperator: colexec.NewMockOperator().
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

func TestHashJoinPrepareRecomputesRecursiveProbeForFastPath(t *testing.T) {
	typ := types.T_int32.ToType()
	conditions := [][]*plan.Expr{{newExpr(0, typ)}, {newExpr(0, typ)}}
	tc := newTestCase(t, []bool{false}, []types.Type{typ}, []colexec.ResultPos{
		colexec.NewResultPos(0, 0),
	}, conditions)
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

func TestHashJoinPropagatesUnmatchedOutputOOM(t *testing.T) {
	limited, err := mpool.NewMPool(t.Name(), 1<<20, mpool.NoFixed)
	require.NoError(t, err)
	typ := types.T_int32.ToType()
	tc := newTestCaseWithMPool(
		t,
		limited,
		[]bool{false},
		[]types.Type{typ},
		[]colexec.ResultPos{colexec.NewResultPos(0, 0)},
		[][]*plan.Expr{{newExpr(0, typ)}, {newExpr(0, typ)}},
	)
	tc.arg.JoinType = plan.Node_LEFT
	tc.arg.NonEqCond = nil

	probe := batch.NewWithSize(1)
	probe.Vecs[0] = testutil.MakeInt32Vector([]int32{2}, nil, limited)
	probe.SetRowCount(1)
	resetChildrenWithBatch(tc.arg, probe)
	build := batch.NewWithSize(1)
	build.Vecs[0] = testutil.MakeInt32Vector([]int32{1}, nil, limited)
	build.SetRowCount(1)
	resetHashBuildChildrenWithBatch(tc.barg, build)

	require.NoError(t, tc.arg.Prepare(tc.proc))
	require.NoError(t, tc.barg.Prepare(tc.proc))
	_, err = vm.Exec(tc.barg, tc.proc)
	require.NoError(t, err)

	remaining := limited.Cap() - limited.CurrNB()
	require.Positive(t, remaining)
	filler, err := limited.Alloc(int(remaining), true)
	require.NoError(t, err)
	_, err = vm.Exec(tc.arg, tc.proc)
	require.ErrorContains(t, err, "mpool out of space")
	limited.Free(filler)

	tc.arg.Reset(tc.proc, false, nil)
	tc.barg.Reset(tc.proc, false, nil)
	tc.arg.Free(tc.proc, false, nil)
	tc.barg.Free(tc.proc, false, nil)
	tc.proc.Free()
	require.Zero(t, limited.CurrNB())
}

func TestHashJoinResetAfterEmptyProbe(t *testing.T) {
	tc := newTestCase(t, []bool{true}, []types.Type{types.T_int32.ToType()}, []colexec.ResultPos{
		colexec.NewResultPos(0, 0),
		colexec.NewResultPos(1, 0),
	}, [][]*plan.Expr{
		{
			newExpr(0, types.T_int32.ToType()),
		},
		{
			newExpr(0, types.T_int32.ToType()),
		},
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

func TestHashJoinConstNullAfterNonEmptyProbe(t *testing.T) {
	tc := newTestCase(t, []bool{true}, []types.Type{types.T_int32.ToType()}, []colexec.ResultPos{
		colexec.NewResultPos(0, 0),
		colexec.NewResultPos(1, 0),
	}, [][]*plan.Expr{
		{
			newExpr(0, types.T_int32.ToType()),
		},
		{
			newExpr(0, types.T_int32.ToType()),
		},
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

func TestHashMarkJoinThreeValuedSemantics(t *testing.T) {
	type expectedMark struct {
		value  bool
		isNull bool
	}

	tests := []struct {
		name        string
		buildValues []int32
		buildNulls  []uint64
		expected    []expectedMark
	}{
		{
			name:        "matching value wins over build null",
			buildValues: []int32{2, 4, 0},
			buildNulls:  []uint64{2},
			expected: []expectedMark{
				{isNull: true},
				{value: true},
				{isNull: true},
				{isNull: true},
			},
		},
		{
			name:        "non-null build returns false for misses",
			buildValues: []int32{2, 4},
			expected: []expectedMark{
				{value: false},
				{value: true},
				{value: false},
				{isNull: true},
			},
		},
		{
			name: "empty build is false even for null probe",
			expected: []expectedMark{
				{value: false},
				{value: false},
				{value: false},
				{value: false},
			},
		},
		{
			name:        "all-null build returns unknown",
			buildValues: []int32{0},
			buildNulls:  []uint64{0},
			expected: []expectedMark{
				{isNull: true},
				{isNull: true},
				{isNull: true},
				{isNull: true},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tc := newTestCase(t,
				[]bool{true},
				[]types.Type{types.T_int32.ToType()},
				[]colexec.ResultPos{colexec.NewResultPos(0, 0)},
				[][]*plan.Expr{
					{newExpr(0, types.T_int32.ToType())},
					{newExpr(0, types.T_int32.ToType())},
				})
			tc.arg.JoinType = plan.Node_MARK
			tc.arg.NonEqCond = nil
			tc.arg.ResultCols = []colexec.ResultPos{
				colexec.NewResultPos(0, 0),
				colexec.NewResultPos(-1, 0),
			}
			tc.barg.NeedAllocateSels = false
			tc.barg.NeedBatches = false
			tc.barg.TrackNullKeys = true

			probe := batch.NewWithSize(1)
			probe.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 2, 3, 0}, []uint64{3}, tc.proc.Mp())
			probe.SetRowCount(4)
			resetChildrenWithBatch(tc.arg, probe)

			build := batch.EmptyBatch
			if len(tt.buildValues) > 0 {
				build = batch.NewWithSize(1)
				build.Vecs[0] = testutil.MakeInt32Vector(tt.buildValues, tt.buildNulls, tc.proc.Mp())
				build.SetRowCount(len(tt.buildValues))
			}
			resetHashBuildChildrenWithBatch(tc.barg, build)

			require.NoError(t, tc.arg.Prepare(tc.proc))
			require.NoError(t, tc.barg.Prepare(tc.proc))
			_, err := vm.Exec(tc.barg, tc.proc)
			require.NoError(t, err)

			res, err := vm.Exec(tc.arg, tc.proc)
			require.NoError(t, err)
			require.NotNil(t, res.Batch)
			require.Equal(t, len(tt.expected), res.Batch.RowCount())
			require.Len(t, res.Batch.Vecs, 2)

			marks := vector.GenerateFunctionFixedTypeParameter[bool](res.Batch.Vecs[1])
			for i, expected := range tt.expected {
				value, isNull := marks.GetValue(uint64(i))
				require.Equal(t, expected.isNull, isNull, "row %d null state", i)
				if !isNull {
					require.Equal(t, expected.value, value, "row %d value", i)
				}
			}

			tc.arg.Reset(tc.proc, false, nil)
			tc.barg.Reset(tc.proc, false, nil)
			tc.arg.Free(tc.proc, false, nil)
			tc.barg.Free(tc.proc, false, nil)
			tc.proc.Free()
			require.Equal(t, int64(0), tc.proc.Mp().CurrNB())
		})
	}
}

func TestHashMarkJoinCompositeNotNullKeys(t *testing.T) {
	probeKey0 := newExpr(0, types.T_int32.ToType())
	probeKey1 := newExpr(1, types.T_int32.ToType())
	buildKey0 := newExpr(0, types.T_int32.ToType())
	buildKey1 := newExpr(1, types.T_int32.ToType())
	for _, expr := range []*plan.Expr{probeKey0, probeKey1, buildKey0, buildKey1} {
		expr.Typ.NotNullable = true
	}

	tc := newTestCase(t,
		[]bool{false, false},
		[]types.Type{types.T_int32.ToType(), types.T_int32.ToType()},
		[]colexec.ResultPos{colexec.NewResultPos(0, 0)},
		[][]*plan.Expr{{probeKey0, probeKey1}, {buildKey0, buildKey1}},
	)
	tc.arg.JoinType = plan.Node_MARK
	tc.arg.NonEqCond = nil
	tc.arg.ResultCols = []colexec.ResultPos{
		colexec.NewResultPos(0, 0),
		colexec.NewResultPos(-1, 0),
	}
	tc.barg.NeedAllocateSels = false
	tc.barg.NeedBatches = false
	tc.barg.TrackNullKeys = true

	probe := batch.NewWithSize(2)
	probe.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 1, 3}, nil, tc.proc.Mp())
	probe.Vecs[1] = testutil.MakeInt32Vector([]int32{2, 4, 4}, nil, tc.proc.Mp())
	probe.SetRowCount(3)
	resetChildrenWithBatch(tc.arg, probe)

	build := batch.NewWithSize(2)
	build.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 3}, nil, tc.proc.Mp())
	build.Vecs[1] = testutil.MakeInt32Vector([]int32{2, 4}, nil, tc.proc.Mp())
	build.SetRowCount(2)
	resetHashBuildChildrenWithBatch(tc.barg, build)

	require.NoError(t, tc.arg.Prepare(tc.proc))
	require.NoError(t, tc.barg.Prepare(tc.proc))
	_, err := vm.Exec(tc.barg, tc.proc)
	require.NoError(t, err)
	res, err := vm.Exec(tc.arg, tc.proc)
	require.NoError(t, err)
	require.NotNil(t, res.Batch)
	require.Equal(t, 3, res.Batch.RowCount())
	marks := vector.GenerateFunctionFixedTypeParameter[bool](res.Batch.Vecs[1])
	for row, expected := range []bool{true, false, true} {
		value, isNull := marks.GetValue(uint64(row))
		require.False(t, isNull, "row %d", row)
		require.Equal(t, expected, value, "row %d", row)
	}

	tc.arg.Reset(tc.proc, false, nil)
	tc.barg.Reset(tc.proc, false, nil)
	tc.arg.Free(tc.proc, false, nil)
	tc.barg.Free(tc.proc, false, nil)
	tc.proc.Free()
	require.Equal(t, int64(0), tc.proc.Mp().CurrNB())
}

func TestHashMarkJoinResetClearsBuildNullState(t *testing.T) {
	tc := newTestCase(t,
		[]bool{true},
		[]types.Type{types.T_int32.ToType()},
		[]colexec.ResultPos{colexec.NewResultPos(0, 0)},
		[][]*plan.Expr{
			{newExpr(0, types.T_int32.ToType())},
			{newExpr(0, types.T_int32.ToType())},
		})
	tc.arg.JoinType = plan.Node_MARK
	tc.arg.NonEqCond = nil
	tc.arg.ResultCols = []colexec.ResultPos{
		colexec.NewResultPos(0, 0),
		colexec.NewResultPos(-1, 0),
	}
	tc.barg.NeedAllocateSels = false
	tc.barg.NeedBatches = false
	tc.barg.TrackNullKeys = true

	run := func(probeValue, buildValue int32, buildNull bool) (bool, bool) {
		probe := batch.NewWithSize(1)
		probe.Vecs[0] = testutil.MakeInt32Vector([]int32{probeValue}, nil, tc.proc.Mp())
		probe.SetRowCount(1)
		resetChildrenWithBatch(tc.arg, probe)

		var buildNulls []uint64
		if buildNull {
			buildNulls = []uint64{0}
		}
		build := batch.NewWithSize(1)
		build.Vecs[0] = testutil.MakeInt32Vector([]int32{buildValue}, buildNulls, tc.proc.Mp())
		build.SetRowCount(1)
		resetHashBuildChildrenWithBatch(tc.barg, build)

		require.NoError(t, tc.arg.Prepare(tc.proc))
		require.NoError(t, tc.barg.Prepare(tc.proc))
		_, err := vm.Exec(tc.barg, tc.proc)
		require.NoError(t, err)
		res, err := vm.Exec(tc.arg, tc.proc)
		require.NoError(t, err)
		require.NotNil(t, res.Batch)

		return vector.GenerateFunctionFixedTypeParameter[bool](res.Batch.Vecs[1]).GetValue(0)
	}

	_, isNull := run(1, 0, true)
	require.True(t, isNull)

	tc.arg.Reset(tc.proc, false, nil)
	tc.barg.Reset(tc.proc, false, nil)
	require.False(t, tc.arg.ctr.buildHasNullKey)
	require.Zero(t, tc.arg.ctr.globalBuildRowCnt)
	tc.proc.GetMessageBoard().Reset()

	value, isNull := run(1, 2, false)
	require.False(t, isNull)
	require.False(t, value)

	tc.arg.Reset(tc.proc, false, nil)
	tc.barg.Reset(tc.proc, false, nil)
	tc.arg.Free(tc.proc, false, nil)
	tc.barg.Free(tc.proc, false, nil)
	tc.proc.Free()
	require.Equal(t, int64(0), tc.proc.Mp().CurrNB())
}

func TestHashMarkJoinBatchBoundary(t *testing.T) {
	tc := newTestCase(t,
		[]bool{true},
		[]types.Type{types.T_int32.ToType()},
		[]colexec.ResultPos{colexec.NewResultPos(0, 0)},
		[][]*plan.Expr{
			{newExpr(0, types.T_int32.ToType())},
			{newExpr(0, types.T_int32.ToType())},
		})
	tc.arg.JoinType = plan.Node_MARK
	tc.arg.NonEqCond = nil
	tc.arg.ResultCols = []colexec.ResultPos{
		colexec.NewResultPos(0, 0),
		colexec.NewResultPos(-1, 0),
	}
	tc.barg.NeedAllocateSels = false
	tc.barg.NeedBatches = false
	tc.barg.TrackNullKeys = true

	probeValues := make([]int32, colexec.DefaultBatchSize+1)
	probeValues[len(probeValues)-1] = 2
	probe := batch.NewWithSize(1)
	probe.Vecs[0] = testutil.MakeInt32Vector(probeValues, nil, tc.proc.Mp())
	probe.SetRowCount(len(probeValues))
	resetChildrenWithBatch(tc.arg, probe)

	build := batch.NewWithSize(1)
	build.Vecs[0] = testutil.MakeInt32Vector([]int32{2}, nil, tc.proc.Mp())
	build.SetRowCount(1)
	resetHashBuildChildrenWithBatch(tc.barg, build)

	require.NoError(t, tc.arg.Prepare(tc.proc))
	require.NoError(t, tc.barg.Prepare(tc.proc))
	_, err := vm.Exec(tc.barg, tc.proc)
	require.NoError(t, err)

	first, err := vm.Exec(tc.arg, tc.proc)
	require.NoError(t, err)
	require.NotNil(t, first.Batch)
	require.Equal(t, colexec.DefaultBatchSize, first.Batch.RowCount())

	second, err := vm.Exec(tc.arg, tc.proc)
	require.NoError(t, err)
	require.NotNil(t, second.Batch)
	require.Equal(t, 1, second.Batch.RowCount())
	value, isNull := vector.GenerateFunctionFixedTypeParameter[bool](second.Batch.Vecs[1]).GetValue(0)
	require.False(t, isNull)
	require.True(t, value)

	tc.arg.Reset(tc.proc, false, nil)
	tc.barg.Reset(tc.proc, false, nil)
	tc.arg.Free(tc.proc, false, nil)
	tc.barg.Free(tc.proc, false, nil)
	tc.proc.Free()
	require.Equal(t, int64(0), tc.proc.Mp().CurrNB())
}

func TestHashJoinSingleRejectsMultipleRows(t *testing.T) {
	tests := []struct {
		name        string
		probeValues []int32
		buildValues []int32
		hashOnPK    bool
		isRightJoin bool
		nonEqCond   bool
	}{
		{
			name:        "right single unique build",
			probeValues: []int32{1, 1},
			buildValues: []int32{1},
			hashOnPK:    true,
			isRightJoin: true,
		},
		{
			name:        "right single unique build with non-equi condition",
			probeValues: []int32{1, 1},
			buildValues: []int32{1},
			hashOnPK:    true,
			isRightJoin: true,
			nonEqCond:   true,
		},
		{
			name:        "left single duplicate build",
			probeValues: []int32{1},
			buildValues: []int32{1, 1},
		},
		{
			name:        "right single duplicate probe",
			probeValues: []int32{1, 1},
			buildValues: []int32{1, 2, 2},
			isRightJoin: true,
		},
		{
			name:        "right single duplicate probe with non-equi condition",
			probeValues: []int32{1, 1},
			buildValues: []int32{1, 2, 2},
			isRightJoin: true,
			nonEqCond:   true,
		},
		{
			name:        "left single duplicate build with non-equi condition",
			probeValues: []int32{1},
			buildValues: []int32{1, 1},
			nonEqCond:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tc := newTestCase(t,
				[]bool{false},
				[]types.Type{types.T_int32.ToType()},
				[]colexec.ResultPos{colexec.NewResultPos(0, 0)},
				[][]*plan.Expr{
					{newExpr(0, types.T_int32.ToType())},
					{newExpr(0, types.T_int32.ToType())},
				})
			tc.arg.JoinType = plan.Node_SINGLE
			tc.arg.IsRightJoin = tt.isRightJoin
			tc.arg.HashOnPK = tt.hashOnPK
			tc.barg.HashOnPK = tt.hashOnPK
			if !tt.nonEqCond {
				tc.arg.NonEqCond = nil
			}

			resetChildrenWithBatch(tc.arg, makeInt32Batch(tc.proc, tt.probeValues))
			resetHashBuildChildrenWithBatch(tc.barg, makeInt32Batch(tc.proc, tt.buildValues))
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

func TestHashJoinSingleRejectsDuplicateMatchesAcrossWorkers(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	localMatches := new(bitmap.Bitmap)
	localMatches.InitWithSize(1)
	localMatches.Add(0)
	remoteMatches := localMatches.Clone()

	hashJoin := &HashJoin{
		JoinType: plan.Node_SINGLE,
		NumCPU:   2,
		IsMerger: true,
		Mailbox:  NewBitmapMailbox(2),
	}
	require.True(t, hashJoin.Mailbox.Send(remoteMatches))
	ctr := container{rightRowsMatched: localMatches, probeSingle: true}

	err := ctr.syncBitmap(hashJoin, proc)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrSubqueryNo1Row))
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

// A merger whose syncBitmap is aborted by a nil bitmap from the channel (a
// worker torn down before syncing, e.g. when an outer LIMIT stops the query
// early, sends nil from Reset) must go to End instead of entering Finalize
// with a nil rightMatchedIter. The aborted generation must also drain the
// remaining worker messages so a later run over the same channel does not
// observe stale bitmaps.
func TestHashJoinMergerSyncBitmapAborted(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	rightBat := makeInt32Batch(proc, []int32{10, 20, 30, 40})

	matched := new(bitmap.Bitmap)
	matched.InitWithSize(4)
	matched.Add(0)
	staleMatches := new(bitmap.Bitmap)
	staleMatches.InitWithSize(4)
	staleMatches.Add(1)

	hashJoin := &HashJoin{
		JoinType:    plan.Node_RIGHT,
		IsRightJoin: true,
		NumCPU:      3,
		IsMerger:    true,
		Mailbox:     NewBitmapMailbox(3),
		ResultCols:  []colexec.ResultPos{colexec.NewResultPos(1, 0)},
		RightTypes:  []types.Type{types.T_int32.ToType()},
	}
	// Worker A was torn down before syncing (its Reset sends nil); worker B
	// synced normally and its bitmap lands after the abort marker.
	require.True(t, hashJoin.Mailbox.Send(nil))
	require.True(t, hashJoin.Mailbox.Send(staleMatches))
	hashJoin.ctr.state = SyncBitmap
	hashJoin.ctr.rightRowsMatched = matched
	hashJoin.ctr.rightBats = []*batch.Batch{rightBat}

	result, err := hashJoin.Call(proc)
	require.NoError(t, err)
	require.Nil(t, result.Batch)
	require.Equal(t, vm.ExecStop, result.Status)
	// Worker B's bitmap must not be left behind in the shared mailbox.
	require.Empty(t, hashJoin.Mailbox.ch)

	// The merger already synced this generation, so Reset must not push the
	// nil abort marker either.
	hashJoin.Reset(proc, false, nil)
	require.Empty(t, hashJoin.Mailbox.ch)

	// Next generation over the same operator and channel: a clean sync must
	// only observe this generation's bitmaps.
	matched2 := new(bitmap.Bitmap)
	matched2.InitWithSize(4)
	matched2.Add(0)
	workerMatches1 := new(bitmap.Bitmap)
	workerMatches1.InitWithSize(4)
	workerMatches1.Add(1)
	workerMatches2 := new(bitmap.Bitmap)
	workerMatches2.InitWithSize(4)
	workerMatches2.Add(2)

	hashJoin.Mailbox = NewBitmapMailbox(3)
	require.True(t, hashJoin.Mailbox.Send(workerMatches1))
	require.True(t, hashJoin.Mailbox.Send(workerMatches2))
	hashJoin.ctr.state = SyncBitmap
	hashJoin.ctr.rightRowsMatched = matched2
	hashJoin.ctr.rightBats = []*batch.Batch{rightBat}

	result, err = hashJoin.Call(proc)
	require.NoError(t, err)
	require.NotNil(t, result.Batch)
	require.Equal(t, 1, result.Batch.RowCount())
	vals := vector.MustFixedColWithTypeCheck[int32](result.Batch.Vecs[0])
	require.Equal(t, []int32{40}, vals[:1])

	result, err = hashJoin.Call(proc)
	require.NoError(t, err)
	require.Nil(t, result.Batch)
	require.Equal(t, vm.ExecStop, result.Status)

	rightBat.Clean(proc.Mp())
	hashJoin.Free(proc, false, nil)
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestHashJoinMergerFinalizeEmitsUnmatchedBuildRows(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	rightBat := makeInt32Batch(proc, []int32{10, 20, 30, 40})

	matched := new(bitmap.Bitmap)
	matched.InitWithSize(4)
	matched.Add(0)
	remoteMatches := new(bitmap.Bitmap)
	remoteMatches.InitWithSize(4)
	remoteMatches.Add(1)

	hashJoin := &HashJoin{
		JoinType:    plan.Node_RIGHT,
		IsRightJoin: true,
		NumCPU:      2,
		IsMerger:    true,
		Mailbox:     NewBitmapMailbox(2),
		ResultCols:  []colexec.ResultPos{colexec.NewResultPos(1, 0)},
		RightTypes:  []types.Type{types.T_int32.ToType()},
	}
	require.True(t, hashJoin.Mailbox.Send(remoteMatches))
	hashJoin.ctr.state = SyncBitmap
	hashJoin.ctr.rightRowsMatched = matched
	hashJoin.ctr.rightBats = []*batch.Batch{rightBat}

	result, err := hashJoin.Call(proc)
	require.NoError(t, err)
	require.NotNil(t, result.Batch)
	require.Equal(t, 2, result.Batch.RowCount())
	vals := vector.MustFixedColWithTypeCheck[int32](result.Batch.Vecs[0])
	require.Equal(t, []int32{30, 40}, vals[:2])

	result, err = hashJoin.Call(proc)
	require.NoError(t, err)
	require.Nil(t, result.Batch)
	require.Equal(t, vm.ExecStop, result.Status)

	rightBat.Clean(proc.Mp())
	hashJoin.Free(proc, false, nil)
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestHashJoinTracksBuildMatchesWhenFullOuterIsNotRightOriented(t *testing.T) {
	type joinedRow struct {
		left      int32
		right     int32
		leftNull  bool
		rightNull bool
	}

	want := []joinedRow{
		{left: 1, rightNull: true},
		{left: 2, right: 2},
		{left: 4, rightNull: true},
		{leftNull: true, right: 3},
	}

	for _, test := range []struct {
		name        string
		hashOnPK    bool
		useResidual bool
	}{
		{name: "non-unique build without residual"},
		{name: "non-unique build with residual", useResidual: true},
		{name: "unique build without residual", hashOnPK: true},
		{name: "unique build with residual", hashOnPK: true, useResidual: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			typ := types.T_int32.ToType()
			tc := newTestCase(t,
				[]bool{true},
				[]types.Type{typ},
				[]colexec.ResultPos{
					colexec.NewResultPos(0, 0),
					colexec.NewResultPos(1, 0),
				},
				[][]*plan.Expr{{newExpr(0, typ)}, {newExpr(0, typ)}},
			)
			defer func() {
				tc.arg.Reset(tc.proc, false, nil)
				tc.barg.Reset(tc.proc, false, nil)
				tc.arg.Free(tc.proc, false, nil)
				tc.barg.Free(tc.proc, false, nil)
				tc.proc.Free()
				tc.cancel()
				require.Equal(t, int64(0), tc.proc.Mp().CurrNB())
			}()
			tc.arg.JoinType = plan.Node_OUTER
			tc.arg.IsRightJoin = false
			tc.arg.HashOnPK = test.hashOnPK
			tc.barg.HashOnPK = test.hashOnPK
			if !test.useResidual {
				tc.arg.NonEqCond = nil
			}

			resetChildrenWithBatch(tc.arg, makeInt32Batch(tc.proc, []int32{1, 2, 4}))
			resetHashBuildChildrenWithBatch(tc.barg, makeInt32Batch(tc.proc, []int32{2, 3}))

			require.NoError(t, tc.arg.Prepare(tc.proc))
			require.NoError(t, tc.barg.Prepare(tc.proc))
			buildResult, err := vm.Exec(tc.barg, tc.proc)
			require.NoError(t, err)
			require.Nil(t, buildResult.Batch)

			var got []joinedRow
			for {
				result, err := vm.Exec(tc.arg, tc.proc)
				require.NoError(t, err)
				if result.Batch != nil {
					left := vector.MustFixedColWithTypeCheck[int32](result.Batch.Vecs[0])
					right := vector.MustFixedColWithTypeCheck[int32](result.Batch.Vecs[1])
					for row := 0; row < result.Batch.RowCount(); row++ {
						joined := joinedRow{
							leftNull:  result.Batch.Vecs[0].GetNulls().Contains(uint64(row)),
							rightNull: result.Batch.Vecs[1].GetNulls().Contains(uint64(row)),
						}
						if !joined.leftNull {
							joined.left = left[row]
						}
						if !joined.rightNull {
							joined.right = right[row]
						}
						got = append(got, joined)
					}
				}
				if result.Status == vm.ExecStop {
					break
				}
			}

			require.ElementsMatch(t, want, got)
		})
	}
}

func makeInt32Batch(proc *process.Process, values []int32) *batch.Batch {
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = testutil.MakeInt32Vector(values, nil, proc.Mp())
	bat.SetRowCount(len(values))
	return bat
}

func TestAsofLeftJoinEndToEnd(t *testing.T) {
	keyType := types.T_int32.ToType()
	timeType := types.T_timestamp.ToType()
	tc := newTestCase(t,
		[]bool{false, true},
		[]types.Type{keyType, keyType},
		[]colexec.ResultPos{colexec.NewResultPos(0, 0), colexec.NewResultPos(1, 1)},
		[][]*plan.Expr{{newExpr(0, keyType)}, {newExpr(0, keyType)}},
	)
	defer func() {
		tc.arg.Reset(tc.proc, false, nil)
		tc.barg.Reset(tc.proc, false, nil)
		tc.arg.Free(tc.proc, false, nil)
		tc.barg.Free(tc.proc, false, nil)
		tc.proc.Free()
		tc.cancel()
		require.Equal(t, int64(0), tc.proc.Mp().CurrNB())
	}()

	tc.arg.JoinType = plan.Node_ASOF_LEFT
	tc.arg.LeftTypes = []types.Type{keyType, timeType}
	tc.arg.RightTypes = []types.Type{keyType, timeType}
	tc.arg.AsofRightCol = 1
	tc.arg.NonEqCond = makeAsofCondition(t, timeType, ">=")

	probe := makeAsofBatch(tc.proc, []int32{1, 2, 3}, []string{
		"2026-01-01 10:00:00", "2026-01-01 07:00:00", "2026-01-01 10:00:00",
	})
	build := makeAsofBatch(tc.proc, []int32{1, 1, 1, 2}, []string{
		"2026-01-01 11:00:00", "2026-01-01 07:00:00", "2026-01-01 09:00:00", "2026-01-01 08:00:00",
	})
	resetChildrenWithBatch(tc.arg, probe)
	resetHashBuildChildrenWithBatch(tc.barg, build)

	require.NoError(t, tc.arg.Prepare(tc.proc))
	require.NoError(t, tc.barg.Prepare(tc.proc))
	_, err := vm.Exec(tc.barg, tc.proc)
	require.NoError(t, err)
	result, err := vm.Exec(tc.arg, tc.proc)
	require.NoError(t, err)
	require.NotNil(t, result.Batch)
	require.Equal(t, 3, result.Batch.RowCount())
	require.Equal(t, []int32{1, 2, 3}, vector.MustFixedColWithTypeCheck[int32](result.Batch.Vecs[0]))
	rightTimes := vector.MustFixedColWithTypeCheck[types.Timestamp](result.Batch.Vecs[1])
	want, err := types.ParseTimestamp(time.Local, "2026-01-01 09:00:00", 6)
	require.NoError(t, err)
	require.Equal(t, want, rightTimes[0])
	require.False(t, result.Batch.Vecs[1].GetNulls().Contains(0))
	require.True(t, result.Batch.Vecs[1].GetNulls().Contains(1))
	require.True(t, result.Batch.Vecs[1].GetNulls().Contains(2))
}

func TestAsofBuildLeftStreamsRightAndKeepsOneCandidate(t *testing.T) {
	keyType := types.T_int32.ToType()
	timeType := types.T_timestamp.ToType()
	textType := types.T_varchar.ToType()
	equality := [][]*plan.Expr{{newExpr(0, keyType)}, {newExpr(0, keyType)}}
	tc := newTestCase(t,
		[]bool{false, true},
		[]types.Type{keyType, keyType},
		[]colexec.ResultPos{
			colexec.NewResultPos(0, 1),
			colexec.NewResultPos(1, 1),
			colexec.NewResultPos(1, 2),
		},
		equality,
	)
	defer func() {
		tc.arg.Reset(tc.proc, false, nil)
		tc.barg.Reset(tc.proc, false, nil)
		tc.arg.Free(tc.proc, false, nil)
		tc.barg.Free(tc.proc, false, nil)
		tc.proc.Free()
		tc.cancel()
		require.Equal(t, int64(0), tc.proc.Mp().CurrNB())
	}()

	tc.arg.JoinType = plan.Node_ASOF_LEFT
	tc.arg.LeftTypes = []types.Type{keyType, timeType}
	tc.arg.RightTypes = []types.Type{keyType, timeType, textType}
	tc.arg.AsofRightCol = 1
	tc.arg.AsofBuildLeft = true
	tc.arg.NonEqCond = makeAsofCondition(t, timeType, ">=")
	tc.barg.Conditions = equality[0]

	logicalLeft := makeAsofBatch(tc.proc, []int32{1, 1}, []string{
		"2026-01-01 10:00:00", "2026-01-01 15:00:00",
	})
	logicalRight := makeAsofPayloadBatch(
		tc.proc,
		[]int32{1, 1, 1, 1, 1, 1},
		[]types.Timestamp{20, 7, 14, 9, 9, 16},
		[]string{"p20", "p07", "p14", "p09", "p09-later", "p16"},
	)
	// Use the same small integer timestamp domain on both sides so the expected
	// predecessor is obvious and independent of time-zone parsing.
	logicalLeft.Vecs[1].Free(tc.proc.Mp())
	logicalLeft.Vecs[1] = vector.NewVec(timeType)
	require.NoError(t, vector.AppendFixedList(
		logicalLeft.Vecs[1], []types.Timestamp{10, 15}, nil, tc.proc.Mp()))

	resetHashBuildChildrenWithBatch(tc.barg, logicalLeft)
	resetChildrenWithBatch(tc.arg, logicalRight)
	require.NoError(t, tc.arg.Prepare(tc.proc))
	require.NoError(t, tc.barg.Prepare(tc.proc))
	_, err := vm.Exec(tc.barg, tc.proc)
	require.NoError(t, err)

	result, err := vm.Exec(tc.arg, tc.proc)
	require.NoError(t, err)
	require.NotNil(t, result.Batch)
	require.Equal(t, 2, result.Batch.RowCount())
	require.Equal(t, []types.Timestamp{10, 15},
		vector.MustFixedColWithTypeCheck[types.Timestamp](result.Batch.Vecs[0]))
	require.Equal(t, []types.Timestamp{9, 14},
		vector.MustFixedColWithTypeCheck[types.Timestamp](result.Batch.Vecs[1]))
	require.Equal(t, []string{"p09", "p14"},
		vector.InefficientMustStrCol(result.Batch.Vecs[2]))
	require.Empty(t, tc.arg.ctr.asofIndexes)
	require.Len(t, tc.arg.ctr.asofBuildLeftMatched, 2)
	require.Equal(t, []uint8{1, 1}, tc.arg.ctr.asofBuildLeftMatched)
}

func TestAsofBuildLeftEmptyLeftSkipsRightScan(t *testing.T) {
	keyType := types.T_int32.ToType()
	timeType := types.T_timestamp.ToType()
	equality := [][]*plan.Expr{{newExpr(0, keyType)}, {newExpr(0, keyType)}}
	tc := newTestCase(t,
		[]bool{false},
		[]types.Type{keyType, keyType},
		[]colexec.ResultPos{colexec.NewResultPos(0, 0)},
		equality,
	)
	defer func() {
		tc.arg.Reset(tc.proc, false, nil)
		tc.barg.Reset(tc.proc, false, nil)
		tc.arg.Free(tc.proc, false, nil)
		tc.barg.Free(tc.proc, false, nil)
		tc.proc.Free()
		tc.cancel()
		require.Zero(t, tc.proc.Mp().CurrNB())
	}()

	tc.arg.JoinType = plan.Node_ASOF_LEFT
	tc.arg.LeftTypes = []types.Type{keyType, timeType}
	tc.arg.RightTypes = []types.Type{keyType, timeType}
	tc.arg.AsofRightCol = 1
	tc.arg.AsofBuildLeft = true
	tc.arg.NonEqCond = makeAsofCondition(t, timeType, ">=")
	tc.barg.Conditions = equality[0]
	resetHashBuildChildrenWithBatch(tc.barg, batch.EmptyBatch)

	probeCalls := 0
	logicalRight := makeAsofPayloadBatch(
		tc.proc, []int32{1}, []types.Timestamp{1}, []string{"unused"})
	logicalRight.Vecs[2].Free(tc.proc.Mp())
	logicalRight.Vecs = logicalRight.Vecs[:2]
	tc.arg.Children = nil
	tc.arg.AppendChild(colexec.NewMockOperator().
		WithBatchs([]*batch.Batch{logicalRight}).
		WithBatchCallback(func(int) { probeCalls++ }))

	require.NoError(t, tc.arg.Prepare(tc.proc))
	require.NoError(t, tc.barg.Prepare(tc.proc))
	_, err := vm.Exec(tc.barg, tc.proc)
	require.NoError(t, err)
	result, err := vm.Exec(tc.arg, tc.proc)
	require.NoError(t, err)
	require.Equal(t, vm.ExecStop, result.Status)
	require.Nil(t, result.Batch)
	require.Zero(t, probeCalls, "an empty logical left must short-circuit the huge right scan")
}

func TestAsofBuildLeftStrictPredicateRejectsEqualTimestamp(t *testing.T) {
	keyType := types.T_int32.ToType()
	timeType := types.T_timestamp.ToType()
	equality := [][]*plan.Expr{{newExpr(0, keyType)}, {newExpr(0, keyType)}}
	tc := newTestCase(t,
		[]bool{false},
		[]types.Type{keyType, keyType},
		[]colexec.ResultPos{colexec.NewResultPos(1, 1)},
		equality,
	)
	defer func() {
		tc.arg.Reset(tc.proc, false, nil)
		tc.barg.Reset(tc.proc, false, nil)
		tc.arg.Free(tc.proc, false, nil)
		tc.barg.Free(tc.proc, false, nil)
		tc.proc.Free()
		tc.cancel()
		require.Zero(t, tc.proc.Mp().CurrNB())
	}()

	tc.arg.JoinType = plan.Node_ASOF
	tc.arg.LeftTypes = []types.Type{keyType, timeType}
	tc.arg.RightTypes = []types.Type{keyType, timeType}
	tc.arg.AsofRightCol = 1
	tc.arg.AsofBuildLeft = true
	tc.arg.NonEqCond = makeAsofCondition(t, timeType, ">")
	tc.barg.Conditions = equality[0]
	logicalLeft := makeAsofPayloadBatch(
		tc.proc, []int32{1}, []types.Timestamp{10}, []string{"unused"})
	logicalLeft.Vecs[2].Free(tc.proc.Mp())
	logicalLeft.Vecs = logicalLeft.Vecs[:2]
	logicalRight := makeAsofPayloadBatch(
		tc.proc, []int32{1, 1}, []types.Timestamp{10, 9}, []string{"unused", "unused"})
	logicalRight.Vecs[2].Free(tc.proc.Mp())
	logicalRight.Vecs = logicalRight.Vecs[:2]
	resetHashBuildChildrenWithBatch(tc.barg, logicalLeft)
	resetChildrenWithBatch(tc.arg, logicalRight)

	require.NoError(t, tc.arg.Prepare(tc.proc))
	require.NoError(t, tc.barg.Prepare(tc.proc))
	_, err := vm.Exec(tc.barg, tc.proc)
	require.NoError(t, err)
	result, err := vm.Exec(tc.arg, tc.proc)
	require.NoError(t, err)
	require.NotNil(t, result.Batch)
	require.Equal(t, []types.Timestamp{9},
		vector.MustFixedColWithTypeCheck[types.Timestamp](result.Batch.Vecs[0]))
}

func TestAsofBuildLeftCompactsRepeatedVarlenaReplacement(t *testing.T) {
	keyType := types.T_int32.ToType()
	timeType := types.T_timestamp.ToType()
	textType := types.T_varchar.ToType()
	equality := [][]*plan.Expr{{newExpr(0, keyType)}, {newExpr(0, keyType)}}
	tc := newTestCase(t,
		[]bool{false},
		[]types.Type{keyType, keyType},
		[]colexec.ResultPos{colexec.NewResultPos(1, 2)},
		equality,
	)
	defer func() {
		tc.arg.Reset(tc.proc, false, nil)
		tc.barg.Reset(tc.proc, false, nil)
		tc.arg.Free(tc.proc, false, nil)
		tc.barg.Free(tc.proc, false, nil)
		tc.proc.Free()
		tc.cancel()
		require.Equal(t, int64(0), tc.proc.Mp().CurrNB())
	}()

	tc.arg.JoinType = plan.Node_ASOF
	tc.arg.LeftTypes = []types.Type{keyType, timeType}
	tc.arg.RightTypes = []types.Type{keyType, timeType, textType}
	tc.arg.AsofRightCol = 1
	tc.arg.AsofBuildLeft = true
	tc.arg.NonEqCond = makeAsofCondition(t, timeType, ">=")
	tc.barg.Conditions = equality[0]

	logicalLeft := makeAsofPayloadBatch(
		tc.proc,
		[]int32{1, 1},
		[]types.Timestamp{10000, 10000},
		[]string{"unused", "unused"},
	)
	logicalLeft.Vecs[2].Free(tc.proc.Mp())
	logicalLeft.Vecs = logicalLeft.Vecs[:2]
	const (
		rightBatchCount = 5
		rightBatchRows  = 1024
		rightRows       = rightBatchCount * rightBatchRows
	)
	payloads := make([]string, rightRows)
	rightBatches := make([]*batch.Batch, 0, rightBatchCount)
	for batchIndex := range rightBatchCount {
		keys := make([]int32, rightBatchRows)
		timestamps := make([]types.Timestamp, rightBatchRows)
		batchPayloads := make([]string, rightBatchRows)
		for row := range rightBatchRows {
			globalRow := batchIndex*rightBatchRows + row
			keys[row] = 1
			timestamps[row] = types.Timestamp(globalRow)
			payloads[globalRow] = strings.Repeat("x", 1000) + string(rune('a'+globalRow%26))
			batchPayloads[row] = payloads[globalRow]
		}
		rightBatches = append(rightBatches,
			makeAsofPayloadBatch(tc.proc, keys, timestamps, batchPayloads))
	}
	resetHashBuildChildrenWithBatch(tc.barg, logicalLeft)
	tc.arg.Children = nil
	tc.arg.AppendChild(colexec.NewMockOperator().WithBatchs(rightBatches))
	require.NoError(t, tc.arg.Prepare(tc.proc))
	require.NoError(t, tc.barg.Prepare(tc.proc))
	_, err := vm.Exec(tc.barg, tc.proc)
	require.NoError(t, err)

	result, err := vm.Exec(tc.arg, tc.proc)
	require.NoError(t, err)
	require.NotNil(t, result.Batch)
	require.Equal(t, 2, result.Batch.RowCount())
	require.Equal(t, []string{payloads[rightRows-1], payloads[rightRows-1]},
		vector.InefficientMustStrCol(result.Batch.Vecs[0]))
	require.Zero(t, tc.arg.ctr.asofBuildLeftDeadBytes)
	require.Equal(t, int64(2*len(payloads[rightRows-1])),
		tc.arg.ctr.asofBuildLeftLiveBytes)
	require.LessOrEqual(t,
		len(tc.arg.ctr.asofBuildLeftBestRight.Vecs[0].GetArea()),
		2*len(payloads[rightRows-1]))
}

func makeAsofPayloadBatch(
	proc *process.Process,
	keys []int32,
	timestamps []types.Timestamp,
	payloads []string,
) *batch.Batch {
	bat := batch.NewWithSize(3)
	bat.Vecs[0] = testutil.MakeInt32Vector(keys, nil, proc.Mp())
	bat.Vecs[1] = vector.NewVec(types.T_timestamp.ToType())
	if err := vector.AppendFixedList(bat.Vecs[1], timestamps, nil, proc.Mp()); err != nil {
		panic(err)
	}
	bat.Vecs[2] = testutil.MakeVarcharVector(payloads, nil, proc.Mp())
	bat.SetRowCount(len(keys))
	return bat
}

func makeAsofBatch(proc *process.Process, keys []int32, timestamps []string) *batch.Batch {
	bat := batch.NewWithSize(2)
	bat.Vecs[0] = testutil.MakeInt32Vector(keys, nil, proc.Mp())
	bat.Vecs[1] = testutil.NewTimestampVector(len(timestamps), types.T_timestamp.ToType(), proc.Mp(), false, nil, timestamps)
	bat.SetRowCount(len(keys))
	return bat
}

func makeAsofCondition(t *testing.T, timeType types.Type, operator string) *plan.Expr {
	leftTime := newExpr(1, timeType)
	leftTime.GetCol().RelPos = 0
	rightTime := newExpr(1, timeType)
	rightTime.GetCol().RelPos = 1
	fn, err := function.GetFunctionByName(context.Background(), operator, []types.Type{timeType, timeType})
	require.NoError(t, err)
	return &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_bool)},
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{Obj: fn.GetEncodedOverloadID(), ObjName: operator},
			Args: []*plan.Expr{leftTime, rightTime},
		}},
	}
}

func TestFindAsofPredecessor(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	keyType := types.T_int32.ToType()
	timeType := types.T_timestamp.ToType()
	arg := &HashJoin{
		JoinType:     plan.Node_ASOF_LEFT,
		LeftTypes:    []types.Type{keyType, timeType},
		RightTypes:   []types.Type{keyType, timeType},
		EqConds:      [][]*plan.Expr{{newExpr(0, keyType)}, {newExpr(0, keyType)}},
		NonEqCond:    makeAsofCondition(t, timeType, ">="),
		AsofRightCol: 1,
	}
	installTestAllocation(t, arg)
	require.NoError(t, arg.Prepare(proc))

	left := batch.NewWithSize(2)
	left.Vecs[0] = testutil.MakeInt32Vector([]int32{1}, nil, proc.Mp())
	left.Vecs[1] = testutil.NewTimestampVector(1, timeType, proc.Mp(), false, nil,
		[]string{"2026-01-01 10:00:00"})
	left.SetRowCount(1)
	right := batch.NewWithSize(2)
	right.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 1, 1, 1, 1}, nil, proc.Mp())
	right.Vecs[1] = testutil.NewTimestampVector(5, timeType, proc.Mp(), false, nil,
		[]string{
			"2026-01-01 11:00:00", "2026-01-01 07:00:00", "2026-01-01 09:00:00",
			"2026-01-01 05:00:00", "2026-01-01 08:00:00",
		})
	right.SetRowCount(5)

	arg.ctr.leftBat = left
	arg.ctr.rightBats = []*batch.Batch{right}
	arg.ctr.joinBats[0], arg.ctr.cfs1 = colexec.NewJoinBatch(left, proc.Mp())
	arg.ctr.joinBats[1], arg.ctr.cfs2 = colexec.NewJoinBatch(right, proc.Mp())
	candidates := []int32{0, 1, 2, 3, 4}
	best, found, err := arg.ctr.findAsofPredecessor(arg, proc, 0, 0, candidates)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, int32(2), best)
	_, found, err = arg.ctr.findAsofPredecessor(arg, proc, 0, 1, candidates)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, 2, arg.ctr.asofIndexCount)

	// Equal-timestamp ties follow the materialized build order for this map.
	right.Vecs[1].CleanOnlyData()
	require.NoError(t, vector.AppendFixedList(
		right.Vecs[1], []types.Timestamp{9, 9, 9, 9, 9, 9}, nil, proc.Mp(),
	))
	require.NoError(t, vector.AppendFixed(right.Vecs[0], int32(1), false, proc.Mp()))
	right.SetRowCount(6)
	changedCandidates := []int32{0, 1, 2, 3, 4, 5}
	best, found, err = arg.ctr.findAsofPredecessor(arg, proc, 0, 0, changedCandidates)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, int32(0), best)
	// A changed group belongs to a new immutable-map generation. Rebuilding it
	// drops every old open-addressed entry rather than breaking a collision chain.
	require.Equal(t, 1, arg.ctr.asofIndexCount)
	_, found, err = arg.ctr.findAsofPredecessor(arg, proc, 0, 0, changedCandidates)
	require.NoError(t, err)
	require.True(t, found)

	arg.ctr.leftBat = nil
	arg.ctr.rightBats = nil
	arg.Free(proc, false, nil)
	left.Clean(proc.Mp())
	right.Clean(proc.Mp())
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestAsofIndexChoosesOrderedOrAdaptiveSearch(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	keyType := types.T_int32.ToType()
	timeType := types.T_timestamp.ToType()
	arg := &HashJoin{
		JoinType:     plan.Node_ASOF_LEFT,
		LeftTypes:    []types.Type{keyType, timeType},
		RightTypes:   []types.Type{keyType, timeType},
		EqConds:      [][]*plan.Expr{{newExpr(0, keyType)}, {newExpr(0, keyType)}},
		NonEqCond:    makeAsofCondition(t, timeType, ">="),
		AsofRightCol: 1,
	}
	installTestAllocation(t, arg)
	require.NoError(t, arg.Prepare(proc))

	left := batch.NewWithSize(2)
	left.Vecs[0] = testutil.MakeInt32Vector([]int32{1}, nil, proc.Mp())
	left.Vecs[1] = vector.NewVec(timeType)
	require.NoError(t, vector.AppendFixedList(left.Vecs[1], []types.Timestamp{10}, nil, proc.Mp()))
	left.SetRowCount(1)

	// Four equality groups: ascending, descending, unordered, and unordered
	// with a NULL temporal value. Ordered groups reuse JoinMap selections. An
	// unordered group starts with a one-best-row scan and no per-row index.
	rightTimes := []types.Timestamp{
		5, 7, 9, 11, 13,
		13, 11, 9, 7, 5,
		9, 7, 9, 5, 11,
		0, 9, 8, 6, 12,
	}
	nulls := make([]bool, len(rightTimes))
	nulls[15] = true
	right := batch.NewWithSize(2)
	right.Vecs[0] = testutil.MakeInt32Vector(make([]int32, len(rightTimes)), nil, proc.Mp())
	right.Vecs[1] = vector.NewVec(timeType)
	require.NoError(t, vector.AppendFixedList(right.Vecs[1], rightTimes, nulls, proc.Mp()))
	right.SetRowCount(len(rightTimes))

	arg.ctr.leftBat = left
	arg.ctr.rightBats = []*batch.Batch{right}
	arg.ctr.joinBats[0], arg.ctr.cfs1 = colexec.NewJoinBatch(left, proc.Mp())
	arg.ctr.joinBats[1], arg.ctr.cfs2 = colexec.NewJoinBatch(right, proc.Mp())
	// Tiny groups always use the literal one-best-row scan, even under reuse:
	// no per-group metadata and no per-row index are retained.
	for range 8 {
		best, found, err := arg.ctr.findAsofPredecessor(arg, proc, 0, 99, []int32{10, 11, 12})
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, int32(10), best)
	}
	require.Empty(t, arg.ctr.asofIndexes)
	require.Zero(t, arg.ctr.asofIndexCount)

	tests := []struct {
		key        uint64
		candidates []int32
		want       int32
		order      asofIndexOrder
		entries    int
	}{
		{key: 1, candidates: []int32{0, 1, 2, 3, 4}, want: 2, order: asofIndexAscending},
		{key: 2, candidates: []int32{5, 6, 7, 8, 9}, want: 7, order: asofIndexDescending},
		{key: 3, candidates: []int32{10, 11, 12, 13, 14}, want: 10, order: asofIndexLinear},
		{key: 4, candidates: []int32{15, 16, 17, 18, 19}, want: 16, order: asofIndexLinear},
	}
	for _, test := range tests {
		best, found, err := arg.ctr.findAsofPredecessor(arg, proc, 0, test.key, test.candidates)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, test.want, best)
		index := arg.ctr.asofIndexes[arg.ctr.findAsofIndexSlot(test.key)]
		require.Equal(t, test.order, index.order)
		require.Len(t, index.entries, test.entries)
	}

	// The unordered group is promoted only after its prior scans have paid the
	// estimated fill+sort cost. Equal timestamps still select the first row in
	// the materialized JoinMap selection after promotion.
	unordered := []int32{10, 11, 12, 13, 14}
	index := &arg.ctr.asofIndexes[arg.ctr.findAsofIndexSlot(3)]
	for index.linearProbes < asofIndexPromotionScans(index.candidateCount, index.validCount) {
		best, found, err := arg.ctr.findAsofPredecessor(arg, proc, 0, 3, unordered)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, int32(10), best)
	}
	best, found, err := arg.ctr.findAsofPredecessor(arg, proc, 0, 3, unordered)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, int32(10), best)
	require.Equal(t, asofIndexSorted, index.order)
	require.Len(t, index.entries, 5)
	require.Equal(t, int32(10), searchSortedAsof(index, 9, false))
	require.Equal(t, int32(11), searchSortedAsof(index, 9, true))

	arg.ctr.leftBat = nil
	arg.ctr.rightBats = nil
	arg.Free(proc, false, nil)
	left.Clean(proc.Mp())
	right.Clean(proc.Mp())
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestAsofUnorderedIndexPromotesAfterAmortizationAndReusesAllocation(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	keyType := types.T_int32.ToType()
	timeType := types.T_timestamp.ToType()
	arg := &HashJoin{
		JoinType:     plan.Node_ASOF,
		LeftTypes:    []types.Type{keyType, timeType},
		RightTypes:   []types.Type{keyType, timeType},
		EqConds:      [][]*plan.Expr{{newExpr(0, keyType)}, {newExpr(0, keyType)}},
		NonEqCond:    makeAsofCondition(t, timeType, ">="),
		AsofRightCol: 1,
	}
	installTestAllocation(t, arg)
	require.NoError(t, arg.Prepare(proc))

	const rowCount = 4095
	const repeatedProbes = 1024
	const probeRows = repeatedProbes + 16 // promotion uses at most eight prior scans
	leftTimestamps := make([]types.Timestamp, probeRows)
	leftKeys := make([]int32, probeRows)
	for i := range probeRows {
		leftKeys[i] = 1
		// Exercise forward and backward probe-time movement across both the
		// linear and sorted states. The build side contains every value.
		leftTimestamps[i] = types.Timestamp((i*997 + 17) % rowCount)
	}
	leftTimestamps[0] = rowCount - 1
	left := batch.NewWithSize(2)
	left.Vecs[0] = testutil.MakeInt32Vector(leftKeys, nil, proc.Mp())
	left.Vecs[1] = vector.NewVec(timeType)
	require.NoError(t, vector.AppendFixedList(left.Vecs[1], leftTimestamps, nil, proc.Mp()))
	left.SetRowCount(probeRows)

	candidates := make([]int32, rowCount)
	timestamps := make([]types.Timestamp, rowCount)
	keys := make([]int32, rowCount)
	for i := range rowCount {
		candidates[i] = int32(i)
		keys[i] = 1
		// 37 is coprime with 4095, producing a deterministic permutation.
		timestamps[i] = types.Timestamp((i * 37) % rowCount)
	}
	right := batch.NewWithSize(2)
	right.Vecs[0] = testutil.MakeInt32Vector(keys, nil, proc.Mp())
	right.Vecs[1] = vector.NewVec(timeType)
	require.NoError(t, vector.AppendFixedList(right.Vecs[1], timestamps, nil, proc.Mp()))
	right.SetRowCount(rowCount)

	arg.ctr.leftBat = left
	arg.ctr.rightBats = []*batch.Batch{right}
	arg.ctr.joinBats[0], arg.ctr.cfs1 = colexec.NewJoinBatch(left, proc.Mp())
	arg.ctr.joinBats[1], arg.ctr.cfs2 = colexec.NewJoinBatch(right, proc.Mp())

	best, found, err := arg.ctr.findAsofPredecessor(arg, proc, 0, 1, candidates)
	require.NoError(t, err)
	require.True(t, found)
	bestValue, valid := arg.ctr.asofRightTemporalValue(arg, best)
	require.True(t, valid)
	require.Equal(t, int64(rowCount-1), bestValue)
	index := &arg.ctr.asofIndexes[arg.ctr.findAsofIndexSlot(1)]
	require.Equal(t, asofIndexLinear, index.order)
	require.Empty(t, index.entries)
	promotionScans := asofIndexPromotionScans(index.candidateCount, index.validCount)
	require.Greater(t, promotionScans, uint32(1))
	probeRow := int64(1)
	for index.linearProbes < promotionScans {
		best, found, err = arg.ctr.findAsofPredecessor(arg, proc, probeRow, 1, candidates)
		require.NoError(t, err)
		require.True(t, found)
		bestValue, valid = arg.ctr.asofRightTemporalValue(arg, best)
		require.True(t, valid)
		require.Equal(t, int64(leftTimestamps[probeRow]), bestValue)
		require.Equal(t, asofIndexLinear, index.order)
		require.Empty(t, index.entries)
		probeRow++
	}

	// The next probe buys the compact sorted representation. No AVL/tree nodes
	// or retained per-probe candidates are involved.
	best, found, err = arg.ctr.findAsofPredecessor(arg, proc, probeRow, 1, candidates)
	require.NoError(t, err)
	require.True(t, found)
	bestValue, valid = arg.ctr.asofRightTemporalValue(arg, best)
	require.True(t, valid)
	require.Equal(t, int64(leftTimestamps[probeRow]), bestValue)
	probeRow++
	require.Equal(t, asofIndexSorted, index.order)
	require.Len(t, index.entries, rowCount)

	// Repeated probes reuse the immutable index and add no retained memory.
	retained := proc.Mp().CurrNB()
	for range repeatedProbes {
		best, found, err = arg.ctr.findAsofPredecessor(arg, proc, probeRow, 1, candidates)
		require.NoError(t, err)
		require.True(t, found)
		bestValue, valid = arg.ctr.asofRightTemporalValue(arg, best)
		require.True(t, valid)
		require.Equal(t, int64(leftTimestamps[probeRow]), bestValue)
		probeRow++
	}
	require.Equal(t, retained, proc.Mp().CurrNB())

	arg.ctr.leftBat = nil
	arg.ctr.rightBats = nil
	arg.Free(proc, false, nil)
	left.Clean(proc.Mp())
	right.Clean(proc.Mp())
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestAsofIndexPromotionScansBalancesReuseAndBuildCost(t *testing.T) {
	tests := []struct {
		name       string
		candidates int32
		valid      int32
		want       uint32
	}{
		{name: "empty", candidates: 0, valid: 0, want: ^uint32(0)},
		{name: "smallest adaptive group", candidates: 5, valid: 5, want: 4},
		{name: "ordinary unordered", candidates: 16, valid: 16, want: 4},
		{name: "large unordered", candidates: 4095, valid: 4095, want: 4},
		{name: "mostly null", candidates: 4095, valid: 1, want: 2},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, asofIndexPromotionScans(test.candidates, test.valid))
		})
	}
}

func TestAsofSortedIndexAllocationFailureKeepsLinearState(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	registry, err := mpool.NewAllocationAccountRegistry(1, 8)
	require.NoError(t, err)
	account, err := registry.Open(1)
	require.NoError(t, err)
	arg := &HashJoin{AsofRightCol: 0}
	require.NoError(t, arg.SetAllocationAccount(account))

	right := batch.NewWithSize(1)
	right.Vecs[0] = vector.NewVec(types.T_timestamp.ToType())
	require.NoError(t, vector.AppendFixedList(
		right.Vecs[0], []types.Timestamp{3, 1, 2}, nil, proc.Mp(),
	))
	right.SetRowCount(3)
	arg.ctr.rightBats = []*batch.Batch{right}
	index := &asofIndex{
		validCount:   3,
		linearProbes: 8,
		order:        asofIndexLinear,
	}

	err = arg.ctr.buildSortedAsofIndex(arg, proc, index, []int32{0, 1, 2})
	require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
	require.Equal(t, asofIndexLinear, index.order)
	require.Empty(t, index.entries)
	require.Zero(t, account.Snapshot().Used)

	arg.ctr.rightBats = nil
	right.Clean(proc.Mp())
	require.NoError(t, arg.ClearAllocationAccount(account))
	account.Seal()
	_, err = registry.Finalize(account)
	require.NoError(t, err)
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

var benchmarkAsofBest int32

func BenchmarkAsofUnorderedGroupLookup(b *testing.B) {
	for _, test := range []struct {
		name     string
		rowCount int
		stride   int
	}{
		{name: "rows-3", rowCount: 3, stride: 2},
		{name: "rows-16", rowCount: 16, stride: 5},
		{name: "rows-256", rowCount: 256, stride: 37},
		{name: "rows-4095", rowCount: 4095, stride: 37},
	} {
		b.Run(test.name, func(b *testing.B) {
			benchmarkAsofUnorderedGroupLookup(b, test.rowCount, test.stride)
		})
	}
}

func benchmarkAsofUnorderedGroupLookup(b *testing.B, rowCount, stride int) {
	proc := testutil.NewProcessWithMPool(b, "", mpool.MustNewZero())
	arg := &HashJoin{AsofRightCol: 1}
	installTestAllocation(b, arg)

	candidates := make([]int32, rowCount)
	timestamps := make([]types.Timestamp, rowCount)
	for i := range rowCount {
		candidates[i] = int32(i)
		timestamps[i] = types.Timestamp((i * stride) % rowCount)
	}
	right := batch.NewWithSize(2)
	right.Vecs[1] = vector.NewVec(types.T_timestamp.ToType())
	if err := vector.AppendFixedList(right.Vecs[1], timestamps, nil, proc.Mp()); err != nil {
		b.Fatal(err)
	}
	right.SetRowCount(rowCount)
	arg.ctr.rightBats = []*batch.Batch{right}

	index := &asofIndex{validCount: int32(rowCount)}
	if err := arg.ctr.buildSortedAsofIndex(arg, proc, index, candidates); err != nil {
		b.Fatal(err)
	}
	defer func() {
		mpool.FreeSlice(proc.Mp(), index.entries)
		arg.ctr.rightBats = nil
		right.Clean(proc.Mp())
		proc.Free()
	}()

	b.Run("linear", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			benchmarkAsofBest = arg.ctr.scanAsofBest(arg, candidates, int64(rowCount-1), false)
		}
	})
	b.Run("build-sorted", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			candidateIndex := &asofIndex{validCount: int32(rowCount)}
			if err := arg.ctr.buildSortedAsofIndex(arg, proc, candidateIndex, candidates); err != nil {
				b.Fatal(err)
			}
			mpool.FreeSlice(proc.Mp(), candidateIndex.entries)
		}
	})
	b.Run("sorted", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			benchmarkAsofBest = searchSortedAsof(index, int64(rowCount-1), false)
		}
	})
}

func TestAsofIndexMetadataGrowsAmortizedAndCleans(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	keyType := types.T_int32.ToType()
	timeType := types.T_timestamp.ToType()
	arg := &HashJoin{
		JoinType:     plan.Node_ASOF_LEFT,
		LeftTypes:    []types.Type{keyType, timeType},
		RightTypes:   []types.Type{keyType, timeType},
		EqConds:      [][]*plan.Expr{{newExpr(0, keyType)}, {newExpr(0, keyType)}},
		NonEqCond:    makeAsofCondition(t, timeType, ">="),
		AsofRightCol: 1,
	}
	installTestAllocation(t, arg)
	require.NoError(t, arg.Prepare(proc))
	left := batch.NewWithSize(2)
	left.Vecs[0] = testutil.MakeInt32Vector([]int32{1}, nil, proc.Mp())
	left.Vecs[1] = testutil.NewTimestampVector(1, timeType, proc.Mp(), false, nil, []string{"2026-01-01 10:00:00"})
	left.SetRowCount(1)
	right := batch.NewWithSize(2)
	right.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 1, 1, 1, 1}, nil, proc.Mp())
	right.Vecs[1] = testutil.NewTimestampVector(5, timeType, proc.Mp(), false, nil,
		[]string{
			"2026-01-01 09:00:00", "2026-01-01 08:00:00", "2026-01-01 07:00:00",
			"2026-01-01 06:00:00", "2026-01-01 05:00:00",
		})
	right.SetRowCount(5)
	arg.ctr.leftBat = left
	arg.ctr.rightBats = []*batch.Batch{right}
	arg.ctr.joinBats[0], arg.ctr.cfs1 = colexec.NewJoinBatch(left, proc.Mp())
	arg.ctr.joinBats[1], arg.ctr.cfs2 = colexec.NewJoinBatch(right, proc.Mp())
	// Probe in reverse order so insertion is not an append-shaped workload.
	// The index must use bounded-amortized placement rather than shifting all
	// previously seen groups for each first touch.
	for group := uint64(64); group > 0; group-- {
		_, found, err := arg.ctr.findAsofPredecessor(
			arg, proc, 0, group, []int32{0, 1, 2, 3, 4},
		)
		require.NoError(t, err)
		require.True(t, found)
	}
	require.Equal(t, 64, arg.ctr.asofIndexCount)
	require.GreaterOrEqual(t, cap(arg.ctr.asofIndexes), arg.ctr.asofIndexCount)
	arg.ctr.leftBat = nil
	arg.ctr.rightBats = nil
	arg.Free(proc, false, nil)
	left.Clean(proc.Mp())
	right.Clean(proc.Mp())
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestAsofTemporalMetadataFindsNestedAndCommutedPredicate(t *testing.T) {
	timeType := types.T_timestamp.ToType()
	left := newExpr(1, timeType)
	left.GetCol().RelPos = 0
	right := newExpr(1, timeType)
	right.GetCol().RelPos = 1
	commuted := &plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{
		Func: &plan.ObjectRef{ObjName: "<"}, Args: []*plan.Expr{right, left},
	}}}
	tolerance := &plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{
		Func: &plan.ObjectRef{ObjName: ">="}, Args: []*plan.Expr{right, left},
	}}}
	nested := &plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{
		Func: &plan.ObjectRef{ObjName: "and"}, Args: []*plan.Expr{commuted, tolerance},
	}}}
	leftCol, rightCol, strict := asofTemporalMetadata(nested)
	require.Equal(t, 1, leftCol)
	require.Equal(t, 1, rightCol)
	require.True(t, strict)
}

func TestAsofPrepareRejectsMismatchedRightTemporalColumn(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	keyType := types.T_int32.ToType()
	timeType := types.T_timestamp.ToType()
	arg := &HashJoin{
		JoinType:     plan.Node_ASOF,
		LeftTypes:    []types.Type{keyType, timeType},
		RightTypes:   []types.Type{keyType, timeType, timeType},
		EqConds:      [][]*plan.Expr{{newExpr(0, keyType)}, {newExpr(0, keyType)}},
		NonEqCond:    makeAsofCondition(t, timeType, ">="),
		AsofRightCol: 2,
	}
	require.ErrorContains(t, arg.Prepare(proc), "invalid ASOF temporal predicate metadata")
}

func TestAsofAllocationAccountCannotDetachWithEmptyIndexTable(t *testing.T) {
	arg := &HashJoin{}
	account := installTestAllocation(t, arg)
	arg.ctr.asofIndexes = make([]asofIndex, 8)
	require.ErrorIs(t, arg.ClearAllocationAccount(account), mpool.ErrAllocationAccountInvariant)
	arg.ctr.asofIndexes = nil
	require.NoError(t, arg.ClearAllocationAccount(account))
}

/*
	func BenchmarkJoin(b *testing.B) {
		for i := 0; i < b.N; i++ {
			tcs = []joinTestCase{
				newTestCase([]bool{false}, []types.Type{types.T_int8.ToType()}, []colexec.ResultPos{colexec.NewResultPos(0, 0), colexec.NewResultPos(1, 0)},
					[][]*plan.Expr{
						{
							newExpr(0, types.T_int8.ToType()),
						},
						{
							newExpr(0, types.T_int8.ToType()),
						},
					}),
				newTestCase([]bool{true}, []types.Type{types.T_int8.ToType()}, []colexec.ResultPos{colexec.NewResultPos(0, 0), colexec.NewResultPos(1, 0)},
					[][]*plan.Expr{
						{
							newExpr(0, types.T_int8.ToType()),
						},
						{
							newExpr(0, types.T_int8.ToType()),
						},
					}),
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
				tc.proc.Reg.MergeReceivers[1].Ch <- testutil.NewRegMsg(bats[0])
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
func newExpr(pos int32, typ types.Type) *plan.Expr {
	return &plan.Expr{
		Typ: plan.Type{
			Scale: typ.Scale,
			Width: typ.Width,
			Id:    int32(typ.Oid),
		},
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				ColPos: pos,
			},
		},
	}
}

func newTestCase(t *testing.T, flgs []bool, ts []types.Type, rp []colexec.ResultPos, cs [][]*plan.Expr) joinTestCase {
	return newTestCaseWithMPool(t, mpool.MustNewZero(), flgs, ts, rp, cs)
}

func newTestCaseWithMPool(
	t *testing.T,
	m *mpool.MPool,
	flgs []bool,
	ts []types.Type,
	rp []colexec.ResultPos,
	cs [][]*plan.Expr,
) joinTestCase {
	proc := testutil.NewProcessWithMPool(t, "", m)
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
	tc := joinTestCase{
		types:  ts,
		flgs:   flgs,
		proc:   proc,
		cancel: cancel,
		arg: &HashJoin{
			LeftTypes:  ts,
			RightTypes: ts,
			ResultCols: rp,
			EqConds:    cs,
			NumCPU:     1,
			IsMerger:   true,
			NonEqCond:  cond,
			OperatorBase: vm.OperatorBase{
				OperatorInfo: vm.OperatorInfo{
					Idx:     0,
					IsFirst: false,
					IsLast:  false,
				},
			},
			JoinMapTag: tag,
		},
		barg: &hashbuild.HashBuild{
			NeedHashMap: true,
			Conditions:  cs[1],
			OperatorBase: vm.OperatorBase{
				OperatorInfo: vm.OperatorInfo{
					Idx:     0,
					IsFirst: false,
					IsLast:  false,
				},
			},
			NeedAllocateSels: true,
			NeedBatches:      true,
			JoinMapTag:       tag,
			JoinMapRefCnt:    1,
		},
		resultBatch: resultBatch,
	}
	installTestAllocation(t, tc.arg, tc.barg)
	return tc
}

func resetChildren(arg *HashJoin, m *mpool.MPool) {
	resetChildrenWithBatch(arg, colexec.MakeMockBatchs(m))
}

func resetChildrenWithBatch(arg *HashJoin, bat *batch.Batch) {
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

func TestHashJoinTypeName(t *testing.T) {
	arg := NewArgument()
	require.Equal(t, "hash_join", arg.TypeName())
	arg.Release()
}

func TestAsofNeedsBuildBatches(t *testing.T) {
	arg := NewArgument()
	arg.JoinType = plan.Node_ASOF
	require.True(t, arg.NeedBuildBatches())
	require.True(t, arg.IsAsof())
	arg.Release()
}

func TestHashJoinOpType(t *testing.T) {
	arg := NewArgument()
	require.Equal(t, vm.HashJoin, arg.OpType())
	arg.Release()
}

func TestHashJoinReleaseAndReuse(t *testing.T) {
	arg := NewArgument()
	arg.JoinMapTag = 100
	arg.Release()

	arg2 := NewArgument()
	require.Equal(t, int32(0), arg2.JoinMapTag)
	arg2.Release()
}

func TestHashJoinTypeCheckers(t *testing.T) {
	tests := []struct {
		name        string
		joinType    plan.Node_JoinType
		isRightJoin bool
		checks      map[string]bool
	}{
		{
			name:     "inner join",
			joinType: plan.Node_INNER,
			checks: map[string]bool{
				"IsInner": true, "IsLeftOuter": false, "IsRightOuter": false,
				"IsFullOuter": false, "IsSemi": false, "IsAnti": false, "IsSingle": false,
				"EmitUnmatchedProbe": false, "EmitUnmatchedBuild": false,
			},
		},
		{
			name:     "left outer join",
			joinType: plan.Node_LEFT,
			checks: map[string]bool{
				"IsInner": false, "IsLeftOuter": true, "IsRightOuter": false,
				"EmitUnmatchedProbe": true, "EmitUnmatchedBuild": false,
			},
		},
		{
			name:        "right outer join",
			joinType:    plan.Node_RIGHT,
			isRightJoin: true,
			checks: map[string]bool{
				"IsInner": false, "IsLeftOuter": false, "IsRightOuter": true,
				"EmitUnmatchedProbe": false, "EmitUnmatchedBuild": true,
			},
		},
		{
			name:        "full outer join",
			joinType:    plan.Node_OUTER,
			isRightJoin: true,
			checks: map[string]bool{
				"IsFullOuter": true, "IsInner": false,
				"IsLeftOuter": false, "IsRightOuter": false,
				"IsLeftSemi": false, "IsRightSemi": false,
				"IsLeftAnti": false, "IsRightAnti": false,
				"IsLeftSingle": false, "IsRightSingle": false,
				"EmitUnmatchedProbe": true, "EmitUnmatchedBuild": true,
			},
		},
		{
			name:     "left semi join",
			joinType: plan.Node_SEMI,
			checks: map[string]bool{
				"IsSemi": true, "IsLeftSemi": true, "IsRightSemi": false,
			},
		},
		{
			name:        "right semi join",
			joinType:    plan.Node_SEMI,
			isRightJoin: true,
			checks: map[string]bool{
				"IsSemi": true, "IsLeftSemi": false, "IsRightSemi": true,
			},
		},
		{
			name:     "left anti join",
			joinType: plan.Node_ANTI,
			checks: map[string]bool{
				"IsAnti": true, "IsLeftAnti": true, "IsRightAnti": false,
			},
		},
		{
			name:        "right anti join",
			joinType:    plan.Node_ANTI,
			isRightJoin: true,
			checks: map[string]bool{
				"IsAnti": true, "IsLeftAnti": false, "IsRightAnti": true,
			},
		},
		{
			name:     "left single join",
			joinType: plan.Node_SINGLE,
			checks: map[string]bool{
				"IsSingle": true, "IsLeftSingle": true, "IsRightSingle": false,
			},
		},
		{
			name:        "right single join",
			joinType:    plan.Node_SINGLE,
			isRightJoin: true,
			checks: map[string]bool{
				"IsSingle": true, "IsLeftSingle": false, "IsRightSingle": true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			arg := &HashJoin{
				JoinType:    tt.joinType,
				IsRightJoin: tt.isRightJoin,
			}

			if expected, ok := tt.checks["IsInner"]; ok {
				require.Equal(t, expected, arg.IsInner())
			}
			if expected, ok := tt.checks["IsLeftOuter"]; ok {
				require.Equal(t, expected, arg.IsLeftOuter())
			}
			if expected, ok := tt.checks["IsRightOuter"]; ok {
				require.Equal(t, expected, arg.IsRightOuter())
			}
			if expected, ok := tt.checks["IsFullOuter"]; ok {
				require.Equal(t, expected, arg.IsFullOuter())
			}
			if expected, ok := tt.checks["IsSemi"]; ok {
				require.Equal(t, expected, arg.IsSemi())
			}
			if expected, ok := tt.checks["IsLeftSemi"]; ok {
				require.Equal(t, expected, arg.IsLeftSemi())
			}
			if expected, ok := tt.checks["IsRightSemi"]; ok {
				require.Equal(t, expected, arg.IsRightSemi())
			}
			if expected, ok := tt.checks["IsAnti"]; ok {
				require.Equal(t, expected, arg.IsAnti())
			}
			if expected, ok := tt.checks["IsLeftAnti"]; ok {
				require.Equal(t, expected, arg.IsLeftAnti())
			}
			if expected, ok := tt.checks["IsRightAnti"]; ok {
				require.Equal(t, expected, arg.IsRightAnti())
			}
			if expected, ok := tt.checks["IsSingle"]; ok {
				require.Equal(t, expected, arg.IsSingle())
			}
			if expected, ok := tt.checks["IsLeftSingle"]; ok {
				require.Equal(t, expected, arg.IsLeftSingle())
			}
			if expected, ok := tt.checks["IsRightSingle"]; ok {
				require.Equal(t, expected, arg.IsRightSingle())
			}
			if expected, ok := tt.checks["EmitUnmatchedProbe"]; ok {
				require.Equal(t, expected, arg.EmitUnmatchedProbe(),
					"EmitUnmatchedProbe mismatch for %s", tt.name)
			}
			if expected, ok := tt.checks["EmitUnmatchedBuild"]; ok {
				require.Equal(t, expected, arg.EmitUnmatchedBuild(),
					"EmitUnmatchedBuild mismatch for %s", tt.name)
			}
		})
	}
}

func TestHashJoinGetOperatorBase(t *testing.T) {
	arg := NewArgument()
	base := arg.GetOperatorBase()
	require.NotNil(t, base)
	arg.Release()
}

func TestHashJoinExecProjection(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	arg := NewArgument()
	bat := testutil.NewBatch([]types.Type{types.T_int32.ToType()}, false, 10, proc.Mp())
	result, err := arg.ExecProjection(proc, bat)
	require.NoError(t, err)
	require.Equal(t, bat, result)
	arg.Release()
	proc.Free()
}
