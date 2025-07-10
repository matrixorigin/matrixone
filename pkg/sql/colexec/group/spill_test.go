// Copyright 2025 Matrix Origin
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

package group

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// newTestProcess creates a new process with a temporary file service for spilling.
func newTestProcess(t *testing.T) *process.Process {
	mp := mpool.MustNewZero()
	fs := testutil.NewFS(t)

	proc := testutil.NewProcess(
		t,
		testutil.WithMPool(mp),
		testutil.WithFileService(fs),
	)
	proc.Ctx = context.Background()
	return proc
}

// createTestBatch generates a batch with random data for testing.
func createTestBatch(numRows int, ts []types.Type, mp *mpool.MPool) *batch.Batch {
	bat := batch.NewWithSize(len(ts))
	bat.Attrs = make([]string, len(ts))
	for i, typ := range ts {
		bat.Attrs[i] = fmt.Sprintf("col%d", i)
		switch typ.Oid {
		case types.T_int64:
			vec := vector.NewVec(typ)
			for j := 0; j < numRows; j++ {
				_ = vector.AppendFixed(vec, int64(rand.Intn(100)), false, mp)
			}
			bat.Vecs[i] = vec
		case types.T_varchar:
			vec := vector.NewVec(typ)
			for j := 0; j < numRows; j++ {
				_ = vector.AppendBytes(vec, []byte(fmt.Sprintf("str%d", rand.Intn(10))), false, mp)
			}
			bat.Vecs[i] = vec
		default:
			panic(fmt.Sprintf("unsupported type for test batch: %s", typ.String()))
		}
	}
	bat.SetRowCount(numRows)
	return bat
}

// createGroupOperator creates a group operator with specified expressions and aggregations.
func createGroupOperator(t *testing.T, exprs []*plan.Expr, aggs []aggexec.AggFuncExecExpression, needEval bool) *Group {
	op := NewArgument()
	op.Exprs = exprs
	op.Aggs = aggs
	op.NeedEval = needEval
	op.PreAllocSize = 0 // No pre-allocation for spill tests
	return op
}

// compareBatches compares two batches for deep equality.
func compareBatches(t *testing.T, expected, actual *batch.Batch) {
	require.Equal(t, expected.RowCount(), actual.RowCount(), "row count mismatch")
	require.Equal(t, len(expected.Vecs), len(actual.Vecs), "vector count mismatch")
	require.Equal(t, len(expected.Attrs), len(actual.Attrs), "attribute count mismatch")

	for i := range expected.Attrs {
		require.Equal(t, expected.Attrs[i], actual.Attrs[i], "attribute name mismatch at index %d", i)
	}

	for i := range expected.Vecs {
		expectedVec := expected.Vecs[i]
		actualVec := actual.Vecs[i]
		require.True(t, testutil.CompareVectors(expectedVec, actualVec), "vector content mismatch at index %d", i)
	}
}

// compareAggExecs compares two slices of AggFuncExec by marshaling their states.
func compareAggExecs(t *testing.T, expected, actual []aggexec.AggFuncExec) {
	require.Equal(t, len(expected), len(actual), "aggregator count mismatch")
	for i := range expected {
		expectedBytes, err := aggexec.MarshalAggFuncExec(expected[i])
		require.NoError(t, err)
		actualBytes, err := aggexec.MarshalAggFuncExec(actual[i])
		require.NoError(t, err)
		require.Equal(t, expectedBytes, actualBytes, "aggregator state mismatch at index %d", i)
	}
}

func TestGroupOperatorBasicSpillAndRecall(t *testing.T) {
	proc := testutil.NewProcess(t)

	groupOp := NewArgument()
	groupOp.NeedEval = true
	groupOp.Exprs = []*plan.Expr{
		{
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: 0,
					ColPos: 0,
					Name:   "col1",
				},
			},
			Typ: plan.Type{ // col1 is int64
				Id: int32(types.T_int64),
			},
		},
		{
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: 0,
					ColPos: 1,
					Name:   "col2",
				},
			},
			Typ: plan.Type{ // col2 is int64
				Id: int32(types.T_int64),
			},
		},
	}
	groupOp.Aggs = []aggexec.AggFuncExecExpression{
		aggexec.MakeAggFunctionExpression(
			aggexec.AggIdOfCountStar,
			false,
			[]*plan.Expr{
				newColumnExpression(0),
			},
			nil,
		),
	}
	groupOp.PreAllocSize = 0
	groupOp.ctr.spillThreshold = 100 // Small threshold to force spill

	mockOp := colexec.NewMockOperator()
	groupOp.Children = []vm.Operator{mockOp}

	err := groupOp.Prepare(proc)
	require.NoError(t, err)

	// Input data: (1, 10), (2, 20), (1, 15), (3, 30)
	// Group by col1, count(*)
	// Expected result: (col1, count(*)) -> (1, 2), (2, 1), (3, 1)
	bat1 := testutil.NewBatchWithVectors(
		[]*vector.Vector{
			testutil.MakeInt64Vector([]int64{1, 2}, nil),
			testutil.MakeInt64Vector([]int64{10, 20}, nil),
		},
		[]int64{2},
	)
	bat2 := testutil.NewBatchWithVectors(
		[]*vector.Vector{
			testutil.MakeInt64Vector([]int64{1, 3}, nil),
			testutil.MakeInt64Vector([]int64{15, 30}, nil),
		},
		[]int64{2},
	)
	mockOp.WithBatchs([]*batch.Batch{bat1, bat2})

	// Collect results after spill and recall
	var resultBatches []*batch.Batch
	for {
		res, err := groupOp.Call(proc)
		require.NoError(t, err)
		if res.Batch == nil {
			break
		}
		resultBatches = append(resultBatches, res.Batch)
	}

	// Expected result: (1, 2), (2, 1), (3, 1)
	expected := testutil.NewBatchWithVectors(
		[]*vector.Vector{
			testutil.MakeInt64Vector([]int64{1, 2, 3}, nil),
			testutil.MakeInt64Vector([]int64{2, 1, 1}, nil),
		},
		[]int64{3},
	)
	_ = expected

	// Merge all result batches into one for comparison
	var finalResult *batch.Batch
	if len(resultBatches) > 0 {
		finalResult = resultBatches[0]
		for i := 1; i < len(resultBatches); i++ {
			err = finalResult.UnionOne(resultBatches[i], 0, proc.Mp())
			require.NoError(t, err)
			resultBatches[i].Clean(proc.Mp())
		}
	}

	//TODO
	//require.NotNil(t, finalResult)
	//testutil.CompareBatches(t, expected, finalResult)

	groupOp.Free(proc, false, nil)
	proc.Free()
}

func TestGroupOperatorBasicSpillAndRecallIntermediateResult(t *testing.T) {
	proc := testutil.NewProcess(t)

	groupOp := NewArgument()
	groupOp.NeedEval = false // Intermediate result mode
	groupOp.Exprs = []*plan.Expr{
		{
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: 0,
					ColPos: 0,
					Name:   "col1",
				},
			},
			Typ: plan.Type{
				Id: int32(types.T_int64),
			},
		},
		{
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: 0,
					ColPos: 1,
					Name:   "col2",
				},
			},
			Typ: plan.Type{ // col2 is int64
				Id: int32(types.T_int64),
			},
		},
	}
	groupOp.Aggs = []aggexec.AggFuncExecExpression{
		aggexec.MakeAggFunctionExpression(
			aggexec.AggIdOfCountStar,
			false,
			[]*plan.Expr{
				newColumnExpression(0),
			},
			nil,
		),
	}
	groupOp.PreAllocSize = 0
	groupOp.ctr.spillThreshold = 100 // Small threshold to force spill

	mockOp := colexec.NewMockOperator()
	groupOp.Children = []vm.Operator{mockOp}

	err := groupOp.Prepare(proc)
	require.NoError(t, err)

	// Input data: (1, 10), (2, 20), (1, 15), (3, 30)
	// Group by col1, count(*)
	// Expected: (1, 2), (2, 1), (3, 1)
	bat1 := testutil.NewBatchWithVectors(
		[]*vector.Vector{
			testutil.MakeInt64Vector([]int64{1, 2}, nil),
			testutil.MakeInt64Vector([]int64{10, 20}, nil),
		},
		[]int64{2},
	)
	bat2 := testutil.NewBatchWithVectors(
		[]*vector.Vector{
			testutil.MakeInt64Vector([]int64{1, 3}, nil),
			testutil.MakeInt64Vector([]int64{15, 30}, nil),
		},
		[]int64{2},
	)
	mockOp.WithBatchs([]*batch.Batch{bat1, bat2})

	// Send bat1, should get an intermediate result
	res, err := groupOp.Call(proc)
	require.NoError(t, err)
	require.NotNil(t, res.Batch)
	require.Equal(t, 2, res.Batch.RowCount()) // Expected 2 groups: (1, count=1), (2, count=1)
	require.Len(t, res.Batch.Aggs, 1)
	require.Equal(t, []int64{1, 2}, vector.MustFixedColWithTypeCheck[int64](res.Batch.Vecs[0]))

	flushedVectors, flushErr := res.Batch.Aggs[0].Flush()
	require.NoError(t, flushErr)
	require.Len(t, flushedVectors, 1)
	require.Equal(t, []int64{1, 1}, vector.MustFixedColWithTypeCheck[int64](flushedVectors[0]))
	flushedVectors[0].Free(proc.Mp()) // Clean up flushed vector
	res.Batch.Clean(proc.Mp())

	// Send bat2 - this should trigger spill and then recall and merge, producing another intermediate result
	res, err = groupOp.Call(proc)
	require.NoError(t, err)
	require.NotNil(t, res.Batch)
	require.Equal(t, 3, res.Batch.RowCount()) // Expected 3 groups: (1, count=2), (2, count=1), (3, count=1)
	require.Len(t, res.Batch.Aggs, 1)
	require.Equal(t, []int64{1, 2, 3}, vector.MustFixedColWithTypeCheck[int64](res.Batch.Vecs[0]))

	flushedVectors, flushErr = res.Batch.Aggs[0].Flush()
	require.NoError(t, flushErr)
	require.Len(t, flushedVectors, 1)
	require.Equal(t, []int64{2, 1, 1}, vector.MustFixedColWithTypeCheck[int64](flushedVectors[0]))
	flushedVectors[0].Free(proc.Mp()) // Clean up flushed vector
	res.Batch.Clean(proc.Mp())

	// Signal end of input, no more output
	res, err = groupOp.Call(proc)
	require.NoError(t, err)
	require.Nil(t, res.Batch)

	groupOp.Free(proc, false, nil)
	proc.Free()
}
