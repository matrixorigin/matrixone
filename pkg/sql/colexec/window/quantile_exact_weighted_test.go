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

package window

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/stretchr/testify/require"
)

// newQuantileExactWeightedWindowAggExpr resolves the published registry entry
// and strips the level into the aggregate's configuration exactly as
// constructWindow does, leaving the two data arguments for the executor. The
// distinct flag mirrors the bit the window binder records into the plan
// function's obj and constructWindow reads back out.
func newQuantileExactWeightedWindowAggExpr(
	t *testing.T, distinct bool, level string,
) aggexec.AggFuncExecExpression {
	t.Helper()
	valueType := types.T_int32.ToType()
	weightType := types.T_int32.ToType()
	levelType := types.T_float64.ToType()
	e, err := function.GetFunctionByName(context.Background(),
		"quantileexactweighted",
		[]types.Type{valueType, weightType, levelType})
	require.NoError(t, err)
	return aggexec.MakeAggFunctionExpression(
		e.GetEncodedOverloadID(),
		distinct,
		[]*plan.Expr{
			newColExprWithType(0, valueType),
			newColExprWithType(1, weightType),
		},
		aggexec.EncodeQuantileExactWeightedConfig([]byte(level)),
	)
}

func makeWeightedPartition(
	t *testing.T, mp *mpool.MPool,
	values, weights []int32, key int32,
) *batch.Batch {
	t.Helper()
	bat := batch.NewWithSize(3)
	bat.Vecs[0] = testutil.MakeInt32Vector(values, nil, mp)
	bat.Vecs[1] = testutil.MakeInt32Vector(weights, nil, mp)
	keys := make([]int32, len(values))
	for i := range keys {
		keys[i] = key
	}
	bat.Vecs[2] = testutil.MakeInt32Vector(keys, nil, mp)
	bat.SetRowCount(len(values))
	return bat
}

// TestWindowQuantileExactWeightedFullPartitionBroadcasts drives the window
// operator end to end over two partitions and asserts the exact weighted
// percentile each partition broadcasts: the full-partition fast path admits
// the aggregate through the orderedSetWindowAggregate name gate.
func TestWindowQuantileExactWeightedFullPartitionBroadcasts(t *testing.T) {
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	defer func() {
		proc.Free()
		require.Equal(t, int64(0), proc.Mp().CurrNB())
	}()

	first := makeWeightedPartition(t, proc.Mp(),
		[]int32{1, 3}, []int32{1, 3}, 1)
	second := makeWeightedPartition(t, proc.Mp(),
		[]int32{10, 20, 30}, []int32{1, 1, 1}, 2)

	spec := makeAggWindowSpec("quantileExactWeighted")
	spec.GetW().PartitionBy = []*plan.Expr{
		newColExprWithType(2, types.T_int32.ToType()),
	}
	arg := &Window{
		WinSpecList: []*plan.Expr{spec},
		Aggs: []aggexec.AggFuncExecExpression{
			newQuantileExactWeightedWindowAggExpr(t, false, "0.5"),
		},
		OperatorBase: vm.OperatorBase{
			OperatorInfo: vm.OperatorInfo{Idx: 0},
		},
	}
	op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{first, second})
	arg.AppendChild(op)

	require.NoError(t, arg.Prepare(proc))
	var got []int32
	for {
		result, err := vm.Exec(arg, proc)
		require.NoError(t, err)
		if result.Batch == nil {
			break
		}
		got = append(got, vector.MustFixedColWithTypeCheck[int32](
			result.Batch.Vecs[len(result.Batch.Vecs)-1])...)
	}
	// Partition 1: W=4, k=2 selects 3 (F(1)=1 < 2, F(3)=4 >= 2);
	// partition 2: W=3, k=2 selects 20 (F(10)=1 < 2, F(20)=2 >= 2).
	require.Equal(t, []int32{3, 3, 20, 20, 20}, got)

	arg.Free(proc, false, nil)
	op.Free(proc, false, nil)
}

// TestWindowQuantileExactWeightedRetainedResultSurvivesExecutorDestruction is
// the actual window-consumer ordering witness from the design's output
// ownership contract: full-partition results are retained across output
// chunks, and the aggregate executor for the partition generation is
// destroyed as soon as the partition results are flushed, so every later
// output chunk re-reads the retained vector with no executor alive. Two
// output ranges (two chunks) make that ordering observable.
func TestWindowQuantileExactWeightedRetainedResultSurvivesExecutorDestruction(t *testing.T) {
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	defer func() {
		proc.Free()
		require.Equal(t, int64(0), proc.Mp().CurrNB())
	}()

	const rows = 8500 // two output chunks at the default batch size
	values := make([]int32, rows)
	weights := make([]int32, rows)
	for i := range values {
		values[i] = int32(i)
		weights[i] = 1
	}
	bat := makeWeightedPartition(t, proc.Mp(), values, weights, 7)

	spec := makeAggWindowSpec("quantileExactWeighted")
	arg := &Window{
		WinSpecList: []*plan.Expr{spec},
		Aggs: []aggexec.AggFuncExecExpression{
			newQuantileExactWeightedWindowAggExpr(t, false, "0.5"),
		},
		OperatorBase: vm.OperatorBase{
			OperatorInfo: vm.OperatorInfo{Idx: 0},
		},
	}
	op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat})
	arg.AppendChild(op)

	require.NoError(t, arg.Prepare(proc))
	chunks := 0
	outputRows := 0
	for {
		result, err := vm.Exec(arg, proc)
		require.NoError(t, err)
		if result.Batch == nil {
			break
		}
		chunks++
		got := vector.MustFixedColWithTypeCheck[int32](
			result.Batch.Vecs[len(result.Batch.Vecs)-1])
		// W=8500, k=4250 selects the 4250th cumulative weight: value 4249.
		for _, value := range got {
			require.Equal(t, int32(4249), value)
		}
		outputRows += len(got)
		if chunks == 1 {
			// The first output range finished while later ranges had not been
			// produced yet: the aggregate executor for the partition
			// generation is gone (its deferred freeAggFun already ran) and
			// only the retained result vector still serves the next ranges.
			require.NotNil(t, arg.ctr.fullPartitionResults,
				"partition result must survive the first output range")
			require.Nil(t, arg.ctr.batAggs,
				"the partition generation's aggregate executor must already be destroyed")
		}
	}
	require.Equal(t, rows, outputRows)
	require.Equal(t, 2, chunks,
		"this witness requires the two full-partition output ranges")
	require.Nil(t, arg.ctr.fullPartitionResults)

	arg.Free(proc, false, nil)
	op.Free(proc, false, nil)
}

// TestWindowQuantileExactWeightedDistinctWindowAggregates proves the
// DISTINCT window combination end to end at the operator after the window
// binder carries the distinct bit: equal (value, weight) pairs dedupe
// inside a full-partition window and inside a bounded ROWS frame, and the
// plain aggregate over the same rows computes the distinct-free answers
// (the fourth row's frame {2,6,6} discriminates: distinct keeps W=2, k=1
// and selects 2 while the plain sum of weights reaches k=3 at 6).
func TestWindowQuantileExactWeightedDistinctWindowAggregates(t *testing.T) {
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	defer func() {
		proc.Free()
		require.Equal(t, int64(0), proc.Mp().CurrNB())
	}()

	values := []int32{2, 2, 6, 6, 6}
	weights := []int32{1, 1, 1, 1, 1}

	runWindow := func(distinct bool, level string, frame *plan.FrameClause) []int32 {
		spec := makeAggWindowSpec("quantileExactWeighted")
		if frame != nil {
			spec.GetW().Frame = frame
			spec.GetW().OrderBy = []*plan.OrderBySpec{{
				Expr: newColExprWithType(0, types.T_int32.ToType()),
				Flag: plan.OrderBySpec_ASC,
			}}
		}
		arg := &Window{
			WinSpecList: []*plan.Expr{spec},
			Aggs: []aggexec.AggFuncExecExpression{
				newQuantileExactWeightedWindowAggExpr(t, distinct, level)},
			OperatorBase: vm.OperatorBase{
				OperatorInfo: vm.OperatorInfo{Idx: 0},
			},
		}
		op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{
			makeWeightedPartition(t, proc.Mp(), values, weights, 1)})
		arg.AppendChild(op)

		require.NoError(t, arg.Prepare(proc))
		var got []int32
		for {
			result, err := vm.Exec(arg, proc)
			require.NoError(t, err)
			if result.Batch == nil {
				break
			}
			got = append(got, vector.MustFixedColWithTypeCheck[int32](
				result.Batch.Vecs[len(result.Batch.Vecs)-1])...)
		}
		arg.Free(proc, false, nil)
		op.Free(proc, false, nil)
		return got
	}

	// Full partition: distinct pairs (2,1),(6,1) give W=2, k=1 and select 2
	// everywhere, while the plain pairs (2,2),(6,3) give W=5, k=3 -> 6.
	require.Equal(t, []int32{2, 2, 2, 2, 2},
		runWindow(true, "0.5", nil),
		"DISTINCT must dedupe pairs inside the full-partition window")
	require.Equal(t, []int32{6, 6, 6, 6, 6},
		runWindow(false, "0.5", nil),
		"the plain full-partition aggregate must keep every duplicate pair")

	// Bounded ROWS frame (2 PRECEDING .. CURRENT ROW), ordered by value:
	// distinct answers 2,2,2,2,6 versus plain 2,2,2,6,6.
	require.Equal(t, []int32{2, 2, 2, 2, 6},
		runWindow(true, "0.5", makeFiniteCumulativeFrame(2)),
		"DISTINCT must dedupe pairs inside every sliding frame")
	require.Equal(t, []int32{2, 2, 2, 6, 6},
		runWindow(false, "0.5", makeFiniteCumulativeFrame(2)),
		"the plain sliding-frame aggregate must keep every duplicate pair")
}

// TestWindowQuantileExactWeightedLargePartitionHonorsCancellation proves the
// cancellation surface through the window's full-partition path with real
// multi-row work: with the context cancelled before the partition's 40000
// rows are finalized, the operator returns the standard cancellation error
// through vm.Exec instead of completing the partition and broadcasting its
// result. The finalization's own mid-work cancellation points (restore,
// sort and cumulative scan) are pinned deterministically by the
// executor-level regression in pkg/sql/colexec/aggexec.
func TestWindowQuantileExactWeightedLargePartitionHonorsCancellation(t *testing.T) {
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	defer func() {
		proc.Free()
		require.Equal(t, int64(0), proc.Mp().CurrNB())
	}()

	const rows = 40000
	values := make([]int32, rows)
	weights := make([]int32, rows)
	for i := range values {
		values[i] = int32(rows - i) // descending input forces real sort work
		weights[i] = 1
	}
	bat := makeWeightedPartition(t, proc.Mp(), values, weights, 3)

	cancelCtx, cancel := context.WithCancel(context.Background())
	cancel()

	spec := makeAggWindowSpec("quantileExactWeighted")
	arg := &Window{
		WinSpecList: []*plan.Expr{spec},
		Aggs: []aggexec.AggFuncExecExpression{
			newQuantileExactWeightedWindowAggExpr(t, false, "0.5"),
		},
		OperatorBase: vm.OperatorBase{
			OperatorInfo: vm.OperatorInfo{Idx: 0},
		},
	}
	op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat})
	arg.AppendChild(op)

	require.NoError(t, arg.Prepare(proc))
	// The operator consumes proc.Ctx at finalization time
	// (aggexec.FlushWithContext), so a context cancelled after preparation
	// aborts the partition execution before any result row is broadcast.
	proc.Ctx = cancelCtx
	_, err := vm.Exec(arg, proc)
	require.Error(t, err,
		"a cancelled context must abort the partition finalize")
	require.ErrorIs(t, err, context.Canceled)

	arg.Free(proc, false, nil)
	op.Free(proc, false, nil)
}
