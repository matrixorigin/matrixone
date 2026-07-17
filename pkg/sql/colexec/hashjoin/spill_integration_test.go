// Copyright 2026 Matrix Origin
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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/spillutil"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	metricv2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func makeKeyExpr() []*plan.Expr {
	return []*plan.Expr{{
		Typ:  plan.Type{Id: int32(types.T_int32), Width: 32},
		Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}},
	}}
}

// TestGetSpilledInputBatchNoBuckets verifies that getSpilledInputBatch
// returns nil when the engine has no buckets.
func TestGetSpilledInputBatchNoBuckets(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	engine := spillutil.NewSpillEngine(spillutil.SpillEngineConfig{})
	hashJoin := &HashJoin{ctr: container{spillEngine: engine}}
	result, err := hashJoin.getSpilledInputBatch(proc, process.NewAnalyzer(0, false, false, "test"))
	require.NoError(t, err)
	require.Nil(t, result.Batch)
}

// TestEmptyProbeDoesNotPanic verifies that emptyProbe handles empty build
// (ctr.mp == nil) without panicking. This is the path taken when a spill
// bucket returns BucketEmptyBuild for outer joins.
func TestEmptyProbeDoesNotPanic(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	hashJoin := &HashJoin{
		JoinType:   plan.Node_LEFT,
		ResultCols: []colexec.ResultPos{{Rel: 0, Pos: 0}},
		LeftTypes:  []types.Type{types.T_int32.ToType()},
		RightTypes: []types.Type{types.T_int32.ToType()},
	}
	ctr := &hashJoin.ctr

	// Properly initialize resBat.
	ctr.resBat = batch.NewWithSize(len(hashJoin.ResultCols))
	for i, rp := range hashJoin.ResultCols {
		if rp.Rel == 0 {
			ctr.resBat.Vecs[i] = vector.NewVec(hashJoin.LeftTypes[rp.Pos])
		} else {
			ctr.resBat.Vecs[i] = vector.NewVec(hashJoin.RightTypes[rp.Pos])
		}
	}

	// Set up leftBat with some rows.
	ctr.leftBat = batch.NewWithSize(1)
	ctr.leftBat.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 2, 3}, nil, proc.Mp())
	ctr.leftBat.SetRowCount(3)

	// mp is nil (empty build) — emptyProbe must handle this.
	ctr.mp = nil

	var result vm.CallResult
	require.NotPanics(t, func() {
		err := ctr.emptyProbe(hashJoin, proc, &result)
		require.NoError(t, err)
		require.NotNil(t, result.Batch)
		require.Equal(t, 3, result.Batch.RowCount())
	}, "emptyProbe with nil mp must not panic and must emit all probe rows")
}

// TestShuffleJoinFiniteBudgetInitialSpillAndReSpill exercises the complete
// producer/consumer ownership path. The first threshold forces HashBuild to
// spill; a first-level bucket is still larger than the same threshold, so the
// consumer must repartition it before producing the exact join cardinality.
func TestShuffleJoinFiniteBudgetInitialSpillAndReSpill(t *testing.T) {
	tc := newTestCase(
		t,
		[]bool{false},
		[]types.Type{types.T_int32.ToType()},
		[]colexec.ResultPos{colexec.NewResultPos(0, 0)},
		[][]*plan.Expr{makeKeyExpr(), makeKeyExpr()},
	)
	tc.proc.Base.Lim.Size = 8 << 20
	tc.proc.Base.Lim.SpillSize = 64 << 20

	const rows = 8192
	values := make([]int32, rows)
	for i := range values {
		values[i] = int32(i)
	}
	probeValues := make([]int32, rows+1024)
	copy(probeValues, values)
	for i := rows; i < len(probeValues); i++ {
		probeValues[i] = int32(i)
	}
	probe := makeInt32Batch(tc.proc, probeValues)
	build := makeInt32Batch(tc.proc, values)

	tc.arg.NonEqCond = nil
	tc.arg.IsShuffle = true
	tc.arg.ShuffleIdx = 0
	tc.arg.SpillThreshold = 50
	tc.barg.IsShuffle = true
	tc.barg.ShuffleIdx = 0
	tc.barg.SpillThreshold = 50
	tc.barg.NeedBatches = false
	tc.barg.RuntimeFilterSpec = &plan.RuntimeFilterSpec{Tag: tc.arg.JoinMapTag + 1000}
	resetChildrenWithBatch(tc.arg, probe)
	resetHashBuildChildrenWithBatch(tc.barg, build)

	spillBefore := promtestutil.ToFloat64(metricv2.HashBuildSpillDepthCounter.WithLabelValues("spill", "1"))
	respillBefore := promtestutil.ToFloat64(metricv2.HashBuildSpillDepthCounter.WithLabelValues("respill", "2"))
	require.NoError(t, tc.arg.Prepare(tc.proc))
	require.NoError(t, tc.barg.Prepare(tc.proc))
	buildResult, err := vm.Exec(tc.barg, tc.proc)
	require.NoError(t, err)
	require.Nil(t, buildResult.Batch)

	resultRows := 0
	for {
		result, err := vm.Exec(tc.arg, tc.proc)
		require.NoError(t, err)
		if result.Batch != nil {
			resultRows += result.Batch.RowCount()
		}
		if result.Status == vm.ExecStop {
			break
		}
	}
	require.Equal(t, rows, resultRows)
	require.Greater(t, promtestutil.ToFloat64(metricv2.HashBuildSpillDepthCounter.WithLabelValues("spill", "1")), spillBefore)
	require.Greater(t, promtestutil.ToFloat64(metricv2.HashBuildSpillDepthCounter.WithLabelValues("respill", "2")), respillBefore)

	tc.arg.Free(tc.proc, false, nil)
	tc.barg.Free(tc.proc, false, nil)
	budget, err := tc.proc.GetHashBuildBudget()
	require.NoError(t, err)
	require.Zero(t, budget.Used())
	require.Zero(t, budget.SpillDiskUsed())
	require.Zero(t, budget.SpillFDUsed())
	tc.proc.Free()
	require.Zero(t, tc.proc.Mp().CurrNB())
}

func TestShuffleJoinHardBudgetRejectTransitionsToSpill(t *testing.T) {
	tc := newTestCase(
		t,
		[]bool{false},
		[]types.Type{types.T_int32.ToType()},
		[]colexec.ResultPos{colexec.NewResultPos(0, 0)},
		[][]*plan.Expr{makeKeyExpr(), makeKeyExpr()},
	)
	// This cap admits one bounded scatter pass and per-bucket rebuild, but not
	// the complete 8K-row retained build/map. The very high soft threshold
	// proves that spill is entered from hard admission rejection, not policy.
	tc.proc.Base.Lim.Size = 2168 << 10
	tc.proc.Base.Lim.SpillSize = 64 << 20

	const rows = 8192
	values := make([]int32, rows)
	for i := range values {
		values[i] = int32(i)
	}
	probe := makeInt32Batch(tc.proc, values)
	build1 := makeInt32Batch(tc.proc, values[:rows/2])
	build2 := makeInt32Batch(tc.proc, values[rows/2:])

	tc.arg.NonEqCond = nil
	tc.arg.IsShuffle = true
	tc.arg.ShuffleIdx = 0
	tc.arg.SpillThreshold = 1 << 30
	tc.barg.IsShuffle = true
	tc.barg.ShuffleIdx = 0
	tc.barg.SpillThreshold = 1 << 30
	tc.barg.NeedBatches = false
	tc.barg.RuntimeFilterSpec = &plan.RuntimeFilterSpec{Tag: tc.arg.JoinMapTag + 2000}
	resetChildrenWithBatch(tc.arg, probe)
	buildInput := colexec.NewMockOperator().WithBatchs([]*batch.Batch{build1, build2})
	tc.barg.Children = nil
	tc.barg.AppendChild(buildInput)

	rejectBefore := promtestutil.ToFloat64(metricv2.HashBuildBudgetEventCounter.WithLabelValues("memory", "reject", "query"))
	spillBefore := promtestutil.ToFloat64(metricv2.HashBuildSpillDepthCounter.WithLabelValues("spill", "1"))
	require.NoError(t, tc.arg.Prepare(tc.proc))
	require.NoError(t, tc.barg.Prepare(tc.proc))
	buildResult, err := vm.Exec(tc.barg, tc.proc)
	require.NoError(t, err)
	require.Nil(t, buildResult.Batch)

	var resultValues []int32
	for {
		result, err := vm.Exec(tc.arg, tc.proc)
		require.NoError(t, err)
		if result.Batch != nil {
			resultValues = append(resultValues, vector.MustFixedColWithTypeCheck[int32](result.Batch.Vecs[0])...)
		}
		if result.Status == vm.ExecStop {
			break
		}
	}
	require.ElementsMatch(t, values, resultValues)
	require.Greater(t, promtestutil.ToFloat64(metricv2.HashBuildBudgetEventCounter.WithLabelValues("memory", "reject", "query")), rejectBefore)
	require.Greater(t, promtestutil.ToFloat64(metricv2.HashBuildSpillDepthCounter.WithLabelValues("spill", "1")), spillBefore)

	tc.arg.Free(tc.proc, false, nil)
	tc.barg.Free(tc.proc, false, nil)
	budget, err := tc.proc.GetHashBuildBudget()
	require.NoError(t, err)
	require.Zero(t, budget.Used())
	require.Zero(t, budget.SpillDiskUsed())
	require.Zero(t, budget.SpillFDUsed())
	tc.proc.Free()
	require.Zero(t, tc.proc.Mp().CurrNB())
}
