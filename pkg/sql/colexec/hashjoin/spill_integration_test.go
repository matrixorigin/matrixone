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

func newAccountedTestSpillEngine(
	t *testing.T,
	cfg spillutil.SpillEngineConfig,
) *spillutil.SpillEngine {
	t.Helper()
	if cfg.Budget == nil {
		budget := process.MustNewExecutionResourceBudget(1<<60, 1<<60)
		var err error
		cfg.Budget, err = budget.OpenGeneration(1)
		require.NoError(t, err)
	}
	registry, err := mpool.NewAllocationAccountRegistry(1, 1<<20)
	require.NoError(t, err)
	account, err := registry.OpenWithController(1<<60, cfg.Budget)
	require.NoError(t, err)
	engine, err := spillutil.NewSpillEngine(
		cfg,
		account,
		mpool.AllocationOwnerHashBuild,
	)
	require.NoError(t, err)
	return engine
}

func installHashJoinTestAllocation(
	t *testing.T,
	join *HashJoin,
) (*process.ExecutionResourceGeneration, *mpool.AllocationAccountRegistry, *mpool.AllocationAccount) {
	t.Helper()
	budget := process.MustNewExecutionResourceBudget(64<<20, 64<<20)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	registry, err := mpool.NewAllocationAccountRegistry(1, 1<<20)
	require.NoError(t, err)
	account, err := registry.OpenWithController(64<<20, generation)
	require.NoError(t, err)
	if join.allocationAccount != nil {
		require.NoError(t, join.ClearAllocationAccount(join.allocationAccount))
	}
	require.NoError(t, join.SetAllocationAccount(account))
	return generation, registry, account
}

// TestGetSpilledInputBatchNoBuckets verifies that getSpilledInputBatch
// returns nil when the engine has no buckets.
func TestGetSpilledInputBatchNoBuckets(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	engine := newAccountedTestSpillEngine(t, spillutil.SpillEngineConfig{})
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
	budget, err := tc.proc.GetExecutionResourceBudget()
	require.NoError(t, err)
	require.Zero(t, budget.Used())
	require.Zero(t, budget.SpillDiskUsed())
	require.Zero(t, budget.SpillFDUsed())
	tc.proc.Free()
	require.Zero(t, tc.proc.Mp().CurrNB())
}

func TestShuffleFullOuterSpillTracksBuildMatchesWithoutRightOrientation(t *testing.T) {
	tc := newTestCase(
		t,
		[]bool{true},
		[]types.Type{types.T_int32.ToType()},
		[]colexec.ResultPos{
			colexec.NewResultPos(0, 0),
			colexec.NewResultPos(1, 0),
		},
		[][]*plan.Expr{makeKeyExpr(), makeKeyExpr()},
	)
	defer func() {
		tc.arg.Reset(tc.proc, false, nil)
		tc.barg.Reset(tc.proc, false, nil)
		tc.arg.Free(tc.proc, false, nil)
		tc.barg.Free(tc.proc, false, nil)
		budget, err := tc.proc.GetExecutionResourceBudget()
		require.NoError(t, err)
		require.Zero(t, budget.Used())
		require.Zero(t, budget.SpillDiskUsed())
		require.Zero(t, budget.SpillFDUsed())
		tc.proc.Free()
		tc.cancel()
		require.Zero(t, tc.proc.Mp().CurrNB())
	}()

	tc.proc.Base.Lim.Size = 8 << 20
	tc.proc.Base.Lim.SpillSize = 64 << 20

	const rows = 8192
	buildValues := make([]int32, rows)
	probeValues := make([]int32, rows)
	for i := range rows {
		buildValues[i] = int32(i)
		probeValues[i] = int32(i + rows/2)
	}

	tc.arg.JoinType = plan.Node_OUTER
	tc.arg.IsRightJoin = false
	tc.arg.NonEqCond = nil
	tc.arg.IsShuffle = true
	tc.arg.ShuffleIdx = 0
	tc.arg.SpillThreshold = 50
	tc.barg.IsShuffle = true
	tc.barg.ShuffleIdx = 0
	tc.barg.SpillThreshold = 50
	tc.barg.RuntimeFilterSpec = &plan.RuntimeFilterSpec{Tag: tc.arg.JoinMapTag + 1200}
	resetChildrenWithBatch(tc.arg, makeInt32Batch(tc.proc, probeValues))
	resetHashBuildChildrenWithBatch(tc.barg, makeInt32Batch(tc.proc, buildValues))

	spillBefore := promtestutil.ToFloat64(
		metricv2.HashBuildSpillDepthCounter.WithLabelValues("spill", "1"))
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

	// Half of each side overlaps: left-only + matched + right-only.
	require.Equal(t, rows+rows/2, resultRows)
	require.Greater(t, promtestutil.ToFloat64(
		metricv2.HashBuildSpillDepthCounter.WithLabelValues("spill", "1")), spillBefore)
}

func TestShuffleJoinSpillUsesCanonicalGroupingPartitionKey(t *testing.T) {
	for _, test := range []struct {
		name           string
		typ            types.Type
		rows           int
		spillThreshold int64
		wantRespill    bool
		probe          func(*process.Process) *vector.Vector
		build          func(*process.Process) *vector.Vector
	}{
		{
			name:           "varchar",
			typ:            types.T_varchar.ToType(),
			rows:           1,
			spillThreshold: 1,
			probe: func(proc *process.Process) *vector.Vector {
				return testutil.MakeVarcharVector([]string{"probe"}, nil, proc.Mp())
			},
			build: func(proc *process.Process) *vector.Vector {
				return testutil.MakeVarcharVector([]string{"build"}, nil, proc.Mp())
			},
		},
		{
			name:           "int32",
			typ:            types.T_int32.ToType(),
			rows:           1,
			spillThreshold: 1,
			probe: func(proc *process.Process) *vector.Vector {
				return testutil.MakeInt32Vector([]int32{222}, nil, proc.Mp())
			},
			build: func(proc *process.Process) *vector.Vector {
				return testutil.MakeInt32Vector([]int32{111}, nil, proc.Mp())
			},
		},
		{
			name:           "scaled-float32",
			rows:           8192,
			spillThreshold: 50,
			wantRespill:    true,
			typ: func() types.Type {
				typ := types.T_float32.ToType()
				typ.Scale = 2
				return typ
			}(),
			probe: func(proc *process.Process) *vector.Vector {
				values := make([]float32, 8192)
				values[0] = 1.234
				for i := 1; i < len(values); i++ {
					values[i] = float32(i)/10 + 0.001
				}
				vec := testutil.MakeFloat32Vector(values, nil, proc.Mp())
				vec.GetType().Scale = 2
				return vec
			},
			build: func(proc *process.Process) *vector.Vector {
				values := make([]float32, 8192)
				values[0] = 9.876
				for i := 1; i < len(values); i++ {
					values[i] = float32(i) / 10
				}
				vec := testutil.MakeFloat32Vector(values, nil, proc.Mp())
				vec.GetType().Scale = 2
				return vec
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			keyExpr := []*plan.Expr{{
				Typ: plan.Type{
					Id:    int32(test.typ.Oid),
					Width: test.typ.Width,
					Scale: test.typ.Scale,
				},
				Expr: &plan.Expr_Col{Col: &plan.ColRef{
					ColPos: 0,
				}},
			}}
			tc := newTestCase(
				t,
				[]bool{false},
				[]types.Type{test.typ},
				[]colexec.ResultPos{colexec.NewResultPos(0, 0)},
				[][]*plan.Expr{keyExpr, keyExpr},
			)
			tc.arg.NonEqCond = nil
			tc.arg.IsShuffle = true
			tc.arg.ShuffleIdx = 0
			tc.arg.SpillThreshold = test.spillThreshold
			tc.barg.IsShuffle = true
			tc.barg.ShuffleIdx = 0
			tc.barg.SpillThreshold = test.spillThreshold
			tc.barg.NeedBatches = false
			tc.barg.RuntimeFilterSpec = &plan.RuntimeFilterSpec{
				Tag: tc.arg.JoinMapTag + 1_500,
			}

			probe := batch.NewWithSize(1)
			probe.Vecs[0] = test.probe(tc.proc)
			probe.SetRowCount(test.rows)
			build := batch.NewWithSize(1)
			build.Vecs[0] = test.build(tc.proc)
			build.SetRowCount(test.rows)
			probe.Vecs[0].GetGrouping().Add(0)
			build.Vecs[0].GetGrouping().Add(0)
			resetChildrenWithBatch(tc.arg, probe)
			resetHashBuildChildrenWithBatch(tc.barg, build)

			spillBefore := promtestutil.ToFloat64(
				metricv2.HashBuildSpillDepthCounter.WithLabelValues("spill", "1"))
			respillBefore := promtestutil.ToFloat64(
				metricv2.HashBuildSpillDepthCounter.WithLabelValues("respill", "2"))

			require.NoError(t, tc.arg.Prepare(tc.proc))
			require.NoError(t, tc.barg.Prepare(tc.proc))
			_, err := vm.Exec(tc.barg, tc.proc)
			require.NoError(t, err)

			rows := 0
			for {
				result, err := vm.Exec(tc.arg, tc.proc)
				require.NoError(t, err)
				if result.Batch != nil {
					rows += result.Batch.RowCount()
				}
				if result.Status == vm.ExecStop {
					break
				}
			}
			require.Equal(t, test.rows, rows)
			require.Greater(t, promtestutil.ToFloat64(
				metricv2.HashBuildSpillDepthCounter.WithLabelValues("spill", "1")), spillBefore)
			if test.wantRespill {
				require.Greater(t, promtestutil.ToFloat64(
					metricv2.HashBuildSpillDepthCounter.WithLabelValues("respill", "2")), respillBefore)
			}

			tc.arg.Reset(tc.proc, false, nil)
			tc.barg.Reset(tc.proc, false, nil)
			tc.arg.Free(tc.proc, false, nil)
			tc.barg.Free(tc.proc, false, nil)
			tc.proc.Free()
			require.Zero(t, tc.proc.Mp().CurrNB())
		})
	}
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
	// Leave enough capacity for the mandatory bounded recovery pass itself.
	// The retained 8K-row build/map still cannot fit, so ordinary allocation
	// admission is what drives the operator into spill mode.
	tc.proc.Base.Lim.Size = 512 << 10
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
	oldAccount := tc.arg.allocationAccount
	require.NoError(t, tc.arg.ClearAllocationAccount(oldAccount))
	require.NoError(t, tc.barg.ClearAllocationAccount(oldAccount))
	budget, err := tc.proc.GetExecutionResourceBudget()
	require.NoError(t, err)
	registry, err := budget.AllocationAccountRegistry()
	require.NoError(t, err)
	account, err := registry.OpenWithController(budget.Snapshot().Cap, budget)
	require.NoError(t, err)
	require.NoError(t, tc.arg.SetAllocationAccount(account))
	require.NoError(t, tc.barg.SetAllocationAccount(account))

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
	require.Positive(t, tc.barg.OpAnalyzer.GetOpStats().ExtraStats["HashBuildSpillStarts"])
	require.LessOrEqual(t, account.Snapshot().Peak, account.Snapshot().Limit)
	require.Greater(t, promtestutil.ToFloat64(metricv2.HashBuildSpillDepthCounter.WithLabelValues("spill", "1")), spillBefore)

	tc.arg.Free(tc.proc, false, nil)
	tc.barg.Free(tc.proc, false, nil)
	require.Zero(t, budget.Used())
	require.Zero(t, budget.SpillDiskUsed())
	require.Zero(t, budget.SpillFDUsed())
	tc.proc.Free()
	require.Zero(t, tc.proc.Mp().CurrNB())
}
