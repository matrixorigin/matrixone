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

package dedupjoin

import (
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	metricv2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

type dedupKeyContractMode struct {
	name             string
	shuffle          bool
	spillThreshold   int64
	wantInitialSpill bool
	wantReSpill      bool
}

func TestDedupJoinDoubleSignedZeroContract(t *testing.T) {
	// Scalar DOUBLE equality treats +0 and -0 as equal. The false expectations
	// characterize the current key-encoding gap for one representative
	// DedupJoin operation across all three execution modes.
	modes := []dedupKeyContractMode{
		{name: "resident"},
		{name: "initial-spill", shuffle: true, spillThreshold: 64, wantInitialSpill: true},
		{name: "re-spill", shuffle: true, spillThreshold: 2, wantInitialSpill: true, wantReSpill: true},
	}
	for _, mode := range modes {
		t.Run(mode.name, func(t *testing.T) {
			require.False(t, runDedupJoinDoubleSignedZeroContract(t, mode))
		})
	}
}

func runDedupJoinDoubleSignedZeroContract(t *testing.T, mode dedupKeyContractMode) bool {
	proc, ctrl := newCaptureTestProc(t)
	defer ctrl.Finish()
	proc.Base.Lim.Size = 8 << 20
	proc.Base.Lim.SpillSize = 64 << 20

	floatType := types.T_float64.ToType()
	intType := types.T_int32.ToType()
	conditions := [][]*plan.Expr{
		{newExpr(0, floatType)},
		{newExpr(0, floatType)},
	}
	tag++
	joinMapTag := tag

	const buildRows = 256
	buildKeys := vector.NewVec(floatType)
	buildPlaceholder := vector.NewVec(intType)
	require.NoError(t, vector.AppendFixed(buildKeys, float64(0), false, proc.Mp()))
	require.NoError(t, vector.AppendFixed(buildPlaceholder, int32(0), true, proc.Mp()))
	for i := 1; i < buildRows; i++ {
		require.NoError(t, vector.AppendFixed(buildKeys, float64(i), false, proc.Mp()))
		require.NoError(t, vector.AppendFixed(buildPlaceholder, int32(0), true, proc.Mp()))
	}
	buildBatch := batch.NewWithSize(2)
	buildBatch.Vecs[0] = buildKeys
	buildBatch.Vecs[1] = buildPlaceholder
	buildBatch.SetRowCount(buildRows)

	probeBatch := batch.NewWithSize(2)
	probeBatch.Vecs[0] = vector.NewVec(floatType)
	probeBatch.Vecs[1] = vector.NewVec(intType)
	require.NoError(t, vector.AppendFixed(
		probeBatch.Vecs[0],
		math.Copysign(0, -1),
		false,
		proc.Mp(),
	))
	require.NoError(t, vector.AppendFixed(probeBatch.Vecs[1], int32(42), false, proc.Mp()))
	probeBatch.SetRowCount(1)

	dedupArg := &DedupJoin{
		LeftTypes:  []types.Type{floatType, intType},
		RightTypes: []types.Type{floatType, intType},
		Conditions: conditions,
		Result: []colexec.ResultPos{
			colexec.NewResultPos(1, 0),
			colexec.NewResultPos(1, 1),
		},
		OnDuplicateAction:               plan.Node_FAIL,
		OldColCapturePlaceholderIdxList: []int32{1},
		OldColCaptureProbeIdxList:       []int32{1},
		IsShuffle:                       mode.shuffle,
		ShuffleIdx:                      0,
		SpillThreshold:                  mode.spillThreshold,
		JoinMapTag:                      joinMapTag,
	}
	dedupArg.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{probeBatch}))

	buildArg := &hashbuild.HashBuild{
		NeedHashMap:      true,
		NeedBatches:      true,
		Conditions:       conditions[1],
		IsDedup:          true,
		DelColIdx:        -1,
		IsShuffle:        mode.shuffle,
		ShuffleIdx:       0,
		SpillThreshold:   mode.spillThreshold,
		JoinMapTag:       joinMapTag,
		JoinMapRefCnt:    1,
		NeedAllocateSels: false,
	}
	if mode.shuffle {
		buildArg.RuntimeFilterSpec = &plan.RuntimeFilterSpec{Tag: joinMapTag + 8000}
	}
	buildArg.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{buildBatch}))
	defer func() {
		dedupArg.Free(proc, false, nil)
		buildArg.Free(proc, false, nil)
		budget, err := proc.GetHashBuildBudget()
		require.NoError(t, err)
		require.Zero(t, budget.Used())
		require.Zero(t, budget.SpillDiskUsed())
		require.Zero(t, budget.SpillFDUsed())
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	}()

	spillBefore := promtestutil.ToFloat64(
		metricv2.HashBuildSpillDepthCounter.WithLabelValues("spill", "1"),
	)
	reSpillBefore := promtestutil.ToFloat64(
		metricv2.HashBuildSpillDepthCounter.WithLabelValues("respill", "2"),
	)
	require.NoError(t, buildArg.Prepare(proc))
	require.NoError(t, dedupArg.Prepare(proc))
	buildResult, err := vm.Exec(buildArg, proc)
	require.NoError(t, err)
	require.Nil(t, buildResult.Batch)

	targetCaptured := false
	for {
		result, execErr := vm.Exec(dedupArg, proc)
		require.NoError(t, execErr)
		if result.Batch != nil {
			keys := vector.MustFixedColNoTypeCheck[float64](result.Batch.Vecs[0])
			for i, key := range keys {
				if key == 0 && !result.Batch.Vecs[1].GetNulls().Contains(uint64(i)) {
					targetCaptured = true
				}
			}
		}
		if result.Status == vm.ExecStop {
			break
		}
	}

	spillAfter := promtestutil.ToFloat64(
		metricv2.HashBuildSpillDepthCounter.WithLabelValues("spill", "1"),
	)
	reSpillAfter := promtestutil.ToFloat64(
		metricv2.HashBuildSpillDepthCounter.WithLabelValues("respill", "2"),
	)
	if mode.wantInitialSpill {
		require.Greater(t, spillAfter, spillBefore)
	} else {
		require.Equal(t, spillBefore, spillAfter)
	}
	if mode.wantReSpill {
		require.Greater(t, reSpillAfter, reSpillBefore)
	} else {
		require.Equal(t, reSpillBefore, reSpillAfter)
	}

	return targetCaptured
}
