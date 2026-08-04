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
	"sort"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/spillutil"
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

type dedupKeyContractRow struct {
	keyBits      uint64
	captured     int32
	capturedNull bool
}

func TestDedupJoinDoubleSignedZeroContract(t *testing.T) {
	// Scalar DOUBLE equality treats +0 and -0 as equal. Every execution mode
	// must preserve the complete build row set and capture exactly that target.
	modes := []dedupKeyContractMode{
		{name: "resident"},
		{name: "initial-spill", shuffle: true, spillThreshold: hashmap.UnitLimit + 1, wantInitialSpill: true},
		{name: "re-spill", shuffle: true, spillThreshold: 2, wantInitialSpill: true, wantReSpill: true},
	}
	want := expectedDedupJoinDoubleSignedZeroRows()
	for _, mode := range modes {
		t.Run(mode.name, func(t *testing.T) {
			require.Equal(t, want, runDedupJoinDoubleSignedZeroContract(t, mode))
		})
	}
}

func expectedDedupJoinDoubleSignedZeroRows() []dedupKeyContractRow {
	rows := make([]dedupKeyContractRow, hashmap.UnitLimit+1)
	for key := 0; key <= hashmap.UnitLimit; key++ {
		rows[key] = dedupKeyContractRow{
			keyBits:      math.Float64bits(float64(key)),
			capturedNull: key != 0,
		}
	}
	rows[0].captured = 42
	return rows
}

func runDedupJoinDoubleSignedZeroContract(
	t *testing.T,
	mode dedupKeyContractMode,
) []dedupKeyContractRow {
	proc, ctrl := newCaptureTestProc(t)
	defer ctrl.Finish()
	proc.Base.Lim.Size = 8 << 20
	proc.Base.Lim.SpillSize = 64 << 20
	var buildBatch, probeBatch *batch.Batch
	var dedupArg *DedupJoin
	var buildArg *hashbuild.HashBuild
	defer func() {
		if dedupArg != nil {
			dedupArg.Free(proc, false, nil)
		}
		if buildArg != nil {
			buildArg.Free(proc, false, nil)
		}
		if buildBatch != nil {
			buildBatch.Clean(proc.Mp())
		}
		if probeBatch != nil {
			probeBatch.Clean(proc.Mp())
		}
		budget, budgetErr := proc.GetHashBuildBudget()
		var used, diskUsed, fdUsed uint64
		if budgetErr == nil {
			used = budget.Used()
			diskUsed = budget.SpillDiskUsed()
			fdUsed = budget.SpillFDUsed()
		}
		proc.Free()
		mpoolBytes := proc.Mp().CurrNB()
		require.NoError(t, budgetErr)
		require.Zero(t, used)
		require.Zero(t, diskUsed)
		require.Zero(t, fdUsed)
		require.Zero(t, mpoolBytes)
	}()

	floatType := types.T_float64.ToType()
	intType := types.T_int32.ToType()
	conditions := [][]*plan.Expr{
		{newExpr(0, floatType)},
		{newExpr(0, floatType)},
	}
	tag++
	joinMapTag := tag

	const buildRows = hashmap.UnitLimit + 1
	buildKeys := vector.NewVec(floatType)
	buildPlaceholder := vector.NewVec(intType)
	for key := 1; key < buildRows; key++ {
		require.NoError(t, vector.AppendFixed(buildKeys, float64(key), false, proc.Mp()))
		require.NoError(t, vector.AppendFixed(buildPlaceholder, int32(0), true, proc.Mp()))
	}
	// Keep the equality target in the second hashmap chunk (start=UnitLimit).
	require.NoError(t, vector.AppendFixed(buildKeys, float64(0), false, proc.Mp()))
	require.NoError(t, vector.AppendFixed(buildPlaceholder, int32(0), true, proc.Mp()))
	buildBatch = batch.NewWithSize(2)
	buildBatch.Vecs[0] = buildKeys
	buildBatch.Vecs[1] = buildPlaceholder
	buildBatch.SetRowCount(buildRows)
	if mode.wantReSpill {
		requireDedupTargetBucketReSpills(t, buildKeys, buildRows-1, mode.spillThreshold)
	}

	probeBatch = batch.NewWithSize(2)
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

	dedupArg = &DedupJoin{
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
		DelColIdx:                       -1,
		IsShuffle:                       mode.shuffle,
		ShuffleIdx:                      0,
		SpillThreshold:                  mode.spillThreshold,
		JoinMapTag:                      joinMapTag,
	}
	dedupArg.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{probeBatch}))

	buildArg = &hashbuild.HashBuild{
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
	installTestAllocation(t, dedupArg, buildArg)
	buildArg.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{buildBatch}))

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

	resultRows := make([]dedupKeyContractRow, 0, buildRows)
	for {
		result, execErr := vm.Exec(dedupArg, proc)
		require.NoError(t, execErr)
		if result.Batch != nil {
			require.Len(t, result.Batch.Vecs, 2)
			require.True(t, result.Batch.Vecs[0].GetNulls().IsEmpty())
			keys := vector.MustFixedColNoTypeCheck[float64](result.Batch.Vecs[0])
			capturedValues := vector.MustFixedColNoTypeCheck[int32](result.Batch.Vecs[1])
			require.Len(t, keys, result.Batch.RowCount())
			require.Len(t, capturedValues, result.Batch.RowCount())
			for i, key := range keys {
				capturedNull := result.Batch.Vecs[1].GetNulls().Contains(uint64(i))
				captured := int32(0)
				if !capturedNull {
					captured = capturedValues[i]
				}
				resultRows = append(resultRows, dedupKeyContractRow{
					keyBits:      math.Float64bits(key),
					captured:     captured,
					capturedNull: capturedNull,
				})
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

	sort.Slice(resultRows, func(i, j int) bool {
		return resultRows[i].keyBits < resultRows[j].keyBits
	})
	return resultRows
}

func requireDedupTargetBucketReSpills(
	t *testing.T,
	keys *vector.Vector,
	targetRow int,
	spillThreshold int64,
) {
	hashes := make([]uint64, keys.Length())
	spillutil.ComputeXXHash([]*vector.Vector{keys}, hashes, 0)
	mask := uint64(spillutil.SpillNumBuckets - 1)
	targetBucket := hashes[targetRow] & mask
	var bucketRows int64
	for _, hash := range hashes {
		if hash&mask == targetBucket {
			bucketRows++
		}
	}
	require.GreaterOrEqual(t, bucketRows, spillThreshold)
}
