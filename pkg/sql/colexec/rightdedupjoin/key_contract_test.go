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

package rightdedupjoin

import (
	"math"
	"strconv"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/spillutil"
	metricv2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

type rightDedupKeyContractMode struct {
	name             string
	shuffle          bool
	spillThreshold   int64
	wantInitialSpill bool
	wantReSpill      bool
}

func TestRightDedupJoinDoubleSignedZeroContract(t *testing.T) {
	// Pessimistic RightDedupJoin must report a duplicate when -0 probes an
	// existing +0 build key, in every execution mode.
	modes := []rightDedupKeyContractMode{
		{name: "resident"},
		{name: "initial-spill", shuffle: true, spillThreshold: hashmap.UnitLimit + 1, wantInitialSpill: true},
		{name: "re-spill", shuffle: true, spillThreshold: 2, wantInitialSpill: true, wantReSpill: true},
	}
	for _, mode := range modes {
		t.Run(mode.name, func(t *testing.T) {
			runRightDedupJoinDoubleSignedZeroContract(t, mode)
		})
	}
}

func runRightDedupJoinDoubleSignedZeroContract(
	t *testing.T,
	mode rightDedupKeyContractMode,
) {
	proc, ctrl := newRightDedupTestProcess(t, true)
	defer ctrl.Finish()
	proc.Base.Lim.Size = 8 << 20
	proc.Base.Lim.SpillSize = 64 << 20
	var buildBatch, probeBatch *batch.Batch
	var rightDedupArg *RightDedupJoin
	var buildArg *hashbuild.HashBuild
	defer func() {
		if rightDedupArg != nil {
			rightDedupArg.Free(proc, false, nil)
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
	conditions := [][]*plan.Expr{
		{newExpr(0, floatType)},
		{newExpr(0, floatType)},
	}
	tag++
	joinMapTag := tag

	const buildRows = hashmap.UnitLimit + 1
	buildBatch = batch.NewWithSize(1)
	buildBatch.Vecs[0] = vector.NewVec(floatType)
	for key := 1; key < buildRows; key++ {
		require.NoError(t, vector.AppendFixed(buildBatch.Vecs[0], float64(key), false, proc.Mp()))
	}
	// Keep the equality target in the second hashmap chunk (start=UnitLimit).
	require.NoError(t, vector.AppendFixed(buildBatch.Vecs[0], float64(0), false, proc.Mp()))
	buildBatch.SetRowCount(buildRows)
	if mode.wantReSpill {
		requireRightDedupTargetBucketReSpills(
			t,
			buildBatch.Vecs[0],
			buildRows-1,
			mode.spillThreshold,
		)
	}

	probeBatch = batch.NewWithSize(1)
	probeBatch.Vecs[0] = vector.NewVec(floatType)
	require.NoError(t, vector.AppendFixed(
		probeBatch.Vecs[0],
		math.Copysign(0, -1),
		false,
		proc.Mp(),
	))
	probeBatch.SetRowCount(1)

	rightDedupArg = &RightDedupJoin{
		LeftTypes:         []types.Type{floatType},
		RightTypes:        []types.Type{floatType},
		Conditions:        conditions,
		Result:            []colexec.ResultPos{colexec.NewResultPos(0, 0)},
		OnDuplicateAction: plan.Node_FAIL,
		DedupColName:      "contract_key",
		DedupColTypes:     []plan.Type{{Id: int32(types.T_float64)}},
		IsShuffle:         mode.shuffle,
		ShuffleIdx:        0,
		SpillThreshold:    mode.spillThreshold,
		JoinMapTag:        joinMapTag,
	}
	rightDedupArg.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{probeBatch}))

	buildArg = &hashbuild.HashBuild{
		NeedHashMap:    true,
		NeedBatches:    false,
		Conditions:     conditions[1],
		IsShuffle:      mode.shuffle,
		ShuffleIdx:     0,
		SpillThreshold: mode.spillThreshold,
		JoinMapTag:     joinMapTag,
		JoinMapRefCnt:  1,
	}
	if mode.shuffle {
		buildArg.RuntimeFilterSpec = &plan.RuntimeFilterSpec{Tag: joinMapTag + 9000}
	}
	installTestAllocation(t, rightDedupArg, buildArg)
	buildArg.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{buildBatch}))

	spillBefore := promtestutil.ToFloat64(
		metricv2.HashBuildSpillDepthCounter.WithLabelValues("spill", "1"),
	)
	reSpillBefore := promtestutil.ToFloat64(
		metricv2.HashBuildSpillDepthCounter.WithLabelValues("respill", "2"),
	)
	require.NoError(t, buildArg.Prepare(proc))
	require.NoError(t, rightDedupArg.Prepare(proc))
	buildResult, err := vm.Exec(buildArg, proc)
	require.NoError(t, err)
	require.Nil(t, buildResult.Batch)

	outputRows := 0
	for {
		result, execErr := vm.Exec(rightDedupArg, proc)
		if result.Batch != nil {
			outputRows += result.Batch.RowCount()
		}
		if execErr != nil {
			require.True(t, moerr.IsMoErrCode(execErr, moerr.ErrDuplicateEntry))
			require.Contains(t, execErr.Error(), "Duplicate entry '-0' for key 'contract_key'")
			require.Zero(t, outputRows)
			break
		}
		if result.Status == vm.ExecStop {
			t.Fatal("expected signed-zero duplicate")
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
}

func requireRightDedupTargetBucketReSpills(
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

type rightDedupRemainingKeyCase struct {
	name          string
	typ           types.Type
	build         any
	probe         any
	wantDuplicate bool
	filler        func(int) any
}

func TestRightDedupJoinRemainingKeyContracts(t *testing.T) {
	negativeZero := float32(math.Copysign(0, -1))
	cases := []rightDedupRemainingKeyCase{
		{
			name:          "json-numeric-representation",
			typ:           types.T_json.ToType(),
			build:         "1",
			probe:         "1.0",
			wantDuplicate: true,
			filler: func(i int) any {
				return strconv.Itoa(i + 100)
			},
		},
		{
			name:          "vecf32-signed-zero",
			typ:           types.T_array_float32.ToType(),
			build:         []float32{1, 0, 3},
			probe:         []float32{1, negativeZero, 3},
			wantDuplicate: true,
			filler: func(i int) any {
				return []float32{float32(i + 10), 2, 3}
			},
		},
		{
			name:  "float32-nan",
			typ:   types.T_float32.ToType(),
			build: math.Float32frombits(0x7fc00001),
			probe: math.Float32frombits(0x7fc00001),
			filler: func(i int) any {
				return float32(i + 10)
			},
		},
		{
			name:  "float64-nan",
			typ:   types.T_float64.ToType(),
			build: math.Float64frombits(0x7ff8000000000001),
			probe: math.Float64frombits(0x7ff8000000000001),
			filler: func(i int) any {
				return float64(i + 10)
			},
		},
	}
	modes := []rightDedupKeyContractMode{
		{name: "resident"},
		{name: "initial-spill", shuffle: true, spillThreshold: hashmap.UnitLimit + 1, wantInitialSpill: true},
		{name: "re-spill", shuffle: true, spillThreshold: 2, wantInitialSpill: true, wantReSpill: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, mode := range modes {
				t.Run(mode.name, func(t *testing.T) {
					runRightDedupJoinRemainingKeyContract(t, tc, mode)
				})
			}
		})
	}
}

func runRightDedupJoinRemainingKeyContract(
	t *testing.T,
	tc rightDedupRemainingKeyCase,
	mode rightDedupKeyContractMode,
) {
	proc, ctrl := newRightDedupTestProcess(t, true)
	defer ctrl.Finish()
	proc.Base.Lim.Size = 8 << 20
	proc.Base.Lim.SpillSize = 64 << 20
	var buildBatch, probeBatch *batch.Batch
	var rightDedupArg *RightDedupJoin
	var buildArg *hashbuild.HashBuild
	defer func() {
		if rightDedupArg != nil {
			rightDedupArg.Free(proc, false, nil)
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
		require.NoError(t, budgetErr)
		require.Zero(t, used)
		require.Zero(t, diskUsed)
		require.Zero(t, fdUsed)
		require.Zero(t, proc.Mp().CurrNB())
	}()

	conditions := [][]*plan.Expr{
		{newExpr(0, tc.typ)},
		{newExpr(0, tc.typ)},
	}
	tag++
	joinMapTag := tag

	const buildRows = hashmap.UnitLimit + 1
	buildValues := make([]any, buildRows)
	for i := 0; i < buildRows-1; i++ {
		buildValues[i] = tc.filler(i)
	}
	buildValues[buildRows-1] = tc.build
	buildKeys := makeRightDedupContractKeyVector(t, proc, tc.typ, buildValues)
	buildBatch = batch.NewWithSize(1)
	buildBatch.Vecs[0] = buildKeys
	buildBatch.SetRowCount(buildRows)
	if mode.wantReSpill {
		requireRightDedupTargetBucketReSpills(t, buildKeys, buildRows-1, mode.spillThreshold)
	}

	probeValues := []any{tc.probe}
	if !tc.wantDuplicate {
		// RightDedupJoin inserts accepted probe keys into the same map. Two
		// identical NaNs must both remain distinct instead of the first probe
		// making the second one look duplicated.
		probeValues = append(probeValues, tc.probe)
	}
	probeBatch = batch.NewWithSize(1)
	probeBatch.Vecs[0] = makeRightDedupContractKeyVector(t, proc, tc.typ, probeValues)
	probeBatch.SetRowCount(len(probeValues))

	rightDedupArg = &RightDedupJoin{
		LeftTypes:         []types.Type{tc.typ},
		RightTypes:        []types.Type{tc.typ},
		Conditions:        conditions,
		Result:            []colexec.ResultPos{colexec.NewResultPos(0, 0)},
		OnDuplicateAction: plan.Node_FAIL,
		DedupColName:      "contract_key",
		DedupColTypes:     []plan.Type{{Id: int32(tc.typ.Oid)}},
		IsShuffle:         mode.shuffle,
		ShuffleIdx:        0,
		SpillThreshold:    mode.spillThreshold,
		JoinMapTag:        joinMapTag,
	}
	rightDedupArg.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{probeBatch}))

	buildArg = &hashbuild.HashBuild{
		NeedHashMap:    true,
		NeedBatches:    false,
		Conditions:     conditions[1],
		IsShuffle:      mode.shuffle,
		ShuffleIdx:     0,
		SpillThreshold: mode.spillThreshold,
		JoinMapTag:     joinMapTag,
		JoinMapRefCnt:  1,
	}
	if mode.shuffle {
		buildArg.RuntimeFilterSpec = &plan.RuntimeFilterSpec{Tag: joinMapTag + 9100}
	}
	installTestAllocation(t, rightDedupArg, buildArg)
	buildArg.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{buildBatch}))

	spillBefore := promtestutil.ToFloat64(
		metricv2.HashBuildSpillDepthCounter.WithLabelValues("spill", "1"),
	)
	reSpillBefore := promtestutil.ToFloat64(
		metricv2.HashBuildSpillDepthCounter.WithLabelValues("respill", "2"),
	)
	require.NoError(t, buildArg.Prepare(proc))
	require.NoError(t, rightDedupArg.Prepare(proc))
	buildResult, err := vm.Exec(buildArg, proc)
	require.NoError(t, err)
	require.Nil(t, buildResult.Batch)

	outputRows := 0
	duplicate := false
	for {
		result, execErr := vm.Exec(rightDedupArg, proc)
		if result.Batch != nil {
			outputRows += result.Batch.RowCount()
		}
		if execErr != nil {
			require.True(t, moerr.IsMoErrCode(execErr, moerr.ErrDuplicateEntry))
			duplicate = true
			break
		}
		if result.Status == vm.ExecStop {
			break
		}
	}
	require.Equal(t, tc.wantDuplicate, duplicate)
	if tc.wantDuplicate {
		require.Zero(t, outputRows)
	} else {
		require.Equal(t, len(probeValues), outputRows)
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
}

func makeRightDedupContractKeyVector(
	t *testing.T,
	proc *process.Process,
	typ types.Type,
	values []any,
) *vector.Vector {
	vec := vector.NewVec(typ)
	for _, value := range values {
		switch typ.Oid {
		case types.T_float32:
			require.NoError(t, vector.AppendFixed(vec, value.(float32), false, proc.Mp()))
		case types.T_float64:
			require.NoError(t, vector.AppendFixed(vec, value.(float64), false, proc.Mp()))
		case types.T_json:
			jsonValue, err := types.ParseStringToByteJson(value.(string))
			require.NoError(t, err)
			encoded, err := types.EncodeJson(jsonValue)
			require.NoError(t, err)
			require.NoError(t, vector.AppendBytes(vec, encoded, false, proc.Mp()))
		case types.T_array_float32:
			require.NoError(t, vector.AppendBytes(
				vec, types.ArrayToBytes(value.([]float32)), false, proc.Mp(),
			))
		default:
			t.Fatalf("unsupported right-dedup key contract type %s", typ.String())
		}
	}
	return vec
}
