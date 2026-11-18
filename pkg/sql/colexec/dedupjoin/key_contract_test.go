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
	"strconv"
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
	"github.com/matrixorigin/matrixone/pkg/vm/process"
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

func TestDedupJoinAllocationSiteLedger(t *testing.T) {
	require.Equal(t, uint8(82), uint8(dedupJoinAllocationSiteMatched))
	require.Equal(t, uint8(88), uint8(dedupJoinAllocationSiteFinalizeSelections))
	require.Equal(t, uint8(110), uint8(dedupJoinAllocationSiteResultData))
	require.Equal(t, uint8(113), uint8(dedupJoinAllocationSiteResultGrouping))
}

func TestDedupJoinAuxiliaryArbitration(t *testing.T) {
	for _, mode := range []dedupKeyContractMode{
		{name: "resident"},
		{name: "spill", shuffle: true, spillThreshold: hashmap.UnitLimit + 1, wantInitialSpill: true},
		{name: "re-spill", shuffle: true, spillThreshold: 2, wantInitialSpill: true, wantReSpill: true},
	} {
		for _, reservationFirst := range []bool{false, true} {
			t.Run(mode.name+"/reservationFirst="+strconv.FormatBool(reservationFirst), func(t *testing.T) {
				proc, ctrl := newCaptureTestProc(t)
				defer ctrl.Finish()
				proc.Base.Lim.Size = 8 << 20
				proc.Base.Lim.SpillSize = 64 << 20
				session := &dedupJoinWarningSession{}
				proc.Session = session
				proc.SetStmtProfile(&process.StmtProfile{})
				proc.GetStmtProfile().SetStatementRuntimeProfile("Update", "DML", true)
				var join *DedupJoin
				var build *hashbuild.HashBuild
				buildBat, probeBat := batch.NewWithSize(4), batch.NewWithSize(1)
				defer func() {
					if join != nil {
						join.Free(proc, false, nil)
					}
					if build != nil {
						build.Free(proc, false, nil)
					}
					buildBat.Clean(proc.Mp())
					probeBat.Clean(proc.Mp())
					budget, err := proc.GetExecutionResourceBudget()
					var used, disk, descriptors uint64
					if err == nil {
						used, disk, descriptors = budget.Used(), budget.SpillDiskUsed(), budget.SpillFDUsed()
					}
					proc.Free()
					require.NoError(t, err)
					require.Zero(t, used)
					require.Zero(t, disk)
					require.Zero(t, descriptors)
					require.Zero(t, proc.Mp().CurrNB())
				}()
				intType, boolType := types.T_int32.ToType(), types.T_bool.ToType()
				rightTypes := []types.Type{intType, intType, boolType, intType}
				for i, typ := range rightTypes {
					buildBat.Vecs[i] = vector.NewVec(typ)
				}
				probeBat.Vecs[0] = vector.NewVec(intType)
				appendBuild := func(key, previous, id int32, auxiliary, release bool) {
					require.NoError(t, vector.AppendFixed(buildBat.Vecs[0], key, false, proc.Mp()))
					require.NoError(t, vector.AppendFixed(buildBat.Vecs[1], previous, auxiliary && !release, proc.Mp()))
					require.NoError(t, vector.AppendFixed(buildBat.Vecs[2], release, false, proc.Mp()))
					require.NoError(t, vector.AppendFixed(buildBat.Vecs[3], id, auxiliary, proc.Mp()))
				}
				// These unchanged, rejected rows retain reservations and exercise
				// snapshot diagnostic suppression in every materialized partition.
				for i := 0; i < hashmap.UnitLimit; i++ {
					key := int32(1000 + i)
					appendBuild(key, 0, 0, true, false)
					require.NoError(t, vector.AppendFixed(probeBat.Vecs[0], key, false, proc.Mp()))
				}
				if reservationFirst {
					appendBuild(30, 0, 0, true, false)
				}
				appendBuild(30, 20, 2, false, false) // B cannot steal A's accepted 30.
				if !reservationFirst {
					appendBuild(30, 0, 0, true, false)
				}
				appendBuild(20, 0, 0, true, false)
				appendBuild(10, 10, 0, true, true)   // S=10 was released when A accepted 30.
				appendBuild(10, 30, 1, false, false) // A may reclaim the now-unowned 10.
				buildBat.SetRowCount(buildBat.Vecs[0].Length())
				for _, key := range []int32{10, 20} {
					require.NoError(t, vector.AppendFixed(probeBat.Vecs[0], key, false, proc.Mp()))
				}
				probeBat.SetRowCount(probeBat.Vecs[0].Length())
				if mode.wantReSpill {
					requireDedupTargetBucketReSpills(t, buildBat.Vecs[0], buildBat.RowCount()-1, mode.spillThreshold)
				}
				conditions := [][]*plan.Expr{{newExpr(0, intType)}, {newExpr(0, intType)}}
				const mapTag = 328270
				join = &DedupJoin{LeftTypes: []types.Type{intType}, RightTypes: rightTypes, Conditions: conditions,
					Result:            []colexec.ResultPos{colexec.NewResultPos(1, 3), colexec.NewResultPos(1, 0)},
					OnDuplicateAction: plan.Node_IGNORE, DelColIdx: 1, DedupDeleteMarkerColIdx: 2, AuxiliaryRowIdColIdx: 4,
					DedupColName: "uk", DedupColTypes: []plan.Type{conditions[0][0].Typ},
					IsShuffle: mode.shuffle, SpillThreshold: mode.spillThreshold, JoinMapTag: mapTag}
				build = &hashbuild.HashBuild{NeedHashMap: true, NeedBatches: true, NeedAllocateSels: true, Conditions: conditions[1],
					IsDedup: true, OnDuplicateAction: plan.Node_IGNORE, DelColIdx: 1, DedupDeleteMarkerColIdx: 2, AuxiliaryRowIdColIdx: 4,
					DedupColName: join.DedupColName, DedupColTypes: join.DedupColTypes,
					IsShuffle: mode.shuffle, SpillThreshold: mode.spillThreshold, JoinMapTag: mapTag, JoinMapRefCnt: 1}
				if mode.shuffle {
					build.RuntimeFilterSpec = &plan.RuntimeFilterSpec{Tag: mapTag + 1}
				}
				join.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{probeBat}))
				build.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{buildBat}))
				installTestAllocation(t, join, build)
				spillMetric := metricv2.HashBuildSpillDepthCounter.WithLabelValues("spill", "1")
				reSpillMetric := metricv2.HashBuildSpillDepthCounter.WithLabelValues("respill", "2")
				spillBefore, reSpillBefore := promtestutil.ToFloat64(spillMetric), promtestutil.ToFloat64(reSpillMetric)
				require.NoError(t, build.Prepare(proc))
				require.NoError(t, join.Prepare(proc))
				_, err := vm.Exec(build, proc)
				require.NoError(t, err)
				var actual [][]int32
				for {
					result, err := vm.Exec(join, proc)
					require.NoError(t, err)
					if result.Batch != nil {
						for row := 0; row < result.Batch.RowCount(); row++ {
							if !result.Batch.Vecs[0].IsNull(uint64(row)) {
								actual = append(actual, []int32{vector.GetFixedAtNoTypeCheck[int32](result.Batch.Vecs[0], row), vector.GetFixedAtNoTypeCheck[int32](result.Batch.Vecs[1], row)})
							}
						}
					}
					if result.Status == vm.ExecStop {
						break
					}
				}
				require.Equal(t, [][]int32{{1, 10}}, actual)
				require.Equal(t, uint64(1), session.total)
				require.Equal(t, mode.wantInitialSpill, promtestutil.ToFloat64(spillMetric) > spillBefore)
				require.Equal(t, mode.wantReSpill, promtestutil.ToFloat64(reSpillMetric) > reSpillBefore)
			})
		}
	}
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
		budget, budgetErr := proc.GetExecutionResourceBudget()
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

type dedupRemainingKeyCase struct {
	name        string
	typ         types.Type
	build       any
	probe       any
	wantCapture bool
	filler      func(int) any
}

func TestDedupJoinRemainingKeyContracts(t *testing.T) {
	negativeZero := float32(math.Copysign(0, -1))
	cases := []dedupRemainingKeyCase{
		{
			name:        "json-numeric-representation",
			typ:         types.T_json.ToType(),
			build:       "1",
			probe:       "1.0",
			wantCapture: true,
			filler: func(i int) any {
				return strconv.Itoa(i + 100)
			},
		},
		{
			name:        "vecf32-signed-zero",
			typ:         types.T_array_float32.ToType(),
			build:       []float32{1, 0, 3},
			probe:       []float32{1, negativeZero, 3},
			wantCapture: true,
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
	modes := []dedupKeyContractMode{
		{name: "resident"},
		{name: "initial-spill", shuffle: true, spillThreshold: hashmap.UnitLimit + 1, wantInitialSpill: true},
		{name: "re-spill", shuffle: true, spillThreshold: 2, wantInitialSpill: true, wantReSpill: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, mode := range modes {
				t.Run(mode.name, func(t *testing.T) {
					runDedupJoinRemainingKeyContract(t, tc, mode)
				})
			}
		})
	}
}

func runDedupJoinRemainingKeyContract(
	t *testing.T,
	tc dedupRemainingKeyCase,
	mode dedupKeyContractMode,
) {
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
		budget, budgetErr := proc.GetExecutionResourceBudget()
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

	intType := types.T_int32.ToType()
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
	buildKeys := makeDedupContractKeyVector(t, proc, tc.typ, buildValues)
	buildPlaceholder := vector.NewVec(intType)
	for i := 0; i < buildRows; i++ {
		require.NoError(t, vector.AppendFixed(buildPlaceholder, int32(0), true, proc.Mp()))
	}
	buildBatch = batch.NewWithSize(2)
	buildBatch.Vecs[0] = buildKeys
	buildBatch.Vecs[1] = buildPlaceholder
	buildBatch.SetRowCount(buildRows)
	if mode.wantReSpill {
		requireDedupTargetBucketReSpills(t, buildKeys, buildRows-1, mode.spillThreshold)
	}

	probeBatch = batch.NewWithSize(2)
	probeBatch.Vecs[0] = makeDedupContractKeyVector(t, proc, tc.typ, []any{tc.probe})
	probeBatch.Vecs[1] = vector.NewVec(intType)
	require.NoError(t, vector.AppendFixed(probeBatch.Vecs[1], int32(42), false, proc.Mp()))
	probeBatch.SetRowCount(1)

	dedupArg = &DedupJoin{
		LeftTypes:  []types.Type{tc.typ, intType},
		RightTypes: []types.Type{tc.typ, intType},
		Conditions: conditions,
		Result: []colexec.ResultPos{
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
		buildArg.RuntimeFilterSpec = &plan.RuntimeFilterSpec{Tag: joinMapTag + 8100}
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

	resultRows := 0
	capturedRows := 0
	for {
		result, execErr := vm.Exec(dedupArg, proc)
		require.NoError(t, execErr)
		if result.Batch != nil {
			resultRows += result.Batch.RowCount()
			captured := vector.MustFixedColNoTypeCheck[int32](result.Batch.Vecs[0])
			for row, value := range captured {
				if result.Batch.Vecs[0].GetNulls().Contains(uint64(row)) {
					continue
				}
				require.Equal(t, int32(42), value)
				capturedRows++
			}
		}
		if result.Status == vm.ExecStop {
			break
		}
	}
	require.Equal(t, buildRows, resultRows)
	if tc.wantCapture {
		require.Equal(t, 1, capturedRows)
	} else {
		require.Zero(t, capturedRows)
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

func makeDedupContractKeyVector(
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
			t.Fatalf("unsupported dedup key contract type %s", typ.String())
		}
	}
	return vec
}
