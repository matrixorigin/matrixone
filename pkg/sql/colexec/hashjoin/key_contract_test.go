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
	"context"
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
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	metricv2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

const (
	joinKeyContractBuildRows      = hashmap.UnitLimit + 1
	joinKeyContractEquivalentFrom = hashmap.UnitLimit / 2
)

type joinKeyContractValue struct {
	value any
	null  bool
}

type joinKeyContractCase struct {
	name            string
	typ             types.Type
	probeTyp        types.Type
	build           joinKeyContractValue
	probe           joinKeyContractValue
	sqlEquality     string
	skipReason      string
	makeBuildFiller func(int) joinKeyContractValue
	makeEquivalent  func(int) joinKeyContractValue
}

func (tc joinKeyContractCase) probeType() types.Type {
	if tc.probeTyp.Oid == types.T_any {
		return tc.typ
	}
	return tc.probeTyp
}

type joinKeyExecutionMode struct {
	name             string
	shuffle          bool
	spillThreshold   int64
	wantInitialSpill bool
	wantReSpill      bool
}

var joinKeyExecutionModes = []joinKeyExecutionMode{
	{name: "resident"},
	{name: "initial-spill", shuffle: true, spillThreshold: joinKeyContractBuildRows, wantInitialSpill: true},
	{name: "re-spill", shuffle: true, spillThreshold: 2, wantInitialSpill: true, wantReSpill: true},
}

func TestHashJoinKeyEqualityContract(t *testing.T) {
	// sqlEquality is verified against the real scalar "=" evaluator. Every hash
	// execution mode must then implement that same SQL contract: only TRUE
	// matches; FALSE and NULL do not.
	cases := hashJoinKeyContractCases()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.sqlEquality, runScalarEqualityOracle(t, tc))
			if tc.skipReason != "" {
				t.Skipf("#26432: %s", tc.skipReason)
			}
			wantPayloads := expectedJoinKeyContractPayloads(tc)
			for _, mode := range joinKeyExecutionModes {
				t.Run(mode.name, func(t *testing.T) {
					require.Equal(t, wantPayloads, runHashJoinKeyContract(t, tc, mode))
				})
			}
		})
	}
}

func hashJoinKeyContractCases() []joinKeyContractCase {
	float32Type := types.T_float32.ToType()
	float32Type.Width = 5
	float32Type.Scale = 2
	float32ProbeType := types.T_float32.ToType()
	float32ProbeType.Width = 6
	float32ProbeType.Scale = 3
	vectorZero := make([]float32, 8)
	vectorNegativeZero := make([]float32, 8)
	vectorNegativeZero[3] = float32(math.Copysign(0, -1))

	return []joinKeyContractCase{
		{
			name:        "double-signed-zero",
			typ:         types.T_float64.ToType(),
			build:       joinKeyContractValue{value: float64(0)},
			probe:       joinKeyContractValue{value: math.Copysign(0, -1)},
			sqlEquality: "TRUE",
			makeBuildFiller: func(i int) joinKeyContractValue {
				return joinKeyContractValue{value: float64(i + 1)}
			},
			makeEquivalent: func(i int) joinKeyContractValue {
				if i%2 == 0 {
					return joinKeyContractValue{value: float64(0)}
				}
				return joinKeyContractValue{value: math.Copysign(0, -1)}
			},
		},
		{
			name:        "scaled-float32",
			typ:         float32Type,
			probeTyp:    float32ProbeType,
			build:       joinKeyContractValue{value: float32(1.234)},
			probe:       joinKeyContractValue{value: float32(1.2304)},
			sqlEquality: "TRUE",
			makeBuildFiller: func(i int) joinKeyContractValue {
				return joinKeyContractValue{value: float32(i + 10)}
			},
			makeEquivalent: func(i int) joinKeyContractValue {
				if i%2 == 0 {
					return joinKeyContractValue{value: float32(1.234)}
				}
				return joinKeyContractValue{value: float32(1.23)}
			},
		},
		{
			name:        "scaled-float32-non-equivalent",
			typ:         float32Type,
			probeTyp:    float32ProbeType,
			build:       joinKeyContractValue{value: float32(1.234)},
			probe:       joinKeyContractValue{value: float32(1.2306)},
			sqlEquality: "FALSE",
			makeBuildFiller: func(i int) joinKeyContractValue {
				return joinKeyContractValue{value: float32(i + 10)}
			},
			makeEquivalent: func(i int) joinKeyContractValue {
				if i%2 == 0 {
					return joinKeyContractValue{value: float32(1.234)}
				}
				return joinKeyContractValue{value: float32(1.23)}
			},
		},
		{
			name:        "json-numeric-representation",
			typ:         types.T_json.ToType(),
			build:       joinKeyContractValue{value: "1"},
			probe:       joinKeyContractValue{value: "1.0"},
			sqlEquality: "TRUE",
			skipReason:  "JSON numeric key encoding is pending",
			makeBuildFiller: func(i int) joinKeyContractValue {
				return joinKeyContractValue{value: strconv.Itoa(i + 100)}
			},
			makeEquivalent: func(i int) joinKeyContractValue {
				if i%2 == 0 {
					return joinKeyContractValue{value: "1"}
				}
				return joinKeyContractValue{value: "1.0"}
			},
		},
		{
			name:        "vecf32-signed-zero",
			typ:         types.T_array_float32.ToType(),
			build:       joinKeyContractValue{value: vectorZero},
			probe:       joinKeyContractValue{value: vectorNegativeZero},
			sqlEquality: "TRUE",
			skipReason:  "VECF32 element key encoding is pending",
			makeBuildFiller: func(i int) joinKeyContractValue {
				return joinKeyContractValue{value: []float32{float32(i + 1), 1, 2, 3, 4, 5, 6, 7}}
			},
			makeEquivalent: func(i int) joinKeyContractValue {
				if i%2 == 0 {
					return joinKeyContractValue{value: vectorZero}
				}
				return joinKeyContractValue{value: vectorNegativeZero}
			},
		},
		{
			name:        "double-nan",
			typ:         types.T_float64.ToType(),
			build:       joinKeyContractValue{value: math.Float64frombits(0x7ff8000000000001)},
			probe:       joinKeyContractValue{value: math.Float64frombits(0x7ff8000000000001)},
			sqlEquality: "FALSE",
			skipReason:  "NaN non-match key handling is pending",
			makeBuildFiller: func(i int) joinKeyContractValue {
				return joinKeyContractValue{value: float64(i + 1)}
			},
			makeEquivalent: func(int) joinKeyContractValue {
				return joinKeyContractValue{value: math.Float64frombits(0x7ff8000000000001)}
			},
		},
		{
			name:        "scaled-float32-nan",
			typ:         float32Type,
			build:       joinKeyContractValue{value: math.Float32frombits(0x7fc00001)},
			probe:       joinKeyContractValue{value: math.Float32frombits(0x7fc00001)},
			sqlEquality: "FALSE",
			skipReason:  "NaN non-match key handling is pending",
			makeBuildFiller: func(i int) joinKeyContractValue {
				return joinKeyContractValue{value: float32(i + 1)}
			},
			makeEquivalent: func(int) joinKeyContractValue {
				return joinKeyContractValue{value: math.Float32frombits(0x7fc00001)}
			},
		},
		{
			name:        "double-null",
			typ:         types.T_float64.ToType(),
			build:       joinKeyContractValue{null: true},
			probe:       joinKeyContractValue{null: true},
			sqlEquality: "NULL",
			makeBuildFiller: func(i int) joinKeyContractValue {
				return joinKeyContractValue{value: float64(i + 1)}
			},
			makeEquivalent: func(int) joinKeyContractValue {
				return joinKeyContractValue{null: true}
			},
		},
	}
}

func expectedJoinKeyContractPayloads(tc joinKeyContractCase) []int32 {
	if tc.sqlEquality != "TRUE" {
		return nil
	}
	payloads := make([]int32, 0, joinKeyContractBuildRows-joinKeyContractEquivalentFrom)
	for row := joinKeyContractEquivalentFrom; row < joinKeyContractBuildRows; row++ {
		payloads = append(payloads, int32(row))
	}
	return payloads
}

func runScalarEqualityOracle(t *testing.T, tc joinKeyContractCase) string {
	forward := runScalarEquality(t, tc.typ, tc.build, tc.probeType(), tc.probe)
	reverse := runScalarEquality(t, tc.probeType(), tc.probe, tc.typ, tc.build)
	require.Equal(t, forward, reverse, "SQL equality must be symmetric")
	return forward
}

func runScalarEquality(
	t *testing.T,
	leftType types.Type,
	leftValue joinKeyContractValue,
	rightType types.Type,
	rightValue joinKeyContractValue,
) string {
	proc := testutil.NewProcess(t)
	var left, right, result *vector.Vector
	defer func() {
		if result != nil {
			result.Free(proc.Mp())
		}
		if left != nil {
			left.Free(proc.Mp())
		}
		if right != nil {
			right.Free(proc.Mp())
		}
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	}()
	left = makeJoinKeyVector(t, proc, leftType, []joinKeyContractValue{leftValue})
	right = makeJoinKeyVector(t, proc, rightType, []joinKeyContractValue{rightValue})

	fn, err := function.GetFunctionByName(context.Background(), "=", []types.Type{leftType, rightType})
	require.NoError(t, err)
	result, err = function.RunFunctionDirectly(
		proc,
		fn.GetEncodedOverloadID(),
		[]*vector.Vector{left, right},
		1,
	)
	require.NoError(t, err)

	got := "NULL"
	if !result.GetNulls().Contains(0) {
		if vector.MustFixedColNoTypeCheck[bool](result)[0] {
			got = "TRUE"
		} else {
			got = "FALSE"
		}
	}

	return got
}

func runHashJoinKeyContract(
	t *testing.T,
	keyCase joinKeyContractCase,
	mode joinKeyExecutionMode,
) []int32 {
	probeType := keyCase.probeType()
	keyExprs := [][]*plan.Expr{
		{newExpr(0, probeType)},
		{newExpr(0, keyCase.typ)},
	}
	proc := testutil.NewProcess(t)
	proc.SetMessageBoard(message.NewMessageBoard())
	proc.Base.Lim.Size = 8 << 20
	proc.Base.Lim.SpillSize = 64 << 20
	tag++
	joinMapTag := tag
	payloadType := types.T_int32.ToType()
	arg := &HashJoin{
		LeftTypes:      []types.Type{probeType},
		RightTypes:     []types.Type{keyCase.typ, payloadType},
		ResultCols:     []colexec.ResultPos{colexec.NewResultPos(1, 1)},
		EqConds:        keyExprs,
		NumCPU:         1,
		IsMerger:       true,
		IsShuffle:      mode.shuffle,
		ShuffleIdx:     0,
		SpillThreshold: mode.spillThreshold,
		JoinMapTag:     joinMapTag,
	}
	buildArg := &hashbuild.HashBuild{
		NeedHashMap:      true,
		NeedBatches:      true,
		NeedAllocateSels: true,
		Conditions:       keyExprs[1],
		IsShuffle:        mode.shuffle,
		ShuffleIdx:       0,
		SpillThreshold:   mode.spillThreshold,
		JoinMapTag:       joinMapTag,
		JoinMapRefCnt:    1,
	}
	if mode.shuffle {
		buildArg.RuntimeFilterSpec = &plan.RuntimeFilterSpec{Tag: joinMapTag + 7000}
	}
	installTestAllocation(t, arg, buildArg)
	var build, probe *batch.Batch
	defer func() {
		arg.Free(proc, false, nil)
		buildArg.Free(proc, false, nil)
		if build != nil {
			build.Clean(proc.Mp())
		}
		if probe != nil {
			probe.Clean(proc.Mp())
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

	buildValues := make([]joinKeyContractValue, joinKeyContractBuildRows)
	buildPayloads := make([]int32, joinKeyContractBuildRows)
	for row := 0; row < hashmap.UnitLimit; row++ {
		if row < joinKeyContractEquivalentFrom {
			buildValues[row] = keyCase.makeBuildFiller(row)
		} else {
			buildValues[row] = keyCase.makeEquivalent(row - joinKeyContractEquivalentFrom)
		}
		buildPayloads[row] = int32(row)
	}
	buildValues[hashmap.UnitLimit] = keyCase.build
	buildPayloads[hashmap.UnitLimit] = hashmap.UnitLimit
	build = batch.NewWithSize(2)
	build.Vecs[0] = makeJoinKeyVector(t, proc, keyCase.typ, buildValues)
	build.Vecs[1] = testutil.MakeInt32Vector(buildPayloads, nil, proc.Mp())
	build.SetRowCount(joinKeyContractBuildRows)
	if mode.wantReSpill {
		requireHashJoinTargetBucketReSpills(
			t,
			build.Vecs[0],
			hashmap.UnitLimit,
			mode.spillThreshold,
		)
	}
	probe = batch.NewWithSize(1)
	probe.Vecs[0] = makeJoinKeyVector(t, proc, probeType, []joinKeyContractValue{keyCase.probe})
	probe.SetRowCount(1)

	resetChildrenWithBatch(arg, probe)
	resetHashBuildChildrenWithBatch(buildArg, build)

	spillBefore := promtestutil.ToFloat64(
		metricv2.HashBuildSpillDepthCounter.WithLabelValues("spill", "1"),
	)
	reSpillBefore := promtestutil.ToFloat64(
		metricv2.HashBuildSpillDepthCounter.WithLabelValues("respill", "2"),
	)
	require.NoError(t, arg.Prepare(proc))
	require.NoError(t, buildArg.Prepare(proc))
	buildResult, err := vm.Exec(buildArg, proc)
	require.NoError(t, err)
	require.Nil(t, buildResult.Batch)

	var resultPayloads []int32
	for {
		result, execErr := vm.Exec(arg, proc)
		require.NoError(t, execErr)
		if result.Batch != nil {
			require.Len(t, result.Batch.Vecs, 1)
			require.True(t, result.Batch.Vecs[0].GetNulls().IsEmpty())
			payloads := vector.MustFixedColNoTypeCheck[int32](result.Batch.Vecs[0])
			require.Len(t, payloads, result.Batch.RowCount())
			resultPayloads = append(resultPayloads, payloads...)
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

	sort.Slice(resultPayloads, func(i, j int) bool {
		return resultPayloads[i] < resultPayloads[j]
	})
	return resultPayloads
}

func requireHashJoinTargetBucketReSpills(
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

func makeJoinKeyVector(
	t *testing.T,
	proc *process.Process,
	typ types.Type,
	values []joinKeyContractValue,
) *vector.Vector {
	vec := vector.NewVec(typ)
	for _, value := range values {
		switch typ.Oid {
		case types.T_float32:
			v, _ := value.value.(float32)
			require.NoError(t, vector.AppendFixed(vec, v, value.null, proc.Mp()))
		case types.T_float64:
			v, _ := value.value.(float64)
			require.NoError(t, vector.AppendFixed(vec, v, value.null, proc.Mp()))
		case types.T_json:
			if value.null {
				require.NoError(t, vector.AppendBytes(vec, nil, true, proc.Mp()))
				continue
			}
			jsonText, ok := value.value.(string)
			require.True(t, ok)
			jsonValue, err := types.ParseStringToByteJson(jsonText)
			require.NoError(t, err)
			encoded, err := types.EncodeJson(jsonValue)
			require.NoError(t, err)
			require.NoError(t, vector.AppendBytes(vec, encoded, false, proc.Mp()))
		case types.T_array_float32:
			if value.null {
				require.NoError(t, vector.AppendBytes(vec, nil, true, proc.Mp()))
				continue
			}
			arrayValue, ok := value.value.([]float32)
			require.True(t, ok)
			require.NoError(t, vector.AppendBytes(
				vec,
				types.ArrayToBytes(arrayValue),
				false,
				proc.Mp(),
			))
		default:
			t.Fatalf("unsupported join-key contract type %s", typ.String())
		}
	}
	return vec
}
