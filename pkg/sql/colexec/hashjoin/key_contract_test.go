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
	"strconv"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	metricv2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

type joinKeyContractValue struct {
	value any
	null  bool
}

type joinKeyContractCase struct {
	name            string
	typ             types.Type
	build           joinKeyContractValue
	probe           joinKeyContractValue
	sqlEquality     string
	makeBuildFiller func(int) joinKeyContractValue
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
	{name: "initial-spill", shuffle: true, spillThreshold: 64, wantInitialSpill: true},
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
			wantMatch := tc.sqlEquality == "TRUE"
			for _, mode := range joinKeyExecutionModes {
				t.Run(mode.name, func(t *testing.T) {
					require.Equal(t, wantMatch, runHashJoinKeyContract(t, tc, mode))
				})
			}
		})
	}
}

func hashJoinKeyContractCases() []joinKeyContractCase {
	float32Type := types.T_float32.ToType()
	float32Type.Scale = 2
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
		},
		{
			name:        "scaled-float32",
			typ:         float32Type,
			build:       joinKeyContractValue{value: float32(1.234)},
			probe:       joinKeyContractValue{value: float32(1.23)},
			sqlEquality: "TRUE",
			makeBuildFiller: func(i int) joinKeyContractValue {
				return joinKeyContractValue{value: float32(i + 10)}
			},
		},
		{
			name:        "json-numeric-representation",
			typ:         types.T_json.ToType(),
			build:       joinKeyContractValue{value: "1"},
			probe:       joinKeyContractValue{value: "1.0"},
			sqlEquality: "TRUE",
			makeBuildFiller: func(i int) joinKeyContractValue {
				return joinKeyContractValue{value: strconv.Itoa(i + 100)}
			},
		},
		{
			name:        "vecf32-signed-zero",
			typ:         types.T_array_float32.ToType(),
			build:       joinKeyContractValue{value: vectorZero},
			probe:       joinKeyContractValue{value: vectorNegativeZero},
			sqlEquality: "TRUE",
			makeBuildFiller: func(i int) joinKeyContractValue {
				return joinKeyContractValue{value: []float32{float32(i + 1), 1, 2, 3, 4, 5, 6, 7}}
			},
		},
		{
			name:        "double-nan",
			typ:         types.T_float64.ToType(),
			build:       joinKeyContractValue{value: math.Float64frombits(0x7ff8000000000001)},
			probe:       joinKeyContractValue{value: math.Float64frombits(0x7ff8000000000001)},
			sqlEquality: "FALSE",
			makeBuildFiller: func(i int) joinKeyContractValue {
				return joinKeyContractValue{value: float64(i + 1)}
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
		},
	}
}

func runScalarEqualityOracle(t *testing.T, tc joinKeyContractCase) string {
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
	left = makeJoinKeyVector(t, proc, tc.typ, []joinKeyContractValue{tc.build})
	right = makeJoinKeyVector(t, proc, tc.typ, []joinKeyContractValue{tc.probe})

	fn, err := function.GetFunctionByName(context.Background(), "=", []types.Type{tc.typ, tc.typ})
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

func runHashJoinKeyContract(t *testing.T, keyCase joinKeyContractCase, mode joinKeyExecutionMode) bool {
	keyExprs := [][]*plan.Expr{
		{newExpr(0, keyCase.typ)},
		{newExpr(0, keyCase.typ)},
	}
	tc := newTestCase(
		t,
		[]bool{true},
		[]types.Type{keyCase.typ},
		[]colexec.ResultPos{colexec.NewResultPos(0, 0)},
		keyExprs,
	)
	tc.proc.Base.Lim.Size = 8 << 20
	tc.proc.Base.Lim.SpillSize = 64 << 20
	defer func() {
		tc.arg.Free(tc.proc, false, nil)
		tc.barg.Free(tc.proc, false, nil)
		budget, err := tc.proc.GetHashBuildBudget()
		require.NoError(t, err)
		require.Zero(t, budget.Used())
		require.Zero(t, budget.SpillDiskUsed())
		require.Zero(t, budget.SpillFDUsed())
		tc.proc.Free()
		require.Zero(t, tc.proc.Mp().CurrNB())
	}()

	const buildRows = 256
	buildValues := make([]joinKeyContractValue, buildRows)
	buildValues[0] = keyCase.build
	for i := 1; i < buildRows; i++ {
		buildValues[i] = keyCase.makeBuildFiller(i)
	}
	build := batch.NewWithSize(1)
	build.Vecs[0] = makeJoinKeyVector(t, tc.proc, keyCase.typ, buildValues)
	build.SetRowCount(buildRows)
	probe := batch.NewWithSize(1)
	probe.Vecs[0] = makeJoinKeyVector(t, tc.proc, keyCase.typ, []joinKeyContractValue{keyCase.probe})
	probe.SetRowCount(1)

	tc.arg.NonEqCond = nil
	tc.arg.IsShuffle = mode.shuffle
	tc.arg.ShuffleIdx = 0
	tc.arg.SpillThreshold = mode.spillThreshold
	tc.barg.IsShuffle = mode.shuffle
	tc.barg.ShuffleIdx = 0
	tc.barg.SpillThreshold = mode.spillThreshold
	tc.barg.NeedBatches = false
	if mode.shuffle {
		tc.barg.RuntimeFilterSpec = &plan.RuntimeFilterSpec{Tag: tc.arg.JoinMapTag + 7000}
	}
	resetChildrenWithBatch(tc.arg, probe)
	resetHashBuildChildrenWithBatch(tc.barg, build)

	spillBefore := promtestutil.ToFloat64(
		metricv2.HashBuildSpillDepthCounter.WithLabelValues("spill", "1"),
	)
	reSpillBefore := promtestutil.ToFloat64(
		metricv2.HashBuildSpillDepthCounter.WithLabelValues("respill", "2"),
	)
	require.NoError(t, tc.arg.Prepare(tc.proc))
	require.NoError(t, tc.barg.Prepare(tc.proc))
	buildResult, err := vm.Exec(tc.barg, tc.proc)
	require.NoError(t, err)
	require.Nil(t, buildResult.Batch)

	resultRows := 0
	for {
		result, execErr := vm.Exec(tc.arg, tc.proc)
		require.NoError(t, execErr)
		if result.Batch != nil {
			resultRows += result.Batch.RowCount()
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

	return resultRows == 1
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
