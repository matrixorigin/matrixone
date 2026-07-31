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

package runtimefilter

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap/keycodec"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	planfunction "github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func exactContractPlanType(typ types.Type) *plan.Type {
	return &plan.Type{
		Id:    int32(typ.Oid),
		Width: typ.Width,
		Scale: typ.Scale,
	}
}

func TestTupleContractRejectsForgedNonVarcharResult(t *testing.T) {
	rt := moruntime.ServiceRuntime("")
	original, hadOriginal := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	rt.SetGlobalVariables(
		moruntime.MOProtocolVersion, defines.MORPCVersion7)
	t.Cleanup(func() {
		if hadOriginal {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, original)
		} else {
			rt.SetGlobalVariables(
				moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
	})

	intType := types.T_int32.ToType()
	intPlanType := *exactContractPlanType(intType)
	spec := &plan.RuntimeFilterSpec{
		BuildExpr: &plan.Expr{
			Typ: intPlanType,
			Expr: &plan.Expr_F{F: &plan.Function{
				Func: &plan.ObjectRef{
					ObjName: planfunction.SerialFunctionName,
					Obj:     planfunction.SerialFunctionEncodeID,
				},
				Args: []*plan.Expr{exactContractCol(intType)},
			}},
		},
		ProbeType:              &intPlanType,
		KeyComponentProbeTypes: []plan.Type{intPlanType},
		KeyEncoding: plan.
			RuntimeFilterKeyEncoding_RUNTIME_FILTER_KEY_SERIAL_V1,
	}

	require.Equal(t, keycodec.ExactRuntimeFilterUnsupported,
		ExactKeyEncodingWithComponents(
			spec, intType, []types.Type{intType}, ""))
}

func exactContractCol(typ types.Type) *plan.Expr {
	return &plan.Expr{
		Typ: *exactContractPlanType(typ),
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{ColPos: 0},
		},
	}
}

func exactRawContract(
	probeType, declaredBuildType types.Type,
) *plan.RuntimeFilterSpec {
	return &plan.RuntimeFilterSpec{
		BuildExpr:   exactContractCol(declaredBuildType),
		ProbeType:   exactContractPlanType(probeType),
		KeyEncoding: plan.RuntimeFilterKeyEncoding_RUNTIME_FILTER_KEY_RAW_V1,
	}
}

func TestExactKeyEncodingRejectsUnprovableWireContracts(t *testing.T) {
	intType := types.T_int32.ToType()
	varcharType := types.T_varchar.ToType()

	tests := []struct {
		name        string
		spec        *plan.RuntimeFilterSpec
		payloadType types.Type
		want        keycodec.ExactRuntimeFilterEncoding
	}{
		{
			name:        "guarded raw contract",
			spec:        exactRawContract(intType, intType),
			payloadType: intType,
			want:        keycodec.ExactRuntimeFilterRaw,
		},
		{
			name: "real stale decimal shape checks probe against spec and payload",
			spec: exactRawContract(
				types.New(types.T_decimal64, 18, 2),
				types.New(types.T_decimal64, 18, 3),
			),
			payloadType: types.New(types.T_decimal64, 18, 3),
			want:        keycodec.ExactRuntimeFilterUnsupported,
		},
		{
			name: "legacy numeric tag one cannot masquerade as guarded raw",
			spec: &plan.RuntimeFilterSpec{
				Expr:        exactContractCol(intType),
				KeyEncoding: plan.RuntimeFilterKeyEncoding(1),
			},
			payloadType: intType,
			want:        keycodec.ExactRuntimeFilterUnsupported,
		},
		{
			name: "legacy expression remains insufficient with forged probe type",
			spec: &plan.RuntimeFilterSpec{
				Expr:        exactContractCol(intType),
				ProbeType:   exactContractPlanType(intType),
				KeyEncoding: plan.RuntimeFilterKeyEncoding(1),
			},
			payloadType: intType,
			want:        keycodec.ExactRuntimeFilterUnsupported,
		},
		{
			name: "identical raw compatibility expressions",
			spec: func() *plan.RuntimeFilterSpec {
				spec := exactRawContract(intType, intType)
				spec.Expr = exactContractCol(intType)
				return spec
			}(),
			payloadType: intType,
			want:        keycodec.ExactRuntimeFilterRaw,
		},
		{
			name: "mismatched raw compatibility expressions",
			spec: func() *plan.RuntimeFilterSpec {
				spec := exactRawContract(intType, intType)
				spec.Expr = exactContractCol(intType)
				spec.Expr.GetCol().ColPos = 1
				return spec
			}(),
			payloadType: intType,
			want:        keycodec.ExactRuntimeFilterUnsupported,
		},
		{
			name: "metadata dependent raw rejects legacy expression",
			spec: func() *plan.RuntimeFilterSpec {
				decimalType := types.New(
					types.T_decimal64, 18, 2)
				spec := exactRawContract(decimalType, decimalType)
				spec.Expr = exactContractCol(decimalType)
				return spec
			}(),
			payloadType: types.New(types.T_decimal64, 18, 2),
			want:        keycodec.ExactRuntimeFilterUnsupported,
		},
		{
			name: "unknown future encoding",
			spec: func() *plan.RuntimeFilterSpec {
				spec := exactRawContract(intType, intType)
				spec.KeyEncoding = plan.RuntimeFilterKeyEncoding(99)
				return spec
			}(),
			payloadType: intType,
			want:        keycodec.ExactRuntimeFilterUnsupported,
		},
		{
			name: "prefix consumer rejects integer raw payload",
			spec: func() *plan.RuntimeFilterSpec {
				spec := exactRawContract(intType, intType)
				spec.MatchPrefix = true
				return spec
			}(),
			payloadType: intType,
			want:        keycodec.ExactRuntimeFilterUnsupported,
		},
		{
			name: "prefix consumer accepts varchar raw payload",
			spec: func() *plan.RuntimeFilterSpec {
				spec := exactRawContract(varcharType, varcharType)
				spec.MatchPrefix = true
				return spec
			}(),
			payloadType: varcharType,
			want:        keycodec.ExactRuntimeFilterRaw,
		},
		{
			name: "float closure cannot feed prefix consumer",
			spec: &plan.RuntimeFilterSpec{
				BuildExpr:   exactContractCol(types.T_float64.ToType()),
				ProbeType:   exactContractPlanType(types.T_float64.ToType()),
				KeyEncoding: plan.RuntimeFilterKeyEncoding_RUNTIME_FILTER_KEY_FLOAT_ZERO_CLOSED_V1,
				MatchPrefix: true,
			},
			payloadType: types.T_float64.ToType(),
			want:        keycodec.ExactRuntimeFilterUnsupported,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			wire, err := test.spec.Marshal()
			require.NoError(t, err)
			decoded := new(plan.RuntimeFilterSpec)
			require.NoError(t, decoded.Unmarshal(wire))
			require.Equal(t, test.spec.KeyEncoding, decoded.KeyEncoding)
			require.Equal(t, test.want,
				ExactKeyEncoding(decoded, test.payloadType, ""))
		})
	}
}

func TestMarshalExactFilterVectorUsesWireSizedBudget(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := vector.NewVec(types.T_int8.ToType())
	defer func() {
		vec.Free(mp)
		require.Zero(t, mp.CurrNB())
	}()
	for i := 0; i < 1024; i++ {
		require.NoError(t, vector.AppendFixed(
			vec, int8(i), false, mp))
	}

	aggregate := process.MustNewHashBuildBudget(1<<20, 1<<20)
	budget, err := aggregate.OpenGeneration(1)
	require.NoError(t, err)
	data, release, err := MarshalExactFilterVector(vec, budget)
	require.NoError(t, err)
	require.Len(t, data, 34+vec.Length())
	// The retained charge is the actual bytes.Buffer capacity, not a
	// row-count-derived metadata estimate.
	require.LessOrEqual(t, budget.Used(), uint64(2*len(data)))
	require.NotZero(t, budget.Used())
	release()
	require.Zero(t, budget.Used())
}

func TestMarshalExactFilterVectorAdmissionFailsBeforeAllocation(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := vector.NewVec(types.T_int64.ToType())
	defer func() {
		vec.Free(mp)
		require.Zero(t, mp.CurrNB())
	}()
	require.NoError(t, vector.AppendFixed(vec, int64(7), false, mp))

	aggregate := process.MustNewHashBuildBudget(1, 1)
	budget, err := aggregate.OpenGeneration(1)
	require.NoError(t, err)
	data, release, err := MarshalExactFilterVector(vec, budget)
	require.ErrorIs(t, err, process.ErrHashBuildBudgetAdmission)
	require.Nil(t, data)
	require.Nil(t, release)
	require.Zero(t, budget.Used())
}
