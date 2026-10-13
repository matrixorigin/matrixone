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
	"context"
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap/keycodec"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	planfunction "github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestClassifyOptionalFallbackFatalFirst(t *testing.T) {
	mpoolErr := mpool.ErrAllocationAccountCapacity
	providerErr := errors.New("budget provider failed")
	var nilBudgetErr *process.ExecutionResourceError
	budgetErr := func(
		kind process.ExecutionResourceErrorKind,
		component process.ExecutionResourceComponent,
	) error {
		return &process.ExecutionResourceError{Kind: kind, Component: component}
	}
	marked := MarkOptionalAllocationError
	memoryAdmission := func() error {
		return budgetErr(
			process.ExecutionResourceErrorAdmission,
			process.ExecutionResourceComponentMemory,
		)
	}

	tests := []struct {
		name string
		err  error
		want OptionalFallbackKind
	}{
		{name: "nil", want: OptionalFallbackNone},
		{name: "typed nil budget error", err: nilBudgetErr, want: OptionalFallbackNone},
		{name: "typed memory admission", err: memoryAdmission(), want: OptionalFallbackBudgetAdmission},
		{name: "marked typed memory admission", err: marked(memoryAdmission()), want: OptionalFallbackBudgetAdmission},
		{name: "typed spill disk admission", err: budgetErr(process.ExecutionResourceErrorAdmission, process.ExecutionResourceComponentSpillDisk), want: OptionalFallbackNone},
		{name: "typed spill fd admission", err: budgetErr(process.ExecutionResourceErrorAdmission, process.ExecutionResourceComponentSpillFD), want: OptionalFallbackNone},
		{name: "typed admission without component", err: budgetErr(process.ExecutionResourceErrorAdmission, 0), want: OptionalFallbackNone},
		{name: "joined admission and closed", err: errors.Join(memoryAdmission(), budgetErr(process.ExecutionResourceErrorClosed, 0)), want: OptionalFallbackNone},
		{name: "marked mpool allocation", err: marked(mpoolErr), want: OptionalFallbackAllocation},
		{name: "plain mpool error", err: mpoolErr, want: OptionalFallbackNone},
		{name: "marked sealed", err: marked(mpool.ErrAllocationAccountSealed), want: OptionalFallbackNone},
		{name: "marked mismatch", err: marked(mpool.ErrAllocationAccountMismatch), want: OptionalFallbackNone},
		{name: "marked invariant", err: marked(mpool.ErrAllocationAccountInvariant), want: OptionalFallbackNone},
		{name: "marked allocator limit", err: marked(mpool.ErrAllocationAllocatorLimit), want: OptionalFallbackNone},
		{name: "marked suspended", err: marked(mpool.ErrAllocationAdmissionSuspended), want: OptionalFallbackNone},
		{name: "marked joined capacity and invariant", err: marked(errors.Join(mpool.ErrAllocationAccountCapacity, mpool.ErrAllocationAccountInvariant)), want: OptionalFallbackNone},
		{name: "marked joined mpool capacity and invalid", err: marked(errors.Join(moerr.NewMPoolCapacityNoCtxf("test"), mpool.ErrAllocationAccountInvalid)), want: OptionalFallbackNone},
		{name: "plain provider error", err: providerErr, want: OptionalFallbackNone},
		{name: "raw admission sentinel", err: process.ErrExecutionResourceAdmission, want: OptionalFallbackNone},
		{name: "typed closed", err: budgetErr(process.ExecutionResourceErrorClosed, 0), want: OptionalFallbackNone},
		{name: "marked typed closed", err: marked(budgetErr(process.ExecutionResourceErrorClosed, 0)), want: OptionalFallbackNone},
		{name: "typed invalid", err: budgetErr(process.ExecutionResourceErrorInvalid, 0), want: OptionalFallbackNone},
		{name: "marked typed invalid", err: marked(budgetErr(process.ExecutionResourceErrorInvalid, 0)), want: OptionalFallbackNone},
		{name: "typed ceiling missing", err: budgetErr(process.ExecutionResourceErrorCeilingMissing, 0), want: OptionalFallbackNone},
		{name: "marked typed ceiling missing", err: marked(budgetErr(process.ExecutionResourceErrorCeilingMissing, 0)), want: OptionalFallbackNone},
		{name: "marked canceled", err: marked(context.Canceled), want: OptionalFallbackNone},
		{name: "marked deadline", err: marked(context.DeadlineExceeded), want: OptionalFallbackNone},
		{name: "marked raw closed", err: marked(process.ErrExecutionResourceClosed), want: OptionalFallbackNone},
		{name: "marked raw invalid", err: marked(process.ErrExecutionResourceInvalid), want: OptionalFallbackNone},
		{name: "marked raw ceiling", err: marked(process.ErrExecutionMemoryCeilingMissing), want: OptionalFallbackNone},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, ClassifyOptionalFallback(test.err))
		})
	}
}

func exactContractPlanType(typ types.Type) *plan.Type {
	return &plan.Type{
		Id:    int32(typ.Oid),
		Width: typ.Width,
		Scale: typ.Scale,
	}
}

func TestTupleContractRejectsForgedNonVarcharResult(t *testing.T) {
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
			spec, intType, []types.Type{intType}))
}

func exactContractCol(typ types.Type) *plan.Expr {
	return &plan.Expr{
		Typ: *exactContractPlanType(typ),
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{ColPos: 0},
		},
	}
}

func TestTupleComponentSlotCastContract(t *testing.T) {
	makeCast := func(sourceType, targetType types.Type, name string) *plan.Expr {
		return &plan.Expr{
			Typ: *exactContractPlanType(targetType),
			Expr: &plan.Expr_F{F: &plan.Function{
				Func: &plan.ObjectRef{ObjName: name},
				Args: []*plan.Expr{
					exactContractCol(sourceType),
					{Typ: *exactContractPlanType(targetType), Expr: &plan.Expr_T{T: &plan.TargetType{}}},
				},
			}},
		}
	}

	for _, test := range []struct {
		name       string
		sourceType types.Type
		targetType types.Type
		function   string
		valid      bool
	}{
		{name: "signed narrowing", sourceType: types.T_int64.ToType(), targetType: types.T_int32.ToType(), function: "cast", valid: true},
		{name: "unsigned narrowing", sourceType: types.T_uint64.ToType(), targetType: types.T_uint16.ToType(), function: "cast", valid: true},
		{name: "cross signedness", sourceType: types.T_int64.ToType(), targetType: types.T_uint32.ToType(), function: "cast"},
		{name: "reverse widening", sourceType: types.T_int32.ToType(), targetType: types.T_int64.ToType(), function: "cast"},
		{name: "non integer", sourceType: types.T_varchar.ToType(), targetType: types.T_int32.ToType(), function: "cast"},
		{name: "forged function", sourceType: types.T_int64.ToType(), targetType: types.T_int32.ToType(), function: "plus"},
	} {
		t.Run(test.name, func(t *testing.T) {
			slot, source, ok := TupleComponentSlot(makeCast(
				test.sourceType, test.targetType, test.function))
			require.Equal(t, test.valid, ok)
			if test.valid {
				require.Zero(t, slot)
				require.Equal(t, test.sourceType, source)
			}
		})
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
				ExactKeyEncoding(decoded, test.payloadType))
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

	aggregate := process.MustNewExecutionResourceBudget(1<<20, 1<<20)
	budget, err := aggregate.OpenGeneration(1)
	require.NoError(t, err)
	registry, err := mpool.NewAllocationAccountRegistry(1, 16)
	require.NoError(t, err)
	account, err := registry.OpenWithController(1<<20, budget)
	require.NoError(t, err)
	data, release, err := MarshalExactFilterVector(vec, mp, account, 1, 1)
	require.NoError(t, err)
	require.Len(t, data, 34+vec.Length())
	// The retained charge is the actual caller-owned buffer capacity, not a
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

	aggregate := process.MustNewExecutionResourceBudget(1, 1)
	budget, err := aggregate.OpenGeneration(1)
	require.NoError(t, err)
	registry, err := mpool.NewAllocationAccountRegistry(1, 16)
	require.NoError(t, err)
	account, err := registry.OpenWithController(1<<20, budget)
	require.NoError(t, err)
	data, release, err := MarshalExactFilterVector(vec, mp, account, 1, 1)
	require.ErrorIs(t, err, process.ErrExecutionResourceAdmission)
	require.Nil(t, data)
	require.Nil(t, release)
	require.Zero(t, budget.Used())
}
