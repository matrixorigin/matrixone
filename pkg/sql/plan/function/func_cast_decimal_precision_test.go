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

package function

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestDecimalCastPrecisionContract(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	decimal256Overflow, err := types.ParseDecimal256("100000000000000000000000000000000000000.00", 41, 2)
	require.NoError(t, err)
	small256, err := types.ParseDecimal256("99.99", 4, 2)
	require.NoError(t, err)
	max64 := types.Decimal64(999999999999999999)
	max128, err := types.ParseDecimal128("9999999999999999.9900", 20, 4)
	require.NoError(t, err)
	max256, err := types.ParseDecimal256("9999999999999999.9900", 20, 4)
	require.NoError(t, err)
	wide128, err := types.ParseDecimal128("999999999999999999999999999999999999.99", 38, 2)
	require.NoError(t, err)
	wide256, err := types.ParseDecimal256("999999999999999999999999999999999999.9900", 40, 4)
	require.NoError(t, err)
	largeScale, err := types.ParseDecimal256("12.34", 66, 30)
	require.NoError(t, err)
	// Exact coefficient cutoffs for reductions across the 19-digit chunk.
	// 1.499999999999999999999 / 1.500000000000000000000 at scale 21.
	below21 := types.Decimal128{B0_63: 0x50ae84a8cdefffff, B64_127: 0x51}
	half21 := types.Decimal128{B0_63: 0x50ae84a8cdf00000, B64_127: 0x51}
	below21Wide := types.Decimal256{B0_63: below21.B0_63, B64_127: below21.B64_127}
	half21Wide := types.Decimal256{B0_63: half21.B0_63, B64_127: half21.B64_127}
	// 0.49999999999999999999999999999999999999 / 0.5 at scale 38.
	below38 := types.Decimal128{B0_63: 0x4c5111fffffffff, B64_127: 0x259da6542d43623d}
	half38 := types.Decimal128{B0_63: 0x4c5112000000000, B64_127: 0x259da6542d43623d}
	for _, tc := range []struct {
		name           string
		source, target types.Type
		values, want   any
		nulls          []bool
		constant       bool
		wantErr        string
	}{
		{name: "narrow_decimal64",
			source:  types.New(types.T_decimal64, 6, 2),
			target:  types.New(types.T_decimal64, 5, 2),
			values:  []types.Decimal64{99999, 100000, types.Decimal64(99999).Minus(), types.Decimal64(100000).Minus()},
			wantErr: "Decimal64(5,2)"},
		{name: "narrow_decimal128",
			source: types.New(types.T_decimal128, 20, 2),
			target: types.New(types.T_decimal128, 19, 2),
			values: []types.Decimal128{
				{B0_63: 9999999999999999999},
				{B0_63: 10000000000000000000},
			},
			wantErr: "Decimal128(19,2)"},
		{name: "narrow_decimal256",
			source:  types.New(types.T_decimal256, 41, 2),
			target:  types.New(types.T_decimal256, 40, 2),
			values:  []types.Decimal256{decimal256Overflow},
			wantErr: "Decimal256(40,2)"},
		{name: "growth_reject_decimal64",
			source:  types.New(types.T_decimal64, 4, 2),
			target:  types.New(types.T_decimal64, 5, 4),
			values:  []types.Decimal64{9999},
			wantErr: "Decimal64(5,4)"},
		{name: "growth_reject_decimal128",
			source:  types.New(types.T_decimal128, 4, 2),
			target:  types.New(types.T_decimal128, 5, 4),
			values:  []types.Decimal128{{B0_63: 9999}},
			wantErr: "Decimal128(5,4)"},
		{name: "growth_reject_decimal256",
			source:  types.New(types.T_decimal256, 4, 2),
			target:  types.New(types.T_decimal256, 5, 4),
			values:  []types.Decimal256{small256},
			wantErr: "Decimal256(5,4)"},
		{name: "growth_reject_decimal64_to_decimal128",
			source:  types.New(types.T_decimal64, 4, 2),
			target:  types.New(types.T_decimal128, 5, 4),
			values:  []types.Decimal64{9999},
			wantErr: "Decimal128(5,4)"},
		{name: "growth_reject_decimal64_to_decimal256",
			source:  types.New(types.T_decimal64, 4, 2),
			target:  types.New(types.T_decimal256, 5, 4),
			values:  []types.Decimal64{9999},
			wantErr: "Decimal256(5,4)"},
		{name: "growth_reject_decimal128_to_decimal256",
			source:  types.New(types.T_decimal128, 4, 2),
			target:  types.New(types.T_decimal256, 5, 4),
			values:  []types.Decimal128{{B0_63: 9999}},
			wantErr: "Decimal256(5,4)"},
		{name: "growth_boundary_NULL",
			source: types.New(types.T_decimal64, 3, 2),
			target: types.New(types.T_decimal64, 5, 4),
			values: []types.Decimal64{999, 0},
			want:   []types.Decimal64{99900, 0},
			nulls:  []bool{false, true}},
		{name: "safe_decimal64",
			source: types.New(types.T_decimal64, 12, 2),
			target: types.New(types.T_decimal64, 14, 4),
			values: []types.Decimal64{1234, types.Decimal64(1234).Minus(), 0, 0},
			want:   []types.Decimal64{123400, types.Decimal64(123400).Minus(), 0, 0},
			nulls:  []bool{false, false, false, true}},
		{name: "safe_decimal128",
			source: types.New(types.T_decimal128, 20, 2),
			target: types.New(types.T_decimal128, 22, 4),
			values: []types.Decimal128{{B0_63: 1234}, (types.Decimal128{B0_63: 1234}).Minus(), {}, {}},
			want:   []types.Decimal128{{B0_63: 123400}, (types.Decimal128{B0_63: 123400}).Minus(), {}, {}},
			nulls:  []bool{false, false, false, true}},
		{name: "safe_decimal256",
			source: types.New(types.T_decimal256, 40, 2),
			target: types.New(types.T_decimal256, 42, 4),
			values: []types.Decimal256{{B0_63: 1234}, (types.Decimal256{B0_63: 1234}).Minus(), {}, {}},
			want:   []types.Decimal256{{B0_63: 123400}, (types.Decimal256{B0_63: 123400}).Minus(), {}, {}},
			nulls:  []bool{false, false, false, true}},
		{name: "cross_64-to-128",
			source: types.New(types.T_decimal64, 18, 2),
			target: types.New(types.T_decimal128, 20, 4),
			values: []types.Decimal64{1234, types.Decimal64(1234).Minus(), max64, 0},
			want:   []types.Decimal128{{B0_63: 123400}, (types.Decimal128{B0_63: 123400}).Minus(), max128, {}},
			nulls:  []bool{false, false, false, true}},
		{name: "cross_64-to-256",
			source: types.New(types.T_decimal64, 18, 2),
			target: types.New(types.T_decimal256, 20, 4),
			values: []types.Decimal64{1234, types.Decimal64(1234).Minus(), max64, 0},
			want: []types.Decimal256{{B0_63: 123400}, (types.Decimal256{B0_63: 123400}).Minus(),
				max256, {}},
			nulls: []bool{false, false, false, true}},
		{name: "cross_128-to-256",
			source: types.New(types.T_decimal128, 38, 2),
			target: types.New(types.T_decimal256, 40, 4),
			values: []types.Decimal128{{B0_63: 1234}, (types.Decimal128{B0_63: 1234}).Minus(), wide128, {}},
			want:   []types.Decimal256{{B0_63: 123400}, (types.Decimal256{B0_63: 123400}).Minus(), wide256, {}},
			nulls:  []bool{false, false, false, true}},
		{name: "cross_64-to-256-large-scale",
			source: types.New(types.T_decimal64, 18, 2),
			target: types.New(types.T_decimal256, 66, 30),
			values: []types.Decimal64{1234, 0, 0, 0},
			want:   []types.Decimal256{largeScale, {}, {}, {}},
			nulls:  []bool{false, false, false, true}},
		{name: "cross_128-to-256-large-scale",
			source: types.New(types.T_decimal128, 38, 2),
			target: types.New(types.T_decimal256, 66, 30),
			values: []types.Decimal128{{B0_63: 1234}, {}, {}, {}},
			want:   []types.Decimal256{largeScale, {}, {}, {}},
			nulls:  []bool{false, false, false, true}},
		{name: "reduce_decimal128",
			source: types.New(types.T_decimal128, 38, 38),
			target: types.New(types.T_decimal128, 38, 36),
			values: []types.Decimal128{{B0_63: 49}, (types.Decimal128{B0_63: 49}).Minus(), {B0_63: 50}, (types.Decimal128{B0_63: 50}).Minus(), (types.Decimal128{B0_63: 149}), (types.Decimal128{B0_63: 150}), (types.Decimal128{B0_63: 149}).Minus(), (types.Decimal128{B0_63: 150}).Minus(), {}},
			want:   []types.Decimal128{{}, {}, {B0_63: 1}, (types.Decimal128{B0_63: 1}).Minus(), {B0_63: 1}, {B0_63: 2}, {B0_63: ^uint64(0), B64_127: ^uint64(0)}, {B0_63: ^uint64(1), B64_127: ^uint64(0)}, {}},
			nulls:  []bool{false, false, false, false, false, false, false, false, true}},
		{name: "reduce_decimal128 long scale reduction",
			source: types.New(types.T_decimal128, 38, 38),
			target: types.New(types.T_decimal128, 5, 0),
			values: []types.Decimal128{{B0_63: 49}, (types.Decimal128{B0_63: 49}).Minus(), below38, half38, below38.Minus(), half38.Minus(), {}},
			want:   []types.Decimal128{{}, {}, {}, {B0_63: 1}, {}, {B0_63: ^uint64(0), B64_127: ^uint64(0)}, {}},
			nulls:  []bool{false, false, false, false, false, false, true}},
		{name: "reduce_decimal128 to decimal64",
			source: types.New(types.T_decimal128, 38, 38),
			target: types.New(types.T_decimal64, 18, 17),
			values: []types.Decimal128{{B0_63: 49}, (types.Decimal128{B0_63: 49}).Minus(), below21, half21, below21.Minus(), half21.Minus(), {}},
			want:   []types.Decimal64{0, 0, 1, 2, types.Decimal64(^uint64(0)), types.Decimal64(^uint64(1)), 0},
			nulls:  []bool{false, false, false, false, false, false, true}},
		{name: "reduce_decimal128 to decimal256",
			source: types.New(types.T_decimal128, 38, 38),
			target: types.New(types.T_decimal256, 40, 36),
			values: []types.Decimal128{{B0_63: 49}, (types.Decimal128{B0_63: 49}).Minus(), (types.Decimal128{B0_63: 149}), (types.Decimal128{B0_63: 150}), (types.Decimal128{B0_63: 149}).Minus(), (types.Decimal128{B0_63: 150}).Minus(), {}},
			want:   []types.Decimal256{{}, {}, {B0_63: 1}, {B0_63: 2}, {B0_63: ^uint64(0), B64_127: ^uint64(0), B128_191: ^uint64(0), B192_255: ^uint64(0)}, {B0_63: ^uint64(1), B64_127: ^uint64(0), B128_191: ^uint64(0), B192_255: ^uint64(0)}, {}},
			nulls:  []bool{false, false, false, false, false, false, true}},
		{name: "reduce_decimal256",
			source: types.New(types.T_decimal256, 38, 38),
			target: types.New(types.T_decimal256, 38, 36),
			values: []types.Decimal256{{B0_63: 49}, (types.Decimal256{B0_63: 49}).Minus(), {B0_63: 50}, (types.Decimal256{B0_63: 50}).Minus(), (types.Decimal256{B0_63: 149}), (types.Decimal256{B0_63: 150}), (types.Decimal256{B0_63: 149}).Minus(), (types.Decimal256{B0_63: 150}).Minus(), {}},
			want:   []types.Decimal256{{}, {}, {B0_63: 1}, (types.Decimal256{B0_63: 1}).Minus(), {B0_63: 1}, {B0_63: 2}, {B0_63: ^uint64(0), B64_127: ^uint64(0), B128_191: ^uint64(0), B192_255: ^uint64(0)}, {B0_63: ^uint64(1), B64_127: ^uint64(0), B128_191: ^uint64(0), B192_255: ^uint64(0)}, {}},
			nulls:  []bool{false, false, false, false, false, false, false, false, true}},
		{name: "reduce_decimal256 to decimal128",
			source: types.New(types.T_decimal256, 38, 38),
			target: types.New(types.T_decimal128, 38, 36),
			values: []types.Decimal256{{B0_63: 49}, (types.Decimal256{B0_63: 49}).Minus(), (types.Decimal256{B0_63: 149}), (types.Decimal256{B0_63: 150}), (types.Decimal256{B0_63: 149}).Minus(), (types.Decimal256{B0_63: 150}).Minus(), {}},
			want:   []types.Decimal128{{}, {}, {B0_63: 1}, {B0_63: 2}, {B0_63: ^uint64(0), B64_127: ^uint64(0)}, {B0_63: ^uint64(1), B64_127: ^uint64(0)}, {}},
			nulls:  []bool{false, false, false, false, false, false, true}},
		{name: "reduce_decimal256 to decimal64",
			source: types.New(types.T_decimal256, 38, 38),
			target: types.New(types.T_decimal64, 18, 17),
			values: []types.Decimal256{{B0_63: 49}, (types.Decimal256{B0_63: 49}).Minus(), below21Wide, half21Wide, below21Wide.Minus(), half21Wide.Minus(), {}},
			want:   []types.Decimal64{0, 0, 1, 2, types.Decimal64(^uint64(0)), types.Decimal64(^uint64(1)), 0},
			nulls:  []bool{false, false, false, false, false, false, true}},
		{name: "constant64_to128_positive",
			source:   types.New(types.T_decimal64, 18, 2),
			target:   types.New(types.T_decimal128, 20, 4),
			values:   []types.Decimal64{1234},
			want:     []types.Decimal128{{B0_63: 123400}},
			constant: true},
		{name: "constant64_to128_negative",
			source:   types.New(types.T_decimal64, 18, 2),
			target:   types.New(types.T_decimal128, 20, 4),
			values:   []types.Decimal64{types.Decimal64(1234).Minus()},
			want:     []types.Decimal128{{B0_63: ^uint64(123399), B64_127: ^uint64(0)}},
			constant: true},
		{name: "round_carry_neighbor",
			source: types.New(types.T_decimal256, 4, 3),
			target: types.New(types.T_decimal256, 3, 2),
			values: []types.Decimal256{{B0_63: 9994}},
			want:   []types.Decimal256{{B0_63: 999}}},
		{name: "round_positive_carry_reject",
			source:  types.New(types.T_decimal128, 4, 3),
			target:  types.New(types.T_decimal64, 3, 2),
			values:  []types.Decimal128{{B0_63: 9995}},
			wantErr: "Decimal64(3,0)"},
		{name: "round_negative_carry_reject",
			source:  types.New(types.T_decimal256, 4, 3),
			target:  types.New(types.T_decimal128, 3, 2),
			values:  []types.Decimal256{(types.Decimal256{B0_63: 9995}).Minus()},
			wantErr: "Decimal128(3,0)"},
		{name: "narrow_negative64",
			source:  types.New(types.T_decimal64, 6, 2),
			target:  types.New(types.T_decimal64, 5, 2),
			values:  []types.Decimal64{types.Decimal64(100000).Minus()},
			wantErr: "Decimal64(5,2)"},
		{name: "narrow_negative128",
			source:  types.New(types.T_decimal128, 20, 2),
			target:  types.New(types.T_decimal128, 19, 2),
			values:  []types.Decimal128{(types.Decimal128{B0_63: 10000000000000000000}).Minus()},
			wantErr: "Decimal128(19,2)"},
		{name: "narrow_negative256",
			source:  types.New(types.T_decimal256, 41, 2),
			target:  types.New(types.T_decimal256, 40, 2),
			values:  []types.Decimal256{decimal256Overflow.Minus()},
			wantErr: "Decimal256(40,2)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := NewFunctionTestInput(tc.source, tc.values, tc.nulls)
			input.isConst = tc.constant
			fc := NewFunctionTestCase(proc, []FunctionTestInput{input, NewFunctionTestInput(tc.target, emptyCastTargetValues(tc.target), nil)}, NewFunctionTestResult(tc.target, tc.wantErr != "", tc.want, tc.nulls), NewCast)
			defer fc.Free()
			if tc.wantErr != "" {
				_, err := fc.DebugRun()
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput), "error %v", err)
				require.ErrorContains(t, err, tc.wantErr)
			} else {
				ok, info := fc.Run()
				require.True(t, ok, info)
			}
			require.Equal(t, tc.source, *fc.parameters[0].GetType())
			require.Equal(t, tc.target, *fc.GetResultVectorDirectly().GetType())
		})
	}
}

func TestDecimalCastScaleGrowthAdmission(t *testing.T) {
	for _, tc := range []struct {
		from, to types.Type
		want     bool
	}{
		{types.New(types.T_decimal64, 12, 2), types.New(types.T_decimal64, 14, 4), true},
		{types.New(types.T_decimal64, 12, 2), types.New(types.T_decimal64, 13, 4), false},
		{types.New(types.T_decimal128, 38, 36), types.New(types.T_decimal128, 38, 38), false},
		{types.New(types.T_decimal128, 20, 4), types.New(types.T_decimal128, 22, 2), false},
		{types.New(types.T_decimal256, 76, 0), types.New(types.T_decimal256, 76, 1), false},
		{types.New(types.T_decimal64, 0, 2), types.New(types.T_decimal64, 14, 4), false},
		{types.New(types.T_decimal64, 12, 2), types.New(types.T_decimal64, 19, 4), false},
		{types.New(types.T_decimal64, 18, 2), types.New(types.T_decimal128, 20, 4), true},
		{types.New(types.T_decimal64, 18, 2), types.New(types.T_decimal256, 20, 4), true},
		{types.New(types.T_decimal128, 38, 2), types.New(types.T_decimal256, 40, 4), true},
		{types.New(types.T_decimal64, 18, 2), types.New(types.T_decimal128, 19, 4), false},
		{types.New(types.T_decimal128, 38, 2), types.New(types.T_decimal256, 39, 4), false},
		{types.New(types.T_decimal128, 38, 2), types.New(types.T_decimal64, 18, 4), false},
	} {
		require.Equal(t, tc.want, canWidenDecimalScale(tc.from, tc.to), "%v -> %v", tc.from, tc.to)
	}
}

func BenchmarkDecimalSafeScaleGrowth(b *testing.B) {
	proc := testutil.NewProcess(b)
	defer proc.Free()
	values64 := make([]types.Decimal64, 256)
	values128 := make([]types.Decimal128, 256)
	values256 := make([]types.Decimal256, 256)
	wanted64 := make([]types.Decimal64, 256)
	wanted128 := make([]types.Decimal128, 256)
	wanted256 := make([]types.Decimal256, 256)
	for i := range values64 {
		values64[i] = 1234
		values128[i] = types.Decimal128{B0_63: 1234}
		values256[i] = types.Decimal256{B0_63: 1234}
		// 12.34 represented at scale four has the literal coefficient 123400.
		wanted64[i] = 123400
		wanted128[i] = types.Decimal128{B0_63: 123400}
		wanted256[i] = types.Decimal256{B0_63: 123400}
	}
	for _, tc := range []struct {
		name   string
		source types.Type
		target types.Type
		values any
		wanted any
	}{
		{"decimal64", types.New(types.T_decimal64, 12, 2), types.New(types.T_decimal64, 14, 4), values64, wanted64},
		{"decimal128", types.New(types.T_decimal128, 20, 2), types.New(types.T_decimal128, 22, 4), values128, wanted128},
		{"decimal256", types.New(types.T_decimal256, 40, 2), types.New(types.T_decimal256, 42, 4), values256, wanted256},
		{"64-to-128", types.New(types.T_decimal64, 12, 2), types.New(types.T_decimal128, 14, 4), values64, wanted128},
		{"64-to-256", types.New(types.T_decimal64, 12, 2), types.New(types.T_decimal256, 14, 4), values64, wanted256},
		{"128-to-256", types.New(types.T_decimal128, 20, 2), types.New(types.T_decimal256, 22, 4), values128, wanted256},
	} {
		b.Run(tc.name, func(b *testing.B) {
			fc := NewFunctionTestCase(proc, []FunctionTestInput{
				NewFunctionTestInput(tc.source, tc.values, nil),
				NewFunctionTestInput(tc.target, emptyCastTargetValues(tc.target), nil),
			}, NewFunctionTestResult(tc.target, false, tc.wanted, nil), NewCast)
			defer fc.Free()
			fc.Benchmark(b)
			require.Equal(b, tc.source, *fc.parameters[0].GetType())
			require.Equal(b, tc.target, *fc.GetResultVectorDirectly().GetType())
		})
	}
}

func TestDecimal128WideningCastDoesNotRetypeSource(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	sourceType := types.New(types.T_decimal128, 19, 2)
	targetType := types.New(types.T_decimal128, 20, 2)
	value := types.Decimal128{B0_63: 12345}

	testCase := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(sourceType, []types.Decimal128{value, {}}, []bool{false, true}),
			NewFunctionTestInput(targetType, []types.Decimal128{}, nil),
		},
		NewFunctionTestResult(targetType, false, []types.Decimal128{value, {}}, []bool{false, true}), NewCast)
	defer testCase.Free()
	result, err := testCase.DebugRun()
	require.NoError(t, err)
	require.Equal(t, sourceType, *testCase.parameters[0].GetType())
	require.Equal(t, targetType, *result.GetType())
	require.Equal(t, []types.Decimal128{value, {}}, vector.MustFixedColWithTypeCheck[types.Decimal128](result))
}
