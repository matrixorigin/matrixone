// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package function

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
	"math"
	"testing"
)

func TestDecimalIntegerDivResolution(t *testing.T) {
	for _, tc := range []struct {
		name        string
		left, right types.Type
		oid         types.T
		width       int32
		overload    int32
	}{
		{"narrow signed", types.New(types.T_decimal64, 10, 3), types.T_int32.ToType(), types.T_int64, 8, 0},
		{"wide signed", types.New(types.T_decimal64, 18, 3), types.T_int32.ToType(), types.T_int64, 16, 0},
		{"unsigned divisor", types.New(types.T_decimal64, 10, 3), types.T_uint64.ToType(), types.T_uint64, 7, 1},
		{"unsigned dividend", types.T_uint64.ToType(), types.New(types.T_decimal128, 20, 3), types.T_uint64, 19, 1},
		{"divisor scale", types.New(types.T_decimal64, 10, 3), types.New(types.T_decimal128, 20, 2), types.T_int64, 10, 0},
		{"decimal256", types.New(types.T_decimal256, 65, 30), types.T_uint32.ToType(), types.T_uint64, 19, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args := []types.Type{tc.left, tc.right}
			got, err := GetFunctionByName(context.Background(), "div", args)
			require.NoError(t, err)
			require.Equal(t, types.New(tc.oid, tc.width, 0), got.GetReturnType())
			_, id := DecodeOverloadID(got.GetEncodedOverloadID())
			require.Equal(t, tc.overload, id)
			speculative, ok := GetFunctionByNameWithoutError("div", args)
			require.True(t, ok)
			require.Equal(t, got.GetReturnType(), speculative.GetReturnType())
			_, _, numeric, ok := ResolveNumericBinaryTypes("div", tc.left, tc.right, nil)
			require.True(t, ok)
			require.Equal(t, got.GetReturnType(), numeric)
			if tc.overload == 1 {
				targets, _ := got.ShouldDoImplicitTypeCast()
				require.Equal(t, types.T_decimal256, targets[0].Oid)
				require.Equal(t, max32(tc.left.Scale, 0), targets[0].Scale)
			}
		})
	}
	explicit, err := GetFunctionByNameWithOverload(context.Background(), "div", []types.Type{types.New(types.T_decimal64, 10, 3), types.New(types.T_decimal64, 10, 3)}, DecimalIntegerDivUnsignedOverload)
	require.NoError(t, err)
	operands, cast := explicit.ShouldDoImplicitTypeCast()
	require.True(t, cast)
	require.Equal(t, types.T_decimal256, operands[0].Oid)
	require.Equal(t, types.T_uint64, explicit.GetReturnType().Oid)
	_, err = GetFunctionByNameWithOverload(context.Background(), "div", []types.Type{types.T_int64.ToType(), types.T_int64.ToType()}, DecimalIntegerDivUnsignedOverload)
	require.Error(t, err)
	legacy, err := GetFunctionByNameWithOverload(context.Background(), "div", []types.Type{types.New(types.T_decimal64, 10, 3), types.T_uint64.ToType()}, 0)
	require.NoError(t, err)
	require.Equal(t, types.T_int64, legacy.GetReturnType().Oid)
}
func TestDecimalIntegerDivUnsignedBoundaries(t *testing.T) {
	parse := func(s string) types.Decimal256 {
		x, _, err := types.Parse256(s)
		require.NoError(t, err)
		return x
	}
	for _, tc := range []struct {
		name, left, right string
		s0, s1            int32
		want              uint64
		bad               bool
	}{
		{"positive", "5678", "2", 3, 0, 2, false},
		{"negative", "-5678", "2", 3, 0, 0, true},
		{"negative zero", "-1", "2", 0, 0, 0, false},
		{"negative divisor zero", "1", "-2", 0, 0, 0, false},
		{"negative divisor", "5", "-2", 0, 0, 0, true},
		{"signed max", "9223372036854775807", "1", 0, 0, math.MaxInt64, false},
		{"above signed max", "9223372036854775808", "1", 0, 0, 1 << 63, false},
		{"unsigned max", "18446744073709551615", "1", 0, 0, math.MaxUint64, false},
		{"overflow", "18446744073709551616", "1", 0, 0, 0, true},
		{"scale down", "123456789012345678901234567890123456789012345678901234567890", "100000000000000000000000000000000000000000000000000000000000000", 30, 0, 0, false},
		{"scale overflow fallback", "1000000000000000000000000000000000000000000000000000000000000", "10000000000000000000000000000000000000000000000000000000000000000000000000", 0, 20, 10000000, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out := make([]uint64, 1)
			err := decimalIntegerDivUnsigned([]types.Decimal256{parse(tc.left)}, []types.Decimal256{parse(tc.right)}, out, tc.s0, tc.s1, nulls.NewWithSize(1), true)
			if tc.bad {
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrOutOfRange), err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.want, out[0])
			}
		})
	}
}
func TestDecimalIntegerDivUnsignedVectorAdmission(t *testing.T) {
	proc := testutil.NewProcess(t)
	typ := types.New(types.T_decimal256, 20, 0)
	for _, tc := range []struct {
		name        string
		left, right []types.Decimal256
		ns          []bool
		want        []uint64
		wantNull    []bool
	}{
		{"vector const", []types.Decimal256{{B0_63: 5}, {B0_63: 9}}, []types.Decimal256{{B0_63: 2}}, []bool{false, false}, []uint64{2, 4}, []bool{false, false}},
		{"const vector", []types.Decimal256{{B0_63: 5}}, []types.Decimal256{{B0_63: 2}, {B0_63: 3}}, []bool{false}, []uint64{2, 1}, []bool{false, false}},
		{"null zero", []types.Decimal256{{B0_63: 5}, types.Decimal256{}.Minus()}, []types.Decimal256{{B0_63: 2}, {}}, []bool{false, true}, []uint64{2, 0}, []bool{false, true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rightNull := make([]bool, len(tc.right))
			left := NewFunctionTestInput(typ, tc.left, tc.ns)
			right := NewFunctionTestInput(typ, tc.right, rightNull)
			left.isConst = len(tc.left) == 1
			right.isConst = len(tc.right) == 1
			fc := NewFunctionTestCase(proc, []FunctionTestInput{left, right}, NewFunctionTestResult(types.T_uint64.ToType(), false, tc.want, tc.wantNull), decimalIntegerDivUnsignedFn)
			ok, info := fc.RunAndFree()
			require.True(t, ok, info)
		})
	}
	// The existing wrapper must mask a bad negative row before the kernel runs.
	fc := NewFunctionTestCase(proc, []FunctionTestInput{NewFunctionTestInput(typ, []types.Decimal256{{B0_63: 5}, {B0_63: ^uint64(4), B64_127: ^uint64(0), B128_191: ^uint64(0), B192_255: ^uint64(0)}}, []bool{false, false}), NewFunctionTestInput(typ, []types.Decimal256{{B0_63: 2}, {B0_63: 2}}, []bool{false, false})}, NewFunctionTestResult(types.T_uint64.ToType(), false, []uint64{2, 0}, []bool{false, true}), decimalIntegerDivUnsignedFn)
	fc.selectList = &FunctionSelectList{AnyNull: true, SelectList: []bool{true, false}}
	ok, info := fc.RunAndFree()
	require.True(t, ok, info)
}

// Exercise the new overload's diagnostic boundary, including failed kernels.
func TestDecimalIntegerDivUnsignedWarnings(t *testing.T) {
	for _, tc := range []struct {
		name                                      string
		disabled, dml, allMasked, empty, negative bool
		code                                      uint16
		want                                      uint64
	}{
		{name: "enabled", want: 1},
		{name: "disabled", disabled: true},
		{name: "strict DML", dml: true, code: moerr.ErrDivByZero},
		{name: "all masked", allMasked: true},
		{name: "empty", empty: true},
		{name: "negative result", negative: true, code: moerr.ErrOutOfRange},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			defer proc.Free()
			proc.SetStmtProfile(&process.StmtProfile{})
			proc.GetStmtProfile().SetStatementRuntimeProfile("Select", "DQL", false)
			if tc.dml {
				proc.GetStmtProfile().SetStatementRuntimeProfile("Insert", "DML", false)
			}
			mode := "TRADITIONAL"
			if tc.disabled {
				mode = ""
			}
			proc.GetSessionInfo().SqlMode = mode
			proc.SetResolveVariableFunc(func(string, bool, bool) (any, error) { return mode, nil })
			sink := &arithmeticWarningSink{limit: 1}
			proc.WarningSink = sink
			typ := types.New(types.T_decimal256, 20, 0)
			left := []types.Decimal256{{B0_63: 5}, {B0_63: 5}, {B0_63: 5}, {B0_63: 5}, {B0_63: 5}}
			right := []types.Decimal256{{}, {}, {}, {B0_63: 2}, {}}
			if tc.negative {
				left[0] = left[0].Minus()
				right[0] = types.Decimal256{B0_63: 2}
			}
			test := NewFunctionTestCase(proc, []FunctionTestInput{
				NewFunctionTestInput(typ, left, []bool{false, true, false, false, false}),
				NewFunctionTestInput(typ, right, []bool{false, false, true, false, false}),
			}, NewFunctionTestResult(types.T_uint64.ToType(), false, nil, nil), decimalIntegerDivUnsignedFn)
			defer test.Free()
			length := 5
			if tc.empty {
				length = 0
			}
			selection := &FunctionSelectList{AnyNull: true, AllNull: tc.allMasked, SelectList: []bool{true, true, true, true, false}}
			require.NoError(t, test.result.PreExtendAndReset(length))
			err := test.fn(test.parameters, test.result, proc, length, selection)
			if tc.code != 0 {
				require.True(t, moerr.IsMoErrCode(err, tc.code), err)
			} else {
				require.NoError(t, err)
				if !tc.allMasked && !tc.empty {
					for _, row := range []uint64{0, 1, 2, 4} {
						require.True(t, test.GetResultVectorDirectly().IsNull(row))
					}
					require.Equal(t, uint64(2), vector.GetFixedAtNoTypeCheck[uint64](test.GetResultVectorDirectly(), 3))
				}
			}
			require.Equal(t, tc.want, sink.total)
			require.Len(t, sink.records, int(tc.want))
			if tc.want > 0 {
				require.Equal(t, uncompressWarningRecord{code: moerr.ER_DIVISION_BY_ZERO, message: "Division by 0"}, sink.records[0])
			}
		})
	}
}

func BenchmarkDecimalIntegerDivUnsigned(b *testing.B) {
	const n = 256
	left, right := make([]types.Decimal256, n), []types.Decimal256{{B0_63: 2}}
	for i := range left {
		left[i] = types.Decimal256{B0_63: 5678}
	}
	out := make([]uint64, n)
	ns := nulls.NewWithSize(n)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := decimalIntegerDivUnsigned(left, right, out, 3, 0, ns, false); err != nil {
			b.Fatal(err)
		}
	}
}
