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
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestIntegerAssignmentContracts(t *testing.T) {
	proc := newMemoryFunctionTestProcess(t)
	t.Cleanup(func() {
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	})
	t.Run("decimal unsigned rounding", func(t *testing.T) {
		t.Cleanup(func() { require.Zero(t, proc.Mp().CurrNB()) })
		for _, oid := range []types.T{types.T_decimal64, types.T_decimal128, types.T_decimal256} {
			t.Run(oid.String(), func(t *testing.T) {
				typ := oid.ToType()
				typ.Scale = 1
				var values any
				switch oid {
				case types.T_decimal64:
					values = []types.Decimal64{25, 35, 0}
				case types.T_decimal128:
					values = []types.Decimal128{{B0_63: 25}, {B0_63: 35}, {}}
				case types.T_decimal256:
					values = []types.Decimal256{{B0_63: 25}, {B0_63: 35}, {}}
				}
				for _, tc := range []struct {
					target         types.T
					want, ordinary any
				}{
					{types.T_uint8, []uint8{3, 4, 0}, []uint8{2, 3, 0}},
					{types.T_uint16, []uint16{3, 4, 0}, []uint16{2, 3, 0}},
					{types.T_uint32, []uint32{3, 4, 0}, []uint32{2, 3, 0}},
					{types.T_uint64, []uint64{3, 4, 0}, []uint64{2, 3, 0}},
				} {

					inputs := []FunctionTestInput{
						NewFunctionTestInput(typ, values, []bool{false, false, true}),
						NewFunctionTestInput(tc.target.ToType(), tc.want, nil),
					}
					expected := NewFunctionTestResult(tc.target.ToType(), false, tc.want, []bool{false, false, true})
					testCase := NewFunctionTestCase(proc, inputs, expected, NewAssignCast)
					ok, info := testCase.RunAndFree()
					require.True(t, ok, "source=%s target=%s op=assignment: %s", oid, tc.target, info)
					testCase = NewFunctionTestCase(proc, inputs, expected, NewAssignIgnoreCast)
					ok, info = testCase.RunAndFree()
					require.True(t, ok, "source=%s target=%s op=ignore assignment: %s", oid, tc.target, info)
					expected = NewFunctionTestResult(tc.target.ToType(), false, tc.ordinary, []bool{false, false, true})
					testCase = NewFunctionTestCase(proc, inputs, expected, NewCast)
					ok, info = testCase.RunAndFree()
					require.True(t, ok, "source=%s target=%s op=ordinary: %s", oid, tc.target, info)

				}
			})
		}
	})
	t.Run("float integer rounding", func(t *testing.T) {
		t.Cleanup(func() { require.Zero(t, proc.Mp().CurrNB()) })
		for _, tc := range []struct {
			target types.T
			want   any
		}{
			{types.T_int8, []int8{2, 4, 0}}, {types.T_int16, []int16{2, 4, 0}},
			{types.T_int32, []int32{2, 4, 0}}, {types.T_int64, []int64{2, 4, 0}},
			{types.T_uint8, []uint8{2, 4, 0}}, {types.T_uint16, []uint16{2, 4, 0}},
			{types.T_uint32, []uint32{2, 4, 0}}, {types.T_uint64, []uint64{2, 4, 0}},
		} {

			for _, src := range []FunctionTestInput{
				NewFunctionTestInput(types.T_float32.ToType(), []float32{2.5, 3.5, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_float64.ToType(), []float64{2.5, 3.5, 0}, []bool{false, false, true}),
			} {
				inputs := []FunctionTestInput{src, NewFunctionTestInput(tc.target.ToType(), tc.want, nil)}
				expected := NewFunctionTestResult(tc.target.ToType(), false, tc.want, []bool{false, false, true})
				testCase := NewFunctionTestCase(proc, inputs, expected, NewAssignCast)
				ok, info := testCase.RunAndFree()
				require.True(t, ok, "source=%s target=%s op=assignment: %s", src.typ.Oid, tc.target, info)
				testCase = NewFunctionTestCase(proc, inputs, expected, NewAssignIgnoreCast)
				ok, info = testCase.RunAndFree()
				require.True(t, ok, "source=%s target=%s op=ignore assignment: %s", src.typ.Oid, tc.target, info)
			}

		}
	})
	t.Run("bounds and reuse", func(t *testing.T) {
		t.Cleanup(func() { require.Zero(t, proc.Mp().CurrNB()) })
		for _, tc := range []struct {
			name   string
			target types.T
			input  float64
			want   any
			fails  bool
		}{
			{"negative_even", types.T_int64, -2.5, []int64{-2}, false},
			{"negative_odd", types.T_int64, -3.5, []int64{-4}, false},
			{"signed_min_tie", types.T_int8, -128.5, []int8{-128}, false},
			{"signed_max_tie", types.T_int8, 127.5, []int8{0}, true},
			{"unsigned_negative", types.T_uint64, -1, []uint64{0}, true},
			{"signed_2pow63", types.T_int64, math.Ldexp(1, 63), []int64{0}, true},
			{"signed_min", types.T_int64, -math.Ldexp(1, 63), []int64{math.MinInt64}, false},
			{"unsigned_2pow64", types.T_uint64, math.Ldexp(1, 64), []uint64{0}, true},
			{"unsigned_predecessor", types.T_uint64, math.Nextafter(math.Ldexp(1, 64), 0), []uint64{math.MaxUint64 - 2047}, false},
			{"nan", types.T_int64, math.NaN(), []int64{0}, true},
			{"infinity", types.T_int64, math.Inf(1), []int64{0}, true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				inputs := []FunctionTestInput{
					NewFunctionTestInput(types.T_float64.ToType(), []float64{tc.input}, nil),
					NewFunctionTestInput(tc.target.ToType(), tc.want, nil),
				}
				expected := NewFunctionTestResult(tc.target.ToType(), tc.fails, tc.want, nil)
				testCase := NewFunctionTestCase(proc, inputs, expected, NewAssignCast)
				if tc.fails {
					defer testCase.Free()
					_, err := testCase.DebugRun()
					require.True(t, moerr.IsMoErrCode(err, moerr.ErrOutOfRange), "expected OutOfRange, got %v", err)
				} else {
					ok, info := testCase.RunAndFree()
					require.True(t, ok, info)
				}
			})
		}
		t.Run("batch transitions", func(t *testing.T) {
			for _, tc := range []struct {
				name     string
				source   FunctionTestInput
				setValid func(*vector.Vector) error
			}{
				{"float32", NewFunctionTestInput(types.T_float32.ToType(), []float32{7, 255.5, 9}, nil), func(v *vector.Vector) error { return vector.SetFixedAtWithTypeCheck(v, 1, float32(8)) }},
				{"float64", NewFunctionTestInput(types.T_float64.ToType(), []float64{7, 255.5, 9}, nil), func(v *vector.Vector) error { return vector.SetFixedAtWithTypeCheck(v, 1, float64(8)) }},
				{"decimal64", NewFunctionTestInput(types.T_decimal64.ToTypeWithScale(1), []types.Decimal64{70, 2555, 90}, nil), func(v *vector.Vector) error { return vector.SetFixedAtWithTypeCheck(v, 1, types.Decimal64(80)) }},
				{"decimal128", NewFunctionTestInput(types.T_decimal128.ToTypeWithScale(1), []types.Decimal128{{B0_63: 70}, {B0_63: 2555}, {B0_63: 90}}, nil), func(v *vector.Vector) error { return vector.SetFixedAtWithTypeCheck(v, 1, types.Decimal128{B0_63: 80}) }},
				{"decimal256", NewFunctionTestInput(types.T_decimal256.ToTypeWithScale(1), []types.Decimal256{{B0_63: 70}, {B0_63: 2555}, {B0_63: 90}}, nil), func(v *vector.Vector) error { return vector.SetFixedAtWithTypeCheck(v, 1, types.Decimal256{B0_63: 80}) }},
			} {
				t.Run(tc.name, func(t *testing.T) {
					baseline := proc.Mp().CurrNB()
					fc := NewFunctionTestCase(proc, []FunctionTestInput{tc.source,
						NewFunctionTestInput(types.T_uint8.ToType(), []uint8{}, nil),
					}, NewFunctionTestResult(types.T_uint8.ToType(), false, []uint8{7, 0, 9}, []bool{false, true, false}), NewAssignCast)
					t.Cleanup(func() { fc.Free(); require.Equal(t, baseline, proc.Mp().CurrNB()) })
					steps := []string{"overflow", "masked", "NULL", "valid"}
					// AllNull admission and uint8 result reset have the same owner for every source.
					if tc.source.typ.Oid == types.T_float64 {
						steps = []string{"overflow", "masked", "NULL", "all masked", "overflow again", "valid"}
					}
					for _, step := range steps {
						fc.selectList = nil
						fc.parameters[0].GetNulls().Reset()
						fc.expected.nullList = []bool{false, true, false}
						switch step {
						case "masked":
							fc.selectList = &FunctionSelectList{AnyNull: true, SelectList: []bool{true, false, true}}
						case "NULL":
							fc.parameters[0].GetNulls().Add(1)
						case "valid":
							require.NoError(t, tc.setValid(fc.parameters[0]), "step=%s", step)
							fc.expected.wanted = []uint8{7, 8, 9}
							fc.expected.nullList = nil
						case "all masked":
							fc.selectList = &FunctionSelectList{AllNull: true}
							fc.expected.nullList = []bool{true, true, true}
						}
						if step == "overflow" || step == "overflow again" {
							v, err := fc.DebugRun()
							require.True(t, moerr.IsMoErrCode(err, moerr.ErrOutOfRange), "step=%s: expected OutOfRange, got %v", step, err)
							require.Equal(t, types.T_uint8.ToType(), *v.GetType(), "step=%s", step)
							value, isNull := vector.GenerateFunctionFixedTypeParameter[uint8](v).GetValue(0)
							require.Equal(t, uint8(7), value, "step=%s", step)
							require.False(t, isNull, "step=%s", step)
						} else {
							ok, info := fc.Run()
							require.True(t, ok, "step=%s: %s", step, info)
						}

					}
				})
			}
		})
	})
}

func TestMediumIntAssignmentBounds(t *testing.T) {
	newProc := func(sqlMode string) *process.Process {
		proc := newMemoryFunctionTestProcess(t)
		proc.SetResolveVariableFunc(func(name string, _, _ bool) (interface{}, error) {
			if name == "sql_mode" {
				return sqlMode, nil
			}
			return nil, moerr.NewInternalError(proc.Ctx, "unexpected variable "+name)
		})
		return proc
	}

	for _, tc := range []struct {
		name   string
		source types.Type
		values any
		target types.Type
		want   any
		fail   bool
	}{
		{
			name:   "signed inclusive boundaries",
			source: types.T_int32.ToType(), values: []int32{-1 << 23, (1 << 23) - 1},
			target: types.New(types.T_int32, 24, -1), want: []int32{-1 << 23, (1 << 23) - 1},
		},
		{
			name:   "signed upper overflow",
			source: types.T_int32.ToType(), values: []int32{1 << 23},
			target: types.New(types.T_int32, 24, -1), want: []int32{0}, fail: true,
		},
		{
			name:   "signed lower overflow",
			source: types.T_int32.ToType(), values: []int32{-(1 << 23) - 1},
			target: types.New(types.T_int32, 24, -1), want: []int32{0}, fail: true,
		},
		{
			name:   "unsigned inclusive boundaries",
			source: types.T_uint32.ToType(), values: []uint32{0, (1 << 24) - 1},
			target: types.New(types.T_uint32, 24, -1), want: []uint32{0, (1 << 24) - 1},
		},
		{
			name:   "unsigned upper overflow",
			source: types.T_uint32.ToType(), values: []uint32{1 << 24},
			target: types.New(types.T_uint32, 24, -1), want: []uint32{0}, fail: true,
		},
		{
			name:   "same medium type still checks legacy value",
			source: types.New(types.T_int32, 24, -1), values: []int32{1 << 23},
			target: types.New(types.T_int32, 24, -1), want: []int32{0}, fail: true,
		},
		{
			name:   "widening legacy medium value to int preserves stored value",
			source: types.New(types.T_int32, 24, -1), values: []int32{1 << 23},
			target: types.T_int32.ToType(), want: []int32{1 << 23},
		},
		{
			name:   "rounded float overflow",
			source: types.T_float64.ToType(), values: []float64{8388607.6},
			target: types.New(types.T_int32, 24, -1), want: []int32{0}, fail: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := newProc("STRICT_TRANS_TABLES")
			t.Cleanup(func() {
				proc.Free()
				require.Zero(t, proc.Mp().CurrNB())
			})
			inputs := []FunctionTestInput{
				NewFunctionTestInput(tc.source, tc.values, nil),
				NewFunctionTestInput(tc.target, tc.want, nil),
			}
			testCase := NewFunctionTestCase(proc, inputs,
				NewFunctionTestResult(tc.target, tc.fail, tc.want, nil), NewAssignCast)
			ok, info := testCase.RunAndFree()
			require.True(t, ok, info)
		})
	}

	t.Run("non-strict clips representable overflow", func(t *testing.T) {
		proc := newProc("")
		t.Cleanup(func() {
			proc.Free()
			require.Zero(t, proc.Mp().CurrNB())
		})
		for _, tc := range []struct {
			source types.Type
			values any
			target types.Type
			want   any
		}{
			{types.T_int32.ToType(), []int32{1 << 23}, types.New(types.T_int32, 24, -1), []int32{(1 << 23) - 1}},
			{types.T_int32.ToType(), []int32{-(1 << 23) - 1}, types.New(types.T_int32, 24, -1), []int32{-1 << 23}},
			{types.T_uint32.ToType(), []uint32{1 << 24}, types.New(types.T_uint32, 24, -1), []uint32{(1 << 24) - 1}},
		} {
			inputs := []FunctionTestInput{
				NewFunctionTestInput(tc.source, tc.values, nil),
				NewFunctionTestInput(tc.target, tc.want, nil),
			}
			testCase := NewFunctionTestCase(proc, inputs,
				NewFunctionTestResult(tc.target, false, tc.want, nil), NewAssignCast)
			ok, info := testCase.RunAndFree()
			require.True(t, ok, info)
		}
	})
}
