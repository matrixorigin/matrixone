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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// TestLowPrecisionFloatComparisonKernels checks the comparison operators on two operands
// of the same bf16, float16, float8 or float4 type, compared by value: -0 equals +0, and
// a NULL row yields NULL except for <=>.
func TestLowPrecisionFloatComparisonKernels(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	// rows: (1, 2), (2, 1), (+0, -0), (1.5, 1.5), (NULL, 1)
	nulls := []bool{false, false, false, false, true}
	inputsFor := func(oid types.T) []FunctionTestInput {
		switch oid {
		case types.T_bf16:
			return []FunctionTestInput{
				NewFunctionTestInput(oid.ToType(), []types.BF16{types.BF16FromFloat32(1), types.BF16FromFloat32(2), 0, types.BF16FromFloat32(1.5), 0}, nulls),
				NewFunctionTestInput(oid.ToType(), []types.BF16{types.BF16FromFloat32(2), types.BF16FromFloat32(1), 0x8000, types.BF16FromFloat32(1.5), types.BF16FromFloat32(1)}, nil),
			}
		case types.T_float16:
			return []FunctionTestInput{
				NewFunctionTestInput(oid.ToType(), []types.Float16{types.Float16FromFloat32(1), types.Float16FromFloat32(2), 0, types.Float16FromFloat32(1.5), 0}, nulls),
				NewFunctionTestInput(oid.ToType(), []types.Float16{types.Float16FromFloat32(2), types.Float16FromFloat32(1), 0x8000, types.Float16FromFloat32(1.5), types.Float16FromFloat32(1)}, nil),
			}
		case types.T_float8:
			return []FunctionTestInput{
				NewFunctionTestInput(oid.ToType(), []types.Float8{types.Float8FromFloat32(1), types.Float8FromFloat32(2), 0, types.Float8FromFloat32(1.5), 0}, nulls),
				NewFunctionTestInput(oid.ToType(), []types.Float8{types.Float8FromFloat32(2), types.Float8FromFloat32(1), 0x80, types.Float8FromFloat32(1.5), types.Float8FromFloat32(1)}, nil),
			}
		default:
			return []FunctionTestInput{
				NewFunctionTestInput(oid.ToType(), []types.Float4{types.Float4FromFloat32(1), types.Float4FromFloat32(2), 0, types.Float4FromFloat32(1.5), 0}, nulls),
				NewFunctionTestInput(oid.ToType(), []types.Float4{types.Float4FromFloat32(2), types.Float4FromFloat32(1), 0x08, types.Float4FromFloat32(1.5), types.Float4FromFloat32(1)}, nil),
			}
		}
	}
	for _, oid := range []types.T{types.T_bf16, types.T_float16, types.T_float8, types.T_float4} {
		for _, test := range []struct {
			name      string
			fn        fEvalFn
			want      []bool
			wantNulls []bool
		}{
			{"equal", equalFn, []bool{false, false, true, true, false}, nulls},
			{"null safe equal", nullSafeEqualFn, []bool{false, false, true, true, false}, nil},
			{"not equal", notEqualFn, []bool{true, true, false, false, false}, nulls},
			{"greater than", greatThanFn, []bool{false, true, false, false, false}, nulls},
			{"greater equal", greatEqualFn, []bool{false, true, true, true, false}, nulls},
			{"less than", lessThanFn, []bool{true, false, false, false, false}, nulls},
			{"less equal", lessEqualFn, []bool{true, false, true, true, false}, nulls},
		} {
			expect := NewFunctionTestResult(types.T_bool.ToType(), false, test.want, test.wantNulls)
			testCase := NewFunctionTestCase(proc, inputsFor(oid), expect, test.fn)
			ok, info := testCase.Run()
			require.True(t, ok, "%s %s: %s", oid, test.name, info)
		}
	}
}

// TestCastNullToLowPrecisionFloat checks CAST(NULL AS bf16/float16/float8/float4).
func TestCastNullToLowPrecisionFloat(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	for _, oid := range []types.T{types.T_bf16, types.T_float16, types.T_float8, types.T_float4} {
		nullVec := vector.NewConstNull(types.T_any.ToType(), 2, proc.Mp())
		target := vector.NewConstNull(oid.ToType(), 2, proc.Mp())
		result := vector.NewFunctionResultWrapper(oid.ToType(), proc.Mp())
		require.NoError(t, result.PreExtendAndReset(2))
		require.NoError(t, NewCast([]*vector.Vector{nullVec, target}, result, proc, 2, nil), oid.String())
		out := result.GetResultVector()
		require.Equal(t, oid, out.GetType().Oid)
		require.True(t, out.IsNull(0) && out.IsNull(1), oid.String())
		result.Free()
		nullVec.Free(proc.Mp())
		target.Free(proc.Mp())
	}
}

// TestLowPrecisionFloatCastsMatchFloat32 checks that casting bf16, float16, float8 and
// float4 values gives the same result as casting the same values from float32, including
// the half-way rounding of an explicit cast to an integer.
func TestLowPrecisionFloatCastsMatchFloat32(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	vals := []float32{0.5, 1.5, -0.5, -1.5, 2}
	build := func(oid types.T) *vector.Vector {
		vec := vector.NewVec(oid.ToType())
		for _, v := range vals {
			var err error
			switch oid {
			case types.T_float32:
				err = vector.AppendFixed(vec, v, false, proc.Mp())
			case types.T_bf16:
				err = vector.AppendFixed(vec, types.BF16FromFloat32(v), false, proc.Mp())
			case types.T_float16:
				err = vector.AppendFixed(vec, types.Float16FromFloat32(v), false, proc.Mp())
			case types.T_float8:
				err = vector.AppendFixed(vec, types.Float8FromFloat32(v), false, proc.Mp())
			case types.T_float4:
				err = vector.AppendFixed(vec, types.Float4FromFloat32(v), false, proc.Mp())
			}
			require.NoError(t, err)
		}
		return vec
	}
	cast := func(from *vector.Vector, to types.Type) ([]string, error) {
		target := vector.NewConstNull(to, len(vals), proc.Mp())
		defer target.Free(proc.Mp())
		result := vector.NewFunctionResultWrapper(to, proc.Mp())
		defer result.Free()
		if err := result.PreExtendAndReset(len(vals)); err != nil {
			return nil, err
		}
		if err := NewCast([]*vector.Vector{from, target}, result, proc, len(vals), nil); err != nil {
			return nil, err
		}
		out := result.GetResultVector()
		strs := make([]string, len(vals))
		for i := range vals {
			if out.GetType().Oid == types.T_varchar {
				strs[i] = out.GetStringAt(i)
			} else {
				strs[i] = out.RowToString(i)
			}
		}
		return strs, nil
	}
	f32 := build(types.T_float32)
	defer f32.Free(proc.Mp())
	for _, to := range []types.Type{
		types.T_int64.ToType(), types.T_int8.ToType(), types.T_uint8.ToType(),
		types.New(types.T_decimal64, 10, 0), types.T_varchar.ToType(), types.T_float64.ToType(),
	} {
		want, wantErr := cast(f32, to)
		for _, oid := range []types.T{types.T_bf16, types.T_float16, types.T_float8, types.T_float4} {
			vec := build(oid)
			got, err := cast(vec, to)
			if wantErr != nil {
				require.Error(t, err, "%s -> %s", oid, to)
			} else {
				require.NoError(t, err, "%s -> %s", oid, to)
				require.Equal(t, want, got, "%s -> %s", oid, to)
			}
			vec.Free(proc.Mp())
		}
	}
}

// TestFormatNumericValueLowPrecisionFloat checks that FORMAT reads bf16, float16, float8
// and float4 values as their float32 value.
func TestFormatNumericValueLowPrecisionFloat(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	for _, tc := range []struct {
		oid types.T
		add func(*vector.Vector) error
	}{
		{types.T_bf16, func(v *vector.Vector) error {
			return vector.AppendFixed(v, types.BF16FromFloat32(1.5), false, proc.Mp())
		}},
		{types.T_float16, func(v *vector.Vector) error {
			return vector.AppendFixed(v, types.Float16FromFloat32(1.5), false, proc.Mp())
		}},
		{types.T_float8, func(v *vector.Vector) error {
			return vector.AppendFixed(v, types.Float8FromFloat32(1.5), false, proc.Mp())
		}},
		{types.T_float4, func(v *vector.Vector) error {
			return vector.AppendFixed(v, types.Float4FromFloat32(1.5), false, proc.Mp())
		}},
	} {
		vec := vector.NewVec(tc.oid.ToType())
		require.NoError(t, tc.add(vec))
		got, exact, isNull, err := formatNumericValueAt(vec, 0)
		require.NoError(t, err, tc.oid.String())
		require.Equal(t, "1.5", got)
		require.False(t, exact)
		require.False(t, isNull)
		vec.Free(proc.Mp())
	}
}

// TestIntegerArgumentLowPrecisionFloat checks that bf16, float16, float8 and float4
// values convert to an integer argument (HEX, CONV, ...) as their float32 value does.
func TestIntegerArgumentLowPrecisionFloat(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	convert := func(src *vector.Vector) []int64 {
		w := vector.NewFunctionResultWrapper(types.T_int64.ToType(), proc.Mp())
		defer w.Free()
		require.NoError(t, w.PreExtendAndReset(src.Length()))
		rs := vector.MustFunctionResult[int64](w)
		require.NoError(t, integerArgumentCast(src, rs, proc, src.Length(), nil, false))
		return append([]int64(nil), vector.MustFixedColNoTypeCheck[int64](w.GetResultVector())...)
	}
	vals := []float32{1.5, -2, 4, 0.5}
	f32 := vector.NewVec(types.T_float32.ToType())
	for _, v := range vals {
		require.NoError(t, vector.AppendFixed(f32, v, false, proc.Mp()))
	}
	want := convert(f32)
	f32.Free(proc.Mp())
	for _, oid := range []types.T{types.T_bf16, types.T_float16, types.T_float8, types.T_float4} {
		vec := vector.NewVec(oid.ToType())
		for _, v := range vals {
			var err error
			switch oid {
			case types.T_bf16:
				err = vector.AppendFixed(vec, types.BF16FromFloat32(v), false, proc.Mp())
			case types.T_float16:
				err = vector.AppendFixed(vec, types.Float16FromFloat32(v), false, proc.Mp())
			case types.T_float8:
				err = vector.AppendFixed(vec, types.Float8FromFloat32(v), false, proc.Mp())
			case types.T_float4:
				err = vector.AppendFixed(vec, types.Float4FromFloat32(v), false, proc.Mp())
			}
			require.NoError(t, err)
		}
		require.Equal(t, want, convert(vec), oid.String())
		vec.Free(proc.Mp())
	}
}

// TestBlockScaledComparisonKernels checks the comparison operators on vecf8 and vecf4
// cells: element-wise by the dequantized values, and a value quantized from the same text
// as a stored cell compares equal to it.
func TestBlockScaledComparisonKernels(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	for _, f := range []types.BlockScaledFormat{types.BlockScaledMXFP8, types.BlockScaledNVFP4} {
		oid := types.T_array_float8
		if f == types.BlockScaledNVFP4 {
			oid = types.T_array_float4
		}
		cell := func(v ...float32) []byte {
			c, err := types.AppendBlockScaled(nil, f, v)
			require.NoError(t, err)
			return c
		}
		// rows: equal (from the same text), less, greater
		// the less/greater rows share a maximum of 4, so elements are exact in both formats
		left := [][]byte{cell(0.3, 1.7, -2.2, 0.01), cell(1, 2, 0, 4), cell(2, 0, 0, 4)}
		right := [][]byte{cell(0.3, 1.7, -2.2, 0.01), cell(1, 2, 1, 4), cell(1, 4, 4, 4)}
		build := func(rows [][]byte) *vector.Vector {
			vec := vector.NewVec(types.New(oid, 4, 0))
			for _, r := range rows {
				require.NoError(t, vector.AppendBytes(vec, r, false, proc.Mp()))
			}
			return vec
		}
		for _, test := range []struct {
			name string
			fn   fEvalFn
			want []bool
		}{
			{"equal", equalFn, []bool{true, false, false}},
			{"null safe equal", nullSafeEqualFn, []bool{true, false, false}},
			{"not equal", notEqualFn, []bool{false, true, true}},
			{"greater than", greatThanFn, []bool{false, false, true}},
			{"greater equal", greatEqualFn, []bool{true, false, true}},
			{"less than", lessThanFn, []bool{false, true, false}},
			{"less equal", lessEqualFn, []bool{true, true, false}},
		} {
			l, r := build(left), build(right)
			w := vector.NewFunctionResultWrapper(types.T_bool.ToType(), proc.Mp())
			require.NoError(t, w.PreExtendAndReset(3))
			require.NoError(t, test.fn([]*vector.Vector{l, r}, w, proc, 3, nil), "%s %s", oid, test.name)
			require.Equal(t, test.want, vector.MustFixedColNoTypeCheck[bool](w.GetResultVector()), "%s %s", oid, test.name)
			w.Free()
			l.Free(proc.Mp())
			r.Free(proc.Mp())
		}
	}
}

// TestNarrowCastHonorsSelectList checks that casting text to bf16/float16/float8/float4 and
// to vecf8/vecf4, and vecf8/vecf4 to vecf32, leaves a row outside the select list NULL
// without converting it, so an invalid value there is not an error.
func TestNarrowCastHonorsSelectList(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	run := func(from *vector.Vector, to types.Type) vector.FunctionResultWrapper {
		target := vector.NewConstNull(to, 2, proc.Mp())
		defer target.Free(proc.Mp())
		result := vector.NewFunctionResultWrapper(to, proc.Mp())
		require.NoError(t, result.PreExtendAndReset(2))
		selectList := &FunctionSelectList{AnyNull: true, SelectList: []bool{true, false}}
		require.NoError(t, NewCast([]*vector.Vector{from, target}, result, proc, 2, selectList), to.String())
		return result
	}
	text := func(values ...string) *vector.Vector {
		v := vector.NewVec(types.T_varchar.ToType())
		for _, s := range values {
			require.NoError(t, vector.AppendBytes(v, []byte(s), false, proc.Mp()))
		}
		return v
	}
	for _, oid := range []types.T{types.T_bf16, types.T_float16, types.T_float8, types.T_float4} {
		from := text("1.5", "invalid")
		result := run(from, oid.ToType())
		out := result.GetResultVector()
		require.False(t, out.IsNull(0), oid.String())
		require.True(t, out.IsNull(1), oid.String())
		f, ok := vector.GetLowPrecisionFloatAt(out, 0)
		require.True(t, ok, oid.String())
		require.Equal(t, float32(1.5), f, oid.String())
		result.Free()
		from.Free(proc.Mp())
	}
	for _, oid := range []types.T{types.T_array_float8, types.T_array_float4} {
		from := text("[1,2]", "[1,2,3]")
		result := run(from, types.New(oid, 2, 0))
		out := result.GetResultVector()
		require.False(t, out.IsNull(0), oid.String())
		require.True(t, out.IsNull(1), oid.String())
		back := run(out, types.New(types.T_array_float32, 2, 0))
		require.Equal(t, []float32{1, 2}, types.BytesToArray[float32](back.GetResultVector().GetBytesAt(0)), oid.String())
		require.True(t, back.GetResultVector().IsNull(1), oid.String())
		back.Free()
		result.Free()
		from.Free(proc.Mp())
	}
}
