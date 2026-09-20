// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package function

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestIntegerArgumentCanonicalBinding(t *testing.T) {
	for _, source := range []types.T{types.T_int8, types.T_int64, types.T_uint64, types.T_bit, types.T_decimal64, types.T_decimal128, types.T_decimal256, types.T_float32, types.T_float64, types.T_varchar, types.T_bool, types.T_any} {
		t.Run(source.String(), func(t *testing.T) {
			inputs := []types.Type{types.T_varchar.ToType(), types.T_varchar.ToType(), source.ToType()}
			original := append([]types.Type(nil), inputs...)
			check := func(result FuncGetResult) {
				fid, id := DecodeOverloadID(result.GetEncodedOverloadID())
				require.Equal(t, int32(SUBSTRING_INDEX), fid)
				require.Equal(t, int32(2), id)
				casts, needed := result.ShouldDoImplicitTypeCast()
				if source != types.T_int64 {
					require.True(t, needed)
					require.Equal(t, types.T_int64, casts[2].Oid)
				}
			}
			result, err := GetFunctionByName(context.Background(), "substring_index", inputs)
			require.NoError(t, err)
			check(result)
			result, ok := GetFunctionByNameWithoutError("substring_index", inputs)
			require.True(t, ok)
			check(result)
			result, err = GetFunctionByNameWithStringDomainCheckModes(context.Background(), "substring_index", inputs, make([]StringDomainCheckMode, 3))
			require.NoError(t, err)
			check(result)
			require.Equal(t, original, inputs)
		})
	}
	inputs := []types.Type{types.T_varchar.ToType(), types.T_varchar.ToType(), types.T_int64.ToType()}
	for _, legacy := range []int32{0, 1} {
		_, err := GetFunctionByNameWithOverload(context.Background(), "substring_index", inputs, legacy)
		require.ErrorContains(t, err, "legacy execution only")
		_, err = GetFunctionById(context.Background(), encodeOverloadID(SUBSTRING_INDEX, legacy))
		require.NoError(t, err)
	}
	// Genuine fractional parameters and unrelated numeric overloads are untouched.
	_, ok := IntegerArgumentTarget("sleep", 0)
	require.False(t, ok)
	_, ok = IntegerArgumentTarget("unknown", 0)
	require.False(t, ok)
	_, ok = IntegerArgumentTarget("substring_index", 0)
	require.False(t, ok)
	target, ok := IntegerArgumentTarget("substring_index", 2)
	require.True(t, ok)
	require.Equal(t, types.T_int64, target)
	_, err := GetFunctionByName(context.Background(), "substring_index", []types.Type{types.T_varchar.ToType()})
	require.Error(t, err)
}

func TestIntegerArgumentAdditionalSignatures(t *testing.T) {
	for _, name := range []string{"period_add", "period_diff", "ceil", "ceiling", "floor", "round", "truncate", "from_days", "week", "yearweek", "timestampadd", "subvector", "last_query_id", "random_bytes", "sha2", "split_part", "regexp_instr", "regexp_replace", "regexp_substr"} {
		id, ok := getFunctionIdByNameWithoutErr(name)
		require.True(t, ok, name)
		fn := allSupportedFunctions[id]
		for index, ov := range fn.Overloads {
			if !fn.bindsOverload(index) {
				_, err := GetFunctionById(context.Background(), encodeOverloadID(int32(id), int32(index)))
				require.NoError(t, err)
				continue
			}
			for _, source := range []types.T{types.T_int64, types.T_float64, types.T_decimal128, types.T_varchar, types.T_year, types.T_any} {
				t.Run(fmt.Sprintf("%s/%d/%s", name, index, source), func(t *testing.T) {
					inputs := make([]types.Type, len(ov.args))
					for i, typ := range ov.args {
						inputs[i] = typ.ToType()
					}
					for _, param := range fn.integerParameters {
						if param.position < len(inputs) {
							inputs[param.position] = source.ToType()
						}
					}
					original := append([]types.Type{}, inputs...)
					check := func(result FuncGetResult) {
						_, selected := DecodeOverloadID(result.GetEncodedOverloadID())
						require.True(t, fn.bindsOverload(int(selected)))
						targets, cast := result.ShouldDoImplicitTypeCast()
						if !cast {
							targets = inputs
						}
						for _, param := range fn.integerParameters {
							if param.position < len(inputs) {
								require.Equal(t, ov.args[param.position], targets[param.position].Oid, "retain physical narrow adapters without changing the logical integer context")
							}
						}
					}
					result, err := GetFunctionByName(context.Background(), name, inputs)
					require.NoError(t, err)
					check(result)
					result, ok := GetFunctionByNameWithoutError(name, inputs)
					require.True(t, ok)
					check(result)
					result, err = GetFunctionByNameWithStringDomainCheckModes(context.Background(), name, inputs, make([]StringDomainCheckMode, len(inputs)))
					require.NoError(t, err)
					check(result)
					require.Equal(t, original, inputs)
				})
			}
		}
	}
}

func TestIntegerArgumentTextBits(t *testing.T) {
	proc := testutil.NewProcess(t)
	target := types.T_uint64.ToType()
	for _, tc := range []struct {
		input   FunctionTestInput
		want    []uint64
		nulls   []bool
		mask    *FunctionSelectList
		wantErr bool
	}{
		{input: NewFunctionTestInput(types.T_text.ToType(), []string{"-9223372036854775808", "9223372036854775808", "18446744073709551615", "65.9tail", "abc"}, nil), want: []uint64{1 << 63, 1 << 63, math.MaxUint64, 65, 0}},
		{input: NewFunctionTestInput(types.T_text.ToType(), []string{"18446744073709551616", "-1", "-9223372036854775809"}, nil), want: []uint64{0, math.MaxUint64, 0}, nulls: []bool{true, false, true}, mask: &FunctionSelectList{AnyNull: true, SelectList: []bool{false, true, false}}},
		{input: NewFunctionTestConstInput(types.T_text.ToType(), []string{"18446744073709551616"}, []bool{true}), want: []uint64{0}, nulls: []bool{true}},
		{input: NewFunctionTestInput(types.T_text.ToType(), []string{"18446744073709551616"}, nil), wantErr: true},
		{input: NewFunctionTestInput(types.T_text.ToType(), []string{"-9223372036854775809"}, nil), wantErr: true},
		{input: NewFunctionTestInput(types.T_int64.ToType(), []int64{-1}, nil), wantErr: true},
	} {
		test := NewFunctionTestCase(proc, []FunctionTestInput{tc.input, NewFunctionTestInput(target, []uint64{}, nil)}, NewFunctionTestResult(target, tc.wantErr, tc.want, tc.nulls), NewTextIntegerBitsCast).WithSelectList(tc.mask)
		ok, info := test.Run()
		require.True(t, ok, info)
	}
}

func TestIntegerArgumentYear(t *testing.T) {
	proc := testutil.NewProcess(t)
	test := NewFunctionTestCase(proc, []FunctionTestInput{
		NewFunctionTestInput(types.T_year.ToType(), []types.MoYear{0, 1901, 2155, 0}, []bool{false, false, false, true}),
		NewFunctionTestInput(types.T_int64.ToType(), []int64{}, nil),
	}, NewFunctionTestResult(types.T_int64.ToType(), false, []int64{0, 1901, 2155, 0}, []bool{false, false, false, true}), NewIntegerArgumentCast)
	ok, info := test.Run()
	require.True(t, ok, info)
}

func TestIntegerArgumentRealEvaluation(t *testing.T) {
	proc := testutil.NewProcess(t)
	for _, tc := range []struct {
		name string
		fn   fEvalFn
		want []int64
	}{
		{"nearest even", NewIntegerArgumentCast, []int64{-2, -2, -2, 0, 0, 2, 2, 2}},
		{"explicit real cast", NewTruncatedIntegerArgumentCast, []int64{-2, -1, -1, 0, 0, 1, 1, 2}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			test := NewFunctionTestCase(proc, []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(), []float64{-2.5, -1.9, -1.5, -0.5, 0.5, 1.5, 1.9, 2.5}, nil),
				NewFunctionTestInput(types.T_int64.ToType(), []int64{}, nil),
			}, NewFunctionTestResult(types.T_int64.ToType(), false, tc.want, nil), tc.fn)
			ok, info := test.Run()
			require.True(t, ok, info)
		})
	}
	for _, v := range []float64{math.NaN(), math.Inf(1), math.Inf(-1), 0x1p63, math.Nextafter(-0x1p63, math.Inf(-1)), 0x1p64} {
		_, err := checkedIntegerArgument[int64](realIntegerArgument(v, false), proc)
		require.Error(t, err)
	}
	v, err := checkedIntegerArgument[int64](realIntegerArgument(-0x1p63, false), proc)
	require.NoError(t, err)
	require.Equal(t, int64(math.MinInt64), v)
	v, err = checkedIntegerArgument[int64](realIntegerArgument(math.Nextafter(0x1p63, 0), false), proc)
	require.NoError(t, err)
	require.Equal(t, int64(9223372036854774784), v)
	_, err = checkedIntegerArgument[uint64](realIntegerArgument(0x1p64, false), proc)
	require.Error(t, err)
	u, err := checkedIntegerArgument[uint64](realIntegerArgument(math.Nextafter(0x1p64, 0), false), proc)
	require.NoError(t, err)
	require.Equal(t, uint64(18446744073709549568), u)
}

func TestIntegerArgumentExactDomains(t *testing.T) {
	proc := testutil.NewProcess(t)
	for _, s := range []string{"-2.5", "-1.5", "1.5", "2.5", "9007199254740993.0"} {
		t.Run(s, func(t *testing.T) {
			expected := map[string]int64{"-2.5": -3, "-1.5": -2, "1.5": 2, "2.5": 3, "9007199254740993.0": 9007199254740993}[s]
			d64, err := types.ParseDecimal64(s, 18, 1)
			require.NoError(t, err)
			d128, err := types.ParseDecimal128(s, 20, 1)
			require.NoError(t, err)
			d256, err := types.ParseDecimal256(s, 40, 1)
			require.NoError(t, err)
			for _, input := range []FunctionTestInput{
				NewFunctionTestInput(types.New(types.T_decimal64, 18, 1), []types.Decimal64{d64}, nil),
				NewFunctionTestInput(types.New(types.T_decimal128, 20, 1), []types.Decimal128{d128}, nil),
				NewFunctionTestInput(types.New(types.T_decimal256, 40, 1), []types.Decimal256{d256}, nil),
			} {
				test := NewFunctionTestCase(proc, []FunctionTestInput{input, NewFunctionTestInput(types.T_int64.ToType(), []int64{}, nil)}, NewFunctionTestResult(types.T_int64.ToType(), false, []int64{expected}, nil), NewIntegerArgumentCast)
				ok, info := test.Run()
				require.True(t, ok, info)
			}
		})
	}
	// Decimal scale reduction is single-rounding, including UINT targets.
	for _, source := range []string{"1.49999999999999999999999999999999999999", "1.50000000000000000000000000000000000000"} {
		d, err := types.ParseDecimal256(source, 76, 38)
		require.NoError(t, err)
		value, err := decimal256IntegerArgument(d, 38)
		require.NoError(t, err)
		u, err := checkedIntegerArgument[uint64](value, proc)
		require.NoError(t, err)
		if source[2] == '4' {
			require.Equal(t, uint64(1), u)
		} else {
			require.Equal(t, uint64(2), u)
		}
	}
	for _, value := range []integerArgumentValue{
		{magnitude: math.MaxUint64}, {magnitude: 1 << 63},
	} {
		_, err := checkedIntegerArgument[int64](value, proc)
		require.Error(t, err)
		u, err := checkedIntegerArgument[uint64](value, proc)
		require.NoError(t, err)
		require.Equal(t, value.magnitude, u)
	}
	_, err := checkedIntegerArgument[uint64](integerArgumentValue{magnitude: 1, negative: true}, proc)
	require.Error(t, err)
}

func TestIntegerArgumentVectorSourcesAndMasks(t *testing.T) {
	proc := testutil.NewProcess(t)
	for _, input := range []FunctionTestInput{
		NewFunctionTestInput(types.T_int8.ToType(), []int8{1, 2}, nil),
		NewFunctionTestInput(types.T_int16.ToType(), []int16{1, 2}, nil),
		NewFunctionTestInput(types.T_int32.ToType(), []int32{1, 2}, nil),
		NewFunctionTestInput(types.T_int64.ToType(), []int64{1, 2}, nil),
		NewFunctionTestInput(types.T_uint8.ToType(), []uint8{1, 2}, nil),
		NewFunctionTestInput(types.T_uint16.ToType(), []uint16{1, 2}, nil),
		NewFunctionTestInput(types.T_uint32.ToType(), []uint32{1, 2}, nil),
		NewFunctionTestInput(types.T_uint64.ToType(), []uint64{1, 2}, nil),
		NewFunctionTestInput(types.T_bit.ToType(), []uint64{1, 2}, nil),
		NewFunctionTestInput(types.T_float32.ToType(), []float32{1, 1.5}, nil),
		NewFunctionTestInput(types.T_varchar.ToType(), []string{"1.9tail", "2.5"}, nil),
	} {
		t.Run(input.typ.Oid.String(), func(t *testing.T) {
			test := NewFunctionTestCase(proc, []FunctionTestInput{input, NewFunctionTestInput(types.T_uint64.ToType(), []uint64{}, nil)}, NewFunctionTestResult(types.T_uint64.ToType(), false, []uint64{1, 2}, nil), NewIntegerArgumentCast)
			ok, info := test.Run()
			require.True(t, ok, info)
		})
	}
	for _, tc := range []struct {
		input FunctionTestInput
		mask  *FunctionSelectList
		want  []int64
		nulls []bool
	}{
		{NewFunctionTestInput(types.T_bool.ToType(), []bool{false, true, false}, []bool{false, false, true}), nil, []int64{0, 1, 0}, []bool{false, false, true}},
		{NewFunctionTestConstInput(types.T_float64.ToType(), []float64{math.Inf(1)}, []bool{true}), nil, []int64{0}, []bool{true}},
		{NewFunctionTestInput(types.T_float64.ToType(), []float64{math.Inf(1), 1.5, math.NaN()}, nil), &FunctionSelectList{AnyNull: true, SelectList: []bool{false, true, false}}, []int64{0, 2, 0}, []bool{true, false, true}},
		{NewFunctionTestInput(types.T_varchar.ToType(), []string{"18446744073709551616", "2.5", "9223372036854775808"}, nil), &FunctionSelectList{AnyNull: true, SelectList: []bool{false, true, false}}, []int64{0, 2, 0}, []bool{true, false, true}},
		{NewFunctionTestInput(types.T_float64.ToType(), []float64{math.Inf(1)}, nil), &FunctionSelectList{AllNull: true}, []int64{0}, []bool{true}},
	} {
		test := NewFunctionTestCase(proc, []FunctionTestInput{tc.input, NewFunctionTestInput(types.T_int64.ToType(), []int64{}, nil)}, NewFunctionTestResult(types.T_int64.ToType(), false, tc.want, tc.nulls), NewIntegerArgumentCast).WithSelectList(tc.mask)
		ok, info := test.Run()
		require.True(t, ok, info)
	}
	large, err := types.ParseDecimal256("10000000000000000000000000000000000000000000000000000000000000000000000", 76, 0)
	require.NoError(t, err)
	test := NewFunctionTestCase(proc, []FunctionTestInput{
		NewFunctionTestInput(types.New(types.T_decimal256, 76, 0), []types.Decimal256{large, {B0_63: 2}}, nil),
		NewFunctionTestInput(types.T_int64.ToType(), []int64{}, nil),
	}, NewFunctionTestResult(types.T_int64.ToType(), false, []int64{0, 2}, []bool{true, false}), NewIntegerArgumentCast).WithSelectList(&FunctionSelectList{AnyNull: true, SelectList: []bool{false, true}})
	ok, info := test.Run()
	require.True(t, ok, info)
	value, err := decimal256IntegerArgument(large, 0)
	require.NoError(t, err)
	require.True(t, value.overflow)
}

func TestIntegerArgumentText(t *testing.T) {
	proc := testutil.NewProcess(t)
	for _, tc := range []struct {
		input   string
		want    int64
		invalid bool
	}{
		{"1.9tail", 1, false}, {"  -2.9", -2, false}, {"3e4", 3, false}, {"+.5", 0, false}, {"", 0, false}, {"bad", 0, false},
		{"9223372036854775807", math.MaxInt64, false}, {"-9223372036854775808", math.MinInt64, false},
		{"9223372036854775808", 0, true}, {"-9223372036854775809", 0, true}, {"18446744073709551616", 0, true},
	} {
		t.Run(tc.input, func(t *testing.T) {
			actual, err := checkedIntegerArgument[int64](textIntegerArgument([]byte(tc.input), false), proc)
			if tc.invalid {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.want, actual)
			}
		})
	}
	for _, binary := range [][]byte{{0xff}, {0, 0, 0, 0, 0, 0, 0, 1}} {
		v, err := checkedIntegerArgument[uint64](textIntegerArgument(binary, true), proc)
		require.NoError(t, err)
		require.Equal(t, uint64(binary[len(binary)-1]), v)
	}
	value := textIntegerArgument(make([]byte, 9), true)
	require.False(t, value.overflow)
	require.Zero(t, value.magnitude)
	require.True(t, textIntegerArgument(append([]byte{1}, make([]byte, 8)...), true).overflow)
}

func TestStringIntegerArgumentConsumers(t *testing.T) {
	for _, tc := range []struct {
		name      string
		positions []int
	}{
		{"left", []int{1}}, {"right", []int{1}},
		{"substring", []int{1, 2}}, {"substr", []int{1, 2}}, {"mid", []int{1, 2}},
		{"lpad", []int{1}}, {"rpad", []int{1}}, {"insert", []int{1, 2}},
		{"locate", []int{2}}, {"repeat", []int{1}}, {"space", []int{0}}, {"elt", []int{0}},
	} {
		name := tc.name
		t.Run(name, func(t *testing.T) {
			id, ok := getFunctionIdByNameWithoutErr(name)
			require.True(t, ok)
			f := allSupportedFunctions[id]
			require.Len(t, f.integerParameters, len(tc.positions))
			for i, position := range tc.positions {
				require.Equal(t, integerParameter{position: position, target: types.T_int64}, f.integerParameters[i])
			}
			index := 0
			if len(f.bindingOverloads) > 0 {
				index = f.bindingOverloads[0]
			}
			inputs := make([]types.Type, len(f.Overloads[index].args))
			for i, arg := range f.Overloads[index].args {
				inputs[i] = arg.ToType()
			}
			if name == "elt" {
				inputs = []types.Type{types.T_int64.ToType(), types.T_varchar.ToType()}
			}
			if name == "locate" {
				inputs = append(inputs, types.T_int64.ToType())
			}
			for _, parameter := range f.integerParameters {
				if parameter.position < len(inputs) {
					inputs[parameter.position] = types.New(types.T_decimal128, 20, 1)
				}
			}
			original := append([]types.Type(nil), inputs...)
			result, err := GetFunctionByName(context.Background(), name, inputs)
			require.NoError(t, err)
			_, executionID := DecodeOverloadID(result.GetEncodedOverloadID())
			require.True(t, f.bindsOverload(int(executionID)))
			casts, needed := result.ShouldDoImplicitTypeCast()
			require.True(t, needed)
			for _, parameter := range f.integerParameters {
				if parameter.position < len(inputs) {
					require.Equal(t, parameter.target, casts[parameter.position].Oid)
				}
			}
			require.Equal(t, original, inputs)
		})
	}
}
