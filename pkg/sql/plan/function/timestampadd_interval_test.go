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
	"fmt"
	"math"
	"math/big"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestTimestampAddSecondExactTruncation(t *testing.T) {
	for _, tc := range []struct {
		text string
		want int64
	}{
		{"1.4", 1400000}, {"1.5", 1500000}, {"1.6", 1600000}, {"-1.4", -1400000}, {"-1.5", -1500000}, {"-1.6", -1600000},
		{"0.000001", 1}, {"-0.000001", -1}, {"0.0000009", 0}, {"-0.0000009", 0}, {"1.9999999", 1999999}, {"-1.9999999", -1999999},
		{"00001.000001000000000000000000000000000001", 1000001}, {" .5", 500000}, {"-.5", -500000}, {" +1.5tail", 1500000},
		{"1.5e-3", 1500}, {"1e999999999999999999", math.MinInt64}, {"1e-999999999999999999", 0}, {"0e999999999999999999", 0},
		{"9223372036854.7758079", math.MaxInt64}, {"9223372036854.775808", math.MinInt64}, {"-9223372036854.7758079", -math.MaxInt64},
		{"9223372036854775807000000000000000000000", math.MinInt64}, {"nonnumeric", 0},
	} {
		t.Run(tc.text, func(t *testing.T) { require.Equal(t, tc.want, timestampAddSecondMicroseconds(tc.text)) })
	}
}

func TestTimestampAddSecondSourceVectors(t *testing.T) {
	proc := testutil.NewProcess(t)
	d64, err := types.ParseDecimal64("-1.9999999", 18, 7)
	require.NoError(t, err)
	d128, err := types.ParseDecimal128("-1.9999999", 30, 7)
	require.NoError(t, err)
	d256, err := types.ParseDecimal256("-1.999999900000000000000000000001", 60, 30)
	require.NoError(t, err)
	for _, input := range []FunctionTestInput{
		NewFunctionTestInput(types.New(types.T_decimal64, 18, 7), []types.Decimal64{d64, 0}, []bool{false, true}),
		NewFunctionTestInput(types.New(types.T_decimal128, 30, 7), []types.Decimal128{d128, {}}, []bool{false, true}),
		NewFunctionTestInput(types.New(types.T_decimal256, 60, 30), []types.Decimal256{d256, {}}, []bool{false, true}),
		NewFunctionTestInput(types.T_float32.ToType(), []float32{-1.9999999, 0}, []bool{false, true}),
		NewFunctionTestInput(types.T_float64.ToType(), []float64{-1.9999999, 0}, []bool{false, true}),
		NewFunctionTestInput(types.T_varchar.ToType(), []string{"-1.9999999", ""}, []bool{false, true}),
	} {
		tc := NewFunctionTestCase(proc, []FunctionTestInput{input}, NewFunctionTestResult(types.T_int64.ToType(), false, []int64{-1999999, 0}, []bool{false, true}), TimestampAddSecondInterval)
		ok, info := tc.RunAndFree()
		require.True(t, ok, info)
	}
	tc := NewFunctionTestCase(proc, []FunctionTestInput{NewFunctionTestInput(types.T_float64.ToType(), []float64{1.000001, math.Inf(1), math.NaN(), math.MaxFloat64, 1e30}, nil)}, NewFunctionTestResult(types.T_int64.ToType(), false, []int64{1000001, 0, 0, math.MinInt64, 0}, []bool{false, true, true, false, true}), TimestampAddSecondInterval).WithSelectList(&FunctionSelectList{AnyNull: true, SelectList: []bool{true, true, true, true, false}})
	ok, info := tc.RunAndFree()
	require.True(t, ok, info)
}

func TestTimestampAddSecondPromotedFloat(t *testing.T) {
	proc := testutil.NewProcess(t)
	tc := NewFunctionTestCase(proc, []FunctionTestInput{NewFunctionTestInput(types.T_float32.ToType(), []float32{1677721.625, -1677721.625, 1.000001, -1.000001}, nil)}, NewFunctionTestResult(types.T_int64.ToType(), false, []int64{1677721625000, -1677721625000, 1000000, -1000000}, nil), TimestampAddSecondInterval)
	ok, info := tc.RunAndFree()
	require.True(t, ok, info)
}

func TestTimestampAddSecondDecimalCoefficients(t *testing.T) {
	proc := testutil.NewProcess(t)
	// Full signed coefficients cover narrowing, sign extension and truncation of
	// minimum magnitudes, including values reduced into the int64 output range.
	for _, oid := range []types.T{types.T_decimal64, types.T_decimal128, types.T_decimal256} {
		scales := []int32{0, 1, 6, 7, 18}
		if oid == types.T_decimal128 {
			scales = append(scales, 25, 38)
		}
		if oid == types.T_decimal256 {
			scales = append(scales, 25, 38, 63, 76)
		}
		for _, scale := range scales {
			t.Run(fmt.Sprint(oid, "/", scale), func(t *testing.T) {
				texts := []string{"0", "19999999", "-19999999", "9223372036854775807", "-9223372036854775808"}
				var values any
				switch oid {
				case types.T_decimal64:
					values = []types.Decimal64{0, 19999999, types.Decimal64(19999999).Minus(), types.Decimal64(math.MaxInt64), types.Decimal64(uint64(1) << 63)}
				case types.T_decimal128:
					vals := []types.Decimal128{{}, types.Decimal128FromInt64(19999999), types.Decimal128FromInt64(-19999999), types.Decimal128FromInt64(math.MaxInt64), types.Decimal128FromInt64(math.MinInt64), {B0_63: uint64(1) << 63}, types.Decimal128Min, types.Decimal128Max}
					texts = append(texts, "9223372036854775808", "-170141183460469231731687303715884105728", "170141183460469231731687303715884105727")
					values = vals
				case types.T_decimal256:
					vals := []types.Decimal256{{}, types.Decimal256FromInt64(19999999), types.Decimal256FromInt64(-19999999), types.Decimal256FromInt64(math.MaxInt64), types.Decimal256FromInt64(math.MinInt64), {B0_63: uint64(1) << 63}, {B192_255: uint64(1) << 63}, {B0_63: math.MaxUint64, B64_127: math.MaxUint64, B128_191: math.MaxUint64, B192_255: math.MaxInt64}}
					texts = append(texts, "9223372036854775808", "-57896044618658097711785492504343953926634992332820282019728792003956564819968", "57896044618658097711785492504343953926634992332820282019728792003956564819967")
					values = vals
				}
				wanted := make([]int64, len(texts))
				for i, text := range texts {
					coefficient, ok := new(big.Int).SetString(text, 10)
					require.True(t, ok)
					power := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(max(scale-6, 6-scale))), nil)
					if scale > 6 {
						coefficient.Quo(coefficient, power)
					} else {
						coefficient.Mul(coefficient, power)
					}
					if coefficient.IsInt64() {
						wanted[i] = coefficient.Int64()
					} else {
						wanted[i] = math.MinInt64
					}
				}
				width := int32(18)
				if oid == types.T_decimal128 {
					width = 38
				}
				if oid == types.T_decimal256 {
					width = 76
				}
				tc := NewFunctionTestCase(proc, []FunctionTestInput{NewFunctionTestInput(types.New(oid, width, scale), values, nil)}, NewFunctionTestResult(types.T_int64.ToType(), false, wanted, nil), TimestampAddSecondInterval)
				ok, info := tc.RunAndFree()
				require.True(t, ok, info)
			})
		}
	}
}

// Compare the replaced lexical conversion and the current typed path on the
// same vectors. This measures normalization, independently of SQL scheduling.
func BenchmarkTimestampAddSecondDecimal(b *testing.B) {
	proc := newMemoryFunctionTestProcess(b)
	for _, oid := range []types.T{types.T_decimal64, types.T_decimal128, types.T_decimal256} {
		const rows = 1024
		wanted := make([]int64, rows)
		var values any
		switch oid {
		case types.T_decimal64:
			v := make([]types.Decimal64, rows)
			for i := range v {
				v[i] = 15
				wanted[i] = 1500000
			}
			values = v
		case types.T_decimal128:
			v := make([]types.Decimal128, rows)
			for i := range v {
				v[i] = types.Decimal128FromInt64(15)
				wanted[i] = 1500000
			}
			values = v
		case types.T_decimal256:
			v := make([]types.Decimal256, rows)
			for i := range v {
				v[i] = types.Decimal256FromInt64(15)
				wanted[i] = 1500000
			}
			values = v
		}
		for _, lexical := range []bool{true, false} {
			b.Run(fmt.Sprint(oid, "/lexical=", lexical), func(b *testing.B) {
				fn := TimestampAddSecondInterval
				if lexical {
					fn = timestampAddSecondLexicalBenchmark
				}
				tc := NewFunctionTestCase(proc, []FunctionTestInput{NewFunctionTestInput(types.New(oid, 18, 1), values, nil)}, NewFunctionTestResult(types.T_int64.ToType(), false, wanted, nil), fn)
				defer tc.Free()
				tc.Benchmark(b)
			})
		}
	}
}

func timestampAddSecondLexicalBenchmark(vecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int, selected *FunctionSelectList) error {
	switch vecs[0].GetType().Oid {
	case types.T_decimal64:
		return timestampAddTypedSeconds(vecs[0], result, length, selected, func(v types.Decimal64, s int32) (int64, bool) {
			return timestampAddSecondMicroseconds(v.Format(s)), false
		})
	case types.T_decimal128:
		return timestampAddTypedSeconds(vecs[0], result, length, selected, func(v types.Decimal128, s int32) (int64, bool) {
			return timestampAddSecondMicroseconds(v.Format(s)), false
		})
	default:
		return timestampAddTypedSeconds(vecs[0], result, length, selected, func(v types.Decimal256, s int32) (int64, bool) {
			return timestampAddSecondMicroseconds(v.Format(s)), false
		})
	}
}
