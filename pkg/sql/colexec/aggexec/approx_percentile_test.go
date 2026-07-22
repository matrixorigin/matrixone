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

package aggexec

import (
	"bytes"
	"math"
	"math/big"
	"strconv"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

// --- percentile computation algorithm tests ---

func TestPercentileNumericVals_Basic(t *testing.T) {
	// N=10 values 1..10
	vals := []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

	// p=0.0 returns first element
	require.Equal(t, float64(1), percentileNumericVals(vals, 0.0))
	// p=0.5 returns 5.5 (median)
	require.Equal(t, 5.5, percentileNumericVals(vals, 0.5))
	// p=0.95 returns 9.55
	require.InDelta(t, 9.55, percentileNumericVals(vals, 0.95), 1e-10)
	// p=0.99 returns 9.91
	require.InDelta(t, 9.91, percentileNumericVals(vals, 0.99), 1e-10)
	// p=1.0 returns last element
	require.Equal(t, float64(10), percentileNumericVals(vals, 1.0))
}

func TestPercentileNumericVals_EvenN(t *testing.T) {
	vals := []float64{1.0, 2.0, 4.0, 5.0}

	// p=0.0 -> 1.0 (min)
	require.Equal(t, float64(1), percentileNumericVals(vals, 0.0))
	// p=0.5 -> 3.0 (interpolation: lo=1, hi=2, idx=1.5, vLo=2, vHi=4)
	require.InDelta(t, 3.0, percentileNumericVals(vals, 0.5), 1e-10)
	// p=0.25 -> idx=0.75, lo=0(1.0), hi=1(2.0) => 1.0 + 0.75*(2.0-1.0) = 1.75
	require.InDelta(t, 1.75, percentileNumericVals(vals, 0.25), 1e-10)
	// p=0.75 -> idx=2.25, lo=2(4.0), hi=3(5.0) => 4.0 + 0.25*(5.0-4.0) = 4.25
	require.InDelta(t, 4.25, percentileNumericVals(vals, 0.75), 1e-10)
	// p=1.0 -> 5.0 (max)
	require.Equal(t, float64(5), percentileNumericVals(vals, 1.0))
}

func TestPercentileNumericVals_SingleValue(t *testing.T) {
	vals := []int64{42}

	require.Equal(t, float64(42), percentileNumericVals(vals, 0.0))
	require.Equal(t, float64(42), percentileNumericVals(vals, 0.5))
	require.Equal(t, float64(42), percentileNumericVals(vals, 1.0))
}

func TestPercentileNumericVals_TwoValues(t *testing.T) {
	vals := []int64{10, 20}

	require.Equal(t, float64(10), percentileNumericVals(vals, 0.0))
	require.Equal(t, float64(15), percentileNumericVals(vals, 0.5))
	require.Equal(t, float64(20), percentileNumericVals(vals, 1.0))
}

func TestPercentileNumericVals_EdgeCases(t *testing.T) {
	tests := []struct {
		name string
		vals []int64
		p    float64
		want float64
	}{
		// Empty - returns NaN
		{name: "empty_p05", vals: nil, p: 0.5, want: math.NaN()},
		{name: "empty_p00", vals: []int64{}, p: 0.0, want: math.NaN()},
		// p < 0 - returns NaN
		{name: "negative_p", vals: []int64{1, 2, 3}, p: -0.1, want: math.NaN()},
		// p > 1 - returns NaN
		{name: "above_one_p", vals: []int64{1, 2, 3}, p: 1.1, want: math.NaN()},
		// N=3, p=0.0 -> min
		{name: "three_min", vals: []int64{3, 1, 2}, p: 0.0, want: 1},
		// N=3, p=0.5 -> median
		{name: "three_median", vals: []int64{3, 1, 2}, p: 0.5, want: 2},
		// N=3, p=1.0 -> max
		{name: "three_max", vals: []int64{3, 1, 2}, p: 1.0, want: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := percentileNumericVals(tt.vals, tt.p)
			if math.IsNaN(tt.want) {
				require.True(t, math.IsNaN(result))
			} else {
				require.Equal(t, tt.want, result)
			}
		})
	}
}

func TestPercentileNumericVals_Int64Overflow(t *testing.T) {
	vals := []int64{math.MaxInt64, math.MaxInt64}
	require.Equal(t, float64(math.MaxInt64), percentileNumericVals(vals, 0.5))
}

func TestPercentileNumericVals_Int64Precision(t *testing.T) {
	require.Equal(t, -0.5, percentileNumericVals([]int64{-9007199254740993, 9007199254740992}, 0.5))
	require.Equal(t, -0.5, percentileNumericVals([]int64{math.MinInt64, math.MaxInt64}, 0.5))
	require.Equal(t, float64(9007199254740994), percentileNumericVals([]uint64{9007199254740993, 9007199254740994}, 0.5))
}

func TestPercentileNumericVals_Float64ExtremeInterpolation(t *testing.T) {
	vals := []float64{-math.MaxFloat64, math.MaxFloat64}
	require.Equal(t, 0.0, percentileNumericVals(vals, 0.5))
	require.Equal(t, -math.MaxFloat64, percentileNumericVals(vals, 0))
	require.Equal(t, math.MaxFloat64, percentileNumericVals(vals, 1))
}

func TestInterpolateFloat64(t *testing.T) {
	tests := []struct {
		name         string
		lo, hi, frac float64
		want         float64
	}{
		{name: "opposite extreme midpoint", lo: -math.MaxFloat64, hi: math.MaxFloat64, frac: 0.5, want: 0},
		{name: "opposite extreme quarter", lo: -math.MaxFloat64, hi: math.MaxFloat64, frac: 0.25, want: -math.MaxFloat64 / 2},
		{name: "positive same sign", lo: math.MaxFloat64 / 2, hi: math.MaxFloat64, frac: 0.5, want: math.MaxFloat64 * 0.75},
		{name: "negative same sign", lo: -math.MaxFloat64, hi: -math.MaxFloat64 / 2, frac: 0.5, want: -math.MaxFloat64 * 0.75},
		{name: "subnormal midpoint", lo: -math.SmallestNonzeroFloat64, hi: math.SmallestNonzeroFloat64, frac: 0.5, want: 0},
		{name: "left endpoint", lo: -math.MaxFloat64, hi: math.MaxFloat64, frac: 0, want: -math.MaxFloat64},
		{name: "right endpoint", lo: -math.MaxFloat64, hi: math.MaxFloat64, frac: 1, want: math.MaxFloat64},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := interpolateFloat64(test.lo, test.hi, test.frac)
			if test.want == 0 || test.frac == 0 || test.frac == 1 {
				require.Equal(t, test.want, got)
			} else {
				require.InEpsilon(t, test.want, got, 1e-15)
			}
		})
	}

	// Keep the prior IEEE-754 behavior for non-finite data values.
	require.True(t, math.IsNaN(interpolateFloat64(math.NaN(), 1, 0.5)))
	require.True(t, math.IsNaN(interpolateFloat64(math.Inf(-1), math.Inf(1), 0.5)))
	require.True(t, math.IsNaN(interpolateFloat64(math.Inf(1), math.Inf(1), 0)))
}

func TestPercentileDecimal64Vals(t *testing.T) {
	vals := mustDecimal64s(t, "1.00", "2.00", "3.00", "4.00", "5.00", "6.00", "7.00", "8.00", "9.00", "10.00")

	tests := []struct {
		name string
		p    float64
		want string
	}{
		{name: "p00", p: 0.0, want: "1.000"},
		{name: "p50", p: 0.5, want: "5.500"},
		{name: "p95", p: 0.95, want: "9.550"},
		{name: "p100", p: 1.0, want: "10.000"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d128, err := percentileDecimal64Vals(vals, tt.p, 2)
			require.NoError(t, err)
			require.Equal(t, tt.want, d128.Format(3))
		})
	}
}

func TestPercentileDecimal64Vals_EdgeCases(t *testing.T) {
	// Empty
	d128, err := percentileDecimal64Vals(nil, 0.5, 2)
	require.NoError(t, err)
	require.Equal(t, types.Decimal128{}, d128)

	d128, err = percentileDecimal64Vals(nil, 1.5, 2)
	require.NoError(t, err)
	require.Equal(t, types.Decimal128{}, d128)

	// Single value
	vals := mustDecimal64s(t, "42.00")
	d128, err = percentileDecimal64Vals(vals, 0.5, 2)
	require.NoError(t, err)
	require.Equal(t, "42.000", d128.Format(3))

	// Two values
	vals2 := mustDecimal64s(t, "10.00", "20.00")
	d128, err = percentileDecimal64Vals(vals2, 0.5, 2)
	require.NoError(t, err)
	require.Equal(t, "15.000", d128.Format(3))

	// Negative values
	valsNeg := mustDecimal64s(t, "-5.00", "5.00", "-3.00", "3.00")
	d128, err = percentileDecimal64Vals(valsNeg, 0.5, 2)
	require.NoError(t, err)
	require.Equal(t, "0.000", d128.Format(3))
}

func TestPercentileDecimal128Vals(t *testing.T) {
	vals := mustDecimal128s(t, "1.00", "2.00", "3.00", "4.00", "5.00", "6.00", "7.00", "8.00", "9.00", "10.00")

	tests := []struct {
		name string
		p    float64
		want string
	}{
		{name: "p00", p: 0.0, want: "1.000"},
		{name: "p50", p: 0.5, want: "5.500"},
		{name: "p95", p: 0.95, want: "9.550"},
		{name: "p100", p: 1.0, want: "10.000"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d128, err := percentileDecimal128Vals(vals, tt.p, 20, 2)
			require.NoError(t, err)
			require.Equal(t, tt.want, d128.Format(3))
		})
	}
}

func TestPercentileDecimal128Vals_EdgeCases(t *testing.T) {
	// Empty
	d128, err := percentileDecimal128Vals(nil, 0.5, 20, 2)
	require.NoError(t, err)
	require.Equal(t, types.Decimal128{}, d128)

	// Single value
	vals := mustDecimal128s(t, "99.99")
	d128, err = percentileDecimal128Vals(vals, 0.5, 20, 2)
	require.NoError(t, err)
	require.Equal(t, "99.990", d128.Format(3))

	// Two values
	vals2 := mustDecimal128s(t, "10.00", "20.00")
	d128, err = percentileDecimal128Vals(vals2, 0.75, 20, 2)
	require.NoError(t, err)
	require.Equal(t, "17.500", d128.Format(3))
}

func TestPercentileDecimal128MaxPrecisionEndpoints(t *testing.T) {
	mp := mpool.MustNewZero()
	values := NewVectors[types.Decimal128](types.New(types.T_decimal128, 38, 0))
	t.Cleanup(func() {
		values.Free(mp)
		require.Equal(t, int64(0), mp.CurrNB())
	})

	maxValue, err := types.ParseDecimal128("99999999999999999999999999999999999999", 38, 0)
	require.NoError(t, err)
	minValue, err := types.ParseDecimal128("-99999999999999999999999999999999999999", 38, 0)
	require.NoError(t, err)
	require.NoError(t, vector.AppendFixedList(values.vecs[0], []types.Decimal128{minValue, maxValue}, nil, mp))

	got, err := PercentileDecimal128(values, 0, 0)
	require.NoError(t, err)
	require.Equal(t, "-99999999999999999999999999999999999999", got.Format(0))
	got, err = PercentileDecimal128(values, 1, 0)
	require.NoError(t, err)
	require.Equal(t, "99999999999999999999999999999999999999", got.Format(0))
}

func TestPercentileDecimal128ScaleBoundaries(t *testing.T) {
	tests := []struct {
		name     string
		typ      types.Type
		values   []string
		p        float64
		want     string
		outScale int32
	}{
		{
			name:     "width 37 adds one result scale digit",
			typ:      types.New(types.T_decimal128, 37, 2),
			values:   []string{"1.00", "2.00"},
			p:        0.5,
			want:     "1.500",
			outScale: 3,
		},
		{
			name:     "width 38 retains maximum scale",
			typ:      types.New(types.T_decimal128, 38, 38),
			values:   []string{"0.1", "0.2"},
			p:        0.5,
			want:     "0.15000000000000000000000000000000000000",
			outScale: 38,
		},
		{
			name:     "width 38 retained scale rounds midpoint",
			typ:      types.New(types.T_decimal128, 38, 0),
			values:   []string{"-1", "0"},
			p:        0.5,
			want:     "-1",
			outScale: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			values := NewVectors[types.Decimal128](tt.typ)
			t.Cleanup(func() {
				values.Free(mp)
				require.Equal(t, int64(0), mp.CurrNB())
			})

			parsed := make([]types.Decimal128, len(tt.values))
			for i, value := range tt.values {
				var err error
				parsed[i], err = types.ParseDecimal128(value, tt.typ.Width, tt.typ.Scale)
				require.NoError(t, err)
			}
			require.NoError(t, vector.AppendFixedList(values.vecs[0], parsed, nil, mp))

			got, err := PercentileDecimal128(values, tt.p, tt.typ.Scale)
			require.NoError(t, err)
			require.Equal(t, tt.want, got.Format(tt.outScale))
		})
	}
}

// --- Executor tests ---

func formatFloatConfig(p float64) string {
	return strconv.FormatFloat(p, 'f', -1, 64)
}

func TestApproxPercentileReturnType(t *testing.T) {
	require.Equal(t, types.New(types.T_decimal128, 38, 3),
		ApproxPercentileReturnType([]types.Type{types.New(types.T_decimal128, 37, 2)}))
	require.Equal(t, types.New(types.T_decimal128, 38, 0),
		ApproxPercentileReturnType([]types.Type{types.New(types.T_decimal128, 38, 0)}))
	require.Equal(t, types.New(types.T_decimal128, 38, 38),
		ApproxPercentileReturnType([]types.Type{types.New(types.T_decimal128, 38, 38)}))
	require.Equal(t, types.T_float64.ToType(),
		ApproxPercentileReturnType([]types.Type{types.T_int64.ToType()}))
}

func TestApproxPercentileExecAcrossSupportedTypes(t *testing.T) {
	mp := mpool.MustNewZero()

	cases := []struct {
		name   string
		typ    types.Type
		values any
		p      float64
		want   any
	}{
		{name: "bit", typ: types.T_bit.ToType(), values: []uint64{1, 3, 2}, p: 0.5, want: 2.0},
		{name: "int8", typ: types.T_int8.ToType(), values: []int8{1, 3, 2}, p: 0.5, want: 2.0},
		{name: "int16", typ: types.T_int16.ToType(), values: []int16{1, 3, 2}, p: 0.5, want: 2.0},
		{name: "int32", typ: types.T_int32.ToType(), values: []int32{1, 3, 2}, p: 0.5, want: 2.0},
		{name: "int64", typ: types.T_int64.ToType(), values: []int64{1, 3, 2}, p: 0.5, want: 2.0},
		{name: "int64_large_opposite", typ: types.T_int64.ToType(), values: []int64{-9007199254740993, 9007199254740992}, p: 0.5, want: -0.5},
		{name: "uint8", typ: types.T_uint8.ToType(), values: []uint8{1, 3, 2}, p: 0.5, want: 2.0},
		{name: "uint16", typ: types.T_uint16.ToType(), values: []uint16{1, 3, 2}, p: 0.5, want: 2.0},
		{name: "uint32", typ: types.T_uint32.ToType(), values: []uint32{1, 3, 2}, p: 0.5, want: 2.0},
		{name: "uint64", typ: types.T_uint64.ToType(), values: []uint64{1, 3, 2}, p: 0.5, want: 2.0},
		{name: "float32", typ: types.T_float32.ToType(), values: []float32{1, 3, 2}, p: 0.5, want: 2.0},
		{name: "float64", typ: types.T_float64.ToType(), values: []float64{1, 3, 2}, p: 0.5, want: 2.0},
		{name: "decimal64", typ: types.New(types.T_decimal64, 10, 2), values: mustDecimal64s(t, "1.00", "2.00", "3.00"), p: 0.5, want: "2.000"},
		{name: "decimal128", typ: types.New(types.T_decimal128, 20, 2), values: mustDecimal128s(t, "1.00", "2.00", "3.00"), p: 0.5, want: "2.000"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			exec, err := makeApproxPercentile(mp, 1, false, tc.typ)
			require.NoError(t, err)
			require.NoError(t, exec.GroupGrow(1))

			vec := medianTestVector(t, mp, tc.typ, tc.values)
			require.NoError(t, exec.BulkFill(0, []*vector.Vector{vec}))

			// Set percentile via SetExtraInformation
			require.NoError(t, exec.SetExtraInformation([]byte(formatFloatConfig(tc.p)), 0))

			require.GreaterOrEqual(t, exec.Size(), int64(0))

			ret, err := exec.Flush()
			require.NoError(t, err)
			require.Len(t, ret, 1)

			switch want := tc.want.(type) {
			case float64:
				require.Equal(t, want, vector.GetFixedAtNoTypeCheck[float64](ret[0], 0))
			case string:
				require.Equal(t, want, vector.GetFixedAtNoTypeCheck[types.Decimal128](ret[0], 0).Format(ret[0].GetType().Scale))
			}

			vec.Free(mp)
			ret[0].Free(mp)
			exec.Free()
		})
	}
}

func TestApproxPercentileExec_MaxPrecisionDecimal(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Equal(t, int64(0), mp.CurrNB()) }()

	tests := []struct {
		name   string
		typ    types.Type
		values []string
		p      string
		want   string
	}{
		{
			name:   "decimal38 scale0 minimum endpoint",
			typ:    types.New(types.T_decimal128, 38, 0),
			values: []string{"-99999999999999999999999999999999999999", "99999999999999999999999999999999999999"},
			p:      "0",
			want:   "-99999999999999999999999999999999999999",
		},
		{
			name:   "decimal38 scale0 maximum endpoint",
			typ:    types.New(types.T_decimal128, 38, 0),
			values: []string{"-99999999999999999999999999999999999999", "99999999999999999999999999999999999999"},
			p:      "1",
			want:   "99999999999999999999999999999999999999",
		},
		{
			name:   "decimal38 scale38 endpoint",
			typ:    types.New(types.T_decimal128, 38, 38),
			values: []string{"0.1", "0.2"},
			p:      "1",
			want:   "0.20000000000000000000000000000000000000",
		},
		{
			name:   "decimal38 scale0 midpoint rounds at retained scale",
			typ:    types.New(types.T_decimal128, 38, 0),
			values: []string{"0", "1"},
			p:      "0.5",
			want:   "1",
		},
		{
			name:   "decimal38 scale0 negative midpoint rounds away from zero",
			typ:    types.New(types.T_decimal128, 38, 0),
			values: []string{"-1", "0"},
			p:      "0.5",
			want:   "-1",
		},
		{
			name:   "decimal37 gains interpolation scale",
			typ:    types.New(types.T_decimal128, 37, 0),
			values: []string{"0", "1"},
			p:      "0.5",
			want:   "0.5",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			exec, err := makeApproxPercentile(mp, AggIdOfApproxPercentile, false, test.typ)
			require.NoError(t, err)
			require.NoError(t, exec.GroupGrow(1))
			require.NoError(t, exec.SetExtraInformation([]byte(test.p), 0))

			values := make([]types.Decimal128, len(test.values))
			for i, value := range test.values {
				values[i], err = types.ParseDecimal128(value, test.typ.Width, test.typ.Scale)
				require.NoError(t, err)
			}
			vec := medianTestVector(t, mp, test.typ, values)
			require.NoError(t, exec.BulkFill(0, []*vector.Vector{vec}))

			ret, err := exec.Flush()
			require.NoError(t, err)
			require.Equal(t, test.want,
				vector.GetFixedAtNoTypeCheck[types.Decimal128](ret[0], 0).Format(ret[0].GetType().Scale))

			vec.Free(mp)
			ret[0].Free(mp)
			exec.Free()
		})
	}
}

func TestApproxPercentileExec_DifferentPercentiles(t *testing.T) {
	mp := mpool.MustNewZero()

	cases := []struct {
		label string
		p     float64
		want  float64
	}{
		{"p000", 0.00, 1.0},
		{"p025", 0.25, 3.25},
		{"p050", 0.50, 5.5},
		{"p075", 0.75, 7.75},
		{"p095", 0.95, 9.55},
		{"p100", 1.00, 10.0},
	}

	for _, tc := range cases {
		t.Run(tc.label, func(t *testing.T) {
			e, err := makeApproxPercentile(mp, 1, false, types.T_int64.ToType())
			require.NoError(t, err)
			require.NoError(t, e.GroupGrow(1))
			vc := buildFixedVec(t, mp, types.T_int64.ToType(), []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
			defer vc.Free(mp)
			require.NoError(t, e.BulkFill(0, []*vector.Vector{vc}))
			require.NoError(t, e.SetExtraInformation([]byte(formatFloatConfig(tc.p)), 0))

			ret, err := e.Flush()
			require.NoError(t, err)
			require.InDelta(t, tc.want, vector.GetFixedAtNoTypeCheck[float64](ret[0], 0), 1e-10)
			ret[0].Free(mp)
			e.Free()
		})
	}
}

func TestApproxPercentileExec_Float64ExtremeInterpolation(t *testing.T) {
	mp := mpool.MustNewZero()
	exec, err := makeApproxPercentile(mp, 1, false, types.T_float64.ToType())
	require.NoError(t, err)
	require.NoError(t, exec.GroupGrow(1))

	vec := buildFixedVec(t, mp, types.T_float64.ToType(), []float64{-math.MaxFloat64, math.MaxFloat64})
	require.NoError(t, exec.BulkFill(0, []*vector.Vector{vec}))
	for _, test := range []struct {
		percentile string
		want       float64
	}{
		{percentile: "0", want: -math.MaxFloat64},
		{percentile: "0.5", want: 0},
		{percentile: "1", want: math.MaxFloat64},
	} {
		require.NoError(t, exec.SetExtraInformation([]byte(test.percentile), 0))
		ret, err := exec.Flush()
		require.NoError(t, err)
		require.Equal(t, test.want, vector.GetFixedAtNoTypeCheck[float64](ret[0], 0))
		ret[0].Free(mp)
	}

	vec.Free(mp)
	exec.Free()
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestApproxPercentileExec_DistinctNotSupported(t *testing.T) {
	mp := mpool.MustNewZero()

	_, err := makeApproxPercentile(mp, 1, true, types.T_int64.ToType())
	require.Error(t, err)
	require.Contains(t, err.Error(), "distinct")
}

func TestApproxPercentileExec_UnsupportedType(t *testing.T) {
	mp := mpool.MustNewZero()

	_, err := makeApproxPercentile(mp, 1, false, types.T_varchar.ToType())
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported type")
}

func TestApproxPercentileExec_SetExtraInformation_Invalid(t *testing.T) {
	mp := mpool.MustNewZero()

	exec, err := makeApproxPercentile(mp, 1, false, types.T_int64.ToType())
	require.NoError(t, err)

	// Not []byte
	err = exec.SetExtraInformation("not-bytes", 0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "expected []byte config")

	// Not a valid float
	err = exec.SetExtraInformation([]byte("not-a-float"), 0)
	require.Error(t, err)

	// Percentile out of range
	err = exec.SetExtraInformation([]byte("1.5"), 0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "percentile must be in [0,1]")

	err = exec.SetExtraInformation([]byte("-0.5"), 0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "percentile must be in [0,1]")

	for _, value := range []string{"NaN", "+Inf", "-Inf"} {
		err = exec.SetExtraInformation([]byte(value), 0)
		require.Error(t, err, "non-finite percentile %s must be rejected", value)
	}

	exec.Free()
}

func TestApproxPercentileExec_MultipleGroups(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() {
		require.Equal(t, int64(0), mp.CurrNB())
	}()

	exec, err := makeApproxPercentile(mp, AggIdOfApproxPercentile, false, types.T_int64.ToType())
	require.NoError(t, err)
	require.NoError(t, exec.GroupGrow(3))

	// Set percentile config once (shared across groups)
	require.NoError(t, exec.SetExtraInformation([]byte("0.5"), 0))

	// Group 1: values 1,2  -> median 1.5
	// Group 2: values 10,20 -> median 15
	// Group 3: value 100 -> median 100
	vec := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(vec, []int64{1, 2, 10, 20, 0, 0, 100}, []bool{false, false, false, false, true, true, false}, mp))
	defer vec.Free(mp)

	require.NoError(t, exec.BatchFill(0, []uint64{1, 1, 2, 2, 2, GroupNotMatched, 3}, []*vector.Vector{vec}))

	ret, err := exec.Flush()
	require.NoError(t, err)
	require.Len(t, ret, 1)
	require.Equal(t, 1.5, vector.GetFixedAtNoTypeCheck[float64](ret[0], 0))
	require.Equal(t, 15.0, vector.GetFixedAtNoTypeCheck[float64](ret[0], 1))
	require.Equal(t, 100.0, vector.GetFixedAtNoTypeCheck[float64](ret[0], 2))
	ret[0].Free(mp)
	exec.Free()
}

func TestApproxPercentileExec_NullHandling(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() {
		require.Equal(t, int64(0), mp.CurrNB())
	}()

	exec, err := makeApproxPercentile(mp, AggIdOfApproxPercentile, false, types.T_float64.ToType())
	require.NoError(t, err)
	require.NoError(t, exec.GroupGrow(1))
	require.NoError(t, exec.SetExtraInformation([]byte("0.5"), 0))

	vec := vector.NewVec(types.T_float64.ToType())
	require.NoError(t, vector.AppendFixedList(vec, []float64{0, 1, 2, 3, 4, 5}, []bool{true, false, true, false, true, false}, mp))
	defer vec.Free(mp)

	require.NoError(t, exec.BatchFill(0, []uint64{1, 1, 1, 1, 1, 1}, []*vector.Vector{vec}))

	ret, err := exec.Flush()
	require.NoError(t, err)
	require.Len(t, ret, 1)
	// Values: 1, 3, 5 -> sorted -> median = 3
	require.Equal(t, 3.0, vector.GetFixedAtNoTypeCheck[float64](ret[0], 0))
	ret[0].Free(mp)
	exec.Free()
}

func TestApproxPercentileExec_EmptyGroup(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() {
		require.Equal(t, int64(0), mp.CurrNB())
	}()

	exec, err := makeApproxPercentile(mp, AggIdOfApproxPercentile, false, types.T_int64.ToType())
	require.NoError(t, err)
	require.NoError(t, exec.GroupGrow(1))
	require.NoError(t, exec.SetExtraInformation([]byte("0.5"), 0))

	ret, err := exec.Flush()
	require.NoError(t, err)
	require.Len(t, ret, 1)
	// Empty group should return NULL
	require.True(t, ret[0].IsNull(0))
	ret[0].Free(mp)
	exec.Free()
}

func TestApproxPercentileExec_BatchMerge(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() {
		require.Equal(t, int64(0), mp.CurrNB())
	}()

	left, err := makeApproxPercentile(mp, AggIdOfApproxPercentile, false, types.T_int64.ToType())
	require.NoError(t, err)
	right, err := makeApproxPercentile(mp, AggIdOfApproxPercentile, false, types.T_int64.ToType())
	require.NoError(t, err)
	require.NoError(t, left.GroupGrow(2))
	require.NoError(t, right.GroupGrow(2))

	// Set percentile for both
	require.NoError(t, left.SetExtraInformation([]byte("0.5"), 0))
	require.NoError(t, right.SetExtraInformation([]byte("0.5"), 0))

	vecLeft := buildFixedVec(t, mp, types.T_int64.ToType(), []int64{1, 9, 3, 11})
	vecRight := buildFixedVec(t, mp, types.T_int64.ToType(), []int64{5, 13, 7, 15})
	defer vecLeft.Free(mp)
	defer vecRight.Free(mp)

	require.NoError(t, left.BatchFill(0, []uint64{1, 1, 2, 2}, []*vector.Vector{vecLeft}))
	require.NoError(t, right.BatchFill(0, []uint64{1, 1, 2, 2}, []*vector.Vector{vecRight}))
	require.NoError(t, left.BatchMerge(right, 0, []uint64{1, 2}))

	ret, err := left.Flush()
	require.NoError(t, err)
	require.Len(t, ret, 1)
	// Group 1: 1,5,9,13 -> median = (5+9)/2 = 7.0
	// Group 2: 3,7,11,15 -> median = (7+11)/2 = 9.0
	require.Equal(t, 7.0, vector.GetFixedAtNoTypeCheck[float64](ret[0], 0))
	require.Equal(t, 9.0, vector.GetFixedAtNoTypeCheck[float64](ret[0], 1))
	ret[0].Free(mp)
	left.Free()
	right.Free()
}

func TestApproxPercentileExec_Merge(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() {
		require.Equal(t, int64(0), mp.CurrNB())
	}()

	left, err := makeApproxPercentile(mp, AggIdOfApproxPercentile, false, types.T_int64.ToType())
	require.NoError(t, err)
	right, err := makeApproxPercentile(mp, AggIdOfApproxPercentile, false, types.T_int64.ToType())
	require.NoError(t, err)
	require.NoError(t, left.GroupGrow(1))
	require.NoError(t, right.GroupGrow(1))
	require.NoError(t, left.SetExtraInformation([]byte("0.5"), 0))
	require.NoError(t, right.SetExtraInformation([]byte("0.5"), 0))

	vecLeft := buildFixedVec(t, mp, types.T_int64.ToType(), []int64{1, 9})
	vecRight := buildFixedVec(t, mp, types.T_int64.ToType(), []int64{5, 13})
	defer vecLeft.Free(mp)
	defer vecRight.Free(mp)

	require.NoError(t, left.BulkFill(0, []*vector.Vector{vecLeft}))
	require.NoError(t, right.BulkFill(0, []*vector.Vector{vecRight}))
	require.NoError(t, left.Merge(right, 0, 0))

	ret, err := left.Flush()
	require.NoError(t, err)
	require.Len(t, ret, 1)
	// Values: 1,5,9,13 -> median = (5+9)/2 = 7.0
	require.Equal(t, 7.0, vector.GetFixedAtNoTypeCheck[float64](ret[0], 0))
	ret[0].Free(mp)
	left.Free()
	right.Free()
}

func TestApproxPercentileExec_BatchMerge_DifferentPercentile(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() {
		require.Equal(t, int64(0), mp.CurrNB())
	}()

	left, err := makeApproxPercentile(mp, AggIdOfApproxPercentile, false, types.T_int64.ToType())
	require.NoError(t, err)
	right, err := makeApproxPercentile(mp, AggIdOfApproxPercentile, false, types.T_int64.ToType())
	require.NoError(t, err)
	require.NoError(t, left.GroupGrow(1))
	require.NoError(t, right.GroupGrow(1))
	require.NoError(t, left.SetExtraInformation([]byte("0.95"), 0))
	require.NoError(t, right.SetExtraInformation([]byte("0.95"), 0))

	vecLeft := buildFixedVec(t, mp, types.T_int64.ToType(), []int64{1, 2, 3, 4, 5})
	vecRight := buildFixedVec(t, mp, types.T_int64.ToType(), []int64{6, 7, 8, 9, 10})
	defer vecLeft.Free(mp)
	defer vecRight.Free(mp)

	require.NoError(t, left.BulkFill(0, []*vector.Vector{vecLeft}))
	require.NoError(t, right.BulkFill(0, []*vector.Vector{vecRight}))
	require.NoError(t, left.Merge(right, 0, 0))

	ret, err := left.Flush()
	require.NoError(t, err)
	// Values: 1..10, p=0.95, idx=0.95*9=8.55, lo=8(9), hi=9(10), frac=0.55
	// 9 + 0.55*1 = 9.55
	require.InDelta(t, 9.55, vector.GetFixedAtNoTypeCheck[float64](ret[0], 0), 1e-10)
	ret[0].Free(mp)
	left.Free()
	right.Free()
}

func TestApproxPercentileExec_IntermediateRoundTrip(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() {
		require.Equal(t, int64(0), mp.CurrNB())
	}()

	exec, err := makeApproxPercentile(mp, AggIdOfApproxPercentile, false, types.T_int64.ToType())
	require.NoError(t, err)
	require.NoError(t, exec.GroupGrow(3))
	require.NoError(t, exec.SetExtraInformation([]byte("0.5"), 0))

	vec := buildFixedVec(t, mp, types.T_int64.ToType(), []int64{1, 1, 2, 4, 6, 8, 10})
	defer vec.Free(mp)
	require.NoError(t, exec.BatchFill(0, []uint64{1, 1, 1, 2, 2, 3, 3}, []*vector.Vector{vec}))

	var buf bytes.Buffer
	require.NoError(t, exec.SaveIntermediateResult(3, [][]uint8{{1, 1, 1}}, &buf))

	restored, err := makeApproxPercentile(mp, AggIdOfApproxPercentile, false, types.T_int64.ToType())
	require.NoError(t, err)
	require.NoError(t, restored.UnmarshalFromReader(bytes.NewReader(buf.Bytes()), mp))
	require.NoError(t, restored.SetExtraInformation([]byte("0.5"), 0))

	ret, err := restored.Flush()
	require.NoError(t, err)
	require.Len(t, ret, 1)
	require.Equal(t, 1.0, vector.GetFixedAtNoTypeCheck[float64](ret[0], 0))
	require.Equal(t, 5.0, vector.GetFixedAtNoTypeCheck[float64](ret[0], 1))
	require.Equal(t, 9.0, vector.GetFixedAtNoTypeCheck[float64](ret[0], 2))
	ret[0].Free(mp)
	exec.Free()
	restored.Free()
}

func TestApproxPercentileExec_DecimalMerge(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() {
		require.Equal(t, int64(0), mp.CurrNB())
	}()

	typ := types.New(types.T_decimal64, 10, 2)
	left, err := makeApproxPercentile(mp, AggIdOfApproxPercentile, false, typ)
	require.NoError(t, err)
	right, err := makeApproxPercentile(mp, AggIdOfApproxPercentile, false, typ)
	require.NoError(t, err)
	require.NoError(t, left.GroupGrow(1))
	require.NoError(t, right.GroupGrow(1))
	require.NoError(t, left.SetExtraInformation([]byte("0.5"), 0))
	require.NoError(t, right.SetExtraInformation([]byte("0.5"), 0))

	vecLeft := medianTestVector(t, mp, typ, mustDecimal64s(t, "1.00", "3.00", "5.00"))
	vecRight := medianTestVector(t, mp, typ, mustDecimal64s(t, "3.00", "7.00", "9.00"))
	defer vecLeft.Free(mp)
	defer vecRight.Free(mp)

	require.NoError(t, left.BulkFill(0, []*vector.Vector{vecLeft}))
	require.NoError(t, right.BulkFill(0, []*vector.Vector{vecRight}))
	require.NoError(t, left.Merge(right, 0, 0))

	ret, err := left.Flush()
	require.NoError(t, err)
	// Values: 1,3,3,5,7,9 -> median = (3+5)/2 = 4.0 -> "4.000" with scale+1=3
	require.Equal(t, "4.000", vector.GetFixedAtNoTypeCheck[types.Decimal128](ret[0], 0).Format(ret[0].GetType().Scale))
	ret[0].Free(mp)
	left.Free()
	right.Free()
}

func TestApproxPercentileExec_P95LargerDataset(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() {
		require.Equal(t, int64(0), mp.CurrNB())
	}()

	exec, err := makeApproxPercentile(mp, AggIdOfApproxPercentile, false, types.T_int64.ToType())
	require.NoError(t, err)
	require.NoError(t, exec.GroupGrow(1))
	require.NoError(t, exec.SetExtraInformation([]byte("0.95"), 0))

	vec := buildFixedVec(t, mp, types.T_int64.ToType(), []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20})
	defer vec.Free(mp)
	require.NoError(t, exec.BatchFill(0, []uint64{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, []*vector.Vector{vec}))

	ret, err := exec.Flush()
	require.NoError(t, err)
	// idx = 0.95 * 19 = 18.05, lo=18(19), hi=19(20), frac=0.05
	// 19 + 0.05*1 = 19.05
	require.InDelta(t, 19.05, vector.GetFixedAtNoTypeCheck[float64](ret[0], 0), 1e-10)
	ret[0].Free(mp)
	exec.Free()
}

func TestApproxPercentileExec_Decimal128WithP75(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() {
		require.Equal(t, int64(0), mp.CurrNB())
	}()

	typ := types.New(types.T_decimal128, 20, 2)
	exec, err := makeApproxPercentile(mp, AggIdOfApproxPercentile, false, typ)
	require.NoError(t, err)
	require.NoError(t, exec.GroupGrow(1))
	require.NoError(t, exec.SetExtraInformation([]byte("0.75"), 0))

	vals := mustDecimal128s(t, "10.00", "20.00", "30.00", "40.00")
	vec := medianTestVector(t, mp, typ, vals)
	defer vec.Free(mp)
	require.NoError(t, exec.BulkFill(0, []*vector.Vector{vec}))

	ret, err := exec.Flush()
	require.NoError(t, err)
	// idx = 0.75 * 3 = 2.25, lo=2(30), hi=3(40), frac=0.25
	// 30 + 0.25*10 = 32.5
	require.Equal(t, "32.500", vector.GetFixedAtNoTypeCheck[types.Decimal128](ret[0], 0).Format(ret[0].GetType().Scale))
	ret[0].Free(mp)
	exec.Free()
}

func TestApproxPercentileExec_DecimalBatchMerge(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() {
		require.Equal(t, int64(0), mp.CurrNB())
	}()

	typ := types.New(types.T_decimal64, 10, 2)
	left, err := makeApproxPercentile(mp, AggIdOfApproxPercentile, false, typ)
	require.NoError(t, err)
	right, err := makeApproxPercentile(mp, AggIdOfApproxPercentile, false, typ)
	require.NoError(t, err)
	require.NoError(t, left.GroupGrow(2))
	require.NoError(t, right.GroupGrow(2))
	require.NoError(t, left.SetExtraInformation([]byte("0.5"), 0))
	require.NoError(t, right.SetExtraInformation([]byte("0.5"), 0))

	vecLeft := medianTestVector(t, mp, typ, mustDecimal64s(t, "1.00", "9.00", "3.00", "11.00"))
	vecRight := medianTestVector(t, mp, typ, mustDecimal64s(t, "5.00", "13.00", "7.00", "15.00"))
	defer vecLeft.Free(mp)
	defer vecRight.Free(mp)

	require.NoError(t, left.BatchFill(0, []uint64{1, 1, 2, 2}, []*vector.Vector{vecLeft}))
	require.NoError(t, right.BatchFill(0, []uint64{1, 1, 2, 2}, []*vector.Vector{vecRight}))
	require.NoError(t, left.BatchMerge(right, 0, []uint64{1, 2}))

	ret, err := left.Flush()
	require.NoError(t, err)
	require.Len(t, ret, 1)
	// Group 1: 1,5,9,13 -> median = 7.0 -> "7.000"
	// Group 2: 3,7,11,15 -> median = 9.0 -> "9.000"
	require.Equal(t, "7.000", vector.GetFixedAtNoTypeCheck[types.Decimal128](ret[0], 0).Format(ret[0].GetType().Scale))
	require.Equal(t, "9.000", vector.GetFixedAtNoTypeCheck[types.Decimal128](ret[0], 1).Format(ret[0].GetType().Scale))
	ret[0].Free(mp)
	left.Free()
	right.Free()
}

func TestApproxPercentileExec_Decimal128Merge(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() {
		require.Equal(t, int64(0), mp.CurrNB())
	}()

	typ := types.New(types.T_decimal128, 20, 2)
	left, err := makeApproxPercentile(mp, AggIdOfApproxPercentile, false, typ)
	require.NoError(t, err)
	right, err := makeApproxPercentile(mp, AggIdOfApproxPercentile, false, typ)
	require.NoError(t, err)
	require.NoError(t, left.GroupGrow(1))
	require.NoError(t, right.GroupGrow(1))
	require.NoError(t, left.SetExtraInformation([]byte("0.5"), 0))
	require.NoError(t, right.SetExtraInformation([]byte("0.5"), 0))

	vecLeft := medianTestVector(t, mp, typ, mustDecimal128s(t, "2.00", "4.00", "6.00"))
	vecRight := medianTestVector(t, mp, typ, mustDecimal128s(t, "8.00", "10.00", "12.00"))
	defer vecLeft.Free(mp)
	defer vecRight.Free(mp)

	require.NoError(t, left.BulkFill(0, []*vector.Vector{vecLeft}))
	require.NoError(t, right.BulkFill(0, []*vector.Vector{vecRight}))
	require.NoError(t, left.Merge(right, 0, 0))

	ret, err := left.Flush()
	require.NoError(t, err)
	// Values: 2,4,6,8,10,12 -> median = 7.0 -> "7.000"
	require.Equal(t, "7.000", vector.GetFixedAtNoTypeCheck[types.Decimal128](ret[0], 0).Format(ret[0].GetType().Scale))
	ret[0].Free(mp)
	left.Free()
	right.Free()
}

func TestApproxPercentileExec_LargeDecimalDoesNotUseFloat64(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Equal(t, int64(0), mp.CurrNB()) }()

	tests := []struct {
		name   string
		typ    types.Type
		values any
		p      string
		want   string
	}{
		{
			name:   "decimal64 min boundary",
			typ:    types.New(types.T_decimal64, 18, 2),
			values: mustDecimal64sWithType(t, 18, 2, "9007199254740993.00", "9007199254740994.00"),
			p:      "0",
			want:   "9007199254740993.000",
		},
		{
			name:   "decimal64 interpolation",
			typ:    types.New(types.T_decimal64, 18, 2),
			values: mustDecimal64sWithType(t, 18, 2, "9007199254740993.00", "9007199254740994.00"),
			p:      "0.5",
			want:   "9007199254740993.500",
		},
		{
			name:   "decimal128 min boundary",
			typ:    types.New(types.T_decimal128, 30, 2),
			values: mustDecimal128s(t, "9007199254740993.00", "9007199254740994.00"),
			p:      "0",
			want:   "9007199254740993.000",
		},
		{
			name:   "decimal128 negative interpolation",
			typ:    types.New(types.T_decimal128, 30, 2),
			values: mustDecimal128s(t, "-9007199254740994.00", "-9007199254740993.00"),
			p:      "0.5",
			want:   "-9007199254740993.500",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			exec, err := makeApproxPercentile(mp, AggIdOfApproxPercentile, false, test.typ)
			require.NoError(t, err)
			require.NoError(t, exec.GroupGrow(1))
			require.NoError(t, exec.SetExtraInformation([]byte(test.p), 0))
			vec := medianTestVector(t, mp, test.typ, test.values)
			require.NoError(t, exec.BulkFill(0, []*vector.Vector{vec}))

			ret, err := exec.Flush()
			require.NoError(t, err)
			require.Equal(t, test.want,
				vector.GetFixedAtNoTypeCheck[types.Decimal128](ret[0], 0).Format(ret[0].GetType().Scale))

			vec.Free(mp)
			ret[0].Free(mp)
			exec.Free()
		})
	}
}

func mustDecimal64sWithType(t *testing.T, precision, scale int32, values ...string) []types.Decimal64 {
	t.Helper()
	result := make([]types.Decimal64, len(values))
	for i, value := range values {
		decimal, err := types.ParseDecimal64(value, precision, scale)
		require.NoError(t, err)
		result[i] = decimal
	}
	return result
}

func TestApproxPercentileExec_BoundedSketchPartialMerge10K(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Equal(t, int64(0), mp.CurrNB()) }()

	const (
		rowCount = 10_000
		parts    = 4
	)
	execs := make([]AggFuncExec, parts)
	exact := make([]int64, 0, rowCount)
	for part := range parts {
		exec, err := makeApproxPercentile(mp, AggIdOfApproxPercentile, false, types.T_int64.ToType())
		require.NoError(t, err)
		require.NoError(t, exec.GroupGrow(1))
		require.NoError(t, exec.SetExtraInformation([]byte("0.95"), 0))
		execs[part] = exec

		values := make([]int64, rowCount/parts)
		nulls := make([]bool, len(values))
		for row := range values {
			globalRow := part*len(values) + row
			values[row] = int64(globalRow % 1_000)
			nulls[row] = globalRow%11 == 0
			if !nulls[row] {
				exact = append(exact, values[row])
			}
		}
		vec := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixedList(vec, values, nulls, mp))
		require.NoError(t, exec.BulkFill(0, []*vector.Vector{vec}))
		vec.Free(mp)
	}
	defer func() {
		for _, exec := range execs {
			exec.Free()
		}
	}()

	for part := 1; part < parts; part++ {
		require.NoError(t, execs[0].Merge(execs[part], 0, 0))
	}
	impl := execs[0].(*approxPercentileNumericExec[int64])
	sketch := impl.state[0].mobs[0].(*quantileSketch[int64])
	require.Equal(t, uint64(len(exact)), sketch.count)
	require.Less(t, sketch.retained(), 1_000, "retained state must stay bounded well below input rows")

	var intermediate bytes.Buffer
	require.NoError(t, execs[0].SaveIntermediateResult(1, [][]uint8{{1}}, &intermediate))
	require.Less(t, intermediate.Len(), 20_000, "distributed intermediate state must be bounded")

	restored, err := makeApproxPercentile(mp, AggIdOfApproxPercentile, false, types.T_int64.ToType())
	require.NoError(t, err)
	defer restored.Free()
	require.NoError(t, restored.UnmarshalFromReader(bytes.NewReader(intermediate.Bytes()), mp))
	require.NoError(t, restored.SetExtraInformation([]byte("0.95"), 0))

	ret, err := restored.Flush()
	require.NoError(t, err)
	got := vector.GetFixedAtNoTypeCheck[float64](ret[0], 0)
	want := percentileNumericVals(exact, 0.95)
	require.InDelta(t, want, got, 25, "bounded sketch rank error exceeded test tolerance")
	ret[0].Free(mp)

	require.NoError(t, restored.SetExtraInformation([]byte("0"), 0))
	ret, err = restored.Flush()
	require.NoError(t, err)
	require.Equal(t, 0.0, vector.GetFixedAtNoTypeCheck[float64](ret[0], 0))
	ret[0].Free(mp)

	require.NoError(t, restored.SetExtraInformation([]byte("1"), 0))
	ret, err = restored.Flush()
	require.NoError(t, err)
	require.Equal(t, 999.0, vector.GetFixedAtNoTypeCheck[float64](ret[0], 0))
	ret[0].Free(mp)
}

func TestApproxPercentileExec_RejectsDifferentMergeConfig(t *testing.T) {
	mp := mpool.MustNewZero()
	left, err := makeApproxPercentile(mp, AggIdOfApproxPercentile, false, types.T_int64.ToType())
	require.NoError(t, err)
	right, err := makeApproxPercentile(mp, AggIdOfApproxPercentile, false, types.T_int64.ToType())
	require.NoError(t, err)
	defer left.Free()
	defer right.Free()
	require.NoError(t, left.GroupGrow(1))
	require.NoError(t, right.GroupGrow(1))
	require.NoError(t, left.SetExtraInformation([]byte("0.95"), 0))
	require.NoError(t, right.SetExtraInformation([]byte("0.99"), 0))
	require.ErrorContains(t, left.Merge(right, 0, 0), "different percentile configurations")
}

func TestQuantileSketchMPoolLifecycle(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Equal(t, int64(0), mp.CurrNB()) }()

	left := newQuantileSketch[int64](mp, orderedCompare[int64])
	right := newQuantileSketch[int64](mp, orderedCompare[int64])
	restored := newQuantileSketch[int64](mp, orderedCompare[int64])
	defer left.Free()
	defer right.Free()
	defer restored.Free()

	for value := range int64(2_000) {
		require.NoError(t, left.Add(value))
		require.NoError(t, right.Add(value+2_000))
	}
	require.Positive(t, mp.CurrNB(), "retained samples must be tracked by the mpool")

	require.NoError(t, left.Merge(right))
	beforeQuantile := mp.CurrNB()
	_, _, _, err := left.Quantile(big.NewRat(1, 2))
	require.NoError(t, err)
	require.Equal(t, beforeQuantile, mp.CurrNB(), "quantile scratch space must be released")
	encoded, err := left.MarshalBinary()
	require.NoError(t, err)
	require.NoError(t, restored.UnmarshalBinary(encoded))
	require.Equal(t, left.count, restored.count)
	require.Equal(t, left.retained(), restored.retained())

	beforeInvalid := mp.CurrNB()
	invalid := append(append([]byte(nil), encoded...), 0)
	require.Error(t, restored.UnmarshalBinary(invalid))
	require.Equal(t, beforeInvalid, mp.CurrNB(), "failed unmarshal must release its temporary state")
	require.Error(t, restored.UnmarshalBinary(encoded[:len(encoded)-1]))
	require.Equal(t, beforeInvalid, mp.CurrNB(), "truncated unmarshal must release partially decoded levels")
}
