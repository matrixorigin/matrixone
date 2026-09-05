// Copyright 2024 Matrix Origin
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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func buildDecimal256Vector(t *testing.T, mp *mpool.MPool, typ types.Type, nulls []bool, values []types.Decimal256) *vector.Vector {
	vec := vector.NewVec(typ)
	for i, value := range values {
		isNull := len(nulls) > 0 && nulls[i]
		require.NoError(t, vector.AppendFixed(vec, value, isNull, mp))
	}
	return vec
}

func buildAvgFixedVector[T any](t *testing.T, mp *mpool.MPool, typ types.Type, values []T) *vector.Vector {
	t.Helper()
	vec := vector.NewVec(typ)
	require.NoError(t, vector.AppendFixedList(vec, values, nil, mp))
	return vec
}

func TestAvgExactNumericReturnType(t *testing.T) {
	for _, test := range []struct {
		name  string
		input types.Type
		want  types.Type
	}{
		{name: "tinyint", input: types.T_int8.ToType(), want: types.New(types.T_decimal128, 7, 4)},
		{name: "tinyint unsigned", input: types.T_uint8.ToType(), want: types.New(types.T_decimal128, 7, 4)},
		{name: "smallint", input: types.T_int16.ToType(), want: types.New(types.T_decimal128, 9, 4)},
		{name: "smallint unsigned", input: types.T_uint16.ToType(), want: types.New(types.T_decimal128, 9, 4)},
		{name: "int", input: types.T_int32.ToType(), want: types.New(types.T_decimal128, 14, 4)},
		{name: "int unsigned", input: types.T_uint32.ToType(), want: types.New(types.T_decimal128, 14, 4)},
		{name: "bigint", input: types.T_int64.ToType(), want: types.New(types.T_decimal128, 23, 4)},
		{name: "bigint unsigned", input: types.T_uint64.ToType(), want: types.New(types.T_decimal128, 24, 4)},
		{name: "bigint cast domain", input: types.New(types.T_int64, 64, -1), want: types.New(types.T_decimal128, 23, 4)},
		{name: "literal precision", input: types.New(types.T_int64, 1, 0), want: types.New(types.T_decimal128, 5, 4)},
		{name: "literal precision at decimal128 limit", input: types.New(types.T_int64, 34, 0), want: types.New(types.T_decimal128, 38, 4)},
		{name: "literal precision promotes to decimal256", input: types.New(types.T_int64, 35, 0), want: types.New(types.T_decimal256, 39, 4)},
		{name: "literal precision caps decimal256", input: types.New(types.T_int64, 100, 0), want: types.New(types.T_decimal256, 65, 4)},
		{name: "year", input: types.T_year.ToType(), want: types.New(types.T_decimal128, 8, 4)},
		{name: "decimal64", input: types.New(types.T_decimal64, 8, 2), want: types.New(types.T_decimal128, 12, 6)},
		{name: "decimal128", input: types.New(types.T_decimal128, 20, 6), want: types.New(types.T_decimal128, 24, 10)},
		{name: "decimal128 promotes to decimal256", input: types.New(types.T_decimal128, 38, 20), want: types.New(types.T_decimal256, 42, 24)},
		{name: "double", input: types.T_float64.ToType(), want: types.T_float64.ToType()},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, AvgReturnType([]types.Type{test.input}))
		})
	}
}

func TestAvgRoundsDirectlyAtDeclaredScale(t *testing.T) {
	for _, test := range []struct {
		name       string
		sum        types.Decimal128
		count      int64
		argScale   int32
		resultType types.Type
		want       string
	}{
		{
			name:       "integer average",
			sum:        types.Decimal128FromInt64(1),
			count:      113,
			resultType: types.New(types.T_decimal128, 5, 4),
			want:       "0.0088",
		},
		{
			name:       "fractional average",
			sum:        types.Decimal128FromInt64(1),
			count:      113,
			argScale:   2,
			resultType: types.New(types.T_decimal128, 7, 6),
			want:       "0.000088",
		},
		{
			name:       "negative integer average",
			sum:        types.Decimal128FromInt64(-1),
			count:      113,
			resultType: types.New(types.T_decimal128, 5, 4),
			want:       "-0.0088",
		},
		{
			name:       "negative fractional average",
			sum:        types.Decimal128FromInt64(-1),
			count:      113,
			argScale:   2,
			resultType: types.New(types.T_decimal128, 7, 6),
			want:       "-0.000088",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			avg, err := decAvg[types.Decimal128](
				test.sum, test.count, test.argScale, test.resultType)
			require.NoError(t, err)
			require.Equal(t, test.want, avg.Format(test.resultType.Scale))
		})
	}
}

func TestAvgNativeIntegerHelperBoundaries(t *testing.T) {
	for _, test := range []struct {
		name        string
		sum         any
		count       int64
		resultScale int32
		wantErr     string
	}{
		{name: "positive int64", sum: int64(1), count: 113, resultScale: 4},
		{name: "negative int64", sum: int64(-1), count: 113, resultScale: 4},
		{name: "wide int64", sum: int64(1 << 60), count: 1, resultScale: 4},
		{name: "uint64", sum: uint64(1), count: 113, resultScale: 4},
		{name: "zero count", sum: int64(1), count: 0, resultScale: 4, wantErr: "Div by Zero"},
		{name: "invalid scale", sum: int64(1), count: 1, resultScale: -1, wantErr: "invalid native AVG result scale"},
		{name: "scale outside native table", sum: int64(1), count: 1, resultScale: int32(len(types.Pow10)), wantErr: "invalid native AVG result scale"},
		{name: "unsupported float", sum: float64(1), count: 1, resultScale: 4, wantErr: "unsupported native AVG sum type"},
		{name: "uint64 scale overflow", sum: ^uint64(0), count: 1, resultScale: 19, wantErr: "scale overflow"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var (
				got types.Decimal128
				err error
			)
			switch sum := test.sum.(type) {
			case int64:
				got, err = decimal128NativeIntegerAvg(sum, test.count, test.resultScale)
			case uint64:
				got, err = decimal128NativeIntegerAvg(sum, test.count, test.resultScale)
			case float64:
				got, err = decimal128NativeIntegerAvg(sum, test.count, test.resultScale)
			default:
				t.Fatalf("unsupported test value %T", test.sum)
			}
			if test.wantErr == "" {
				require.NoError(t, err)
				require.NotZero(t, got)
			} else {
				require.ErrorContains(t, err, test.wantErr)
			}
		})
	}

	require.Equal(t, types.Decimal128FromInt64(-1), decimal128FromNativeSum(int64(-1)))
	require.Equal(t, types.Decimal128{B0_63: 1}, decimal128FromNativeSum(uint64(1)))
	require.Panics(t, func() { decimal128FromNativeSum(float64(1)) })
	require.Equal(t, types.Decimal256FromInt64(-1), decimal256FromNativeSum(int64(-1)))
	require.Equal(t, types.Decimal256{B0_63: 1}, decimal256FromNativeSum(uint64(1)))
	require.Panics(t, func() { decimal256FromNativeSum(float64(1)) })
}

func TestAvgDecimalConversionHelpers(t *testing.T) {
	positive := types.Decimal256FromInt64(1)
	negative := types.Decimal256FromInt64(-1)
	for _, value := range []types.Decimal256{positive, negative} {
		converted, ok := decimal128FromDecimal256(value)
		require.True(t, ok)
		require.Equal(t, types.Decimal128{B0_63: value.B0_63, B64_127: value.B64_127}, converted)
	}
	_, ok := decimal128FromDecimal256(types.Decimal256{B128_191: 1})
	require.False(t, ok)
	_, ok = decimal128FromDecimal256(types.Decimal256{B64_127: 1 << 63, B192_255: ^uint64(0)})
	require.False(t, ok)

	value := types.Decimal256FromInt64(1)
	_, err := decimal256AvgAtScale(value, 0, 0, 4)
	require.ErrorContains(t, err, "Div by Zero")
	_, err = decimal256AvgAtScale(value, 1, 5, 4)
	require.ErrorContains(t, err, "below input scale")
	_, err = decimal128AvgAtScaleSigned(types.Decimal128FromInt64(1), 0, 0, 4)
	require.ErrorContains(t, err, "Div by Zero")
	_, err = decimal128AvgAtScaleSigned(types.Decimal128FromInt64(1), 1, 5, 4)
	require.ErrorContains(t, err, "below input scale")

	resultType := types.New(types.T_decimal128, 5, 4)
	_, err = decAvg[types.Decimal128](types.Decimal128FromInt64(1), 1, 0, types.New(types.T_decimal256, 42, 4))
	require.ErrorContains(t, err, "invalid decimal avg result type")
	_, err = decAvg[types.Decimal256](types.Decimal256FromInt64(1), 1, 0, resultType)
	require.ErrorContains(t, err, "invalid decimal avg result type")
}

func TestAvgIntegerExpressionPrecisionExecution(t *testing.T) {
	argType := types.New(types.T_int32, 1, 0)
	values := make([]int32, 113)
	values[0] = 1

	for _, test := range []struct {
		name     string
		distinct bool
		window   bool
		values   []int32
	}{
		{name: "ordinary", values: values},
		{
			name:     "distinct",
			distinct: true,
			values: func() []int32 {
				result := make([]int32, 113)
				for i := 0; i < 112; i++ {
					result[i] = int32(i) - 56 // -56 through 55
				}
				result[112] = 57 // the distinct values sum to one
				return result
			}(),
		},
		{name: "window", window: true, values: values},
	} {
		t.Run(test.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			defer mpool.DeleteMPool(mp)
			input := buildAvgFixedVector(t, mp, argType, test.values)
			defer input.Free(mp)

			exec, err := MakeAgg(mp, AggIdOfAvg, test.distinct, argType)
			require.NoError(t, err)
			defer exec.Free()
			require.NoError(t, exec.GroupGrow(1))
			if test.window {
				for row := range test.values {
					require.NoError(t, exec.Fill(0, row, []*vector.Vector{input}))
				}
			} else if test.distinct {
				groups := make([]uint64, len(test.values))
				for i := range groups {
					groups[i] = 1
				}
				require.NoError(t, exec.BatchFill(0, groups, []*vector.Vector{input}))
			} else {
				require.NoError(t, exec.BulkFill(0, []*vector.Vector{input}))
			}

			results, err := exec.Flush()
			require.NoError(t, err)
			defer results[0].Free(mp)
			require.Equal(t, types.New(types.T_decimal128, 5, 4), *results[0].GetType())
			value := vector.GetFixedAtNoTypeCheck[types.Decimal128](results[0], 0)
			require.Equal(t, "0.0088", value.Format(results[0].GetType().Scale))
		})
	}
}

func TestAvgWideIntegerExpressionPrecisionExecutionModes(t *testing.T) {
	argType := types.New(types.T_int64, 37, 0)
	resultType := types.New(types.T_decimal256, 41, 4)
	values := []int64{0}

	for _, test := range []struct {
		name     string
		distinct bool
		window   bool
	}{
		{name: "ordinary"},
		{name: "distinct", distinct: true},
		{name: "window", window: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			defer mpool.DeleteMPool(mp)
			input := buildAvgFixedVector(t, mp, argType, values)
			defer input.Free(mp)

			exec, err := MakeAgg(mp, AggIdOfAvg, test.distinct, argType)
			require.NoError(t, err)
			defer exec.Free()
			require.NoError(t, exec.GroupGrow(1))
			switch {
			case test.window:
				require.NoError(t, exec.Fill(0, 0, []*vector.Vector{input}))
			case test.distinct:
				require.NoError(t, exec.BatchFill(0, []uint64{1}, []*vector.Vector{input}))
			default:
				require.NoError(t, exec.BulkFill(0, []*vector.Vector{input}))
			}

			results, err := exec.Flush()
			require.NoError(t, err)
			require.Len(t, results, 1)
			defer results[0].Free(mp)
			require.Equal(t, resultType, *results[0].GetType())
			require.Equal(t, types.Decimal256{},
				vector.GetFixedAtNoTypeCheck[types.Decimal256](results[0], 0))
		})
	}
}

func TestAvgWideIntegerExpressionNativeFinalizer(t *testing.T) {
	argType := types.New(types.T_int32, 37, 0)
	resultType := types.New(types.T_decimal256, 41, 4)

	for _, test := range []struct {
		name     string
		distinct bool
		window   bool
	}{
		{name: "ordinary"},
		{name: "distinct", distinct: true},
		{name: "window", window: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			defer mpool.DeleteMPool(mp)
			input := buildAvgFixedVector(t, mp, argType, []int32{0})
			defer input.Free(mp)

			exec, err := MakeAgg(mp, AggIdOfAvg, test.distinct, argType)
			require.NoError(t, err)
			defer exec.Free()
			require.NoError(t, exec.GroupGrow(1))
			switch {
			case test.window:
				require.NoError(t, exec.Fill(0, 0, []*vector.Vector{input}))
			case test.distinct:
				require.NoError(t, exec.BatchFill(0, []uint64{1}, []*vector.Vector{input}))
			default:
				require.NoError(t, exec.BulkFill(0, []*vector.Vector{input}))
			}

			results, err := exec.Flush()
			require.NoError(t, err)
			require.Len(t, results, 1)
			defer results[0].Free(mp)
			require.Equal(t, resultType, *results[0].GetType())
			require.Equal(t, types.Decimal256{},
				vector.GetFixedAtNoTypeCheck[types.Decimal256](results[0], 0))
		})
	}
}

func TestAvgDecimalRoundingExecutionModes(t *testing.T) {
	argType := types.New(types.T_decimal64, 8, 2)
	values := make([]types.Decimal64, 113)
	values[0] = 1 // 0.01; the remaining values are zero.

	for _, test := range []struct {
		name     string
		distinct bool
		window   bool
		values   []types.Decimal64
	}{
		{name: "ordinary", values: values},
		{
			name:     "distinct",
			distinct: true,
			values: func() []types.Decimal64 {
				result := make([]types.Decimal64, 113)
				for i := 0; i < 112; i++ {
					result[i] = types.Decimal64(i - 56) // -0.56 through 0.55
				}
				result[112] = 57 // the distinct values sum to 0.01
				return result
			}(),
		},
		{name: "window", window: true, values: values},
	} {
		t.Run(test.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			defer mpool.DeleteMPool(mp)
			input := buildAvgFixedVector(t, mp, argType, test.values)
			defer input.Free(mp)

			exec, err := MakeAgg(mp, AggIdOfAvg, test.distinct, argType)
			require.NoError(t, err)
			defer exec.Free()
			require.NoError(t, exec.GroupGrow(1))
			switch {
			case test.window:
				for row := range test.values {
					require.NoError(t, exec.Fill(0, row, []*vector.Vector{input}))
				}
			case test.distinct:
				groups := make([]uint64, len(test.values))
				for i := range groups {
					groups[i] = 1
				}
				require.NoError(t, exec.BatchFill(0, groups, []*vector.Vector{input}))
			default:
				require.NoError(t, exec.BulkFill(0, []*vector.Vector{input}))
			}

			results, err := exec.Flush()
			require.NoError(t, err)
			defer results[0].Free(mp)
			require.Equal(t, types.New(types.T_decimal128, 12, 6), *results[0].GetType())
			value := vector.GetFixedAtNoTypeCheck[types.Decimal128](results[0], 0)
			require.Equal(t, "0.000088", value.Format(results[0].GetType().Scale))
		})
	}
}

func TestAvgDecimal256PreservesMaximumInputScale(t *testing.T) {
	argType := types.New(types.T_decimal128, 38, 38)
	resultType := AvgReturnType([]types.Type{argType})
	require.Equal(t, types.New(types.T_decimal256, 42, 38), resultType)

	value, err := types.ParseDecimal128(
		"0.12345678901234567890123456789012345678",
		argType.Width,
		argType.Scale,
	)
	require.NoError(t, err)
	avg, err := decAvg[types.Decimal256](
		types.Decimal256FromDecimal128(value), 1, argType.Scale, resultType)
	require.NoError(t, err)
	// Compare the encoded value rather than Decimal256.Format here. Format rounds
	// while repeatedly dividing by ten, which is not a lossless way to inspect a
	// 38-digit value.
	require.Equal(t, types.Decimal256FromDecimal128(value), avg)
}

func TestAvgDecimal256HighScaleExecutionModes(t *testing.T) {
	argType := types.New(types.T_decimal128, 38, 38)
	resultType := AvgReturnType([]types.Type{argType})
	value, err := types.ParseDecimal128(
		"0.12345678901234567890123456789012345678",
		argType.Width,
		argType.Scale,
	)
	require.NoError(t, err)

	for _, test := range []struct {
		name     string
		distinct bool
		window   bool
	}{
		{name: "ordinary"},
		{name: "distinct", distinct: true},
		{name: "window", window: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			defer mpool.DeleteMPool(mp)
			input := buildAvgFixedVector(t, mp, argType, []types.Decimal128{value})
			defer input.Free(mp)

			exec := makeSumAvgExec(mp, false, AggIdOfAvg, test.distinct, argType)
			defer exec.Free()
			require.NoError(t, exec.GroupGrow(1))
			switch {
			case test.window:
				require.NoError(t, exec.Fill(0, 0, []*vector.Vector{input}))
			case test.distinct:
				require.NoError(t, exec.BatchFill(0, []uint64{1}, []*vector.Vector{input}))
			default:
				require.NoError(t, exec.BulkFill(0, []*vector.Vector{input}))
			}

			results, err := exec.Flush()
			require.NoError(t, err)
			require.Len(t, results, 1)
			defer results[0].Free(mp)
			require.Equal(t, resultType, *results[0].GetType())
			require.Equal(t, types.Decimal256FromDecimal128(value),
				vector.GetFixedAtNoTypeCheck[types.Decimal256](results[0], 0))
		})
	}
}

func TestAvgExactIntegerExecution(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	for _, test := range []struct {
		name  string
		typ   types.Type
		input func(*testing.T, *mpool.MPool, types.Type) *vector.Vector
	}{
		{name: "int8", typ: types.T_int8.ToType(), input: func(t *testing.T, mp *mpool.MPool, typ types.Type) *vector.Vector {
			return buildAvgFixedVector(t, mp, typ, []int8{1, 2, 4})
		}},
		{name: "int16", typ: types.T_int16.ToType(), input: func(t *testing.T, mp *mpool.MPool, typ types.Type) *vector.Vector {
			return buildAvgFixedVector(t, mp, typ, []int16{1, 2, 4})
		}},
		{name: "int32", typ: types.T_int32.ToType(), input: func(t *testing.T, mp *mpool.MPool, typ types.Type) *vector.Vector {
			return buildAvgFixedVector(t, mp, typ, []int32{1, 2, 4})
		}},
		{name: "uint8", typ: types.T_uint8.ToType(), input: func(t *testing.T, mp *mpool.MPool, typ types.Type) *vector.Vector {
			return buildAvgFixedVector(t, mp, typ, []uint8{1, 2, 4})
		}},
		{name: "uint16", typ: types.T_uint16.ToType(), input: func(t *testing.T, mp *mpool.MPool, typ types.Type) *vector.Vector {
			return buildAvgFixedVector(t, mp, typ, []uint16{1, 2, 4})
		}},
		{name: "uint32", typ: types.T_uint32.ToType(), input: func(t *testing.T, mp *mpool.MPool, typ types.Type) *vector.Vector {
			return buildAvgFixedVector(t, mp, typ, []uint32{1, 2, 4})
		}},
		{name: "year", typ: types.T_year.ToType(), input: func(t *testing.T, mp *mpool.MPool, typ types.Type) *vector.Vector {
			return buildAvgFixedVector(t, mp, typ, []types.MoYear{2001, 2002, 2004})
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			input := test.input(t, mp, test.typ)
			defer input.Free(mp)
			exec := makeAvgExec(t, mp, test.typ)
			defer exec.Free()
			require.NoError(t, exec.GroupGrow(1))
			require.NoError(t, exec.BulkFill(0, []*vector.Vector{input}))
			results, err := exec.Flush()
			require.NoError(t, err)
			defer results[0].Free(mp)
			require.Equal(t, AvgReturnType([]types.Type{test.typ}), *results[0].GetType())

			value := vector.GetFixedAtNoTypeCheck[types.Decimal128](results[0], 0)
			if test.typ.Oid == types.T_year {
				require.Equal(t, "2002.3333", value.Format(results[0].GetType().Scale))
			} else {
				require.Equal(t, "2.3333", value.Format(results[0].GetType().Scale))
			}
		})
	}
}

func TestDecimal128AvgFinalizationRejectsNarrowResult(t *testing.T) {
	argType := types.New(types.T_decimal128, 38, 10)
	resultType := types.New(types.T_decimal128, 38, 12)
	testCases := []struct {
		name    string
		value   string
		wantErr string
	}{
		{
			name:    "physical overflow",
			value:   "9999999999999999999999999999.1234567890",
			wantErr: "Decimal128 Div overflow",
		},
		{
			name:    "declared precision overflow",
			value:   "100000000000000000000000000.0000000000",
			wantErr: "Decimal128(38,12)",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			value, err := types.ParseDecimal128(tc.value, argType.Width, argType.Scale)
			require.NoError(t, err)
			_, err = decAvg[types.Decimal128](value, 1, argType.Scale, resultType)
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}

func buildNumericTestDataVecs(t *testing.T, mp *mpool.MPool) ([]types.Type, []*vector.Vector, []*vector.Vector) {
	nulls := []bool{false, false, false, false, true, false, false, false, false, true}
	int8s := []int8{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
	int32s := []int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
	int64s := []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
	float32s := []float32{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0}
	float64s := []float64{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0}
	d64s := []types.Decimal64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
	d128s := []types.Decimal128{
		{B0_63: 1, B64_127: 0},
		{B0_63: 2, B64_127: 0},
		{B0_63: 3, B64_127: 0},
		{B0_63: 4, B64_127: 0},
		{B0_63: 5, B64_127: 0},
		{B0_63: 6, B64_127: 0},
		{B0_63: 7, B64_127: 0},
		{B0_63: 8, B64_127: 0},
		{B0_63: 9, B64_127: 0},
		{B0_63: 10, B64_127: 0},
		{B0_63: 11, B64_127: 0},
		{B0_63: 12, B64_127: 0}}
	d256s := []types.Decimal256{
		types.Decimal256FromInt64(1),
		types.Decimal256FromInt64(2),
		types.Decimal256FromInt64(3),
		types.Decimal256FromInt64(4),
		types.Decimal256FromInt64(5),
		types.Decimal256FromInt64(6),
		types.Decimal256FromInt64(7),
		types.Decimal256FromInt64(8),
		types.Decimal256FromInt64(9),
		types.Decimal256FromInt64(10),
		types.Decimal256FromInt64(11),
		types.Decimal256FromInt64(12),
	}

	typs := []types.Type{
		types.T_int8.ToType(),
		types.T_int32.ToType(),
		types.T_int64.ToType(),
		types.T_float32.ToType(),
		types.T_float64.ToType(),
		types.T_decimal64.ToType(),
		types.T_decimal128.ToType(),
		types.T_decimal256.ToType(),
	}

	for i := range typs {
		typs[i].Scale = 0
	}

	vecs := make([]*vector.Vector, 8)
	nvecs := make([]*vector.Vector, 8)
	vecs[0] = testutil.NewInt8Vector(10, typs[0], mp, false, nil, int8s[:10])
	nvecs[0] = testutil.NewInt8Vector(10, typs[0], mp, false, nulls, int8s[2:])
	vecs[1] = testutil.NewInt32Vector(10, typs[1], mp, false, nil, int32s[:10])
	nvecs[1] = testutil.NewInt32Vector(10, typs[1], mp, false, nulls, int32s[2:])
	vecs[2] = testutil.NewInt64Vector(10, typs[2], mp, false, nil, int64s[:10])
	nvecs[2] = testutil.NewInt64Vector(10, typs[2], mp, false, nulls, int64s[2:])
	vecs[3] = testutil.NewFloat32Vector(10, typs[3], mp, false, nil, float32s[:10])
	nvecs[3] = testutil.NewFloat32Vector(10, typs[3], mp, false, nulls, float32s[2:])
	vecs[4] = testutil.NewFloat64Vector(10, typs[4], mp, false, nil, float64s[:10])
	nvecs[4] = testutil.NewFloat64Vector(10, typs[4], mp, false, nulls, float64s[2:])
	vecs[5] = testutil.NewDecimal64Vector(10, typs[5], mp, false, nil, d64s[:10])
	nvecs[5] = testutil.NewDecimal64Vector(10, typs[5], mp, false, nulls, d64s[2:])
	vecs[6] = testutil.NewDecimal128Vector(10, typs[6], mp, false, nil, d128s[:10])
	nvecs[6] = testutil.NewDecimal128Vector(10, typs[6], mp, false, nulls, d128s[2:])
	vecs[7] = buildDecimal256Vector(t, mp, typs[7], nil, d256s[:10])
	nvecs[7] = buildDecimal256Vector(t, mp, typs[7], nulls, d256s[2:])
	return typs, vecs, nvecs
}

type expectedResult struct {
	expected float64
}

func (e *expectedResult) check(val any, scale int32) error {
	switch val := val.(type) {
	case int64:
		if math.Abs(e.expected-float64(val)) > 1e-6 {
			return moerr.NewInternalErrorNoCtxf("expected %f, got %d", e.expected, val)
		}
	case float64:
		if math.Abs(e.expected-val) > 1e-6 {
			return moerr.NewInternalErrorNoCtxf("expected %f, got %f", e.expected, val)
		}
	case types.Decimal128:
		resultFloat := types.Decimal128ToFloat64(val, scale)
		tolerance := 1e-6
		if scale > 0 {
			tolerance = math.Max(tolerance, math.Pow10(-int(scale)))
		}
		if math.Abs(e.expected-resultFloat) > tolerance {
			return moerr.NewInternalErrorNoCtxf("expected %f, got %f", e.expected, resultFloat)
		}
	case types.Decimal256:
		resultFloat := types.Decimal256ToFloat64(val, scale)
		tolerance := 1e-6
		if scale > 0 {
			tolerance = math.Max(tolerance, math.Pow10(-int(scale)))
		}
		if math.Abs(e.expected-resultFloat) > tolerance {
			return moerr.NewInternalErrorNoCtxf("expected %f, got %f", e.expected, resultFloat)
		}
	default:
		return moerr.NewInternalErrorNoCtxf("unsupported type %T", val)
	}
	return nil
}

func (e *expectedResult) checkVecAt(vec *vector.Vector, idx int) error {
	typ := vec.GetType()
	switch typ.Oid {
	case types.T_int64:
		return e.check(vector.MustFixedColNoTypeCheck[int64](vec)[idx], typ.Scale)
	case types.T_float64:
		return e.check(vector.MustFixedColNoTypeCheck[float64](vec)[idx], typ.Scale)
	case types.T_decimal128:
		return e.check(vector.MustFixedColNoTypeCheck[types.Decimal128](vec)[idx], typ.Scale)
	case types.T_decimal256:
		return e.check(vector.MustFixedColNoTypeCheck[types.Decimal256](vec)[idx], typ.Scale)
	}
	return moerr.NewInternalErrorNoCtxf("unsupported type %s", typ.Oid)
}

func checkVecAll(vec *vector.Vector, expected []expectedResult) error {
	for i, expected := range expected {
		if err := expected.checkVecAt(vec, i); err != nil {
			return err
		}
	}
	return nil
}

func requireSingleAggResultEqual(t *testing.T, mp *mpool.MPool, left, right AggFuncExec, wantErr string) {
	t.Helper()

	leftResults, leftErr := left.Flush()
	defer func() {
		for _, result := range leftResults {
			result.Free(mp)
		}
	}()
	rightResults, rightErr := right.Flush()
	defer func() {
		for _, result := range rightResults {
			result.Free(mp)
		}
	}()

	if wantErr != "" {
		require.Nil(t, leftResults)
		require.Nil(t, rightResults)
		require.ErrorContains(t, leftErr, wantErr)
		require.ErrorContains(t, rightErr, wantErr)
		return
	}

	require.NoError(t, leftErr)
	require.NoError(t, rightErr)
	require.Len(t, leftResults, 1)
	require.Len(t, rightResults, 1)
	require.Equal(t, leftResults[0].Length(), rightResults[0].Length())
	require.Equal(t, leftResults[0].GetType().Oid, rightResults[0].GetType().Oid)

	switch leftResults[0].GetType().Oid {
	case types.T_int64:
		require.Equal(t,
			vector.MustFixedColNoTypeCheck[int64](leftResults[0]),
			vector.MustFixedColNoTypeCheck[int64](rightResults[0]))
	case types.T_float64:
		require.Equal(t,
			vector.MustFixedColNoTypeCheck[float64](leftResults[0]),
			vector.MustFixedColNoTypeCheck[float64](rightResults[0]))
	case types.T_decimal128:
		require.Equal(t,
			vector.MustFixedColNoTypeCheck[types.Decimal128](leftResults[0]),
			vector.MustFixedColNoTypeCheck[types.Decimal128](rightResults[0]))
	case types.T_decimal256:
		require.Equal(t,
			vector.MustFixedColNoTypeCheck[types.Decimal256](leftResults[0]),
			vector.MustFixedColNoTypeCheck[types.Decimal256](rightResults[0]))
	default:
		require.Failf(t, "unsupported result type", "%s", leftResults[0].GetType().Oid)
	}
}

type expectedSumAvg struct {
	expected    expectedResult
	b2          [2]expectedResult
	expected20k expectedResult
}

func newExpectedSumAvg(exp1, b2a, b2b, exp20k float64) *expectedSumAvg {
	return &expectedSumAvg{
		expected: expectedResult{expected: exp1},
		b2: [2]expectedResult{
			{expected: b2a},
			{expected: b2b},
		},
		expected20k: expectedResult{expected: exp20k},
	}
}

func TestExpectedSumAvg(t *testing.T) {
	e1 := expectedResult{expected: 100}
	e2 := expectedResult{expected: 200.1230000001}
	e3 := expectedResult{expected: 200.1234}
	require.NoError(t, e1.check(int64(100), 3))
	require.NoError(t, e2.check(float64(200.1230000001), 3))
	require.Error(t, e3.check(float64(200.123456), 3))
}

func makeSumExec(t *testing.T, mp *mpool.MPool, typ types.Type) AggFuncExec {
	agg := makeSumAvgExec(mp, true, AggIdOfSum, false, typ)
	return agg
}

func makeSumDistinctExec(t *testing.T, mp *mpool.MPool, typ types.Type) AggFuncExec {
	agg := makeSumAvgExec(mp, true, AggIdOfSum, true, typ)
	return agg
}

func makeAvgExec(t *testing.T, mp *mpool.MPool, typ types.Type) AggFuncExec {
	agg := makeSumAvgExec(mp, false, AggIdOfAvg, false, typ)
	return agg
}

func makeAvgDistinctExec(t *testing.T, mp *mpool.MPool, typ types.Type) AggFuncExec {
	agg := makeSumAvgExec(mp, false, AggIdOfSum, true, typ)
	return agg
}

func TestSum(t *testing.T) {
	testSumAvg(t, makeSumExec, newExpectedSumAvg(111, 53, 58, 222000))
}

func TestSumDistinct(t *testing.T) {
	testSumAvg(t, makeSumDistinctExec, newExpectedSumAvg(66, 36, 30, 222000))
}

func TestAvg(t *testing.T) {
	testSumAvg(t, makeAvgExec, newExpectedSumAvg(6.1666666666, 5.88888888, 6.4444444444, 126000))
}

func TestAvgDistinct(t *testing.T) {
	testSumAvg(t, makeAvgDistinctExec, newExpectedSumAvg(6, 6, 6, 126000))
}

func TestWindowSlidingSumAvgCapability(t *testing.T) {
	mp := mpool.MustNewZero()
	tests := []struct {
		name     string
		aggID    int64
		distinct bool
		typ      types.Type
		want     bool
	}{
		{name: "int32 sum", aggID: AggIdOfSum, typ: types.T_int32.ToType(), want: true},
		{name: "int64 sum", aggID: AggIdOfSum, typ: types.T_int64.ToType(), want: true},
		{name: "decimal64 sum", aggID: AggIdOfSum, typ: types.New(types.T_decimal64, 18, 2), want: true},
		{name: "narrow decimal128 sum", aggID: AggIdOfSum, typ: types.New(types.T_decimal128, 20, 2)},
		{name: "wide decimal128 sum", aggID: AggIdOfSum, typ: types.New(types.T_decimal128, 38, 2)},
		{name: "float sum", aggID: AggIdOfSum, typ: types.T_float64.ToType()},
		{name: "int32 avg", aggID: AggIdOfAvg, typ: types.T_int32.ToType(), want: true},
		{name: "int64 avg", aggID: AggIdOfAvg, typ: types.T_int64.ToType(), want: true},
		{name: "decimal64 avg", aggID: AggIdOfAvg, typ: types.New(types.T_decimal64, 18, 2), want: true},
		{name: "float avg", aggID: AggIdOfAvg, typ: types.T_float64.ToType()},
		{name: "distinct sum", aggID: AggIdOfSum, distinct: true, typ: types.T_int32.ToType()},
		{name: "distinct avg", aggID: AggIdOfAvg, distinct: true, typ: types.T_int32.ToType()},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			exec, err := MakeAgg(mp, test.aggID, test.distinct, test.typ)
			require.NoError(t, err)
			defer exec.Free()
			require.Equal(t, test.want, SupportsWindowSliding(exec))
		})
	}
	require.Zero(t, mp.CurrNB())
}

func TestWindowSlidingAvgRemoveUpdatesCountAndEmptyState(t *testing.T) {
	t.Run("remaining row", func(t *testing.T) {
		mp := mpool.MustNewZero()
		typ := types.T_int32.ToType()
		input := testutil.NewInt32Vector(3, typ, mp, false,
			[]bool{false, true, false}, []int32{2, 0, 4})
		defer input.Free(mp)

		exec, err := MakeAgg(mp, AggIdOfAvg, false, typ)
		require.NoError(t, err)
		defer exec.Free()
		require.NoError(t, exec.GroupGrow(1))
		require.True(t, SupportsWindowSliding(exec))

		for row := 0; row < input.Length(); row++ {
			require.NoError(t, AddWindowRow(exec, row, []*vector.Vector{input}))
		}
		require.NoError(t, RemoveWindowRow(exec, 1, []*vector.Vector{input}))
		require.NoError(t, RemoveWindowRow(exec, 0, []*vector.Vector{input}))

		results, err := exec.Flush()
		require.NoError(t, err)
		defer results[0].Free(mp)
		require.False(t, results[0].IsNull(0))
		require.Equal(t, types.T_decimal128, results[0].GetType().Oid)
		value := vector.MustFixedColNoTypeCheck[types.Decimal128](results[0])[0]
		require.Equal(t, "4.0000", value.Format(results[0].GetType().Scale))
	})

	t.Run("empty frame", func(t *testing.T) {
		mp := mpool.MustNewZero()
		typ := types.T_int32.ToType()
		input := testutil.NewInt32Vector(1, typ, mp, false, nil, []int32{2})
		defer input.Free(mp)

		exec, err := MakeAgg(mp, AggIdOfAvg, false, typ)
		require.NoError(t, err)
		defer exec.Free()
		require.NoError(t, exec.GroupGrow(1))
		require.NoError(t, AddWindowRow(exec, 0, []*vector.Vector{input}))
		require.NoError(t, RemoveWindowRow(exec, 0, []*vector.Vector{input}))
		require.Error(t, RemoveWindowRow(exec, 0, []*vector.Vector{input}))

		results, err := exec.Flush()
		require.NoError(t, err)
		defer results[0].Free(mp)
		require.True(t, results[0].IsNull(0))
	})
}

func TestWindowSlidingAvgDecimalStates(t *testing.T) {
	tests := []struct {
		name      string
		typ       types.Type
		makeInput func(*mpool.MPool) *vector.Vector
		want      string
	}{
		{
			name: "int64",
			typ:  types.T_int64.ToType(),
			makeInput: func(mp *mpool.MPool) *vector.Vector {
				return testutil.NewInt64Vector(2, types.T_int64.ToType(), mp, false, nil, []int64{2, 4})
			},
			want: "4.0000",
		},
		{
			name: "decimal64",
			typ:  types.New(types.T_decimal64, 18, 2),
			makeInput: func(mp *mpool.MPool) *vector.Vector {
				typ := types.New(types.T_decimal64, 18, 2)
				return testutil.NewDecimal64Vector(
					2, typ, mp, false, nil, []types.Decimal64{200, 400})
			},
			want: "4.000000",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			input := test.makeInput(mp)
			defer input.Free(mp)

			exec, err := MakeAgg(mp, AggIdOfAvg, false, test.typ)
			require.NoError(t, err)
			defer exec.Free()
			require.NoError(t, exec.GroupGrow(1))
			require.True(t, SupportsWindowSliding(exec))
			require.NoError(t, AddWindowRow(exec, 0, []*vector.Vector{input}))
			require.NoError(t, AddWindowRow(exec, 1, []*vector.Vector{input}))
			require.NoError(t, RemoveWindowRow(exec, 0, []*vector.Vector{input}))

			results, err := exec.Flush()
			require.NoError(t, err)
			defer results[0].Free(mp)
			require.False(t, results[0].IsNull(0))
			value := vector.MustFixedColNoTypeCheck[types.Decimal128](results[0])[0]
			require.Equal(t, test.want, value.Format(results[0].GetType().Scale))
		})
	}
}

func TestWindowSlidingSumRemoveRestoresNull(t *testing.T) {
	mp := mpool.MustNewZero()
	typ := types.T_int32.ToType()
	input := testutil.NewInt32Vector(2, typ, mp, false, []bool{false, true}, []int32{7, 0})
	defer input.Free(mp)

	exec, err := MakeAgg(mp, AggIdOfSum, false, typ)
	require.NoError(t, err)
	defer exec.Free()
	require.NoError(t, exec.GroupGrow(1))
	require.True(t, SupportsWindowSliding(exec))

	// Removing a NULL must leave the running state alone; removing the last
	// non-NULL row must restore SUM's empty-frame NULL result.
	require.NoError(t, AddWindowRow(exec, 0, []*vector.Vector{input}))
	require.NoError(t, AddWindowRow(exec, 1, []*vector.Vector{input}))
	require.NoError(t, RemoveWindowRow(exec, 1, []*vector.Vector{input}))
	require.NoError(t, RemoveWindowRow(exec, 0, []*vector.Vector{input}))

	results, err := exec.Flush()
	require.NoError(t, err)
	defer results[0].Free(mp)
	require.True(t, results[0].IsNull(0))
	require.Equal(t, int64(0), vector.MustFixedColNoTypeCheck[int64](results[0])[0])
}

func TestWindowSlidingSumConstantInputAndEmptyRemoval(t *testing.T) {
	mp := mpool.MustNewZero()
	typ := types.T_int32.ToType()
	input, err := vector.NewConstFixed(typ, int32(7), 2, mp)
	require.NoError(t, err)
	defer input.Free(mp)

	exec, err := MakeAgg(mp, AggIdOfSum, false, typ)
	require.NoError(t, err)
	defer exec.Free()
	require.NoError(t, exec.GroupGrow(1))

	// A constant vector stores only row zero. Sliding add/remove must normalize
	// the logical row index and restore the empty-frame NULL state afterwards.
	require.NoError(t, AddWindowRow(exec, 1, []*vector.Vector{input}))
	require.NoError(t, RemoveWindowRow(exec, 1, []*vector.Vector{input}))
	require.Error(t, RemoveWindowRow(exec, 1, []*vector.Vector{input}))

	results, err := exec.Flush()
	require.NoError(t, err)
	defer results[0].Free(mp)
	require.True(t, results[0].IsNull(0))
}

func TestSumAvgBulkFillPreservesBatchFillOverflowSemantics(t *testing.T) {
	mp := mpool.MustNewZero()
	typ := types.T_int64.ToType()
	seed := testutil.NewInt64Vector(1, typ, mp, false, nil, []int64{math.MaxInt64 - 1})
	delta := testutil.NewInt64Vector(3, typ, mp, false, nil, []int64{1, 1, -2})
	defer seed.Free(mp)
	defer delta.Free(mp)

	testCases := []struct {
		name     string
		makeExec func(t *testing.T, mp *mpool.MPool, typ types.Type) AggFuncExec
	}{
		{"sum", makeSumExec},
		{"avg", makeAvgExec},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			batch := tc.makeExec(t, mp, typ)
			bulk := tc.makeExec(t, mp, typ)
			defer batch.Free()
			defer bulk.Free()

			require.NoError(t, batch.GroupGrow(1))
			require.NoError(t, bulk.GroupGrow(1))
			require.NoError(t, batch.BatchFill(0, []uint64{1}, []*vector.Vector{seed}))
			require.NoError(t, bulk.BatchFill(0, []uint64{1}, []*vector.Vector{seed}))

			require.NoError(t, batch.BatchFill(0, []uint64{1, 1, 1}, []*vector.Vector{delta}))
			require.NoError(t, bulk.BulkFill(0, []*vector.Vector{delta}))
			requireSingleAggResultEqual(t, mp, batch, bulk, "")
		})
	}
}

func TestAvgDecimal256FinalizationPrecisionOverflow(t *testing.T) {
	param := types.New(types.T_decimal256, 65, 10)
	testCases := []struct {
		name  string
		value string
	}{
		{
			name:  "positive",
			value: "100000000000000000000000000000000000000000000000000000.0000000000",
		},
		{
			name:  "negative",
			value: "-100000000000000000000000000000000000000000000000000000.0000000000",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			value, err := types.ParseDecimal256(tc.value, param.Width, param.Scale)
			require.NoError(t, err)

			vec := buildDecimal256Vector(t, mp, param, nil, []types.Decimal256{value})
			defer vec.Free(mp)
			exec := makeAvgExec(t, mp, param)
			defer exec.Free()
			require.NoError(t, exec.GroupGrow(1))
			require.NoError(t, exec.BatchFill(0, []uint64{1}, []*vector.Vector{vec}))

			results, err := exec.Flush()
			defer func() {
				for _, result := range results {
					result.Free(mp)
				}
			}()
			require.Nil(t, results)
			require.ErrorContains(t, err, "Decimal256(65,14)")
		})
	}
}

func TestSumAvgDecimal256BulkFillPreservesBatchFillOverflowSemantics(t *testing.T) {
	mp := mpool.MustNewZero()
	typ := types.New(types.T_decimal256, 76, 0)
	one := types.Decimal256FromInt64(1)
	two := types.Decimal256FromInt64(2)
	max := types.Decimal256{
		B0_63:    ^uint64(0),
		B64_127:  ^uint64(0),
		B128_191: ^uint64(0),
		B192_255: ^(uint64(1) << 63),
	}
	seedVal, err := max.Sub256(one)
	require.NoError(t, err)

	seed := buildDecimal256Vector(t, mp, typ, nil, []types.Decimal256{seedVal})
	delta := buildDecimal256Vector(t, mp, typ, nil, []types.Decimal256{one, one, two.Minus()})
	defer seed.Free(mp)
	defer delta.Free(mp)

	testCases := []struct {
		name       string
		isSum      bool
		aggID      int64
		flushError string
	}{
		{name: "sum", isSum: true, aggID: AggIdOfSum},
		{name: "avg", aggID: AggIdOfAvg, flushError: "Decimal256 Div overflow"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			batch := newSumAvgDecExec[types.Decimal256, types.Decimal256](mp, tc.isSum, tc.aggID, false, typ)
			bulk := newSumAvgDecExec[types.Decimal256, types.Decimal256](mp, tc.isSum, tc.aggID, false, typ)
			defer batch.Free()
			defer bulk.Free()

			require.NoError(t, batch.GroupGrow(1))
			require.NoError(t, bulk.GroupGrow(1))
			require.NoError(t, batch.BatchFill(0, []uint64{1}, []*vector.Vector{seed}))
			require.NoError(t, bulk.BatchFill(0, []uint64{1}, []*vector.Vector{seed}))

			require.NoError(t, batch.BatchFill(0, []uint64{1, 1, 1}, []*vector.Vector{delta}))
			require.NoError(t, bulk.BulkFill(0, []*vector.Vector{delta}))
			requireSingleAggResultEqual(t, mp, batch, bulk, tc.flushError)
		})
	}
}

func TestSumAvgBulkFillIntermediateRoundTrip(t *testing.T) {
	mp := mpool.MustNewZero()
	typs, vecs, nvecs := buildNumericTestDataVecs(t, mp)

	testCases := []struct {
		name     string
		makeExec func(t *testing.T, mp *mpool.MPool, typ types.Type) AggFuncExec
		expected *expectedSumAvg
	}{
		{"sum", makeSumExec, newExpectedSumAvg(111, 53, 58, 222000)},
		{"avg", makeAvgExec, newExpectedSumAvg(6.1666666666, 5.88888888, 6.4444444444, 126000)},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			for i, typ := range typs {
				curNB := mp.CurrNB()

				left := tc.makeExec(t, mp, typ)
				right := tc.makeExec(t, mp, typ)
				left.GetOptResult().modifyChunkSize(1)
				right.GetOptResult().modifyChunkSize(1)
				require.NoError(t, left.GroupGrow(1))
				require.NoError(t, right.GroupGrow(1))
				require.NoError(t, left.BulkFill(0, vecs[i:i+1]))
				require.NoError(t, right.BulkFill(0, nvecs[i:i+1]))

				var leftBuf, rightBuf bytes.Buffer
				require.NoError(t, left.SaveIntermediateResult(1, [][]uint8{{1}}, &leftBuf))
				require.NoError(t, right.SaveIntermediateResult(1, [][]uint8{{1}}, &rightBuf))

				restoredLeft := tc.makeExec(t, mp, typ)
				restoredRight := tc.makeExec(t, mp, typ)
				restoredLeft.GetOptResult().modifyChunkSize(1)
				restoredRight.GetOptResult().modifyChunkSize(1)
				require.NoError(t, restoredLeft.UnmarshalFromReader(bytes.NewReader(leftBuf.Bytes()), mp))
				require.NoError(t, restoredRight.UnmarshalFromReader(bytes.NewReader(rightBuf.Bytes()), mp))
				require.NoError(t, restoredLeft.Merge(restoredRight, 0, 0))

				results, err := restoredLeft.Flush()
				require.NoError(t, err)
				require.Len(t, results, 1)
				require.NoError(t, tc.expected.expected.checkVecAt(results[0], 0))

				for _, result := range results {
					result.Free(mp)
				}
				left.Free()
				right.Free()
				restoredLeft.Free()
				restoredRight.Free()
				require.Equal(t, curNB, mp.CurrNB())
			}
		})
	}
}

func TestSumAvgBigIntOverflowUsesDecimal128State(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	cases := []struct {
		name       string
		isSum      bool
		typ        types.Type
		vec        *vector.Vector
		want       string
		wantScale  int32
		wantRetTyp types.T
	}{
		{
			name:       "sum_int64_positive_over_int64",
			isSum:      true,
			typ:        types.T_int64.ToType(),
			vec:        testutil.NewInt64Vector(3, types.T_int64.ToType(), mp, false, nil, []int64{math.MaxInt64, 1, -2}),
			want:       "9223372036854775806",
			wantScale:  0,
			wantRetTyp: types.T_decimal128,
		},
		{
			name:       "sum_int64_negative_under_int64",
			isSum:      true,
			typ:        types.T_int64.ToType(),
			vec:        testutil.NewInt64Vector(3, types.T_int64.ToType(), mp, false, nil, []int64{math.MinInt64, -1, 2}),
			want:       "-9223372036854775807",
			wantScale:  0,
			wantRetTyp: types.T_decimal128,
		},
		{
			name:       "sum_uint64_over_uint64",
			isSum:      true,
			typ:        types.T_uint64.ToType(),
			vec:        testutil.NewUInt64Vector(3, types.T_uint64.ToType(), mp, false, nil, []uint64{1, math.MaxUint64, 3}),
			want:       "18446744073709551619",
			wantScale:  0,
			wantRetTyp: types.T_decimal128,
		},
		{
			name:       "avg_int64_positive_over_int64",
			isSum:      false,
			typ:        types.T_int64.ToType(),
			vec:        testutil.NewInt64Vector(3, types.T_int64.ToType(), mp, false, nil, []int64{math.MaxInt64, 1, -2}),
			want:       "3074457345618258602.0000",
			wantScale:  4,
			wantRetTyp: types.T_decimal128,
		},
		{
			name:       "avg_int64_negative_under_int64",
			isSum:      false,
			typ:        types.T_int64.ToType(),
			vec:        testutil.NewInt64Vector(3, types.T_int64.ToType(), mp, false, nil, []int64{math.MinInt64, -1, 2}),
			want:       "-3074457345618258602.3333",
			wantScale:  4,
			wantRetTyp: types.T_decimal128,
		},
		{
			name:       "avg_uint64_over_uint64",
			isSum:      false,
			typ:        types.T_uint64.ToType(),
			vec:        testutil.NewUInt64Vector(3, types.T_uint64.ToType(), mp, false, nil, []uint64{1, math.MaxUint64, 3}),
			want:       "6148914691236517206.3333",
			wantScale:  4,
			wantRetTyp: types.T_decimal128,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			curNB := mp.CurrNB()
			aggID := AggIdOfSum
			if !tc.isSum {
				aggID = AggIdOfAvg
			}
			exec := makeSumAvgExec(mp, tc.isSum, aggID, false, tc.typ)
			require.NoError(t, exec.GroupGrow(1))
			require.NoError(t, exec.BatchFill(0, []uint64{1, 1, 1}, []*vector.Vector{tc.vec}))

			results, err := exec.Flush()
			require.NoError(t, err)
			require.Len(t, results, 1)
			require.Equal(t, tc.wantRetTyp, results[0].GetType().Oid)
			require.Equal(t, tc.wantScale, results[0].GetType().Scale)

			got := vector.MustFixedColNoTypeCheck[types.Decimal128](results[0])[0]
			want, err := types.ParseDecimal128(tc.want, 38, tc.wantScale)
			require.NoError(t, err)
			require.Equal(t, want, got)

			exec.Free()
			for _, result := range results {
				result.Free(mp)
			}
			tc.vec.Free(mp)
			require.Equal(t, curNB, mp.CurrNB())
		})
	}
}

func TestSumAvgDistinctBigIntOverflowUsesDecimal128State(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	cases := []struct {
		name       string
		isSum      bool
		typ        types.Type
		vec        *vector.Vector
		want       string
		wantScale  int32
		wantRetTyp types.T
	}{
		{
			name:       "sum_distinct_int64_positive_over_int64",
			isSum:      true,
			typ:        types.T_int64.ToType(),
			vec:        testutil.NewInt64Vector(4, types.T_int64.ToType(), mp, false, nil, []int64{math.MaxInt64, 1, 1, -2}),
			want:       "9223372036854775806",
			wantScale:  0,
			wantRetTyp: types.T_decimal128,
		},
		{
			name:       "sum_distinct_uint64_over_uint64",
			isSum:      true,
			typ:        types.T_uint64.ToType(),
			vec:        testutil.NewUInt64Vector(4, types.T_uint64.ToType(), mp, false, nil, []uint64{1, math.MaxUint64, 1, 3}),
			want:       "18446744073709551619",
			wantScale:  0,
			wantRetTyp: types.T_decimal128,
		},
		{
			name:       "avg_distinct_int64_negative_under_int64",
			isSum:      false,
			typ:        types.T_int64.ToType(),
			vec:        testutil.NewInt64Vector(4, types.T_int64.ToType(), mp, false, nil, []int64{math.MinInt64, -1, -1, 2}),
			want:       "-3074457345618258602.3333",
			wantScale:  4,
			wantRetTyp: types.T_decimal128,
		},
		{
			name:       "avg_distinct_uint64_over_uint64",
			isSum:      false,
			typ:        types.T_uint64.ToType(),
			vec:        testutil.NewUInt64Vector(4, types.T_uint64.ToType(), mp, false, nil, []uint64{1, math.MaxUint64, 1, 3}),
			want:       "6148914691236517206.3333",
			wantScale:  4,
			wantRetTyp: types.T_decimal128,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			curNB := mp.CurrNB()
			aggID := AggIdOfSum
			if !tc.isSum {
				aggID = AggIdOfAvg
			}
			exec := makeSumAvgExec(mp, tc.isSum, aggID, true, tc.typ)
			require.NoError(t, exec.GroupGrow(1))
			require.NoError(t, exec.BatchFill(0, []uint64{1, 1, 1, 1}, []*vector.Vector{tc.vec}))

			results, err := exec.Flush()
			require.NoError(t, err)
			require.Len(t, results, 1)
			require.Equal(t, tc.wantRetTyp, results[0].GetType().Oid)
			require.Equal(t, tc.wantScale, results[0].GetType().Scale)

			got := vector.MustFixedColNoTypeCheck[types.Decimal128](results[0])[0]
			want, err := types.ParseDecimal128(tc.want, 38, tc.wantScale)
			require.NoError(t, err)
			require.Equal(t, want, got)

			exec.Free()
			for _, result := range results {
				result.Free(mp)
			}
			tc.vec.Free(mp)
			require.Equal(t, curNB, mp.CurrNB())
		})
	}
}

func TestAvgBigIntColumnScaleMinusOneUsesDecimalScaleZero(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	cases := []struct {
		name string
		typ  types.Type
		vec  *vector.Vector
		want string
	}{
		{
			name: "int64",
			typ:  types.T_int64.ToTypeWithScale(-1),
			vec:  testutil.NewInt64Vector(2, types.T_int64.ToTypeWithScale(-1), mp, false, nil, []int64{2, 2}),
			want: "2.0000",
		},
		{
			name: "uint64",
			typ:  types.T_uint64.ToTypeWithScale(-1),
			vec:  testutil.NewUInt64Vector(2, types.T_uint64.ToTypeWithScale(-1), mp, false, nil, []uint64{3, 4}),
			want: "3.5000",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			curNB := mp.CurrNB()
			exec := makeSumAvgExec(mp, false, AggIdOfAvg, false, tc.typ)
			require.NoError(t, exec.GroupGrow(1))
			require.NoError(t, exec.BatchFill(0, []uint64{1, 1}, []*vector.Vector{tc.vec}))

			results, err := exec.Flush()
			require.NoError(t, err)
			require.Len(t, results, 1)
			require.Equal(t, types.T_decimal128, results[0].GetType().Oid)
			require.Equal(t, int32(4), results[0].GetType().Scale)

			got := vector.MustFixedColNoTypeCheck[types.Decimal128](results[0])[0]
			want, err := types.ParseDecimal128(tc.want, 38, 4)
			require.NoError(t, err)
			require.Equal(t, want, got)

			exec.Free()
			for _, result := range results {
				result.Free(mp)
			}
			tc.vec.Free(mp)
			require.Equal(t, curNB, mp.CurrNB())
		})
	}
}

func TestSumBigIntMergeOverflowUsesDecimal128State(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	cases := []struct {
		name string
		typ  types.Type
		vec1 *vector.Vector
		vec2 *vector.Vector
		want string
	}{
		{
			name: "int64_merge_positive_over_int64",
			typ:  types.T_int64.ToType(),
			vec1: testutil.NewInt64Vector(1, types.T_int64.ToType(), mp, false, nil, []int64{math.MaxInt64}),
			vec2: testutil.NewInt64Vector(1, types.T_int64.ToType(), mp, false, nil, []int64{1}),
			want: "9223372036854775808",
		},
		{
			name: "uint64_merge_over_uint64",
			typ:  types.T_uint64.ToType(),
			vec1: testutil.NewUInt64Vector(1, types.T_uint64.ToType(), mp, false, nil, []uint64{math.MaxUint64}),
			vec2: testutil.NewUInt64Vector(1, types.T_uint64.ToType(), mp, false, nil, []uint64{1}),
			want: "18446744073709551616",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			curNB := mp.CurrNB()
			left := makeSumAvgExec(mp, true, AggIdOfSum, false, tc.typ)
			right := makeSumAvgExec(mp, true, AggIdOfSum, false, tc.typ)
			require.NoError(t, left.GroupGrow(1))
			require.NoError(t, right.GroupGrow(1))
			require.NoError(t, left.BatchFill(0, []uint64{1}, []*vector.Vector{tc.vec1}))
			require.NoError(t, right.BatchFill(0, []uint64{1}, []*vector.Vector{tc.vec2}))
			require.NoError(t, left.BatchMerge(right, 0, []uint64{1}))

			results, err := left.Flush()
			require.NoError(t, err)
			require.Len(t, results, 1)
			require.Equal(t, types.T_decimal128, results[0].GetType().Oid)
			require.Equal(t, int32(0), results[0].GetType().Scale)

			got := vector.MustFixedColNoTypeCheck[types.Decimal128](results[0])[0]
			want, err := types.ParseDecimal128(tc.want, 38, 0)
			require.NoError(t, err)
			require.Equal(t, want, got)

			left.Free()
			right.Free()
			for _, result := range results {
				result.Free(mp)
			}
			tc.vec1.Free(mp)
			tc.vec2.Free(mp)
			require.Equal(t, curNB, mp.CurrNB())
		})
	}
}

func testSumAvg(t *testing.T,
	makeSumAvgExec func(t *testing.T, mp *mpool.MPool, typ types.Type) AggFuncExec,
	expected *expectedSumAvg) {

	mp := mpool.MustNewZero()
	typs, vecs, nvecs := buildNumericTestDataVecs(t, mp)

	t.Run("BulkFill", func(t *testing.T) {
		for i, typ := range typs {
			curNB := mp.CurrNB()
			exec := makeSumAvgExec(t, mp, typ)
			exec.GetOptResult().modifyChunkSize(1)
			require.NoError(t, exec.GroupGrow(1))
			require.NoError(t, exec.BulkFill(0, vecs[i:i+1]))
			require.NoError(t, exec.BulkFill(0, nvecs[i:i+1]))
			results, err := exec.Flush()
			require.NoError(t, err)
			require.Len(t, results, 1)

			require.NoError(t, expected.expected.checkVecAt(results[0], 0))

			exec.Free()
			for _, result := range results {
				result.Free(mp)
			}
			require.Equal(t, curNB, mp.CurrNB())
		}
	})

	t.Run("BatchFill1", func(t *testing.T) {
		for i, typ := range typs {
			curNB := mp.CurrNB()
			exec := makeSumAvgExec(t, mp, typ)
			require.NoError(t, exec.GroupGrow(1))

			require.NoError(t, exec.BatchFill(0, []uint64{1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, vecs[i:i+1]))
			require.NoError(t, exec.BatchFill(0, []uint64{1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, nvecs[i:i+1]))

			results, err := exec.Flush()
			require.NoError(t, err)
			require.Len(t, results, 1)

			require.NoError(t, expected.expected.checkVecAt(results[0], 0))
			exec.Free()
			for _, result := range results {
				result.Free(mp)
			}
			require.Equal(t, curNB, mp.CurrNB())
		}
	})

	t.Run("BatchFill2", func(t *testing.T) {
		for i, typ := range typs {
			curNB := mp.CurrNB()
			exec := makeSumAvgExec(t, mp, typ)
			require.NoError(t, exec.GroupGrow(2))

			require.NoError(t, exec.BatchFill(0, []uint64{1, 2, 1, 2, 1, 2, 1, 2, 1, 2}, vecs[i:i+1]))
			require.NoError(t, exec.BatchFill(0, []uint64{1, 2, 1, 2, 1, 2, 1, 2, 1, 2}, nvecs[i:i+1]))

			results, err := exec.Flush()
			require.NoError(t, err)
			require.Len(t, results, 1)

			require.NoError(t, checkVecAll(results[0], expected.b2[:]))

			exec.Free()
			for _, result := range results {
				result.Free(mp)
			}
			require.Equal(t, curNB, mp.CurrNB())
		}
	})

	t.Run("BatchFill20000", func(t *testing.T) {
		for i, typ := range typs {
			curNB := mp.CurrNB()
			exec := makeSumAvgExec(t, mp, typ)
			// grow twice, so we have 20000 groups.
			require.NoError(t, exec.GroupGrow(10000))
			require.NoError(t, exec.GroupGrow(10000))

			for j := 0; j < 2000; j++ {
				groups := make([]uint64, 10)
				for k := range groups {
					groups[k] = uint64(j*10 + k + 1)
				}

				require.NoError(t, exec.BatchFill(0, groups[:5], vecs[i:i+1]))
				require.NoError(t, exec.BatchFill(5, groups[5:], vecs[i:i+1]))
				require.NoError(t, exec.BatchFill(0, groups[:5], nvecs[i:i+1]))
				require.NoError(t, exec.BatchFill(5, groups[5:], nvecs[i:i+1]))
			}

			results, err := exec.Flush()
			require.NoError(t, err)
			require.Equal(t, 3, len(results))
			require.Equal(t, 8192, results[0].Length())
			require.Equal(t, 8192, results[1].Length())
			require.Equal(t, 3616, results[2].Length())
			require.NoError(t, expected.expected20k.checkVecSum(results))

			for _, result := range results {
				result.Free(mp)
			}
			exec.Free()
			require.Equal(t, curNB, mp.CurrNB())
		}
	})
}

func (e *expectedResult) checkVecSum(vecs []*vector.Vector) error {
	var fsum float64

	for _, vec := range vecs {
		typ := vec.GetType()
		switch typ.Oid {
		case types.T_int64:
			vals := vector.MustFixedColNoTypeCheck[int64](vec)
			var sum int64 = 0
			for _, val := range vals {
				sum += val
			}
			fsum += float64(sum)
		case types.T_float64:
			vals := vector.MustFixedColNoTypeCheck[float64](vec)
			sum := 0.0
			for _, val := range vals {
				sum += val
			}
			fsum += sum
		case types.T_decimal128:
			vals := vector.MustFixedColNoTypeCheck[types.Decimal128](vec)
			sum := types.Decimal128{B0_63: 0, B64_127: 0}
			var err error
			for _, val := range vals {
				sum, err = sum.Add128(val)
				if err != nil {
					return err
				}
			}
			fsum += types.Decimal128ToFloat64(sum, typ.Scale)
		case types.T_decimal256:
			vals := vector.MustFixedColNoTypeCheck[types.Decimal256](vec)
			sum := types.Decimal256{}
			var err error
			for _, val := range vals {
				sum, err = sum.Add256(val)
				if err != nil {
					return err
				}
			}
			fsum += types.Decimal256ToFloat64(sum, typ.Scale)
		default:
			return moerr.NewInternalErrorNoCtxf("unsupported type %s", typ.Oid)
		}
	}

	if math.Abs(e.expected-fsum) > 1e-6 {
		return moerr.NewInternalErrorNoCtxf("expected %f, got %f", e.expected, fsum)
	}
	return nil
}
