// Copyright 2022 Matrix Origin
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
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vectorize/moarray"
	"github.com/stretchr/testify/require"
)

// makeConstArrayVec creates a constant vector holding a single array value repeated length times.
func makeConstArrayVec[T types.RealNumbers](t *testing.T, mp *mpool.MPool, arr []T, length int) *vector.Vector {
	t.Helper()
	b := types.ArrayToBytes[T](arr)
	v, err := vector.NewConstBytes(types.T_array_float32.ToType(), b, length, mp)
	require.NoError(t, err)
	return v
}

// makeConstArrayVec64 is the float64 variant.
func makeConstArrayVec64(t *testing.T, mp *mpool.MPool, arr []float64, length int) *vector.Vector {
	t.Helper()
	b := types.ArrayToBytes[float64](arr)
	v, err := vector.NewConstBytes(types.T_array_float64.ToType(), b, length, mp)
	require.NoError(t, err)
	return v
}

// makeColArrayVec creates a column vector holding one array per row.
func makeColArrayVec[T types.RealNumbers](t *testing.T, mp *mpool.MPool, typ types.Type, rows [][]T) *vector.Vector {
	t.Helper()
	v := vector.NewVec(typ)
	for _, row := range rows {
		require.NoError(t, vector.AppendBytes(v, types.ArrayToBytes[T](row), false, mp))
	}
	return v
}

// approxEqF32 checks the stable float64 batch result against the historical
// float32-shaped expectations used by these ordinary-value tests.
func approxEqF32(a float64, b float32) bool {
	if a == float64(b) {
		return true
	}
	diff := math.Abs(a - float64(b))
	avg := math.Abs((a + float64(b)) / 2.0)
	if avg < 1e-9 {
		return diff < 1e-5
	}
	return diff/avg < 1e-4
}

// TestBatchArrayDistanceSync_L2Sq verifies batchArrayDistanceSync with Metric_L2sqDistance
// on a small const-vs-column input (always CPU path).
func TestBatchArrayDistanceSync_L2Sq(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	query := []float32{1, 0, 0}
	rows := [][]float32{{1, 0, 0}, {0, 1, 0}, {0, 0, 1}, {1, 1, 0}}
	N := len(rows)
	// expected: ||query - row||²
	want := []float32{0, 2, 2, 1}

	constVec := makeConstArrayVec[float32](t, mp, query, N)
	colVec := makeColArrayVec[float32](t, mp, types.T_array_float32.ToType(), rows)

	dist, ok, err := batchArrayDistanceSync[float32](
		[]*vector.Vector{constVec, colVec}, N, metric.Metric_L2sqDistance, nil, nil)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, N, len(dist))
	for i, w := range want {
		require.True(t, approxEqF32(dist[i], w), "row %d: got %v want %v", i, dist[i], w)
	}
}

// TestBatchArrayDistanceSync_L2 verifies batchArrayDistanceSync with Metric_L2Distance.
func TestBatchArrayDistanceSync_L2(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	query := []float32{1, 0, 0}
	rows := [][]float32{{1, 0, 0}, {0, 1, 0}, {0, 0, 1}, {1, 1, 0}}
	N := len(rows)
	sqrt2 := float32(math.Sqrt(2))
	want := []float32{0, sqrt2, sqrt2, 1}

	constVec := makeConstArrayVec[float32](t, mp, query, N)
	colVec := makeColArrayVec[float32](t, mp, types.T_array_float32.ToType(), rows)

	dist, ok, err := batchArrayDistanceSync[float32](
		[]*vector.Vector{constVec, colVec}, N, metric.Metric_L2Distance, nil, nil)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, N, len(dist))
	for i, w := range want {
		require.True(t, approxEqF32(dist[i], w), "row %d: got %v want %v", i, dist[i], w)
	}
}

func TestBatchArrayDistanceSync_ExtremeFloat32UsesFloat64Kernel(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	query := []float32{3e38, 3e38}
	rows := [][]float32{{0, 0}, {3e38, 3e38}}
	constVec := makeConstArrayVec[float32](t, mp, query, len(rows))
	colVec := makeColArrayVec[float32](t, mp, types.T_array_float32.ToType(), rows)

	dist, ok, err := batchArrayDistanceSync[float32]([]*vector.Vector{constVec, colVec}, len(rows), metric.Metric_L2Distance, nil, nil)
	require.NoError(t, err)
	require.True(t, ok)
	require.InEpsilon(t, float64(query[0])*math.Sqrt2, dist[0], 1e-14)
	require.Equal(t, float64(0), dist[1])

	rows = [][]float32{{1, 1}, {3e38, 3e38}}
	colVec = makeColArrayVec[float32](t, mp, types.T_array_float32.ToType(), rows)
	dist, ok, err = batchArrayDistanceSync[float32]([]*vector.Vector{constVec, colVec}, len(rows), metric.Metric_InnerProduct, nil, nil)
	require.NoError(t, err)
	require.True(t, ok)
	require.InEpsilon(t, -float64(query[0])*2, dist[0], 1e-14)
	require.Equal(t, float64(-2)*float64(query[0])*float64(query[0]), dist[1])
}

// TestBatchArrayDistanceSync_InnerProduct verifies Metric_InnerProduct.
// The function returns -dot_product (negated for ANN ordering).
func TestBatchArrayDistanceSync_InnerProduct(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	query := []float32{1, 0, 0}
	rows := [][]float32{{1, 0, 0}, {0, 1, 0}, {0, 0, 1}, {1, 1, 0}}
	N := len(rows)
	// -dot(query, row)
	want := []float32{-1, 0, 0, -1}

	constVec := makeConstArrayVec[float32](t, mp, query, N)
	colVec := makeColArrayVec[float32](t, mp, types.T_array_float32.ToType(), rows)

	dist, ok, err := batchArrayDistanceSync[float32](
		[]*vector.Vector{constVec, colVec}, N, metric.Metric_InnerProduct, nil, nil)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, N, len(dist))
	for i, w := range want {
		require.True(t, approxEqF32(dist[i], w), "row %d: got %v want %v", i, dist[i], w)
	}
}

func TestCosineDistanceArrayScaleInvariance(t *testing.T) {
	t.Run("float32", func(t *testing.T) {
		testCosineArrayScaleInvariance[float32](t, types.T_array_float32.ToType(), 1e-20, 1e20, true)
	})
	t.Run("float64", func(t *testing.T) {
		testCosineArrayScaleInvariance[float64](t, types.T_array_float64.ToType(), 1e-300, 1e300, false)
	})
}

func testCosineArrayScaleInvariance[T types.RealNumbers](t *testing.T, typ types.Type, tiny, large T, batchEligible bool) {
	t.Helper()
	for _, scale := range []struct {
		name string
		a, b T
	}{{"ordinary", 1, 1}, {"tiny_left", tiny, 1}, {"tiny_right", 1, tiny}, {"mixed_extremes", tiny, large}} {
		for _, shape := range []string{"const_column", "column_const", "column_column"} {
			t.Run(scale.name+"/"+shape, func(t *testing.T) {
				mp := mpool.MustNewZero()
				defer mpool.DeleteMPool(mp)
				query := []T{scale.a, scale.a, scale.a}
				rows := [][]T{{2 * scale.b, scale.b, scale.b}, {-2 * scale.b, -scale.b, -scale.b}, {0, 0, 0}}
				constVec, err := vector.NewConstBytes(typ, types.ArrayToBytes(query), len(rows), mp)
				require.NoError(t, err)
				defer constVec.Free(mp)
				colVec := makeColArrayVec(t, mp, typ, rows)
				defer colVec.Free(mp)
				inputs := []*vector.Vector{constVec, colVec}
				if shape == "column_const" {
					inputs = []*vector.Vector{colVec, constVec}
				} else if shape == "column_column" {
					queryCol := makeColArrayVec(t, mp, typ, [][]T{query, query, query})
					defer queryCol.Free(mp)
					inputs = []*vector.Vector{queryCol, colVec}
				}
				_, ok, err := batchArrayDistanceSync[T](inputs, len(rows), metric.Metric_CosineDistance, nil, nil)
				require.NoError(t, err)
				require.Equal(t, batchEligible && shape != "column_column", ok)
				result := vector.NewFunctionResultWrapper(types.T_float64.ToType(), mp)
				defer result.Free()
				require.NoError(t, result.PreExtendAndReset(len(rows)))
				require.NoError(t, CosineDistanceArray[T](inputs, result, nil, len(rows), nil))
				out := result.GetResultVector()
				require.Equal(t, len(rows), out.Length())
				require.Equal(t, types.T_float64, out.GetType().Oid)
				require.False(t, out.GetNulls().Any())
				// Independent angle oracle; agreement between two wrong paths is insufficient.
				want := []float64{1 - 4/math.Sqrt(18), 1 + 4/math.Sqrt(18), 1}
				for i, d := range vector.MustFixedColNoTypeCheck[float64](out) {
					require.InDelta(t, want[i], d, 1e-14)
				}
			})
		}
	}
}

// TestBatchArrayDistanceSync_CosineDistance verifies Metric_CosineDistance.
func TestBatchArrayDistanceSync_CosineDistance(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	query := []float32{1, 0, 0}
	rows := [][]float32{{1, 0, 0}, {0, 1, 0}, {0, 0, 1}, {1, 1, 0}}
	N := len(rows)
	// 1 - cosine_similarity
	oneMinusInvSqrt2 := float32(1.0 - 1.0/math.Sqrt(2))
	want := []float32{0, 1, 1, oneMinusInvSqrt2}

	constVec := makeConstArrayVec[float32](t, mp, query, N)
	colVec := makeColArrayVec[float32](t, mp, types.T_array_float32.ToType(), rows)

	dist, ok, err := batchArrayDistanceSync[float32](
		[]*vector.Vector{constVec, colVec}, N, metric.Metric_CosineDistance, nil, nil)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, N, len(dist))
	for i, w := range want {
		require.True(t, approxEqF32(dist[i], w), "row %d: got %v want %v", i, dist[i], w)
	}
}

// TestBatchArrayDistanceSync_QueryAsSecondArg verifies that the query vector
// can be in ivecs[1] (column in ivecs[0], const in ivecs[1]).
func TestBatchArrayDistanceSync_QueryAsSecondArg(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	query := []float32{1, 0, 0}
	rows := [][]float32{{1, 0, 0}, {0, 1, 0}, {0, 0, 1}, {1, 1, 0}}
	N := len(rows)
	want := []float32{0, 2, 2, 1}

	colVec := makeColArrayVec[float32](t, mp, types.T_array_float32.ToType(), rows)
	constVec := makeConstArrayVec[float32](t, mp, query, N)

	// Note: const is ivecs[1], column is ivecs[0]
	dist, ok, err := batchArrayDistanceSync[float32](
		[]*vector.Vector{colVec, constVec}, N, metric.Metric_L2sqDistance, nil, nil)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, N, len(dist))
	for i, w := range want {
		require.True(t, approxEqF32(dist[i], w), "row %d: got %v want %v", i, dist[i], w)
	}
}

// TestBatchArrayDistanceSync_Float64Declines: the stable SQL batch currently keeps the
// existing native-float32 fast-path scope. Float64 stays on the per-row path so its
// established selection, null, and dimension behavior remains unchanged.
func TestBatchArrayDistanceSync_Float64Declines(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	query := []float64{1, 0, 0}
	rows := [][]float64{{1, 0, 0}, {0, 1, 0}, {0, 0, 1}, {1, 1, 0}}
	N := len(rows)

	constVec := makeConstArrayVec64(t, mp, query, N)
	colVec := makeColArrayVec[float64](t, mp, types.T_array_float64.ToType(), rows)

	_, ok, err := batchArrayDistanceSync[float64](
		[]*vector.Vector{constVec, colVec}, N, metric.Metric_L2sqDistance, nil, nil)
	require.NoError(t, err)
	require.False(t, ok, "float64 must fall through to the per-row kernel")
}

// TestBatchArrayDistanceSync_Float64PrecisionMatchesScalar protects the scalar fallback:
// 16777217 is the first integer float32 cannot represent, but the SQL result is DOUBLE.
func TestBatchArrayDistanceSync_Float64PrecisionMatchesScalar(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	const beyondF32 = 16777217.0
	for _, tc := range []struct {
		name string
		m    metric.MetricType
		want float64
	}{
		{"l1", metric.Metric_L1Distance, beyondF32},
		{"l2", metric.Metric_L2Distance, beyondF32},
	} {
		t.Run(tc.name, func(t *testing.T) {
			constVec := makeConstArrayVec64(t, mp, []float64{beyondF32}, 1)
			colVec := makeColArrayVec[float64](t, mp, types.T_array_float64.ToType(), [][]float64{{0}})

			_, ok, err := batchArrayDistanceSync[float64](
				[]*vector.Vector{constVec, colVec}, 1, tc.m, nil, nil)
			require.NoError(t, err)
			require.False(t, ok)

			// What the per-row path then computes -- the exact value, not 16777216.
			var got float64
			switch tc.m {
			case metric.Metric_L1Distance:
				got, err = moarray.L1Distance[float64]([]float64{0}, []float64{beyondF32})
			default:
				got, err = moarray.L2Distance[float64]([]float64{0}, []float64{beyondF32})
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
			require.NotEqual(t, float64(float32(tc.want)), got, "float32 rounding must be observable here")
		})
	}
}

// TestBatchArrayDistanceSync_DimMismatchDeclines: the metric kernels report a dimension
// mismatch as ErrInternal, but the SQL contract for vector ops of differing dimensions is
// the per-row path's ErrInvalidInput naming both dimensions. Constness must not change the
// error either, so a mismatched row hands the whole batch back.
func TestBatchArrayDistanceSync_DimMismatchDeclines(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	constVec := makeConstArrayVec[float32](t, mp, []float32{1, 2}, 2)
	colVec := makeColArrayVec[float32](t, mp, types.T_array_float32.ToType(),
		[][]float32{{1, 2}, {1, 2, 3}}) // second row has the wrong dimension

	_, ok, err := batchArrayDistanceSync[float32](
		[]*vector.Vector{constVec, colVec}, 2, metric.Metric_L1Distance, nil, nil)
	require.NoError(t, err, "the batch path must not raise the kernel's internal error")
	require.False(t, ok)

	// The per-row path then produces the documented error.
	_, scalarErr := moarray.L1Distance[float32]([]float32{1, 2, 3}, []float32{1, 2})
	require.Error(t, scalarErr)
	require.Contains(t, scalarErr.Error(), "invalid input")
	require.Contains(t, scalarErr.Error(), "(3, 2)")
}

// TestBatchArrayDistanceSync_SelectListDeclines: the batch path evaluates every row and
// lets any row's error escape. When the expression framework has masked rows off, a masked
// row holding a bad dimension must be skipped rather than reported, so the batch path must
// stand down whenever it cannot evaluate everything.
func TestBatchArrayDistanceSync_SelectListDeclines(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	constVec := makeConstArrayVec[float32](t, mp, []float32{1, 2}, 2)
	colVec := makeColArrayVec[float32](t, mp, types.T_array_float32.ToType(),
		[][]float32{{3, 4}, {5, 6}})

	for _, tc := range []struct {
		name string
		sl   *FunctionSelectList
		want bool
	}{
		{"nil evaluates all", nil, true},
		{"no nulls evaluates all", &FunctionSelectList{}, true},
		{"partially masked", &FunctionSelectList{AnyNull: true, SelectList: []bool{true, false}}, false},
		{"all masked", &FunctionSelectList{AnyNull: true, AllNull: true, SelectList: []bool{false, false}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, ok, err := batchArrayDistanceSync[float32](
				[]*vector.Vector{constVec, colVec}, 2, metric.Metric_L1Distance, nil, tc.sl)
			require.NoError(t, err)
			require.Equal(t, tc.want, ok)
		})
	}
}

// TestBatchArrayDistanceSync_BothConst verifies that both-const input returns ok=false.
func TestBatchArrayDistanceSync_BothConst(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	b := types.ArrayToBytes[float32]([]float32{1, 0, 0})
	v0, err := vector.NewConstBytes(types.T_array_float32.ToType(), b, 4, mp)
	require.NoError(t, err)
	v1, err := vector.NewConstBytes(types.T_array_float32.ToType(), b, 4, mp)
	require.NoError(t, err)

	_, ok, err := batchArrayDistanceSync[float32](
		[]*vector.Vector{v0, v1}, 4, metric.Metric_L2sqDistance, nil, nil)
	require.NoError(t, err)
	require.False(t, ok, "both-const should return ok=false")
}

// TestBatchArrayDistanceSync_BothCol verifies that column-vs-column input returns ok=false.
func TestBatchArrayDistanceSync_BothCol(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	rows := [][]float32{{1, 0, 0}, {0, 1, 0}}
	v0 := makeColArrayVec[float32](t, mp, types.T_array_float32.ToType(), rows)
	v1 := makeColArrayVec[float32](t, mp, types.T_array_float32.ToType(), rows)

	_, ok, err := batchArrayDistanceSync[float32](
		[]*vector.Vector{v0, v1}, 2, metric.Metric_L2sqDistance, nil, nil)
	require.NoError(t, err)
	require.False(t, ok, "col-vs-col should return ok=false")
}

// TestBatchArrayDistanceSync_NullConst verifies that a null const vector returns ok=false.
func TestBatchArrayDistanceSync_NullConst(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	constVec := vector.NewConstNull(types.T_array_float32.ToType(), 4, mp)
	rows := [][]float32{{1, 0, 0}, {0, 1, 0}, {0, 0, 1}, {1, 1, 0}}
	colVec := makeColArrayVec[float32](t, mp, types.T_array_float32.ToType(), rows)

	_, ok, err := batchArrayDistanceSync[float32](
		[]*vector.Vector{constVec, colVec}, 4, metric.Metric_L2sqDistance, nil, nil)
	require.NoError(t, err)
	require.False(t, ok, "null const should return ok=false")
}

// TestBatchArrayDistanceSync_NullInColumn verifies that a column with nulls returns ok=false.
func TestBatchArrayDistanceSync_NullInColumn(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	query := []float32{1, 0, 0}
	constVec := makeConstArrayVec[float32](t, mp, query, 3)

	typ := types.T_array_float32.ToType()
	colVec := vector.NewVec(typ)
	require.NoError(t, vector.AppendBytes(colVec, types.ArrayToBytes[float32]([]float32{1, 0, 0}), false, mp))
	require.NoError(t, vector.AppendBytes(colVec, nil, true, mp)) // null row
	require.NoError(t, vector.AppendBytes(colVec, types.ArrayToBytes[float32]([]float32{0, 1, 0}), false, mp))

	_, ok, err := batchArrayDistanceSync[float32](
		[]*vector.Vector{constVec, colVec}, 3, metric.Metric_L2sqDistance, nil, nil)
	require.NoError(t, err)
	require.False(t, ok, "column with nulls should return ok=false")
}
