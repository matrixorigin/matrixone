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
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func buildTestDataVecs(t *testing.T, mp *mpool.MPool) ([]types.Type, []*vector.Vector, []*vector.Vector) {
	nulls := []bool{false, false, false, false, true, false, false, false, false, true}
	int8s := []int8{1, 2, 3, 4, 5, 6, 7, 8, 9, 0, -1, -2}
	int32s := []int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
	int64s := []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
	float32s := []float32{1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8, 9.9, 11.0, 12.1, 13.2}
	float64s := []float64{1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8, 9.9, 11.0, 12.1, 13.2}
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
	ss := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l"}
	strs := []string{strings.Repeat("a", 10),
		strings.Repeat("b", 20),
		strings.Repeat("c", 30),
		strings.Repeat("d", 40),
		strings.Repeat("e", 50),
		strings.Repeat("f", 60),
		strings.Repeat("g", 70),
		strings.Repeat("h", 80),
		strings.Repeat("i", 90),
		strings.Repeat("j", 100),
		strings.Repeat("k", 110),
		strings.Repeat("l", 120),
	}
	ts := []string{"2025-01-01 00:00:00.000000", "2025-01-01 01:00:00.000000", "2025-01-01 02:00:00.000000", "2025-01-01 03:00:00.000000", "2025-01-01 04:00:00.000000",
		"2025-01-01 05:00:00.000000", "2025-01-01 06:00:00.000000", "2025-01-01 07:00:00.000000", "2025-01-01 08:00:00.000000", "2025-01-01 09:00:00.000000",
		"2025-01-01 10:00:00.000000", "2025-01-01 11:00:00.000000"}

	typs := []types.Type{
		types.T_int8.ToType(),
		types.T_int32.ToType(),
		types.T_int64.ToType(),
		types.T_float32.ToType(),
		types.T_float64.ToType(),
		types.T_decimal64.ToType(),
		types.T_decimal128.ToType(),
		types.T_char.ToType(),
		types.T_varchar.ToType(),
		types.T_timestamp.ToType(),
	}
	vecs := make([]*vector.Vector, 10)
	nvecs := make([]*vector.Vector, 10)
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
	vecs[7] = testutil.NewStringVector(10, typs[7], mp, false, nil, ss[:10])
	nvecs[7] = testutil.NewStringVector(10, typs[7], mp, false, nulls, ss[2:])
	vecs[8] = testutil.NewStringVector(10, typs[8], mp, false, nil, strs[:10])
	nvecs[8] = testutil.NewStringVector(10, typs[8], mp, false, nulls, strs[2:])
	vecs[9] = testutil.NewTimestampVector(10, typs[9], mp, false, nil, ts[:10])
	nvecs[9] = testutil.NewTimestampVector(10, typs[9], mp, false, nulls, ts[2:])
	return typs, vecs, nvecs
}

func makeCountStarExec(t *testing.T, mp *mpool.MPool, typ types.Type) AggFuncExec {
	return newCountStarExec(mp, AggIdOfCountStar, false, []types.Type{typ})
}

func makeCountColumnExec(t *testing.T, mp *mpool.MPool, typ types.Type) AggFuncExec {
	return newCountColumnExec(mp, AggIdOfCountColumn, false, []types.Type{typ})
}

func makeCountColumnDistinctExec(t *testing.T, mp *mpool.MPool, typ types.Type) AggFuncExec {
	return newCountColumnExec(mp, AggIdOfCountColumn, true, []types.Type{typ})
}

type expectedCount struct {
	count    int64
	count20k int64
}

func TestCountStarExec(t *testing.T) {
	testAggExec(t, makeCountStarExec, expectedCount{count: 20, count20k: 40000})
}

func TestCountColumnExec(t *testing.T) {
	testAggExec(t, makeCountColumnExec, expectedCount{count: 18, count20k: 36000})
}

func TestCountColumnDistinctExec(t *testing.T) {
	testAggExec(t, makeCountColumnDistinctExec, expectedCount{count: 11, count20k: 36000})
}

// TestCountMultiColumnDistinct tests COUNT(DISTINCT col1, col2) with multiple args.
// This verifies the fix for issue #25284.
func TestCountMultiColumnDistinct(t *testing.T) {
	mp := mpool.MustNewZero()

	// Create two int64 columns.
	// col1: [1, 1, 1, 2, 2, 3, null, null, 4, 4]
	// col2: [null, 5, 5, null, 7, null, 10, null, null, 5]
	// MySQL COUNT(DISTINCT col1, col2) should count distinct non-null (col1, col2) pairs.
	// Non-null pairs: (1,5), (2,7), (4,5) — (1,5) appears twice, so distinct = 3
	col1 := testutil.NewInt64Vector(10, types.T_int64.ToType(), mp, false, []bool{false, false, false, false, false, false, true, true, false, false}, []int64{1, 1, 1, 2, 2, 3, 0, 0, 4, 4})
	col2 := testutil.NewInt64Vector(10, types.T_int64.ToType(), mp, false, []bool{true, false, false, true, false, true, false, true, true, false}, []int64{0, 5, 5, 0, 7, 0, 10, 0, 0, 5})
	defer col1.Free(mp)
	defer col2.Free(mp)

	t.Run("multi-int64-distinct", func(t *testing.T) {
		curNB := mp.CurrNB()
		exec := newCountColumnExec(mp, AggIdOfCountColumn, true,
			[]types.Type{types.T_int64.ToType(), types.T_int64.ToType()})
		require.NoError(t, exec.GroupGrow(1))

		// Pass both column vectors.
		require.NoError(t, exec.BatchFill(0, []uint64{1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, []*vector.Vector{col1, col2}))

		results, err := exec.Flush()
		require.NoError(t, err)
		require.Len(t, results, 1)
		vals := vector.MustFixedColNoTypeCheck[int64](results[0])
		require.Equal(t, int64(3), vals[0], "COUNT(DISTINCT col1, col2) should be 3: (1,5), (2,7), (4,5)")

		exec.Free()
		for _, result := range results {
			result.Free(mp)
		}
		require.Equal(t, curNB, mp.CurrNB())
	})

	t.Run("multi-int64-null-only", func(t *testing.T) {
		// All rows have at least one NULL column → no row should be counted.
		nullCol1 := testutil.NewInt64Vector(3, types.T_int64.ToType(), mp, false, []bool{false, true, false}, []int64{1, 0, 2})
		nullCol2 := testutil.NewInt64Vector(3, types.T_int64.ToType(), mp, false, []bool{true, false, true}, []int64{0, 5, 0})
		defer nullCol1.Free(mp)
		defer nullCol2.Free(mp)

		curNB := mp.CurrNB()
		exec := newCountColumnExec(mp, AggIdOfCountColumn, true,
			[]types.Type{types.T_int64.ToType(), types.T_int64.ToType()})
		require.NoError(t, exec.GroupGrow(1))

		require.NoError(t, exec.BatchFill(0, []uint64{1, 1, 1}, []*vector.Vector{nullCol1, nullCol2}))

		results, err := exec.Flush()
		require.NoError(t, err)
		vals := vector.MustFixedColNoTypeCheck[int64](results[0])
		require.Equal(t, int64(0), vals[0], "All rows have NULL, count should be 0")

		exec.Free()
		for _, result := range results {
			result.Free(mp)
		}
		require.Equal(t, curNB, mp.CurrNB())
	})

	t.Run("multi-two-group-distinct", func(t *testing.T) {
		curNB := mp.CurrNB()
		exec := newCountColumnExec(mp, AggIdOfCountColumn, true,
			[]types.Type{types.T_int64.ToType(), types.T_int64.ToType()})
		require.NoError(t, exec.GroupGrow(2))

		// Group 1: rows 0-4, Group 2: rows 5-9
		groups := []uint64{1, 1, 1, 1, 1, 2, 2, 2, 2, 2}
		require.NoError(t, exec.BatchFill(0, groups, []*vector.Vector{col1, col2}))

		results, err := exec.Flush()
		require.NoError(t, err)
		require.Len(t, results, 1)
		vals := vector.MustFixedColNoTypeCheck[int64](results[0])
		require.Equal(t, 2, len(vals))
		// Group 1 (first 5 rows): (1,null)→skip, (1,5), (1,5)→dup, (2,null)→skip, (2,7) → 2
		require.Equal(t, int64(2), vals[0])
		// Group 2 (last 5 rows): (3,null)→skip, (null,10)→skip, (null,null)→skip, (4,null)→skip, (4,5) → 1
		require.Equal(t, int64(1), vals[1])

		exec.Free()
		for _, result := range results {
			result.Free(mp)
		}
		require.Equal(t, curNB, mp.CurrNB())
	})

	t.Run("multi-merge-distinct", func(t *testing.T) {
		curNB := mp.CurrNB()

		colA1 := testutil.NewInt64Vector(3, types.T_int64.ToType(), mp, false, nil, []int64{1, 2, 3})
		colA2 := testutil.NewInt64Vector(3, types.T_int64.ToType(), mp, false, nil, []int64{10, 20, 30})
		colB1 := testutil.NewInt64Vector(3, types.T_int64.ToType(), mp, false, nil, []int64{3, 4, 5})
		colB2 := testutil.NewInt64Vector(3, types.T_int64.ToType(), mp, false, nil, []int64{30, 40, 50})
		defer colA1.Free(mp)
		defer colA2.Free(mp)
		defer colB1.Free(mp)
		defer colB2.Free(mp)

		execA := newCountColumnExec(mp, AggIdOfCountColumn, true,
			[]types.Type{types.T_int64.ToType(), types.T_int64.ToType()})
		execA.GetOptResult().modifyChunkSize(1)
		require.NoError(t, execA.GroupGrow(1))

		execB := newCountColumnExec(mp, AggIdOfCountColumn, true,
			[]types.Type{types.T_int64.ToType(), types.T_int64.ToType()})
		execB.GetOptResult().modifyChunkSize(1)
		require.NoError(t, execB.GroupGrow(1))

		require.NoError(t, execA.BatchFill(0, []uint64{1, 1, 1}, []*vector.Vector{colA1, colA2}))
		require.NoError(t, execB.BatchFill(0, []uint64{1, 1, 1}, []*vector.Vector{colB1, colB2}))

		execA.Merge(execB, 0, 0)
		results, err := execA.Flush()
		require.NoError(t, err)
		vals := vector.MustFixedColNoTypeCheck[int64](results[0])
		// (1,10),(2,20),(3,30) from A + (3,30) dup + (4,40),(5,50) from B = 5 distinct
		require.Equal(t, int64(5), vals[0])

		execA.Free()
		execB.Free()
		for _, result := range results {
			result.Free(mp)
		}
		require.Equal(t, curNB, mp.CurrNB())
	})
}

func testAggExec(t *testing.T,
	makeAgg func(t *testing.T, mp *mpool.MPool, typ types.Type) AggFuncExec,
	expected expectedCount) {
	mp := mpool.MustNewZero()
	typs, vecs, nvecs := buildTestDataVecs(t, mp)

	t.Run("BulkFill", func(t *testing.T) {
		for i, typ := range typs {
			curNB := mp.CurrNB()
			exec := makeAgg(t, mp, typ)
			exec.GetOptResult().modifyChunkSize(1)
			require.NoError(t, exec.GroupGrow(1))

			require.NoError(t, exec.BulkFill(0, vecs[i:i+1]))
			require.NoError(t, exec.BulkFill(0, nvecs[i:i+1]))

			results, err := exec.Flush()
			require.NoError(t, err)
			require.Len(t, results, 1)
			vals := vector.MustFixedColNoTypeCheck[int64](results[0])
			if expected.count != vals[0] {
				t.Errorf("expected %d, got %d", expected.count, vals[0])
			}
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
			exec := makeAgg(t, mp, typ)
			require.NoError(t, exec.GroupGrow(1))

			require.NoError(t, exec.BatchFill(0, []uint64{1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, vecs[i:i+1]))
			require.NoError(t, exec.BatchFill(0, []uint64{1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, nvecs[i:i+1]))

			results, err := exec.Flush()
			require.NoError(t, err)
			require.Len(t, results, 1)
			vals := vector.MustFixedColNoTypeCheck[int64](results[0])
			require.Equal(t, 1, len(vals))
			require.Equal(t, expected.count, vals[0])
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
			exec := makeAgg(t, mp, typ)
			require.NoError(t, exec.GroupGrow(2))

			require.NoError(t, exec.BatchFill(0, []uint64{1, 2, 1, 2, 1, 2, 1, 2, 1, 2}, vecs[i:i+1]))
			require.NoError(t, exec.BatchFill(0, []uint64{1, 2, 1, 2, 1, 2, 1, 2, 1, 2}, nvecs[i:i+1]))

			results, err := exec.Flush()
			require.NoError(t, err)
			require.Len(t, results, 1)
			vals := vector.MustFixedColNoTypeCheck[int64](results[0])
			require.Equal(t, 2, len(vals))
			require.Equal(t, expected.count, vals[0]+vals[1])
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
			exec := makeAgg(t, mp, typ)
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
			require.Len(t, results, (20000+AggBatchSize-1)/AggBatchSize)
			var totalGrp int
			var totalCnt int64
			for _, result := range results {
				num := int(result.Length())
				vals := vector.MustFixedColNoTypeCheck[int64](result)
				for _, val := range vals {
					totalCnt += val
				}

				totalGrp += num
				result.Free(mp)
			}
			require.Equal(t, 20000, totalGrp)
			require.Equal(t, expected.count20k, totalCnt)
			exec.Free()
			require.Equal(t, curNB, mp.CurrNB())
		}
	})

	t.Run("Merge", func(t *testing.T) {
		for i, typ := range typs {
			curNB := mp.CurrNB()

			execa1 := makeAgg(t, mp, typ)
			execa2 := makeAgg(t, mp, typ)
			execa1.GetOptResult().modifyChunkSize(1)
			execa2.GetOptResult().modifyChunkSize(1)
			require.NoError(t, execa1.GroupGrow(1))
			require.NoError(t, execa2.GroupGrow(1))

			execb1 := makeAgg(t, mp, typ)
			execb2 := makeAgg(t, mp, typ)
			execb1.GetOptResult().modifyChunkSize(1)
			execb1.GroupGrow(1)
			execb2.GetOptResult().modifyChunkSize(1)
			execb2.GroupGrow(1)

			require.NoError(t, execa1.BulkFill(0, vecs[i:i+1]))
			require.NoError(t, execa2.BulkFill(0, nvecs[i:i+1]))

			buf1 := bytes.NewBuffer(make([]byte, 0, common.MiB))
			buf2 := bytes.NewBuffer(make([]byte, 0, common.MiB))

			err := execa1.SaveIntermediateResultOfChunk(0, buf1)
			require.NoError(t, err)
			err = execa2.SaveIntermediateResultOfChunk(0, buf2)
			require.NoError(t, err)

			r1 := bytes.NewReader(buf1.Bytes())
			r2 := bytes.NewReader(buf2.Bytes())

			err = execb1.UnmarshalFromReader(r1, mp)
			require.NoError(t, err)
			err = execb2.UnmarshalFromReader(r2, mp)
			require.NoError(t, err)

			execb1.Merge(execb2, 0, 0)
			results, err := execb1.Flush()
			require.NoError(t, err)
			require.Len(t, results, 1)
			vals := vector.MustFixedColNoTypeCheck[int64](results[0])
			if expected.count != vals[0] {
				t.Errorf("expected %d, got %d", expected.count, vals[0])
			}
			for _, result := range results {
				result.Free(mp)
			}

			execa1.Free()
			execa2.Free()
			execb1.Free()
			execb2.Free()

			require.Equal(t, curNB, mp.CurrNB())
		}
	})

	t.Run("BatchMerge1", func(t *testing.T) {
		for i, typ := range typs {
			curNB := mp.CurrNB()

			execa1 := makeAgg(t, mp, typ)
			execa2 := makeAgg(t, mp, typ)
			require.NoError(t, execa1.GroupGrow(1))
			require.NoError(t, execa2.GroupGrow(1))

			require.NoError(t, execa1.BatchFill(0, []uint64{1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, vecs[i:i+1]))
			require.NoError(t, execa2.BatchFill(0, []uint64{1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, nvecs[i:i+1]))

			buf1 := bytes.NewBuffer(make([]byte, 0, common.MiB))
			buf2 := bytes.NewBuffer(make([]byte, 0, common.MiB))

			err := execa1.SaveIntermediateResult(1, [][]uint8{{1}}, buf1)
			require.NoError(t, err)
			err = execa2.SaveIntermediateResult(1, [][]uint8{{1}}, buf2)
			require.NoError(t, err)

			execb1 := makeAgg(t, mp, typ)
			execb2 := makeAgg(t, mp, typ)

			r1 := bytes.NewReader(buf1.Bytes())
			r2 := bytes.NewReader(buf2.Bytes())

			err = execb1.UnmarshalFromReader(r1, mp)
			require.NoError(t, err)
			err = execb2.UnmarshalFromReader(r2, mp)
			require.NoError(t, err)

			execb1.BatchMerge(execb2, 0, []uint64{1})
			results, err := execb1.Flush()
			require.NoError(t, err)
			require.Len(t, results, 1)
			vals := vector.MustFixedColNoTypeCheck[int64](results[0])
			require.Equal(t, expected.count, vals[0])
			for _, result := range results {
				result.Free(mp)
			}

			execa1.Free()
			execa2.Free()
			execb1.Free()
			execb2.Free()

			require.Equal(t, curNB, mp.CurrNB())
		}
	})
	t.Run("BatchMerge2", func(t *testing.T) {
		for i, typ := range typs {
			curNB := mp.CurrNB()
			execa1 := makeAgg(t, mp, typ)
			execa2 := makeAgg(t, mp, typ)
			require.NoError(t, execa1.GroupGrow(2))
			require.NoError(t, execa2.GroupGrow(2))

			require.NoError(t, execa1.BatchFill(0, []uint64{1, 2, 1, 2, 1, 2, 1, 2, 1, 2}, vecs[i:i+1]))
			require.NoError(t, execa2.BatchFill(0, []uint64{1, 2, 1, 2, 1, 2, 1, 2, 1, 2}, nvecs[i:i+1]))

			buf1 := bytes.NewBuffer(make([]byte, 0, common.MiB))
			buf2 := bytes.NewBuffer(make([]byte, 0, common.MiB))

			err := execa1.SaveIntermediateResult(1, [][]uint8{{1, 0}}, buf1)
			require.NoError(t, err)
			err = execa2.SaveIntermediateResult(1, [][]uint8{{1, 0}}, buf2)
			require.NoError(t, err)

			execb1 := makeAgg(t, mp, typ)
			execb2 := makeAgg(t, mp, typ)

			r1 := bytes.NewReader(buf1.Bytes())
			r2 := bytes.NewReader(buf2.Bytes())

			err = execb1.UnmarshalFromReader(r1, mp)
			require.NoError(t, err)
			err = execb2.UnmarshalFromReader(r2, mp)
			require.NoError(t, err)

			execb1.BatchMerge(execb2, 0, []uint64{1})
			results, err := execb1.Flush()
			require.NoError(t, err)
			require.Len(t, results, 1)
			vals := vector.MustFixedColNoTypeCheck[int64](results[0])
			// distinct, 11, will produce 6.
			require.Equal(t, (expected.count+1)/2, vals[0])
			for _, result := range results {
				result.Free(mp)
			}

			execa1.Free()
			execa2.Free()
			execb1.Free()
			execb2.Free()

			require.Equal(t, curNB, mp.CurrNB())
		}
	})
	t.Run("BatchMerge20000", func(t *testing.T) {
		for i, typ := range typs {
			curNB := mp.CurrNB()
			execa := makeAgg(t, mp, typ)
			require.NoError(t, execa.GroupGrow(20000))

			for j := 0; j < 2000; j++ {
				groups := make([]uint64, 10)
				for k := range groups {
					groups[k] = uint64(j*10 + k + 1)
				}

				require.NoError(t, execa.BatchFill(0, groups[:5], vecs[i:i+1]))
				require.NoError(t, execa.BatchFill(5, groups[5:], vecs[i:i+1]))
				require.NoError(t, execa.BatchFill(0, groups[:5], nvecs[i:i+1]))
				require.NoError(t, execa.BatchFill(5, groups[5:], nvecs[i:i+1]))
			}

			if v, ok := execa.(*countColumnExec); ok {
				v.aggExec.checkArgsSkl()
			}

			// save intermediate result of chunk 1, and 2
			flags1 := make([][]uint8, 3)
			flags1[0] = make([]uint8, 8192)
			flags1[1] = make([]uint8, 8192)
			flags1[2] = make([]uint8, 20000-8192*2)
			flags2 := make([][]uint8, 3)
			flags2[0] = make([]uint8, 8192)
			flags2[1] = make([]uint8, 8192)
			flags2[2] = make([]uint8, 20000-8192*2)

			for j := range flags1 {
				for k := range flags1[j] {
					flags1[j][k] = uint8(k) % 2
				}
			}
			for j := range flags2 {
				for k := range flags2[j] {
					flags2[j][k] = uint8(k+1) % 2
				}
			}

			buf1 := bytes.NewBuffer(make([]byte, 0, common.MiB))
			buf2 := bytes.NewBuffer(make([]byte, 0, common.MiB))

			err := execa.SaveIntermediateResult(10000, flags1, buf1)
			require.NoError(t, err)
			err = execa.SaveIntermediateResult(10000, flags2, buf2)
			require.NoError(t, err)

			execb1 := makeAgg(t, mp, typ)
			execb2 := makeAgg(t, mp, typ)

			r1 := bytes.NewReader(buf1.Bytes())
			r2 := bytes.NewReader(buf2.Bytes())

			err = execb1.UnmarshalFromReader(r1, mp)
			require.NoError(t, err)
			err = execb2.UnmarshalFromReader(r2, mp)
			require.NoError(t, err)

			for i := 0; i < 1000; i++ {
				grps := make([]uint64, 10)
				for k := range grps {
					grps[k] = uint64(i*10 + k + 1)
				}
				err = execb1.BatchMerge(execb2, i*10, grps)
				require.NoError(t, err)
			}

			results, err := execb1.Flush()
			require.NoError(t, err)
			// we have 10000 groups, 8192 + 1808 = 10000
			var totalCnt int64
			require.Len(t, results, 2)
			vals := vector.MustFixedColNoTypeCheck[int64](results[0])
			require.Equal(t, 8192, len(vals))
			for _, val := range vals {
				totalCnt += val
			}
			vals = vector.MustFixedColNoTypeCheck[int64](results[1])
			require.Equal(t, 1808, len(vals))
			for _, val := range vals {
				totalCnt += val
			}

			if expected.count20k != totalCnt {
				t.Errorf("expected %d, got %d", expected.count20k, totalCnt)
			}

			results[0].Free(mp)
			results[1].Free(mp)
			execa.Free()
			execb1.Free()
			execb2.Free()

			require.Equal(t, curNB, mp.CurrNB())
		}
	})

}
