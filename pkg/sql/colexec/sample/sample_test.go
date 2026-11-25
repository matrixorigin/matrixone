// Copyright 2021 Matrix Origin
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

package sample

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

const nullFlag = int64(-65535)

const (
	wideSampleRows  = 64
	wideSampleBytes = 64 << 10
)

type sampleMemoryStats struct {
	retainedBytes int64
	peakBytes     int64
}

func makeSampleMemoryBatch(tb testing.TB, includeWide bool) (*batch.Batch, *mpool.MPool) {
	tb.Helper()

	mp := mpool.MustNewZeroNoFixed()
	columnCount := 1
	if includeWide {
		columnCount++
	}
	bat := batch.NewWithSize(columnCount)
	column := 0
	if includeWide {
		bat.Vecs[column] = vector.NewVec(types.T_text.ToType(), mp)
		payload := bytes.Repeat([]byte{'x'}, wideSampleBytes)
		for range wideSampleRows {
			require.NoError(tb, vector.AppendBytes(bat.Vecs[column], payload, false, mp))
		}
		column++
	}

	bat.Vecs[column] = vector.NewVec(types.T_int8.ToType(), mp)
	for i := range wideSampleRows {
		require.NoError(tb, vector.AppendFixed(bat.Vecs[column], int8(i), false, mp))
	}
	bat.SetRowCount(wideSampleRows)
	return bat, mp
}

func executeSampleMemoryCase(tb testing.TB, proc *process.Process, input *batch.Batch) sampleMemoryStats {
	tb.Helper()

	mp := proc.Mp()
	pool := newSamplePoolByRows(proc, input.RowCount(), len(input.Vecs), false)
	defer pool.Free()
	if err := pool.Sample(1, input.Vecs, nil, input); err != nil {
		tb.Fatal(err)
	}
	result, err := pool.Result(true)
	if err != nil {
		tb.Fatal(err)
	}
	if result.RowCount() != input.RowCount() {
		tb.Fatalf("expected %d sampled rows, got %d", input.RowCount(), result.RowCount())
	}

	stats := sampleMemoryStats{
		retainedBytes: int64(result.Allocated()),
	}
	result.Clean(mp)
	if curr := mp.CurrNB(); curr != 0 {
		tb.Fatalf("sample execution leaked %d mpool bytes", curr)
	}
	return stats
}

func measureSampleMemoryCase(tb testing.TB, includeWide bool) sampleMemoryStats {
	tb.Helper()

	input, inputMP := makeSampleMemoryBatch(tb, includeWide)
	defer mpool.DeleteMPool(inputMP)
	defer input.Clean(inputMP)

	outputMP := mpool.MustNewZeroNoFixed()
	defer mpool.DeleteMPool(outputMP)
	proc := testutil.NewProcessWithMPool(tb, "", outputMP)
	defer proc.Free()

	// Vector memory is off-heap: measure the peak through the output pool.
	stats := executeSampleMemoryCase(tb, proc, input)
	stats.peakBytes = outputMP.Stats().HighWaterMark.Load()
	return stats
}

func TestSamplePrunedWideVarlenMemory(t *testing.T) {
	unpruned := measureSampleMemoryCase(t, true)
	pruned := measureSampleMemoryCase(t, false)

	require.Positive(t, pruned.retainedBytes)
	require.Positive(t, pruned.peakBytes)
	require.Greater(t, unpruned.retainedBytes, pruned.retainedBytes*100)
	require.Greater(t, unpruned.peakBytes, pruned.peakBytes*100)
}

func BenchmarkSamplePrunedWideVarlenMemory(b *testing.B) {
	for _, test := range []struct {
		name        string
		includeWide bool
	}{
		{name: "unpruned_wide", includeWide: true},
		{name: "pruned_carrier", includeWide: false},
	} {
		b.Run(test.name, func(b *testing.B) {
			input, inputMP := makeSampleMemoryBatch(b, test.includeWide)
			defer mpool.DeleteMPool(inputMP)
			defer input.Clean(inputMP)

			outputMP := mpool.MustNewZeroNoFixed()
			defer mpool.DeleteMPool(outputMP)
			proc := testutil.NewProcessWithMPool(b, "", outputMP)
			defer proc.Free()

			var retainedBytes int64
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				stats := executeSampleMemoryCase(b, proc, input)
				retainedBytes += stats.retainedBytes
			}
			b.ReportMetric(float64(retainedBytes)/float64(b.N), "vector-retained-B/op")
		})
	}
}

func TestSamplePool(t *testing.T) {
	proc := testutil.NewProcess(t)

	// data source :
	// (1, 1), (2, 2), (3, 3), (3, 4), (4, 4),
	// (5, 5), (6, 6), (6, 6), (7, 7), (8, null).
	rows1 := [][]int64{
		{1, 1},
		{2, 2},
		{3, 3},
		{3, 4},
		{4, 4},
	}
	row2 := [][]int64{
		{5, 5},
		{6, 6},
		{6, 6},
		{7, 7},
		{8, nullFlag},
	}
	b1, e1 := genSampleBatch(proc, rows1)
	require.NoError(t, e1)
	b2, e2 := genSampleBatch(proc, row2)
	require.NoError(t, e2)

	{
		// sample 5 rows by second column.
		pool1 := newSamplePoolByRows(proc, 5, 1, true)
		err := pool1.sampleFromColumn(1, b1.Vecs[1], b1)
		require.NoError(t, err)
		err = pool1.sampleFromColumn(1, b2.Vecs[1], b2)
		require.NoError(t, err)

		// cannot get any result before end.
		tbat, err := pool1.Result(false)
		require.NoError(t, err)
		require.Equal(t, 0, tbat.RowCount())

		// check the result.
		// due to reorder, the result will be [sample column, normal column, rowsCount column].
		out, err := pool1.Result(true)
		require.NoError(t, err)
		require.Equal(t, 3, len(out.Vecs))
		require.Equal(t, 5, out.Vecs[0].Length())
		require.Equal(t, 5, out.Vecs[1].Length())
		// invalid scan row count was 9. the 10th row with null value at the sample column will be ignored.
		require.Equal(t, int64(9), vector.GetFixedAtWithTypeCheck[int64](out.Vecs[2], 0))

		out.Clean(proc.Mp())
		pool1.Free()
	}

	{
		// sample 5 rows by 2 columns.
		pool2 := newSamplePoolByRows(proc, 5, 2, false)
		err := pool2.sampleFromColumns(1, b1.Vecs, b1)
		require.NoError(t, err)
		err = pool2.sampleFromColumns(1, b2.Vecs, b2)
		require.NoError(t, err)

		tbat, err := pool2.Result(false)
		require.NoError(t, err)
		require.Equal(t, 0, tbat.RowCount())

		out, err := pool2.Result(true)
		require.NoError(t, err)
		// due to we set outputRowCount to false, the result will be [sample column, normal column].
		require.Equal(t, 2, len(out.Vecs))
		require.Equal(t, 5, out.Vecs[0].Length())
		require.Equal(t, 5, out.Vecs[1].Length())

		out.Clean(proc.Mp())
		pool2.Free()
	}

	{
		// sample 100 % rows by the second column.
		pool3 := newSamplePoolByPercent(proc, 100.0, 1)
		err := pool3.sampleFromColumn(1, b1.Vecs[1], b1)
		require.NoError(t, err)

		// can take out the result before an end.
		tbat, err := pool3.Result(false)
		require.NoError(t, err)
		require.Equal(t, 5, tbat.RowCount())

		err = pool3.sampleFromColumn(1, b2.Vecs[1], b2)
		require.NoError(t, err)

		out, err := pool3.Result(true)
		require.NoError(t, err)
		require.Equal(t, 4, out.RowCount())

		tbat.Clean(proc.Mp())
		out.Clean(proc.Mp())
		pool3.Free()
	}

	proc.Free()
	b1.Clean(proc.Mp())
	b2.Clean(proc.Mp())
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestSampleGroupsSQLNullAndConstNullKeysTogether(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	ctr := &container{
		isGroupBy:            true,
		useIntHashMap:        true,
		groupVectorsNullable: true,
		// Capacity 2 keeps both rows, so the pool never replaces a sampled row.
		samplePool: newSamplePoolByRows(proc, 2, 1, false),
	}
	defer freeSampleHashContainer(ctr)

	nullBatch := batch.NewWithSize(2)
	nullBatch.Vecs[0] = vector.NewVec(types.T_int64.ToType(), proc.Mp())
	require.NoError(t, vector.AppendFixed(
		nullBatch.Vecs[0],
		int64(0),
		true,
		proc.Mp(),
	))
	nullBatch.Vecs[1] = vector.NewVec(types.T_int64.ToType(), proc.Mp())
	require.NoError(t, vector.AppendFixed(
		nullBatch.Vecs[1],
		int64(10),
		false,
		proc.Mp(),
	))
	nullBatch.SetRowCount(1)
	ctr.groupVectors = nullBatch.Vecs[:1]
	ctr.sampleVectors = nullBatch.Vecs[1:]
	require.NoError(t, ctr.hashAndSample(nullBatch, proc))
	nullBatch.Clean(proc.Mp())

	// A rolled-up key is a plain NULL constant and shares the SQL NULL group.
	constNull := batch.NewWithSize(2)
	constNull.Vecs[0] = vector.NewConstNull(types.T_int64.ToType(), 1, proc.Mp())
	constNull.Vecs[1] = testutil.MakeInt64Vector([]int64{20}, nil, proc.Mp())
	constNull.SetRowCount(1)
	ctr.groupVectors = constNull.Vecs[:1]
	ctr.sampleVectors = constNull.Vecs[1:]
	require.NoError(t, ctr.hashAndSample(constNull, proc))
	constNull.Clean(proc.Mp())

	require.Equal(t, uint64(1), ctr.intHashMap.GroupCount())
	result, err := ctr.samplePool.Result(true)
	require.NoError(t, err)
	require.Equal(t, 2, result.RowCount())
	require.Equal(t, 2, result.Vecs[0].GetNulls().Count())
	result.Clean(proc.Mp())
}

func BenchmarkSampleGroupedHashFastPath(b *testing.B) {
	proc := testutil.NewProcess(b)
	defer proc.Free()
	input := makeSampleBatch(
		b,
		proc,
		makeSequence(256),
		makeSequence(256),
	)
	defer input.Clean(proc.Mp())
	ctr := &container{
		isGroupBy:     true,
		useIntHashMap: true,
		samplePool:    newSamplePoolByRows(proc, 1, 1, false),
		groupVectors:  input.Vecs[:1],
		sampleVectors: input.Vecs[1:],
	}
	defer freeSampleHashContainer(ctr)
	require.NoError(b, ctr.hashAndSample(input, proc))

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := ctr.hashAndSample(input, proc); err != nil {
			b.Fatal(err)
		}
	}
}

func makeSequence(count int) []int64 {
	values := make([]int64, count)
	for i := range values {
		values[i] = int64(i)
	}
	return values
}

func makeSampleBatch(
	tb testing.TB,
	proc *process.Process,
	groups []int64,
	samples []int64,
) *batch.Batch {
	tb.Helper()
	bat := batch.NewWithSize(2)
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType(), proc.Mp())
	require.NoError(tb, vector.AppendFixedList(
		bat.Vecs[0],
		groups,
		nil,
		proc.Mp(),
	))
	bat.Vecs[1] = vector.NewVec(types.T_int64.ToType(), proc.Mp())
	require.NoError(tb, vector.AppendFixedList(
		bat.Vecs[1],
		samples,
		nil,
		proc.Mp(),
	))
	bat.SetRowCount(len(groups))
	return bat
}

func freeSampleHashContainer(ctr *container) {
	if ctr.intHashMap != nil {
		ctr.intHashMap.Free()
	}
	if ctr.strHashMap != nil {
		ctr.strHashMap.Free()
	}
	ctr.samplePool.Free()
}

func genSampleBatch(proc *process.Process, rows [][]int64) (*batch.Batch, error) {
	b := batch.NewWithSize(len(rows[0]))

	var err error
	for i := range b.Vecs {
		b.Vecs[i] = vector.NewVec(types.T_int64.ToType(), proc.Mp())

		for _, rowValue := range rows {
			err = vector.AppendFixed[int64](b.Vecs[i], rowValue[i], rowValue[i] == nullFlag, proc.Mp())
			if err != nil {
				return nil, err
			}
		}
	}
	b.SetRowCount(len(rows))
	return b, nil
}

func TestSamplePoolOthers(t *testing.T) {
	// merge sample and sample by percent cannot be tested full.
	s1 := newSamplePoolByRows(nil, 1, 1, false)
	s2 := newSamplePoolByPercent(nil, 1.0, 1)
	s3 := newSamplePoolByRowsForMerge(nil, 1, 1, false)

	s1.setPerfFields(false)
	s2.setPerfFields(false)
	s3.setPerfFields(false)
	require.Equal(t, true, s1.canCheckFull)
	require.Equal(t, false, s2.canCheckFull)
	require.Equal(t, false, s3.canCheckFull)

	// once sample for each group, full check is not supported.
	s1.setPerfFields(true)
	s2.setPerfFields(true)
	s3.setPerfFields(true)
	require.Equal(t, false, s1.canCheckFull)
	require.Equal(t, false, s2.canCheckFull)
	require.Equal(t, false, s3.canCheckFull)
}

// TestSamplePoolLowPrecisionFloats samples 3 of 20 rows of a bf16, float16, float8 and float4
// column, which replaces pooled rows once the pool is full.
func TestSamplePoolLowPrecisionFloats(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	for _, oid := range []types.T{types.T_bf16, types.T_float16, types.T_float8, types.T_float4} {
		t.Run(oid.String(), func(t *testing.T) {
			b := batch.NewWithSize(1)
			b.Vecs[0] = vector.NewVec(oid.ToType(), proc.Mp())
			for i := 0; i < 20; i++ {
				v := float32(i%6) - 2
				var err error
				switch oid {
				case types.T_bf16:
					err = vector.AppendFixed(b.Vecs[0], types.BF16FromFloat32(v), false, proc.Mp())
				case types.T_float16:
					err = vector.AppendFixed(b.Vecs[0], types.Float16FromFloat32(v), false, proc.Mp())
				case types.T_float8:
					err = vector.AppendFixed(b.Vecs[0], types.Float8FromFloat32(v), false, proc.Mp())
				default:
					err = vector.AppendFixed(b.Vecs[0], types.Float4FromFloat32(v), false, proc.Mp())
				}
				require.NoError(t, err)
			}
			b.SetRowCount(20)
			defer b.Clean(proc.Mp())

			pool := newSamplePoolByRows(proc, 3, 1, false)
			defer pool.Free()
			require.NoError(t, pool.sampleFromColumn(1, b.Vecs[0], b))
			out, err := pool.Result(true)
			require.NoError(t, err)
			defer out.Clean(proc.Mp())
			require.Equal(t, 3, out.Vecs[0].Length())
			for i := 0; i < 3; i++ {
				v, ok := vector.GetLowPrecisionFloatAt(out.Vecs[0], i)
				require.True(t, ok)
				require.Contains(t, []float32{-2, -1, 0, 1, 2, 3}, v)
			}
		})
	}
}

// A sampled row can be replaced by a NULL row, including a const NULL key
// (rolled-up ROLLUP keys are const NULLs), and later by a value again.
func TestSamplePoolReplaceValidRowCopiesNulls(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	mp := proc.Mp()
	pd := &poolData{validBatch: batch.NewWithSize(1)}
	pd.validBatch.Vecs[0] = testutil.MakeInt64Vector([]int64{5}, nil, mp)
	pd.validBatch.SetRowCount(1)
	defer pd.clean(mp)

	constNull := batch.NewWithSize(1)
	constNull.Vecs[0] = vector.NewConstNull(types.T_int64.ToType(), 3, mp)
	constNull.SetRowCount(3)
	defer constNull.Clean(mp)
	require.NoError(t, pd.replaceValidRow(mp, constNull, 0, 2))
	require.True(t, pd.validBatch.Vecs[0].IsNull(0))

	flat := batch.NewWithSize(1)
	flat.Vecs[0] = testutil.MakeInt64Vector([]int64{7, 9}, []uint64{0}, mp)
	flat.SetRowCount(2)
	defer flat.Clean(mp)
	require.NoError(t, pd.replaceValidRow(mp, flat, 0, 1))
	require.False(t, pd.validBatch.Vecs[0].IsNull(0))
	require.Equal(t, int64(9), vector.GetFixedAtNoTypeCheck[int64](pd.validBatch.Vecs[0], 0))
	require.NoError(t, pd.replaceValidRow(mp, flat, 0, 0))
	require.True(t, pd.validBatch.Vecs[0].IsNull(0))
}
