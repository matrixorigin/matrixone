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
		bat.Vecs[column] = vector.NewVec(types.T_text.ToType())
		payload := bytes.Repeat([]byte{'x'}, wideSampleBytes)
		for range wideSampleRows {
			require.NoError(tb, vector.AppendBytes(bat.Vecs[column], payload, false, mp))
		}
		column++
	}

	bat.Vecs[column] = vector.NewVec(types.T_int8.ToType())
	for i := range wideSampleRows {
		require.NoError(tb, vector.AppendFixed(bat.Vecs[column], int8(i), false, mp))
	}
	bat.SetRowCount(wideSampleRows)
	return bat, mp
}

func initializeOffHeapSampleResult(pool *sPool, input *batch.Batch) {
	result := batch.NewWithSize(len(input.Vecs))
	for i, vec := range input.Vecs {
		result.Vecs[i] = vector.NewOffHeapVecWithType(*vec.GetType())
	}

	if len(input.Vecs) > 1 {
		pool.growMulPool(1, len(input.Vecs))
		pool.mPools[0].data.validBatch = result
	} else {
		pool.growSiPool(1)
		pool.sPools[0].data.validBatch = result
	}
}

func executeSampleMemoryCase(tb testing.TB, proc *process.Process, input *batch.Batch) sampleMemoryStats {
	tb.Helper()

	mp := proc.Mp()
	pool := newSamplePoolByRows(proc, input.RowCount(), len(input.Vecs), false)
	defer pool.Free()
	initializeOffHeapSampleResult(pool, input)
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
		peakBytes:     mp.Stats().HighWaterMark.Load(),
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
	return executeSampleMemoryCase(tb, proc, input)
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
			b.ReportMetric(float64(outputMP.Stats().HighWaterMark.Load()), "mpool-peak-B")
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

func genSampleBatch(proc *process.Process, rows [][]int64) (*batch.Batch, error) {
	b := batch.NewWithSize(len(rows[0]))

	var err error
	for i := range b.Vecs {
		b.Vecs[i] = vector.NewVec(types.T_int64.ToType())

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
