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

package hashjoin

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestComputeXXHash(t *testing.T) {
	mp := mpool.MustNewZero()
	var buf []byte

	t.Run("empty", func(t *testing.T) {
		err := computeXXHash(nil, nil, &buf, 0)
		require.NoError(t, err)
	})

	t.Run("single_int32", func(t *testing.T) {
		vec := testutil.MakeInt32Vector([]int32{1, 2, 3}, nil, mp)
		hashValues := make([]uint64, 3)
		err := computeXXHash([]*vector.Vector{vec}, hashValues, &buf, 0)
		require.NoError(t, err)
		require.NotEqual(t, uint64(0), hashValues[0])
		require.NotEqual(t, hashValues[0], hashValues[1])
		require.NotEqual(t, hashValues[1], hashValues[2])
	})

	t.Run("multiple_columns", func(t *testing.T) {
		vec1 := testutil.MakeInt32Vector([]int32{1, 2}, nil, mp)
		vec2 := testutil.MakeVarcharVector([]string{"a", "b"}, nil, mp)
		hashValues := make([]uint64, 2)
		err := computeXXHash([]*vector.Vector{vec1, vec2}, hashValues, &buf, 0)
		require.NoError(t, err)
		require.NotEqual(t, hashValues[0], hashValues[1])
	})

	t.Run("const_vector", func(t *testing.T) {
		vec := testutil.MakeInt32Vector([]int32{5}, nil, mp)
		vec.SetClass(vector.CONSTANT)
		hashValues := make([]uint64, 3)
		err := computeXXHash([]*vector.Vector{vec}, hashValues, &buf, 0)
		require.NoError(t, err)
		require.Equal(t, hashValues[0], hashValues[1])
		require.Equal(t, hashValues[1], hashValues[2])
	})
}

func TestFlushBucketBuffer(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	spillfs, err := proc.GetSpillFileService()
	require.NoError(t, err)

	file, err := spillfs.CreateFile(context.Background(), "test_flush")
	require.NoError(t, err)
	defer func() {
		file.Close()
		spillfs.Delete(context.Background(), "test_flush")
	}()

	analyzer := process.NewAnalyzer(0, false, false, "test")
	ctr := &container{}

	t.Run("empty_buffer", func(t *testing.T) {
		var buf *batch.Batch
		cnt, err := ctr.flushBucketBuffer(proc, buf, file, analyzer)
		require.NoError(t, err)
		require.Equal(t, int64(0), cnt)
	})

	t.Run("with_data", func(t *testing.T) {
		bat := batch.NewWithSize(1)
		bat.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 2, 3}, nil, proc.Mp())
		bat.SetRowCount(3)

		cnt, err := ctr.flushBucketBuffer(proc, bat, file, analyzer)
		require.NoError(t, err)
		require.Equal(t, int64(3), cnt)
	})
}

func TestCreateProbeSpillFiles(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	buckets, files, err := createRootProbeSpillBucketFiles(proc)
	require.NoError(t, err)
	require.Equal(t, spillNumBuckets, len(buckets))
	require.Equal(t, spillNumBuckets, len(files))

	spillfs, _ := proc.GetSpillFileService()
	for i, file := range files {
		require.NotNil(t, file)
		file.Close()
		spillfs.Delete(context.Background(), buckets[i])
	}
}

func TestBucketBufferReuse(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	spillfs, err := proc.GetSpillFileService()
	require.NoError(t, err)

	analyzer := process.NewAnalyzer(0, false, false, "test")
	file, err := spillfs.CreateFile(context.Background(), "test_reuse")
	require.NoError(t, err)
	defer func() {
		file.Close()
		spillfs.Delete(context.Background(), "test_reuse")
	}()

	ctr := &container{}

	// First batch
	bat1 := batch.NewWithSize(1)
	bat1.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 2}, nil, proc.Mp())
	bat1.SetRowCount(2)

	_, err = ctr.flushBucketBuffer(proc, bat1, file, analyzer)
	require.NoError(t, err)

	// Second batch
	bat2 := batch.NewWithSize(1)
	bat2.Vecs[0] = testutil.MakeInt32Vector([]int32{3, 4}, nil, proc.Mp())
	bat2.SetRowCount(2)

	_, err = ctr.flushBucketBuffer(proc, bat2, file, analyzer)
	require.NoError(t, err)
}

func TestHashDistribution(t *testing.T) {
	mp := mpool.MustNewZero()
	var buf []byte
	// Test that hash values distribute across buckets
	vec := testutil.MakeInt32Vector([]int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
		11, 12, 13, 14, 15, 16, 17, 18, 19, 20}, nil, mp)

	hashValues := make([]uint64, 20)
	err := computeXXHash([]*vector.Vector{vec}, hashValues, &buf, 0)
	require.NoError(t, err)

	bucketCounts := make([]int, spillNumBuckets)
	for _, hash := range hashValues {
		bucketId := hash & (spillNumBuckets - 1)
		bucketCounts[bucketId]++
	}

	// At least some buckets should have values
	nonEmptyBuckets := 0
	for _, count := range bucketCounts {
		if count > 0 {
			nonEmptyBuckets++
		}
	}
	require.Greater(t, nonEmptyBuckets, 1)
}

func TestSpillFileFormat(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	spillfs, err := proc.GetSpillFileService()
	require.NoError(t, err)

	analyzer := process.NewAnalyzer(0, false, false, "test")
	bucketName := "test_format"
	file, err := spillfs.CreateFile(context.Background(), bucketName)
	require.NoError(t, err)

	// Write multiple batches
	for i := 0; i < 3; i++ {
		bat := batch.NewWithSize(1)
		bat.Vecs[0] = testutil.MakeInt32Vector([]int32{int32(i * 10), int32(i*10 + 1)}, nil, proc.Mp())
		bat.SetRowCount(2)

		ctr := &container{}
		_, err = ctr.flushBucketBuffer(proc, bat, file, analyzer)
		require.NoError(t, err)
	}
	file.Close()
}

func TestEmptyBucketHandling(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	// Test with empty spill queue - should return nil immediately
	hashJoin := &HashJoin{
		ctr: container{
			spillQueue: []spillBucket{},
			state:      Probe,
		},
	}

	result, err := hashJoin.getSpilledInputBatch(proc, process.NewAnalyzer(0, false, false, "test"))
	require.NoError(t, err)
	require.Nil(t, result.Batch)
}

func TestMultipleDataTypes(t *testing.T) {
	mp := mpool.MustNewZero()
	var buf []byte

	tests := []struct {
		name string
		vec  *vector.Vector
	}{
		{"int8", testutil.MakeInt8Vector([]int8{1, 2, 3}, nil, mp)},
		{"int16", testutil.MakeInt16Vector([]int16{100, 200, 300}, nil, mp)},
		{"int64", testutil.MakeInt64Vector([]int64{1000, 2000, 3000}, nil, mp)},
		{"uint32", testutil.MakeUint32Vector([]uint32{10, 20, 30}, nil, mp)},
		{"float32", testutil.MakeFloat32Vector([]float32{1.1, 2.2, 3.3}, nil, mp)},
		{"float64", testutil.MakeFloat64Vector([]float64{10.1, 20.2, 30.3}, nil, mp)},
		{"varchar", testutil.MakeVarcharVector([]string{"abc", "def", "ghi"}, nil, mp)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hashValues := make([]uint64, 3)
			err := computeXXHash([]*vector.Vector{tt.vec}, hashValues, &buf, 0)
			require.NoError(t, err)
			require.NotEqual(t, uint64(0), hashValues[0])
			require.NotEqual(t, hashValues[0], hashValues[1])
		})
	}
}

func TestLargeBufferFlush(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	spillfs, err := proc.GetSpillFileService()
	require.NoError(t, err)

	analyzer := process.NewAnalyzer(0, false, false, "test")
	file, err := spillfs.CreateFile(context.Background(), "test_large")
	require.NoError(t, err)
	defer func() {
		file.Close()
		spillfs.Delete(context.Background(), "test_large")
	}()

	// Create large batch
	size := spillBufferSize + 100
	values := make([]int32, size)
	for i := range values {
		values[i] = int32(i)
	}

	bat := batch.NewWithSize(1)
	bat.Vecs[0] = testutil.MakeInt32Vector(values, nil, proc.Mp())
	bat.SetRowCount(size)

	ctr := &container{}
	cnt, err := ctr.flushBucketBuffer(proc, bat, file, analyzer)
	require.NoError(t, err)
	require.Equal(t, int64(size), cnt)
}

func TestSpillFileCleanup(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	spillfs, err := proc.GetSpillFileService()
	require.NoError(t, err)

	analyzer := process.NewAnalyzer(0, false, false, "test")
	bucketName := "test_cleanup"
	file, err := spillfs.CreateFile(context.Background(), bucketName)
	require.NoError(t, err)

	bat := batch.NewWithSize(1)
	bat.Vecs[0] = testutil.MakeInt32Vector([]int32{1}, nil, proc.Mp())
	bat.SetRowCount(1)

	ctr := &container{}
	_, err = ctr.flushBucketBuffer(proc, bat, file, analyzer)
	require.NoError(t, err)
	file.Close()

	// Verify file exists
	_, err = spillfs.OpenFile(context.Background(), bucketName)
	require.NoError(t, err)

	// Clean up
	spillfs.Delete(context.Background(), bucketName)
}

func TestNullValues(t *testing.T) {
	mp := mpool.MustNewZero()
	var buf []byte
	vec := testutil.MakeInt32Vector([]int32{1, 2, 3}, []uint64{1}, mp)
	hashValues := make([]uint64, 3)
	err := computeXXHash([]*vector.Vector{vec}, hashValues, &buf, 0)
	require.NoError(t, err)
	require.NotEqual(t, uint64(0), hashValues[0])
	require.NotEqual(t, uint64(0), hashValues[2])
}

func TestFileWriteError(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	analyzer := process.NewAnalyzer(0, false, false, "test")
	// Create a closed file to trigger write error
	spillfs, _ := proc.GetSpillFileService()
	file, _ := spillfs.CreateFile(context.Background(), "test_error")
	file.Close()

	bat := batch.NewWithSize(1)
	bat.Vecs[0] = testutil.MakeInt32Vector([]int32{1}, nil, proc.Mp())
	bat.SetRowCount(1)

	ctr := &container{}
	_, err := ctr.flushBucketBuffer(proc, bat, file, analyzer)
	require.Error(t, err)

	spillfs.Delete(context.Background(), "test_error")
}

func TestSpillBucketReader(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	spillfs, err := proc.GetSpillFileService()
	require.NoError(t, err)

	analyzer := process.NewAnalyzer(0, false, false, "test")
	bucketName := "test_reader"
	file, err := spillfs.CreateFile(context.Background(), bucketName)
	require.NoError(t, err)

	// Write test batches
	ctr := &container{}
	bat1 := batch.NewOffHeapWithSize(1)
	bat1.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 2, 3}, nil, proc.Mp())
	bat1.SetRowCount(3)

	_, err = ctr.flushBucketBuffer(proc, bat1, file, analyzer)
	require.NoError(t, err)

	bat2 := batch.NewOffHeapWithSize(1)
	bat2.Vecs[0] = testutil.MakeInt32Vector([]int32{4, 5}, nil, proc.Mp())
	bat2.SetRowCount(2)

	_, err = ctr.flushBucketBuffer(proc, bat2, file, analyzer)
	require.NoError(t, err)
	file.Close()

	// Test reader
	reader, err := newSpillBucketReader(proc, bucketName)
	require.NoError(t, err)

	reuseBat := batch.NewOffHeapWithSize(0)
	defer reuseBat.Clean(proc.Mp())

	// Read first batch
	bat, err := reader.readBatch(proc, reuseBat)
	require.NoError(t, err)
	require.Equal(t, 3, bat.RowCount())

	// Read second batch
	bat, err = reader.readBatch(proc, reuseBat)
	require.NoError(t, err)
	require.Equal(t, 2, bat.RowCount())

	// EOF
	bat, err = reader.readBatch(proc, reuseBat)
	require.Equal(t, io.EOF, err)
	require.Nil(t, bat)

	reader.close()
	spillfs.Delete(context.Background(), bucketName)
}

func TestAppendProbeBatchToSpillFiles(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	buckets, files, err := createRootProbeSpillBucketFiles(proc)
	require.NoError(t, err)
	defer func() {
		spillfs, _ := proc.GetSpillFileService()
		for i, file := range files {
			file.Close()
			spillfs.Delete(context.Background(), buckets[i])
		}
	}()

	analyzer := process.NewAnalyzer(0, false, false, "test")
	buffers := make([]*batch.Batch, spillNumBuckets)

	// Create test batch
	bat := batch.NewWithSize(2)
	bat.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 2, 3, 4, 5}, nil, proc.Mp())
	bat.Vecs[1] = testutil.MakeVarcharVector([]string{"a", "b", "c", "d", "e"}, nil, proc.Mp())
	bat.SetRowCount(5)

	ctr := &container{
		eqCondVecs: []*vector.Vector{bat.Vecs[0]},
	}

	err = ctr.appendProbeBatchToSpillFiles(proc, bat, files, buffers, analyzer, 0)
	require.NoError(t, err)

	// Flush remaining buffers
	for i, buf := range buffers {
		if buf != nil && buf.RowCount() > 0 {
			_, err := ctr.flushBucketBuffer(proc, buf, files[i], analyzer)
			require.NoError(t, err)
		}
	}

	// Close files
	for _, file := range files {
		file.Close()
	}

	// Verify data was distributed
	totalRows := 0
	for _, bucketName := range buckets {
		reader, err := newSpillBucketReader(proc, bucketName)
		require.NoError(t, err)

		reuseBat := batch.NewOffHeapWithSize(0)
		for {
			bat, err := reader.readBatch(proc, reuseBat)
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			totalRows += bat.RowCount()
		}
		reuseBat.Clean(proc.Mp())
		reader.close()
	}
	require.Equal(t, 5, totalRows)
}

func TestAppendProbeBatchMultipleFlushes(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	buckets, files, err := createRootProbeSpillBucketFiles(proc)
	require.NoError(t, err)
	defer func() {
		spillfs, _ := proc.GetSpillFileService()
		for i, file := range files {
			file.Close()
			spillfs.Delete(context.Background(), buckets[i])
		}
	}()

	analyzer := process.NewAnalyzer(0, false, false, "test")
	buffers := make([]*batch.Batch, spillNumBuckets)

	// Create large batch to trigger buffer flushes
	size := spillBufferSize * 2
	values := make([]int32, size)
	for i := range values {
		values[i] = int32(i)
	}

	bat := batch.NewWithSize(1)
	bat.Vecs[0] = testutil.MakeInt32Vector(values, nil, proc.Mp())
	bat.SetRowCount(size)

	ctr := &container{
		eqCondVecs: []*vector.Vector{bat.Vecs[0]},
	}

	err = ctr.appendProbeBatchToSpillFiles(proc, bat, files, buffers, analyzer, 0)
	require.NoError(t, err)

	// Flush remaining
	for i, buf := range buffers {
		if buf != nil && buf.RowCount() > 0 {
			_, err := ctr.flushBucketBuffer(proc, buf, files[i], analyzer)
			require.NoError(t, err)
		}
	}
}

func TestAppendProbeBatchEmptyBatch(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	buckets, files, err := createRootProbeSpillBucketFiles(proc)
	require.NoError(t, err)
	defer func() {
		spillfs, _ := proc.GetSpillFileService()
		for i, file := range files {
			file.Close()
			spillfs.Delete(context.Background(), buckets[i])
		}
	}()

	analyzer := process.NewAnalyzer(0, false, false, "test")
	buffers := make([]*batch.Batch, spillNumBuckets)

	bat := batch.NewWithSize(1)
	bat.Vecs[0] = testutil.MakeInt32Vector([]int32{}, nil, proc.Mp())
	bat.SetRowCount(0)

	ctr := &container{
		eqCondVecs: []*vector.Vector{bat.Vecs[0]},
	}

	err = ctr.appendProbeBatchToSpillFiles(proc, bat, files, buffers, analyzer, 0)
	require.NoError(t, err)
}

func TestReaderCorruptedMagic(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	spillfs, err := proc.GetSpillFileService()
	require.NoError(t, err)

	bucketName := "test_corrupt"
	file, err := spillfs.CreateFile(context.Background(), bucketName)
	require.NoError(t, err)

	// Write valid batch first
	ctr := &container{}
	bat := batch.NewOffHeapWithSize(1)
	bat.Vecs[0] = testutil.MakeInt32Vector([]int32{1}, nil, proc.Mp())
	bat.SetRowCount(1)

	// Write with wrong magic
	cnt := int64(1)
	ctr.spillWriteBuf.Reset()
	ctr.spillWriteBuf.Write(types.EncodeInt64(&cnt))
	batchSizePos := ctr.spillWriteBuf.Len()
	ctr.spillWriteBuf.Write(types.EncodeInt64(new(int64)))
	batchStartPos := ctr.spillWriteBuf.Len()
	bat.MarshalBinaryWithBuffer(&ctr.spillWriteBuf, false)
	batchSize := int64(ctr.spillWriteBuf.Len() - batchStartPos)
	batchSizeBytes := types.EncodeInt64(&batchSize)
	copy(ctr.spillWriteBuf.Bytes()[batchSizePos:batchSizePos+len(batchSizeBytes)], batchSizeBytes)
	wrongMagic := uint64(0xBADBADBAD)
	ctr.spillWriteBuf.Write(types.EncodeUint64(&wrongMagic))
	_, _ = file.Write(ctr.spillWriteBuf.Bytes())
	file.Close()

	reader, err := newSpillBucketReader(proc, bucketName)
	require.NoError(t, err)

	reuseBat := batch.NewOffHeapWithSize(0)
	defer reuseBat.Clean(proc.Mp())

	_, err = reader.readBatch(proc, reuseBat)
	require.Error(t, err)
	require.Contains(t, err.Error(), "corrupted")

	reader.close()
	spillfs.Delete(context.Background(), bucketName)
}

func TestReaderRowCountMismatch(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	spillfs, err := proc.GetSpillFileService()
	require.NoError(t, err)

	bucketName := "test_mismatch"
	file, err := spillfs.CreateFile(context.Background(), bucketName)
	require.NoError(t, err)

	// Write batch with wrong count
	ctr := &container{}
	bat := batch.NewOffHeapWithSize(1)
	bat.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 2}, nil, proc.Mp())
	bat.SetRowCount(2)

	// Manually write with wrong count
	wrongCnt := int64(5) // claim 5 rows but only have 2
	ctr.spillWriteBuf.Reset()
	ctr.spillWriteBuf.Write(types.EncodeInt64(&wrongCnt))
	batchSizePos := ctr.spillWriteBuf.Len()
	ctr.spillWriteBuf.Write(types.EncodeInt64(new(int64)))
	batchStartPos := ctr.spillWriteBuf.Len()
	bat.MarshalBinaryWithBuffer(&ctr.spillWriteBuf, false)
	batchSize := int64(ctr.spillWriteBuf.Len() - batchStartPos)
	batchSizeBytes := types.EncodeInt64(&batchSize)
	copy(ctr.spillWriteBuf.Bytes()[batchSizePos:batchSizePos+len(batchSizeBytes)], batchSizeBytes)
	magic := uint64(spillMagic)
	ctr.spillWriteBuf.Write(types.EncodeUint64(&magic))
	_, _ = file.Write(ctr.spillWriteBuf.Bytes())
	file.Close()

	reader, err := newSpillBucketReader(proc, bucketName)
	require.NoError(t, err)

	reuseBat := batch.NewOffHeapWithSize(0)
	defer reuseBat.Clean(proc.Mp())

	_, err = reader.readBatch(proc, reuseBat)
	require.Error(t, err)
	require.Contains(t, err.Error(), "mismatch")

	reader.close()
	spillfs.Delete(context.Background(), bucketName)
}

func TestMultiColumnHash(t *testing.T) {
	mp := mpool.MustNewZero()
	var buf []byte

	vec1 := testutil.MakeInt32Vector([]int32{1, 1, 2}, nil, mp)
	vec2 := testutil.MakeVarcharVector([]string{"a", "b", "a"}, nil, mp)

	hashValues := make([]uint64, 3)
	err := computeXXHash([]*vector.Vector{vec1, vec2}, hashValues, &buf, 0)
	require.NoError(t, err)

	// Same key values should produce same hash
	require.NotEqual(t, hashValues[0], hashValues[1])
	require.NotEqual(t, hashValues[0], hashValues[2])
}

func TestHashWithNulls(t *testing.T) {
	mp := mpool.MustNewZero()
	var buf []byte

	vec := testutil.MakeInt32Vector([]int32{1, 2, 3, 4}, []uint64{0, 1}, mp) // nulls at index 1 and 3
	hashValues := make([]uint64, 4)
	err := computeXXHash([]*vector.Vector{vec}, hashValues, &buf, 0)
	require.NoError(t, err)

	// All hashes should be computed
	for _, h := range hashValues {
		require.NotEqual(t, uint64(0), h)
	}
}

func TestSpillBucketReaderDoubleClose(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	spillfs, err := proc.GetSpillFileService()
	require.NoError(t, err)

	bucketName := "test_double_close"
	file, err := spillfs.CreateFile(context.Background(), bucketName)
	require.NoError(t, err)
	file.Close()

	reader, err := newSpillBucketReader(proc, bucketName)
	require.NoError(t, err)

	reader.close()
	reader.close() // Should not panic

	spillfs.Delete(context.Background(), bucketName)
}

func TestCreateProbeSpillFilesError(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	// Normal case should succeed
	buckets, files, err := createRootProbeSpillBucketFiles(proc)
	require.NoError(t, err)
	require.Equal(t, spillNumBuckets, len(buckets))
	require.Equal(t, spillNumBuckets, len(files))

	spillfs, _ := proc.GetSpillFileService()
	for i, file := range files {
		file.Close()
		spillfs.Delete(context.Background(), buckets[i])
	}
}

func TestFlushEmptyBuffer(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	spillfs, err := proc.GetSpillFileService()
	require.NoError(t, err)

	file, err := spillfs.CreateFile(context.Background(), "test_empty")
	require.NoError(t, err)
	defer func() {
		file.Close()
		spillfs.Delete(context.Background(), "test_empty")
	}()

	analyzer := process.NewAnalyzer(0, false, false, "test")
	ctr := &container{}

	// Nil batch
	cnt, err := ctr.flushBucketBuffer(proc, nil, file, analyzer)
	require.NoError(t, err)
	require.Equal(t, int64(0), cnt)

	// Zero row batch
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = testutil.MakeInt32Vector([]int32{}, nil, proc.Mp())
	bat.SetRowCount(0)

	cnt, err = ctr.flushBucketBuffer(proc, bat, file, analyzer)
	require.NoError(t, err)
	require.Equal(t, int64(0), cnt)
}

func TestHashBufferReuse(t *testing.T) {
	mp := mpool.MustNewZero()
	buf := make([]byte, 0, 10)

	vec := testutil.MakeInt32Vector([]int32{1, 2, 3}, nil, mp)
	hashValues := make([]uint64, 3)

	// First call
	err := computeXXHash([]*vector.Vector{vec}, hashValues, &buf, 0)
	require.NoError(t, err)
	firstCap := cap(buf)

	// Second call with larger data
	vec2 := testutil.MakeVarcharVector([]string{"long_string_value", "another_long_value", "third_value"}, nil, mp)
	err = computeXXHash([]*vector.Vector{vec2}, hashValues, &buf, 0)
	require.NoError(t, err)

	// Buffer should grow if needed
	require.GreaterOrEqual(t, cap(buf), firstCap)
}

func TestConstVectorHash(t *testing.T) {
	mp := mpool.MustNewZero()
	var buf []byte

	vec := testutil.MakeInt32Vector([]int32{42}, nil, mp)
	vec.SetClass(vector.CONSTANT)

	hashValues := make([]uint64, 10)
	err := computeXXHash([]*vector.Vector{vec}, hashValues, &buf, 0)
	require.NoError(t, err)

	// All values should be the same for const vector
	for i := 1; i < len(hashValues); i++ {
		require.Equal(t, hashValues[0], hashValues[i])
	}
}

func TestAppendProbeBatchSkipEmptyBuckets(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	buckets, files, err := createRootProbeSpillBucketFiles(proc)
	require.NoError(t, err)
	defer func() {
		spillfs, _ := proc.GetSpillFileService()
		for i, file := range files {
			file.Close()
			spillfs.Delete(context.Background(), buckets[i])
		}
	}()

	analyzer := process.NewAnalyzer(0, false, false, "test")
	buffers := make([]*batch.Batch, spillNumBuckets)

	// Create batch with single value (will go to one bucket)
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = testutil.MakeInt32Vector([]int32{1}, nil, proc.Mp())
	bat.SetRowCount(1)

	ctr := &container{
		eqCondVecs: []*vector.Vector{bat.Vecs[0]},
	}

	err = ctr.appendProbeBatchToSpillFiles(proc, bat, files, buffers, analyzer, 0)
	require.NoError(t, err)

	// Most buffers should be nil
	nilCount := 0
	for _, buf := range buffers {
		if buf == nil {
			nilCount++
		}
	}
	require.Greater(t, nilCount, spillNumBuckets-5)
}

func TestGetSpilledInputBatchNoBuckets(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	hashJoin := &HashJoin{
		ctr: container{
			spillQueue: []spillBucket{},
		},
	}

	result, err := hashJoin.getSpilledInputBatch(proc, process.NewAnalyzer(0, false, false, "test"))
	require.NoError(t, err)
	require.Nil(t, result.Batch)
}

func TestGetSpilledInputBatchAllProcessed(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	hashJoin := &HashJoin{
		ctr: container{
			spillQueue: []spillBucket{}, // empty = all processed
		},
	}

	result, err := hashJoin.getSpilledInputBatch(proc, process.NewAnalyzer(0, false, false, "test"))
	require.NoError(t, err)
	require.Nil(t, result.Batch)
}

func TestGetInputBatchReadsCurrentSpillReaderWhenQueueEmpty(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	spillfs, err := proc.GetSpillFileService()
	require.NoError(t, err)

	const bucketName = "test_get_input_batch_spill_reader"
	file, err := spillfs.CreateFile(context.Background(), bucketName)
	require.NoError(t, err)

	analyzer := process.NewAnalyzer(0, false, false, "test")

	ctr := &container{}
	writeBat := batch.NewWithSize(1)
	writeBat.Vecs[0] = testutil.MakeInt32Vector([]int32{42}, nil, proc.Mp())
	writeBat.SetRowCount(1)
	_, err = ctr.flushBucketBuffer(proc, writeBat, file, analyzer)
	require.NoError(t, err)
	require.NoError(t, file.Close())

	reader, err := newSpillBucketReader(proc, bucketName)
	require.NoError(t, err)

	hashJoin := &HashJoin{
		ctr: container{
			probeBucketReader:   reader,
			probeBucketFileName: bucketName,
		},
	}
	hashJoin.AppendChild(colexec.NewMockOperator())

	result, err := hashJoin.getInputBatch(proc, analyzer)
	require.NoError(t, err)
	require.NotNil(t, result.Batch)
	require.Equal(t, 1, result.Batch.RowCount())

	hashJoin.ctr.cleanBucketBatches(proc)
}

func TestNewSpillBucketReaderError(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	// Try to open non-existent file
	_, err := newSpillBucketReader(proc, "non_existent_bucket")
	require.Error(t, err)
}

func TestReadBatchPartialRead(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	spillfs, err := proc.GetSpillFileService()
	require.NoError(t, err)

	bucketName := "test_partial"
	file, err := spillfs.CreateFile(context.Background(), bucketName)
	require.NoError(t, err)

	// Write incomplete data (only count, no batch)
	cnt := int64(5)
	_, _ = file.Write(types.EncodeInt64(&cnt))
	file.Close()

	reader, err := newSpillBucketReader(proc, bucketName)
	require.NoError(t, err)

	reuseBat := batch.NewOffHeapWithSize(0)
	defer reuseBat.Clean(proc.Mp())

	_, err = reader.readBatch(proc, reuseBat)
	require.Error(t, err)

	reader.close()
	spillfs.Delete(context.Background(), bucketName)
}

func TestAppendProbeBatchWithNulls(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	buckets, files, err := createRootProbeSpillBucketFiles(proc)
	require.NoError(t, err)
	defer func() {
		spillfs, _ := proc.GetSpillFileService()
		for i, file := range files {
			file.Close()
			spillfs.Delete(context.Background(), buckets[i])
		}
	}()

	analyzer := process.NewAnalyzer(0, false, false, "test")
	buffers := make([]*batch.Batch, spillNumBuckets)

	// Create batch with nulls
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 2, 3, 4}, []uint64{1}, proc.Mp()) // null at index 1
	bat.SetRowCount(4)

	ctr := &container{
		eqCondVecs: []*vector.Vector{bat.Vecs[0]},
	}

	err = ctr.appendProbeBatchToSpillFiles(proc, bat, files, buffers, analyzer, 0)
	require.NoError(t, err)

	// Flush remaining
	for i, buf := range buffers {
		if buf != nil && buf.RowCount() > 0 {
			_, err := ctr.flushBucketBuffer(proc, buf, files[i], analyzer)
			require.NoError(t, err)
		}
	}
}

func TestMultipleBatchesInBucket(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	spillfs, err := proc.GetSpillFileService()
	require.NoError(t, err)

	analyzer := process.NewAnalyzer(0, false, false, "test")
	bucketName := "test_multiple"
	file, err := spillfs.CreateFile(context.Background(), bucketName)
	require.NoError(t, err)

	ctr := &container{}

	// Write 5 batches
	for i := 0; i < 5; i++ {
		bat := batch.NewOffHeapWithSize(1)
		bat.Vecs[0] = testutil.MakeInt32Vector([]int32{int32(i * 10), int32(i*10 + 1)}, nil, proc.Mp())
		bat.SetRowCount(2)

		_, err = ctr.flushBucketBuffer(proc, bat, file, analyzer)
		require.NoError(t, err)
	}
	file.Close()

	// Read all batches
	reader, err := newSpillBucketReader(proc, bucketName)
	require.NoError(t, err)

	reuseBat := batch.NewOffHeapWithSize(0)
	defer reuseBat.Clean(proc.Mp())

	totalRows := 0
	batchCount := 0
	for {
		bat, err := reader.readBatch(proc, reuseBat)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		totalRows += bat.RowCount()
		batchCount++
	}

	require.Equal(t, 5, batchCount)
	require.Equal(t, 10, totalRows)

	reader.close()
	spillfs.Delete(context.Background(), bucketName)
}

func TestCreateProbeSpillFilesPartialError(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	// Normal creation should work
	buckets, files, err := createRootProbeSpillBucketFiles(proc)
	require.NoError(t, err)
	require.Len(t, buckets, spillNumBuckets)
	require.Len(t, files, spillNumBuckets)

	spillfs, _ := proc.GetSpillFileService()
	for i, file := range files {
		require.NotNil(t, file)
		file.Close()
		spillfs.Delete(context.Background(), buckets[i])
	}
}

func TestComputeXXHashVectorLengthMismatch(t *testing.T) {
	mp := mpool.MustNewZero()
	var buf []byte

	// Create vectors with different lengths
	vec1 := testutil.MakeInt32Vector([]int32{1, 2, 3}, nil, mp)
	vec2 := testutil.MakeInt32Vector([]int32{4, 5}, nil, mp)

	hashValues := make([]uint64, 3)
	err := computeXXHash([]*vector.Vector{vec1, vec2}, hashValues, &buf, 0)
	require.NoError(t, err)

	// Should handle gracefully
	for _, h := range hashValues {
		require.NotEqual(t, uint64(0), h)
	}
}

func TestAppendProbeBatchLargeData(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	buckets, files, err := createRootProbeSpillBucketFiles(proc)
	require.NoError(t, err)
	defer func() {
		spillfs, _ := proc.GetSpillFileService()
		for i, file := range files {
			file.Close()
			spillfs.Delete(context.Background(), buckets[i])
		}
	}()

	analyzer := process.NewAnalyzer(0, false, false, "test")
	buffers := make([]*batch.Batch, spillNumBuckets)

	// Create batch with large strings
	size := 100
	values := make([]string, size)
	for i := range values {
		values[i] = fmt.Sprintf("large_string_value_%d_with_extra_padding", i)
	}

	bat := batch.NewWithSize(1)
	bat.Vecs[0] = testutil.MakeVarcharVector(values, nil, proc.Mp())
	bat.SetRowCount(size)

	ctr := &container{
		eqCondVecs: []*vector.Vector{bat.Vecs[0]},
	}

	err = ctr.appendProbeBatchToSpillFiles(proc, bat, files, buffers, analyzer, 0)
	require.NoError(t, err)

	// Flush remaining
	for i, buf := range buffers {
		if buf != nil && buf.RowCount() > 0 {
			_, err := ctr.flushBucketBuffer(proc, buf, files[i], analyzer)
			require.NoError(t, err)
		}
	}
}

func TestReaderBatchReuse(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	spillfs, err := proc.GetSpillFileService()
	require.NoError(t, err)

	analyzer := process.NewAnalyzer(0, false, false, "test")
	bucketName := "test_reuse"
	file, err := spillfs.CreateFile(context.Background(), bucketName)
	require.NoError(t, err)

	ctr := &container{}

	// Write batches with different sizes
	bat1 := batch.NewOffHeapWithSize(1)
	bat1.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 2, 3, 4, 5}, nil, proc.Mp())
	bat1.SetRowCount(5)
	_, err = ctr.flushBucketBuffer(proc, bat1, file, analyzer)
	require.NoError(t, err)

	bat2 := batch.NewOffHeapWithSize(1)
	bat2.Vecs[0] = testutil.MakeInt32Vector([]int32{6, 7}, nil, proc.Mp())
	bat2.SetRowCount(2)
	_, err = ctr.flushBucketBuffer(proc, bat2, file, analyzer)
	require.NoError(t, err)

	file.Close()

	// Read with same reusable batch
	reader, err := newSpillBucketReader(proc, bucketName)
	require.NoError(t, err)

	reuseBat := batch.NewOffHeapWithSize(0)
	defer reuseBat.Clean(proc.Mp())

	// First read
	bat, err := reader.readBatch(proc, reuseBat)
	require.NoError(t, err)
	require.Equal(t, 5, bat.RowCount())

	// Second read - batch should be reused
	bat, err = reader.readBatch(proc, reuseBat)
	require.NoError(t, err)
	require.Equal(t, 2, bat.RowCount())

	reader.close()
	spillfs.Delete(context.Background(), bucketName)
}

func TestAppendProbeBatchAllBuckets(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	buckets, files, err := createRootProbeSpillBucketFiles(proc)
	require.NoError(t, err)
	defer func() {
		spillfs, _ := proc.GetSpillFileService()
		for i, file := range files {
			file.Close()
			spillfs.Delete(context.Background(), buckets[i])
		}
	}()

	analyzer := process.NewAnalyzer(0, false, false, "test")
	buffers := make([]*batch.Batch, spillNumBuckets)

	// Create enough data to hit all buckets
	size := spillNumBuckets * 10
	values := make([]int32, size)
	for i := range values {
		values[i] = int32(i)
	}

	bat := batch.NewWithSize(1)
	bat.Vecs[0] = testutil.MakeInt32Vector(values, nil, proc.Mp())
	bat.SetRowCount(size)

	ctr := &container{
		eqCondVecs: []*vector.Vector{bat.Vecs[0]},
	}

	err = ctr.appendProbeBatchToSpillFiles(proc, bat, files, buffers, analyzer, 0)
	require.NoError(t, err)

	// Flush all buffers
	for i, buf := range buffers {
		if buf != nil && buf.RowCount() > 0 {
			_, err := ctr.flushBucketBuffer(proc, buf, files[i], analyzer)
			require.NoError(t, err)
		}
	}

	// Close files
	for _, file := range files {
		file.Close()
	}

	// Verify all buckets have data
	nonEmptyBuckets := 0
	totalRows := 0
	for _, bucketName := range buckets {
		reader, err := newSpillBucketReader(proc, bucketName)
		require.NoError(t, err)

		reuseBat := batch.NewOffHeapWithSize(0)
		bucketRows := 0
		for {
			bat, err := reader.readBatch(proc, reuseBat)
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			bucketRows += bat.RowCount()
		}
		reuseBat.Clean(proc.Mp())
		reader.close()

		if bucketRows > 0 {
			nonEmptyBuckets++
		}
		totalRows += bucketRows
	}

	require.Equal(t, size, totalRows)
	require.Greater(t, nonEmptyBuckets, spillNumBuckets/2) // At least half should have data
}

func TestFlushBucketBufferMultipleCalls(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	spillfs, err := proc.GetSpillFileService()
	require.NoError(t, err)

	file, err := spillfs.CreateFile(context.Background(), "test_multi_flush")
	require.NoError(t, err)
	defer func() {
		file.Close()
		spillfs.Delete(context.Background(), "test_multi_flush")
	}()

	analyzer := process.NewAnalyzer(0, false, false, "test")
	ctr := &container{}

	// Multiple flushes
	totalRows := int64(0)
	for i := 0; i < 10; i++ {
		bat := batch.NewWithSize(1)
		bat.Vecs[0] = testutil.MakeInt32Vector([]int32{int32(i)}, nil, proc.Mp())
		bat.SetRowCount(1)

		cnt, err := ctr.flushBucketBuffer(proc, bat, file, analyzer)
		require.NoError(t, err)
		totalRows += cnt
	}

	require.Equal(t, int64(10), totalRows)
}

func TestNewSpillBucketReaderNonExistent(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	_, err := newSpillBucketReader(proc, "definitely_does_not_exist_bucket")
	require.Error(t, err)
}

func TestHashValuesBufferGrowth(t *testing.T) {
	mp := mpool.MustNewZero()
	var buf []byte

	// Start with small buffer
	vec := testutil.MakeInt32Vector([]int32{1, 2}, nil, mp)
	hashValues := make([]uint64, 2)
	err := computeXXHash([]*vector.Vector{vec}, hashValues, &buf, 0)
	require.NoError(t, err)

	// Larger data should grow buffer
	largeVec := testutil.MakeVarcharVector([]string{
		"very_long_string_value_1",
		"very_long_string_value_2",
		"very_long_string_value_3",
	}, nil, mp)
	largeHashValues := make([]uint64, 3)
	err = computeXXHash([]*vector.Vector{largeVec}, largeHashValues, &buf, 0)
	require.NoError(t, err)

	for _, h := range largeHashValues {
		require.NotEqual(t, uint64(0), h)
	}
}
