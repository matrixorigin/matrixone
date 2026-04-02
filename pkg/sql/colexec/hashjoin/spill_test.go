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
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestComputeXXHash(t *testing.T) {
	mp := mpool.MustNewZero()

	t.Run("empty", func(t *testing.T) {
		err := computeXXHash(nil, nil, 0)
		require.NoError(t, err)
	})

	t.Run("single_int32", func(t *testing.T) {
		vec := testutil.MakeInt32Vector([]int32{1, 2, 3}, nil, mp)
		hashValues := make([]uint64, 3)
		err := computeXXHash([]*vector.Vector{vec}, hashValues, 0)
		require.NoError(t, err)
		require.NotEqual(t, uint64(0), hashValues[0])
		require.NotEqual(t, hashValues[0], hashValues[1])
		require.NotEqual(t, hashValues[1], hashValues[2])
	})

	t.Run("multiple_columns", func(t *testing.T) {
		vec1 := testutil.MakeInt32Vector([]int32{1, 2}, nil, mp)
		vec2 := testutil.MakeVarcharVector([]string{"a", "b"}, nil, mp)
		hashValues := make([]uint64, 2)
		err := computeXXHash([]*vector.Vector{vec1, vec2}, hashValues, 0)
		require.NoError(t, err)
		require.NotEqual(t, hashValues[0], hashValues[1])
	})

	t.Run("const_vector", func(t *testing.T) {
		vec := testutil.MakeInt32Vector([]int32{5}, nil, mp)
		vec.SetClass(vector.CONSTANT)
		hashValues := make([]uint64, 3)
		err := computeXXHash([]*vector.Vector{vec}, hashValues, 0)
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
		spillfs.RemoveFile(context.Background(), "test_flush")
	}()

	analyzer := process.NewAnalyzer(0, false, false, "test")
	ctr := &container{}

	t.Run("empty_buffer", func(t *testing.T) {
		var buf *batch.Batch
		file_sw := spillBucketWriter{file: file}
		cnt, err := ctr.flushBucketBuffer(proc, buf, &file_sw, analyzer)
		require.NoError(t, err)
		require.Equal(t, int64(0), cnt)
	})

	t.Run("with_data", func(t *testing.T) {
		bat := batch.NewWithSize(1)
		bat.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 2, 3}, nil, proc.Mp())
		bat.SetRowCount(3)

		file_sw := spillBucketWriter{file: file}
		cnt, err := ctr.flushBucketBuffer(proc, bat, &file_sw, analyzer)
		require.NoError(t, err)
		require.Equal(t, int64(3), cnt)
	})
}

func TestCreateProbeSpillFiles(t *testing.T) {
	writers, err := createRootProbeSpillBucketFiles()
	require.NoError(t, err)
	require.Equal(t, spillNumBuckets, len(writers))

	for i := range writers {
		require.NotEmpty(t, writers[i].name)
		writers[i].close()
	}
}

// TestLazySpillFileCreation verifies that files are only created when data is written.
// Untouched buckets should have nil files, while buckets with data should get a file
// created on first flush.
func TestLazySpillFileCreation(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	analyzer := process.NewAnalyzer(0, false, false, "test")

	// Create writers without creating any files
	uid, err := uuid.NewV7()
	require.NoError(t, err)
	writers := makeSpillBucketWriters(uid.String(), "lazy_test")

	// All files should be nil before any write
	for i := range writers {
		require.Nil(t, writers[i].file, "bucket %d should have nil file before any write", i)
	}

	// Create a batch with values that will hash to various buckets
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 2, 3}, nil, proc.Mp())
	bat.SetRowCount(3)

	ctr := &container{eqCondVecs: []*vector.Vector{bat.Vecs[0]}}
	buffers := make([]*batch.Batch, spillNumBuckets)

	// Write the batch (this will populate some buckets)
	err = ctr.appendProbeBatchToSpillFiles(proc, bat, writers, buffers, analyzer, 0)
	require.NoError(t, err)

	// Find which buckets got data
	var populatedBucket int
	populated := false
	for i := range buffers {
		if buffers[i] != nil && buffers[i].RowCount() > 0 {
			populated = true
			populatedBucket = i
			break
		}
	}
	require.True(t, populated, "at least one bucket should have data")

	// Flush the populated bucket's buffer (should create the file lazily)
	writer := &writers[populatedBucket]
	require.NotNil(t, buffers[populatedBucket], "buffer should exist before flush")
	cnt, err := ctr.flushBucketBuffer(proc, buffers[populatedBucket], writer, analyzer)
	require.NoError(t, err)
	require.Greater(t, cnt, int64(0))
	require.NotNil(t, writer.file, "file should be created on first flush")

	// Untouched buckets should still have nil files
	for i := range buffers {
		if buffers[i] == nil || buffers[i].RowCount() == 0 {
			require.Nil(t, writers[i].file, "untouched bucket %d should still have nil file", i)
		}
	}

	// Clean up
	for i := range writers {
		writers[i].close()
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
		spillfs.RemoveFile(context.Background(), "test_reuse")
	}()

	ctr := &container{}

	// First batch
	bat1 := batch.NewWithSize(1)
	bat1.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 2}, nil, proc.Mp())
	bat1.SetRowCount(2)

	file_sw := spillBucketWriter{file: file}
	_, err = ctr.flushBucketBuffer(proc, bat1, &file_sw, analyzer)
	require.NoError(t, err)

	// Second batch
	bat2 := batch.NewWithSize(1)
	bat2.Vecs[0] = testutil.MakeInt32Vector([]int32{3, 4}, nil, proc.Mp())
	bat2.SetRowCount(2)

	file_sw = spillBucketWriter{file: file}
	_, err = ctr.flushBucketBuffer(proc, bat2, &file_sw, analyzer)
	require.NoError(t, err)
}

func TestHashDistribution(t *testing.T) {
	mp := mpool.MustNewZero()

	// Test that hash values distribute across buckets
	vec := testutil.MakeInt32Vector([]int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
		11, 12, 13, 14, 15, 16, 17, 18, 19, 20}, nil, mp)

	hashValues := make([]uint64, 20)
	err := computeXXHash([]*vector.Vector{vec}, hashValues, 0)
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
		file_sw := spillBucketWriter{file: file}
		_, err = ctr.flushBucketBuffer(proc, bat, &file_sw, analyzer)
		require.NoError(t, err)
	}
	file.Close()
}

func TestMultipleDataTypes(t *testing.T) {
	mp := mpool.MustNewZero()

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
			err := computeXXHash([]*vector.Vector{tt.vec}, hashValues, 0)
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
		spillfs.RemoveFile(context.Background(), "test_large")
	}()

	// Create large batch
	size := spillRowBufferSize + 100
	values := make([]int32, size)
	for i := range values {
		values[i] = int32(i)
	}

	bat := batch.NewWithSize(1)
	bat.Vecs[0] = testutil.MakeInt32Vector(values, nil, proc.Mp())
	bat.SetRowCount(size)

	ctr := &container{}
	file_sw := spillBucketWriter{file: file}
	cnt, err := ctr.flushBucketBuffer(proc, bat, &file_sw, analyzer)
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
	file_sw := spillBucketWriter{file: file}
	_, err = ctr.flushBucketBuffer(proc, bat, &file_sw, analyzer)
	require.NoError(t, err)
	file.Close()

	// Verify file exists
	_, err = spillfs.OpenFile(context.Background(), bucketName)
	require.NoError(t, err)

	// Clean up
	spillfs.RemoveFile(context.Background(), bucketName)
}

func TestNullValues(t *testing.T) {
	mp := mpool.MustNewZero()

	vec := testutil.MakeInt32Vector([]int32{1, 2, 3}, []uint64{1}, mp)
	hashValues := make([]uint64, 3)
	err := computeXXHash([]*vector.Vector{vec}, hashValues, 0)
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
	file_sw := spillBucketWriter{file: file}
	_, err := ctr.flushBucketBuffer(proc, bat, &file_sw, analyzer)
	require.Error(t, err)

	spillfs.RemoveFile(context.Background(), "test_error")
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

	file_sw := spillBucketWriter{file: file}
	_, err = ctr.flushBucketBuffer(proc, bat1, &file_sw, analyzer)
	require.NoError(t, err)

	bat2 := batch.NewOffHeapWithSize(1)
	bat2.Vecs[0] = testutil.MakeInt32Vector([]int32{4, 5}, nil, proc.Mp())
	bat2.SetRowCount(2)

	file_sw = spillBucketWriter{file: file}
	_, err = ctr.flushBucketBuffer(proc, bat2, &file_sw, analyzer)
	require.NoError(t, err)
	file.Close()

	// Test reader
	reader := &spillBucketReader{}
	err = reader.resetForFile(proc.Ctx, spillfs, bucketName)
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
	spillfs.RemoveFile(context.Background(), bucketName)
}

func TestAppendProbeBatchToSpillFiles(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	writers, err := createRootProbeSpillBucketFiles()
	require.NoError(t, err)
	defer func() {
		for i := range writers {
			writers[i].close()
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

	err = ctr.appendProbeBatchToSpillFiles(proc, bat, writers, buffers, analyzer, 0)
	require.NoError(t, err)

	// Flush remaining buffers
	for i, buf := range buffers {
		if buf != nil && buf.RowCount() > 0 {
			_, err := ctr.flushBucketBuffer(proc, buf, &writers[i], analyzer)
			require.NoError(t, err)
		}
	}

	// Verify data was distributed
	totalRows := 0
	for i := range writers {
		reader := &spillBucketReader{}
		reader.resetForFd(writers[i].handOffFd())

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

	writers, err := createRootProbeSpillBucketFiles()
	require.NoError(t, err)
	defer func() {
		for i := range writers {
			writers[i].close()
		}
	}()

	analyzer := process.NewAnalyzer(0, false, false, "test")
	buffers := make([]*batch.Batch, spillNumBuckets)

	// Create large batch to trigger buffer flushes
	size := spillRowBufferSize * 2
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

	err = ctr.appendProbeBatchToSpillFiles(proc, bat, writers, buffers, analyzer, 0)
	require.NoError(t, err)

	// Flush remaining
	for i, buf := range buffers {
		if buf != nil && buf.RowCount() > 0 {
			_, err := ctr.flushBucketBuffer(proc, buf, &writers[i], analyzer)
			require.NoError(t, err)
		}
	}
}

func TestAppendProbeBatchEmptyBatch(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	writers, err := createRootProbeSpillBucketFiles()
	require.NoError(t, err)
	defer func() {
		for i := range writers {
			writers[i].close()
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

	err = ctr.appendProbeBatchToSpillFiles(proc, bat, writers, buffers, analyzer, 0)
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

	reader := &spillBucketReader{}
	err = reader.resetForFile(proc.Ctx, spillfs, bucketName)
	require.NoError(t, err)

	reuseBat := batch.NewOffHeapWithSize(0)
	defer reuseBat.Clean(proc.Mp())

	_, err = reader.readBatch(proc, reuseBat)
	require.Error(t, err)
	require.Contains(t, err.Error(), "corrupted")

	reader.close()
	spillfs.RemoveFile(context.Background(), bucketName)
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

	reader := &spillBucketReader{}
	err = reader.resetForFile(proc.Ctx, spillfs, bucketName)
	require.NoError(t, err)

	reuseBat := batch.NewOffHeapWithSize(0)
	defer reuseBat.Clean(proc.Mp())

	_, err = reader.readBatch(proc, reuseBat)
	require.Error(t, err)
	require.Contains(t, err.Error(), "mismatch")

	reader.close()
	spillfs.RemoveFile(context.Background(), bucketName)
}

func TestMultiColumnHash(t *testing.T) {
	mp := mpool.MustNewZero()

	vec1 := testutil.MakeInt32Vector([]int32{1, 1, 2}, nil, mp)
	vec2 := testutil.MakeVarcharVector([]string{"a", "b", "a"}, nil, mp)

	hashValues := make([]uint64, 3)
	err := computeXXHash([]*vector.Vector{vec1, vec2}, hashValues, 0)
	require.NoError(t, err)

	// Same key values should produce same hash
	require.NotEqual(t, hashValues[0], hashValues[1])
	require.NotEqual(t, hashValues[0], hashValues[2])
}

func TestHashWithNulls(t *testing.T) {
	mp := mpool.MustNewZero()

	vec := testutil.MakeInt32Vector([]int32{1, 2, 3, 4}, []uint64{0, 1}, mp) // nulls at index 1 and 3
	hashValues := make([]uint64, 4)
	err := computeXXHash([]*vector.Vector{vec}, hashValues, 0)
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

	reader := &spillBucketReader{}
	err = reader.resetForFile(proc.Ctx, spillfs, bucketName)
	require.NoError(t, err)

	reader.close()
	reader.close() // Should not panic

	spillfs.RemoveFile(context.Background(), bucketName)
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
		spillfs.RemoveFile(context.Background(), "test_empty")
	}()

	analyzer := process.NewAnalyzer(0, false, false, "test")
	ctr := &container{}

	// Nil batch
	file_sw := spillBucketWriter{file: file}
	cnt, err := ctr.flushBucketBuffer(proc, nil, &file_sw, analyzer)
	require.NoError(t, err)
	require.Equal(t, int64(0), cnt)

	// Zero row batch
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = testutil.MakeInt32Vector([]int32{}, nil, proc.Mp())
	bat.SetRowCount(0)

	file_sw = spillBucketWriter{file: file}
	cnt, err = ctr.flushBucketBuffer(proc, bat, &file_sw, analyzer)
	require.NoError(t, err)
	require.Equal(t, int64(0), cnt)
}

func TestConstVectorHash(t *testing.T) {
	mp := mpool.MustNewZero()

	vec := testutil.MakeInt32Vector([]int32{42}, nil, mp)
	vec.SetClass(vector.CONSTANT)

	hashValues := make([]uint64, 10)
	err := computeXXHash([]*vector.Vector{vec}, hashValues, 0)
	require.NoError(t, err)

	// All values should be the same for const vector
	for i := 1; i < len(hashValues); i++ {
		require.Equal(t, hashValues[0], hashValues[i])
	}
}

func TestAppendProbeBatchSkipEmptyBuckets(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	writers, err := createRootProbeSpillBucketFiles()
	require.NoError(t, err)
	defer func() {
		for i := range writers {
			writers[i].close()
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

	err = ctr.appendProbeBatchToSpillFiles(proc, bat, writers, buffers, analyzer, 0)
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
	file_sw := spillBucketWriter{file: file}
	_, err = ctr.flushBucketBuffer(proc, writeBat, &file_sw, analyzer)
	require.NoError(t, err)
	require.NoError(t, file.Close())

	reader := &spillBucketReader{}
	err = reader.resetForFile(proc.Ctx, spillfs, bucketName)
	require.NoError(t, err)

	hashJoin := &HashJoin{
		ctr: container{
			probeBucketReader: reader,
			probeBucketActive: true,
		},
	}
	hashJoin.AppendChild(colexec.NewMockOperator())

	result, err := hashJoin.getInputBatch(proc, analyzer)
	require.NoError(t, err)
	require.NotNil(t, result.Batch)
	require.Equal(t, 1, result.Batch.RowCount())

	hashJoin.ctr.cleanBucketBatches(proc)
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

	reader := &spillBucketReader{}
	err = reader.resetForFile(proc.Ctx, spillfs, bucketName)
	require.NoError(t, err)

	reuseBat := batch.NewOffHeapWithSize(0)
	defer reuseBat.Clean(proc.Mp())

	_, err = reader.readBatch(proc, reuseBat)
	require.Error(t, err)

	reader.close()
	spillfs.RemoveFile(context.Background(), bucketName)
}

func TestAppendProbeBatchWithNulls(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	writers, err := createRootProbeSpillBucketFiles()
	require.NoError(t, err)
	defer func() {
		for i := range writers {
			writers[i].close()
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

	err = ctr.appendProbeBatchToSpillFiles(proc, bat, writers, buffers, analyzer, 0)
	require.NoError(t, err)

	// Flush remaining
	for i, buf := range buffers {
		if buf != nil && buf.RowCount() > 0 {
			_, err := ctr.flushBucketBuffer(proc, buf, &writers[i], analyzer)
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

		file_sw := spillBucketWriter{file: file}
		_, err = ctr.flushBucketBuffer(proc, bat, &file_sw, analyzer)
		require.NoError(t, err)
	}
	file.Close()

	// Read all batches
	reader := &spillBucketReader{}
	err = reader.resetForFile(proc.Ctx, spillfs, bucketName)
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
	spillfs.RemoveFile(context.Background(), bucketName)
}

func TestComputeXXHashVectorLengthMismatch(t *testing.T) {
	mp := mpool.MustNewZero()

	// Create vectors with different lengths
	vec1 := testutil.MakeInt32Vector([]int32{1, 2, 3}, nil, mp)
	vec2 := testutil.MakeInt32Vector([]int32{4, 5}, nil, mp)

	hashValues := make([]uint64, 3)
	err := computeXXHash([]*vector.Vector{vec1, vec2}, hashValues, 0)
	require.NoError(t, err)

	// Should handle gracefully
	for _, h := range hashValues {
		require.NotEqual(t, uint64(0), h)
	}
}

func TestAppendProbeBatchLargeData(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	writers, err := createRootProbeSpillBucketFiles()
	require.NoError(t, err)
	defer func() {
		for i := range writers {
			writers[i].close()
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

	err = ctr.appendProbeBatchToSpillFiles(proc, bat, writers, buffers, analyzer, 0)
	require.NoError(t, err)

	// Flush remaining
	for i, buf := range buffers {
		if buf != nil && buf.RowCount() > 0 {
			_, err := ctr.flushBucketBuffer(proc, buf, &writers[i], analyzer)
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
	file_sw := spillBucketWriter{file: file}
	_, err = ctr.flushBucketBuffer(proc, bat1, &file_sw, analyzer)
	require.NoError(t, err)

	bat2 := batch.NewOffHeapWithSize(1)
	bat2.Vecs[0] = testutil.MakeInt32Vector([]int32{6, 7}, nil, proc.Mp())
	bat2.SetRowCount(2)
	file_sw = spillBucketWriter{file: file}
	_, err = ctr.flushBucketBuffer(proc, bat2, &file_sw, analyzer)
	require.NoError(t, err)

	file.Close()

	// Read with same reusable batch
	reader := &spillBucketReader{}
	err = reader.resetForFile(proc.Ctx, spillfs, bucketName)
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
	spillfs.RemoveFile(context.Background(), bucketName)
}

func TestAppendProbeBatchAllBuckets(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	writers, err := createRootProbeSpillBucketFiles()
	require.NoError(t, err)
	defer func() {
		for i := range writers {
			writers[i].close()
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

	err = ctr.appendProbeBatchToSpillFiles(proc, bat, writers, buffers, analyzer, 0)
	require.NoError(t, err)

	// Flush all buffers
	for i, buf := range buffers {
		if buf != nil && buf.RowCount() > 0 {
			_, err := ctr.flushBucketBuffer(proc, buf, &writers[i], analyzer)
			require.NoError(t, err)
		}
	}

	// Verify all buckets have data
	nonEmptyBuckets := 0
	totalRows := 0
	for i := range writers {
		reader := &spillBucketReader{}
		reader.resetForFd(writers[i].handOffFd())

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
		spillfs.RemoveFile(context.Background(), "test_multi_flush")
	}()

	analyzer := process.NewAnalyzer(0, false, false, "test")
	ctr := &container{}

	// Multiple flushes
	totalRows := int64(0)
	for i := 0; i < 10; i++ {
		bat := batch.NewWithSize(1)
		bat.Vecs[0] = testutil.MakeInt32Vector([]int32{int32(i)}, nil, proc.Mp())
		bat.SetRowCount(1)

		file_sw := spillBucketWriter{file: file}
		cnt, err := ctr.flushBucketBuffer(proc, bat, &file_sw, analyzer)
		require.NoError(t, err)
		totalRows += cnt
	}

	require.Equal(t, int64(10), totalRows)
}

func TestHashValuesBufferGrowth(t *testing.T) {
	mp := mpool.MustNewZero()

	// Start with small buffer
	vec := testutil.MakeInt32Vector([]int32{1, 2}, nil, mp)
	hashValues := make([]uint64, 2)
	err := computeXXHash([]*vector.Vector{vec}, hashValues, 0)
	require.NoError(t, err)

	// Larger data should grow buffer
	largeVec := testutil.MakeVarcharVector([]string{
		"very_long_string_value_1",
		"very_long_string_value_2",
		"very_long_string_value_3",
	}, nil, mp)
	largeHashValues := make([]uint64, 3)
	err = computeXXHash([]*vector.Vector{largeVec}, largeHashValues, 0)
	require.NoError(t, err)

	for _, h := range largeHashValues {
		require.NotEqual(t, uint64(0), h)
	}
}

func TestSetSpillThresholdAutoConfig(t *testing.T) {
	ctr := &container{}

	// threshold == 0 triggers auto config path
	ctr.setSpillThreshold(0)
	require.Greater(t, ctr.spillThreshold, int64(0))

	// Verify minimum of 128MB
	require.GreaterOrEqual(t, ctr.spillThreshold, int64(128*1024*1024))
}

func TestSetSpillThresholdExplicit(t *testing.T) {
	ctr := &container{}

	// Non-zero threshold should be used directly
	ctr.setSpillThreshold(1024 * 1024)
	require.Equal(t, int64(1024*1024), ctr.spillThreshold)
}

func TestCleanupSpillFilesWithNonEmptyQueue(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	spillfs, err := proc.GetSpillFileService()
	require.NoError(t, err)

	// Create files for build and probe
	buildFile, err := spillfs.CreateFile(context.Background(), "test_cleanup_build")
	require.NoError(t, err)
	probeFile, err := spillfs.CreateFile(context.Background(), "test_cleanup_probe")
	require.NoError(t, err)

	// Write some data so files have content
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 2, 3}, nil, proc.Mp())
	bat.SetRowCount(3)

	analyzer := process.NewAnalyzer(0, false, false, "test")
	ctr := &container{}
	file_sw := spillBucketWriter{file: buildFile}
	_, err = ctr.flushBucketBuffer(proc, bat, &file_sw, analyzer)
	require.NoError(t, err)
	buildFile.Close()

	file_sw = spillBucketWriter{file: probeFile}
	_, err = ctr.flushBucketBuffer(proc, bat, &file_sw, analyzer)
	require.NoError(t, err)
	probeFile.Close()

	// Create a spillBucket with both buildFd and probeFd set
	buildFd, err := spillfs.OpenFile(context.Background(), "test_cleanup_build")
	require.NoError(t, err)
	probeFd, err := spillfs.OpenFile(context.Background(), "test_cleanup_probe")
	require.NoError(t, err)

	ctr.spillQueue = []spillBucket{
		{baseName: "test_cleanup", buildFd: buildFd, probeFd: probeFd},
	}

	// Cleanup should close both file descriptors
	ctr.cleanupSpillFiles(proc)

	require.Nil(t, ctr.spillQueue)

	// Verify files are closed by trying to use them
	_, err = buildFd.Stat()
	require.Error(t, err) // file should be closed
	_, err = probeFd.Stat()
	require.Error(t, err) // file should be closed

	spillfs.RemoveFile(context.Background(), "test_cleanup_build")
	spillfs.RemoveFile(context.Background(), "test_cleanup_probe")
}

func TestCleanupSpillFilesWithEmptyQueue(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	ctr := &container{}
	ctr.spillQueue = []spillBucket{} // empty queue

	// Should return early without error - queue remains as empty slice
	ctr.cleanupSpillFiles(proc)
	// When spillQueue is empty, function returns early without modifying spillQueue
	require.Equal(t, 0, len(ctr.spillQueue))
}

func TestCleanupSpillFilesWithActiveProbeReader(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	spillfs, err := proc.GetSpillFileService()
	require.NoError(t, err)

	bucketName := "test_probe_cleanup"
	file, err := spillfs.CreateFile(context.Background(), bucketName)
	require.NoError(t, err)
	file.Close()

	reader := &spillBucketReader{}
	err = reader.resetForFile(proc.Ctx, spillfs, bucketName)
	require.NoError(t, err)

	ctr := &container{}
	ctr.probeBucketReader = reader
	ctr.probeBucketActive = true
	ctr.spillQueue = []spillBucket{}

	ctr.cleanupSpillFiles(proc)

	require.False(t, ctr.probeBucketActive)

	spillfs.RemoveFile(context.Background(), bucketName)
}

func TestCleanSpillBufferPoolWithData(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	ctr := &container{}

	// Pre-populate with buffers containing data
	ctr.spillBuildSubBufs = make([]*batch.Batch, 3)
	ctr.spillProbeSubBufs = make([]*batch.Batch, 3)

	for i := range ctr.spillBuildSubBufs {
		ctr.spillBuildSubBufs[i] = batch.NewWithSize(1)
		ctr.spillBuildSubBufs[i].Vecs[0] = testutil.MakeInt32Vector([]int32{1, 2, 3}, nil, proc.Mp())
		ctr.spillBuildSubBufs[i].SetRowCount(3)
	}

	for i := range ctr.spillProbeSubBufs {
		ctr.spillProbeSubBufs[i] = batch.NewWithSize(1)
		ctr.spillProbeSubBufs[i].Vecs[0] = testutil.MakeInt32Vector([]int32{4, 5}, nil, proc.Mp())
		ctr.spillProbeSubBufs[i].SetRowCount(2)
	}

	ctr.cleanSpillBufferPool(proc)

	require.Nil(t, ctr.spillBuildSubBufs)
	require.Nil(t, ctr.spillProbeSubBufs)
}

func TestShouldReSpillByRowCount(t *testing.T) {
	ctr := &container{}

	// Set a small threshold so we use the row-count-based path
	ctr.setSpillThreshold(10) // 10 rows

	builder := &hashbuild.HashmapBuilder{}
	builder.InputBatchRowCount = 5

	// Row count below threshold
	require.False(t, ctr.shouldReSpill(builder))

	builder.InputBatchRowCount = 15
	// Row count above threshold
	require.True(t, ctr.shouldReSpill(builder))
}

func TestShouldReSpillNoThreshold(t *testing.T) {
	ctr := &container{}

	// Zero threshold - should never spill
	ctr.spillThreshold = 0

	builder := &hashbuild.HashmapBuilder{}
	builder.InputBatchRowCount = 1000000

	require.False(t, ctr.shouldReSpill(builder))
}

func TestHandOffFdSeeksToStart(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	spillfs, err := proc.GetSpillFileService()
	require.NoError(t, err)

	f, err := spillfs.CreateFile(context.Background(), "test_handoff")
	require.NoError(t, err)
	defer spillfs.RemoveFile(context.Background(), "test_handoff")

	// Write data and advance position
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = testutil.MakeInt32Vector([]int32{10, 20, 30}, nil, proc.Mp())
	bat.SetRowCount(3)

	analyzer := process.NewAnalyzer(0, false, false, "test")
	ctr := &container{}
	sw := spillBucketWriter{file: f}
	_, err = ctr.flushBucketBuffer(proc, bat, &sw, analyzer)
	require.NoError(t, err)

	// Position should be past the data written
	pos, _ := sw.file.Seek(0, io.SeekCurrent)
	require.Greater(t, pos, int64(0))

	// handOffFd should seek back to 0
	fd := sw.handOffFd()
	require.NotNil(t, fd)

	pos, _ = fd.Seek(0, io.SeekCurrent)
	require.Equal(t, int64(0), pos)

	// Writer should be nil after handoff
	require.Nil(t, sw.file)
	require.False(t, sw.created())

	fd.Close()
}

func TestHandOffFdMultipleCalls(t *testing.T) {
	f, err := os.CreateTemp("", "test_handoff_*")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	sw := spillBucketWriter{file: f}

	fd1 := sw.handOffFd()
	require.NotNil(t, fd1)

	fd2 := sw.handOffFd()
	require.Nil(t, fd2) // already transferred

	fd1.Close()
}

func TestResetForFdNil(t *testing.T) {
	r := &spillBucketReader{}
	r.resetForFd(nil)
	require.True(t, r.empty)
	require.Nil(t, r.file)
}

func TestResetForFdReusesBuffer(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	spillfs, err := proc.GetSpillFileService()
	require.NoError(t, err)

	// Create two files with data
	f1, err := spillfs.CreateFile(context.Background(), "test_reuse_1")
	require.NoError(t, err)
	defer spillfs.RemoveFile(context.Background(), "test_reuse_1")
	f2, err := spillfs.CreateFile(context.Background(), "test_reuse_2")
	require.NoError(t, err)
	defer spillfs.RemoveFile(context.Background(), "test_reuse_2")

	bat := batch.NewWithSize(1)
	bat.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 2, 3}, nil, proc.Mp())
	bat.SetRowCount(3)
	analyzer := process.NewAnalyzer(0, false, false, "test")
	ctr := &container{}
	sw1 := spillBucketWriter{file: f1}
	_, err = ctr.flushBucketBuffer(proc, bat, &sw1, analyzer)
	require.NoError(t, err)
	sw2 := spillBucketWriter{file: f2}
	_, err = ctr.flushBucketBuffer(proc, bat, &sw2, analyzer)
	require.NoError(t, err)

	fd1 := sw1.handOffFd()
	fd2 := sw2.handOffFd()

	r := &spillBucketReader{}

	// First resetForFd: allocates bufio.Reader
	r.resetForFd(fd1)
	require.False(t, r.empty)
	require.NotNil(t, r.reader)
	reader1 := r.reader

	// Second resetForFd: reuses the same bufio.Reader
	r.resetForFd(fd2)
	require.False(t, r.empty)
	require.Same(t, reader1, r.reader) // same buffer instance

	r.close()
	fd1.Close()
}

func TestSpillBucketWriterDelete(t *testing.T) {
	f, err := os.CreateTemp("", "test_delete_*")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	sw := spillBucketWriter{file: f}
	require.True(t, sw.created())

	sw.delete()
	require.Nil(t, sw.file)
	require.False(t, sw.created())

	// Double delete is safe
	sw.delete()
}

func TestResetForFdReadData(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	spillfs, err := proc.GetSpillFileService()
	require.NoError(t, err)

	f, err := spillfs.CreateFile(context.Background(), "test_fd_read")
	require.NoError(t, err)
	defer spillfs.RemoveFile(context.Background(), "test_fd_read")

	// Write batches through the writer
	bat := batch.NewOffHeapWithSize(1)
	bat.Vecs[0] = testutil.MakeInt32Vector([]int32{10, 20, 30}, nil, proc.Mp())
	bat.SetRowCount(3)

	analyzer := process.NewAnalyzer(0, false, false, "test")
	ctr := &container{}
	sw := spillBucketWriter{file: f}
	_, err = ctr.flushBucketBuffer(proc, bat, &sw, analyzer)
	require.NoError(t, err)

	// Hand off and read back
	fd := sw.handOffFd()
	require.NotNil(t, fd)

	r := &spillBucketReader{}
	r.resetForFd(fd)

	reuseBat := batch.NewOffHeapWithSize(0)
	defer reuseBat.Clean(proc.Mp())

	gotBat, err := r.readBatch(proc, reuseBat)
	require.NoError(t, err)
	require.NotNil(t, gotBat)
	require.Equal(t, 3, gotBat.RowCount())

	// Verify data
	col := vector.MustFixedColWithTypeCheck[int32](gotBat.Vecs[0])
	require.Equal(t, int32(10), col[0])
	require.Equal(t, int32(20), col[1])
	require.Equal(t, int32(30), col[2])

	// EOF on next read
	_, err = r.readBatch(proc, reuseBat)
	require.Equal(t, io.EOF, err)

	r.close()
}

func TestWriterHandoffReaderRoundtrip(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	spillfs, err := proc.GetSpillFileService()
	require.NoError(t, err)

	f, err := spillfs.CreateFile(context.Background(), "test_roundtrip")
	require.NoError(t, err)
	defer spillfs.RemoveFile(context.Background(), "test_roundtrip")

	analyzer := process.NewAnalyzer(0, false, false, "test")
	ctr := &container{}
	sw := spillBucketWriter{file: f}

	// Write multiple batches
	for i := 0; i < 3; i++ {
		bat := batch.NewOffHeapWithSize(1)
		bat.Vecs[0] = testutil.MakeInt32Vector([]int32{int32(i * 10), int32(i*10 + 1)}, nil, proc.Mp())
		bat.SetRowCount(2)
		_, err = ctr.flushBucketBuffer(proc, bat, &sw, analyzer)
		require.NoError(t, err)
	}

	// Handoff and read all batches back
	fd := sw.handOffFd()
	r := &spillBucketReader{}
	r.resetForFd(fd)

	reuseBat := batch.NewOffHeapWithSize(0)
	defer reuseBat.Clean(proc.Mp())

	totalRows := 0
	for {
		gotBat, err := r.readBatch(proc, reuseBat)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		totalRows += gotBat.RowCount()
	}
	require.Equal(t, 6, totalRows) // 3 batches × 2 rows

	r.close()
}

func TestScatterSkipsDisabledWriters(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	spillfs, err := proc.GetSpillFileService()
	require.NoError(t, err)

	writers := makeSpillBucketWriters(uuid.NewString(), "probe")
	// Only create files for odd buckets; even buckets will be disabled.
	for i := 1; i < spillNumBuckets; i += 2 {
		f, err := spillfs.CreateFile(context.Background(), writers[i].name)
		require.NoError(t, err)
		writers[i].file = f
	}
	defer func() {
		for i := range writers {
			writers[i].delete()
		}
	}()

	// Disable writers for even-numbered buckets.
	for i := 0; i < spillNumBuckets; i += 2 {
		writers[i].name = ""
	}

	// Build a large enough batch that most buckets get rows.
	const nRows = 10000
	vals := make([]int32, nRows)
	for i := range vals {
		vals[i] = int32(i)
	}
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = testutil.MakeInt32Vector(vals, nil, proc.Mp())
	bat.SetRowCount(nRows)
	defer bat.Clean(proc.Mp())

	buffers := make([]*batch.Batch, spillNumBuckets)
	defer func() {
		for _, buf := range buffers {
			if buf != nil {
				buf.Clean(proc.Mp())
			}
		}
	}()

	ctr := &container{}
	analyzer := process.NewAnalyzer(0, false, false, "test")
	err = ctr.scatterBatchToFiles(proc, bat, bat.Vecs[:1], writers, buffers, analyzer, 0)
	require.NoError(t, err)

	// Flush remaining buffers for odd buckets.
	for i := range writers {
		if buffers[i] != nil {
			_, err := ctr.flushBucketBuffer(proc, buffers[i], &writers[i], analyzer)
			require.NoError(t, err)
		}
	}

	// Disabled (even) buckets must have no buffer allocated and no data written.
	for i := 0; i < spillNumBuckets; i += 2 {
		require.Nil(t, buffers[i], "disabled bucket %d should have no buffer", i)
		require.False(t, writers[i].created(), "disabled bucket %d should not have been written to", i)
	}

	// Enabled (odd) buckets should have received data.
	// Compute expected count: rows whose hash lands in an odd bucket.
	hashValues := make([]uint64, nRows)
	err = computeXXHash(bat.Vecs[:1], hashValues, 0)
	require.NoError(t, err)
	var expectedOddRows int
	for _, h := range hashValues {
		if h&(spillNumBuckets-1)&1 == 1 { // odd bucket
			expectedOddRows++
		}
	}
	require.Greater(t, expectedOddRows, 0, "sanity: some rows should hash to odd buckets")

	var totalRows int
	for i := 1; i < spillNumBuckets; i += 2 {
		fd := writers[i].handOffFd()
		if fd == nil {
			continue
		}
		r := &spillBucketReader{}
		r.resetForFd(fd)
		reuse := batch.NewOffHeapWithSize(0)
		for {
			got, err := r.readBatch(proc, reuse)
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			totalRows += got.RowCount()
		}
		reuse.Clean(proc.Mp())
		r.close()
	}
	require.Equal(t, expectedOddRows, totalRows, "only rows hashing to odd buckets should be written")
}
