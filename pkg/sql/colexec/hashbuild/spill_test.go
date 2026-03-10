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

package hashbuild

import (
	"bytes"
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestComputeXXHashBuild(t *testing.T) {
	mp := mpool.MustNewZero()

	t.Run("empty", func(t *testing.T) {
		err := computeXXHash(nil, nil)
		require.NoError(t, err)
	})

	t.Run("single_column", func(t *testing.T) {
		vec := testutil.MakeInt32Vector([]int32{1, 2, 3}, nil, mp)
		hashValues := make([]uint64, 3)
		err := computeXXHash([]*vector.Vector{vec}, hashValues)
		require.NoError(t, err)
		require.NotEqual(t, uint64(0), hashValues[0])
		require.NotEqual(t, hashValues[0], hashValues[1])
	})

	t.Run("multiple_columns", func(t *testing.T) {
		vec1 := testutil.MakeInt32Vector([]int32{1, 2}, nil, mp)
		vec2 := testutil.MakeVarcharVector([]string{"a", "b"}, nil, mp)
		hashValues := make([]uint64, 2)
		err := computeXXHash([]*vector.Vector{vec1, vec2}, hashValues)
		require.NoError(t, err)
		require.NotEqual(t, hashValues[0], hashValues[1])
	})

	t.Run("const_vector", func(t *testing.T) {
		vec := testutil.MakeInt32Vector([]int32{5}, nil, mp)
		vec.SetClass(vector.CONSTANT)
		hashValues := make([]uint64, 3)
		err := computeXXHash([]*vector.Vector{vec}, hashValues)
		require.NoError(t, err)
		require.Equal(t, hashValues[0], hashValues[1])
	})
}

func TestFlushBucketBufferBuild(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	spillfs, err := proc.GetSpillFileService()
	require.NoError(t, err)

	file, err := spillfs.CreateFile(context.Background(), "test_build_flush")
	require.NoError(t, err)
	defer func() {
		file.Close()
		spillfs.Delete(context.Background(), "test_build_flush")
	}()

	analyzer := process.NewAnalyzer(0, false, false, "test")

	t.Run("empty_buffer", func(t *testing.T) {
		buf := &bucketBuffer{}
		cnt, err := flushBucketBuffer(proc, buf, file, analyzer)
		require.NoError(t, err)
		require.Equal(t, int64(0), cnt)
	})

	t.Run("with_data", func(t *testing.T) {
		bat := batch.NewWithSize(1)
		bat.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 2, 3}, nil, proc.Mp())
		bat.SetRowCount(3)

		buf := &bucketBuffer{bat: bat}
		cnt, err := flushBucketBuffer(proc, buf, file, analyzer)
		require.NoError(t, err)
		require.Equal(t, int64(3), cnt)
		require.Nil(t, buf.bat)
	})
}

func TestCreateSpillFiles(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	buckets, files, err := createSpillFiles(proc)
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

func TestLoadSpilledBuildBucketHashBuild(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	spillfs, err := proc.GetSpillFileService()
	require.NoError(t, err)

	analyzer := process.NewAnalyzer(0, false, false, "test")
	bucketName := "test_build_load"
	file, err := spillfs.CreateFile(context.Background(), bucketName)
	require.NoError(t, err)

	// Write test data
	bat := batch.NewWithSize(2)
	bat.Vecs[0] = testutil.MakeInt32Vector([]int32{10, 20, 30}, nil, proc.Mp())
	bat.Vecs[1] = testutil.MakeVarcharVector([]string{"a", "b", "c"}, nil, proc.Mp())
	bat.SetRowCount(3)

	buf := &bucketBuffer{bat: bat}
	_, err = flushBucketBuffer(proc, buf, file, analyzer)
	require.NoError(t, err)
	file.Close()

	// Load back
	batches, err := loadSpilledBuildBucket(proc, bucketName)
	require.NoError(t, err)
	require.Equal(t, 1, len(batches))
	require.Equal(t, 3, batches[0].RowCount())
	require.Equal(t, 2, len(batches[0].Vecs))

	vec := batches[0].Vecs[0]
	require.Equal(t, int32(10), vector.GetFixedAtNoTypeCheck[int32](vec, 0))
	require.Equal(t, int32(20), vector.GetFixedAtNoTypeCheck[int32](vec, 1))
	require.Equal(t, int32(30), vector.GetFixedAtNoTypeCheck[int32](vec, 2))

	batches[0].Clean(proc.Mp())
}

func TestShouldSpillBatches(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	t.Run("not_shuffle", func(t *testing.T) {
		hb := &HashBuild{
			IsShuffle: false,
		}
		require.False(t, hb.shouldSpillBatches())
	})

	t.Run("no_threshold", func(t *testing.T) {
		hb := &HashBuild{
			IsShuffle:      true,
			SpillThreshold: 0,
		}
		require.False(t, hb.shouldSpillBatches())
	})

	t.Run("below_threshold", func(t *testing.T) {
		hb := &HashBuild{
			IsShuffle:      true,
			SpillThreshold: 1024 * 1024, // 1MB
		}
		hb.ctr.hashmapBuilder.Batches.Buf = []*batch.Batch{
			{Vecs: []*vector.Vector{testutil.MakeInt32Vector([]int32{1, 2}, nil, proc.Mp())}},
		}
		require.False(t, hb.shouldSpillBatches())
	})

	t.Run("above_threshold", func(t *testing.T) {
		hb := &HashBuild{
			IsShuffle:      true,
			SpillThreshold: 1, // 1 byte
		}
		hb.ctr.setSpillThreshold(1)
		bat := batch.NewWithSize(1)
		bat.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 2, 3, 4, 5}, nil, proc.Mp())
		bat.SetRowCount(5)
		hb.ctr.hashmapBuilder.Batches.Buf = []*batch.Batch{bat}
		require.True(t, hb.shouldSpillBatches())
	})
}

func TestSpillFileCorruptionBuild(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	spillfs, err := proc.GetSpillFileService()
	require.NoError(t, err)

	bucketName := "test_build_corrupt"
	file, err := spillfs.CreateFile(context.Background(), bucketName)
	require.NoError(t, err)

	// Write corrupted data
	cnt := int64(1)
	batchSize := int64(100)
	file.Write(types.EncodeInt64(&cnt))
	file.Write(types.EncodeInt64(&batchSize))
	file.Write(make([]byte, 100))
	wrongMagic := uint64(0xDEADBEEF)
	file.Write(types.EncodeUint64(&wrongMagic))
	file.Close()

	_, err = loadSpilledBuildBucket(proc, bucketName)
	require.Error(t, err)
	require.Contains(t, err.Error(), "corrupted")
}

func TestMultipleBatchesSpillFormat(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	spillfs, err := proc.GetSpillFileService()
	require.NoError(t, err)

	analyzer := process.NewAnalyzer(0, false, false, "test")
	bucketName := "test_multi_batch"
	file, err := spillfs.CreateFile(context.Background(), bucketName)
	require.NoError(t, err)

	// Write 3 batches
	for i := 0; i < 3; i++ {
		bat := batch.NewWithSize(1)
		bat.Vecs[0] = testutil.MakeInt32Vector([]int32{int32(i * 10), int32(i*10 + 1)}, nil, proc.Mp())
		bat.SetRowCount(2)

		buf := &bucketBuffer{bat: bat}
		_, err = flushBucketBuffer(proc, buf, file, analyzer)
		require.NoError(t, err)
	}
	file.Close()

	// Load and verify
	batches, err := loadSpilledBuildBucket(proc, bucketName)
	require.NoError(t, err)
	require.Equal(t, 3, len(batches))

	for i, bat := range batches {
		require.Equal(t, 2, bat.RowCount())
		vec := bat.Vecs[0]
		require.Equal(t, int32(i*10), vector.GetFixedAtNoTypeCheck[int32](vec, 0))
		require.Equal(t, int32(i*10+1), vector.GetFixedAtNoTypeCheck[int32](vec, 1))
		bat.Clean(proc.Mp())
	}
}

func TestHashDistributionBuild(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := testutil.MakeInt32Vector([]int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
		11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30}, nil, mp)

	hashValues := make([]uint64, 30)
	err := computeXXHash([]*vector.Vector{vec}, hashValues)
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

func TestLargeBufferFlushBuild(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	spillfs, err := proc.GetSpillFileService()
	require.NoError(t, err)

	analyzer := process.NewAnalyzer(0, false, false, "test")
	file, err := spillfs.CreateFile(context.Background(), "test_large_build")
	require.NoError(t, err)
	defer func() {
		file.Close()
		spillfs.Delete(context.Background(), "test_large_build")
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

	buf := &bucketBuffer{bat: bat}
	cnt, err := flushBucketBuffer(proc, buf, file, analyzer)
	require.NoError(t, err)
	require.Equal(t, int64(size), cnt)
}

func TestSpillFileCleanupBuild(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	spillfs, err := proc.GetSpillFileService()
	require.NoError(t, err)

	analyzer := process.NewAnalyzer(0, false, false, "test")
	bucketName := "test_cleanup_build"
	file, err := spillfs.CreateFile(context.Background(), bucketName)
	require.NoError(t, err)

	bat := batch.NewWithSize(1)
	bat.Vecs[0] = testutil.MakeInt32Vector([]int32{1}, nil, proc.Mp())
	bat.SetRowCount(1)

	buf := &bucketBuffer{bat: bat}
	_, err = flushBucketBuffer(proc, buf, file, analyzer)
	require.NoError(t, err)
	file.Close()

	// Load should delete the file
	_, err = loadSpilledBuildBucket(proc, bucketName)
	require.NoError(t, err)

	// File should no longer exist
	_, err = spillfs.OpenFile(context.Background(), bucketName)
	require.Error(t, err)
}

func TestMultipleDataTypesBuild(t *testing.T) {
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
			err := computeXXHash([]*vector.Vector{tt.vec}, hashValues)
			require.NoError(t, err)
			require.NotEqual(t, uint64(0), hashValues[0])
			require.NotEqual(t, hashValues[0], hashValues[1])
		})
	}
}

func TestNullValuesBuild(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := testutil.MakeInt32Vector([]int32{1, 2, 3}, []uint64{1}, mp)
	hashValues := make([]uint64, 3)
	err := computeXXHash([]*vector.Vector{vec}, hashValues)
	require.NoError(t, err)
	require.NotEqual(t, uint64(0), hashValues[0])
	require.NotEqual(t, uint64(0), hashValues[2])
}

func TestFileWriteErrorBuild(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	analyzer := process.NewAnalyzer(0, false, false, "test")
	spillfs, _ := proc.GetSpillFileService()
	file, _ := spillfs.CreateFile(context.Background(), "test_error_build")
	file.Close()

	bat := batch.NewWithSize(1)
	bat.Vecs[0] = testutil.MakeInt32Vector([]int32{1}, nil, proc.Mp())
	bat.SetRowCount(1)

	buf := &bucketBuffer{bat: bat}
	_, err := flushBucketBuffer(proc, buf, file, analyzer)
	require.Error(t, err)

	spillfs.Delete(context.Background(), "test_error_build")
}

func TestRowCountMismatchBuild(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	spillfs, err := proc.GetSpillFileService()
	require.NoError(t, err)

	bucketName := "test_mismatch_build"
	file, err := spillfs.CreateFile(context.Background(), bucketName)
	require.NoError(t, err)

	// Write mismatched count
	cnt := int64(10) // Wrong count
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 2}, nil, proc.Mp())
	bat.SetRowCount(2)

	batchData := &bytes.Buffer{}
	bat.MarshalBinaryWithBuffer(batchData, false)
	batchSize := int64(batchData.Len())

	file.Write(types.EncodeInt64(&cnt))
	file.Write(types.EncodeInt64(&batchSize))
	file.Write(batchData.Bytes())
	magic := uint64(spillMagic)
	file.Write(types.EncodeUint64(&magic))
	file.Close()

	_, err = loadSpilledBuildBucket(proc, bucketName)
	require.Error(t, err)
	require.Contains(t, err.Error(), "mismatch")
}

func TestAppendBatchToSpillFilesPartitioning(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	buckets, files, err := createSpillFiles(proc)
	require.NoError(t, err)
	defer func() {
		spillfs, _ := proc.GetSpillFileService()
		for i, file := range files {
			file.Close()
			spillfs.Delete(context.Background(), buckets[i])
		}
	}()

	// Create batch with known values
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 2, 3, 4, 5, 6, 7, 8}, nil, proc.Mp())
	bat.SetRowCount(8)

	conditions := []*plan.Expr{
		{
			Typ: plan.Type{Id: int32(types.T_int32)},
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{ColPos: 0},
			},
		},
	}

	buffers := make([]*bucketBuffer, spillNumBuckets)
	for i := range buffers {
		buffers[i] = &bucketBuffer{}
	}

	analyzer := process.NewAnalyzer(0, false, false, "test")
	rowCnts, err := appendBuildBatchToSpillFiles(proc, bat, files, buffers, false, conditions, analyzer)
	require.NoError(t, err)

	// Verify total rows distributed
	totalRows := int64(0)
	for _, cnt := range rowCnts {
		totalRows += cnt
	}
	require.Equal(t, int64(8), totalRows)

	// Flush remaining buffers
	for i, buf := range buffers {
		if buf.bat != nil && buf.bat.RowCount() > 0 {
			_, err := flushBucketBuffer(proc, buf, files[i], analyzer)
			require.NoError(t, err)
		}
	}
}

func TestEmptyBatchSpill(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	buckets, files, err := createSpillFiles(proc)
	require.NoError(t, err)
	defer func() {
		spillfs, _ := proc.GetSpillFileService()
		for i, file := range files {
			file.Close()
			spillfs.Delete(context.Background(), buckets[i])
		}
	}()

	bat := batch.NewWithSize(1)
	bat.Vecs[0] = testutil.MakeInt32Vector([]int32{}, nil, proc.Mp())
	bat.SetRowCount(0)

	conditions := []*plan.Expr{
		{
			Typ: plan.Type{Id: int32(types.T_int32)},
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{ColPos: 0},
			},
		},
	}

	buffers := make([]*bucketBuffer, spillNumBuckets)
	for i := range buffers {
		buffers[i] = &bucketBuffer{}
	}

	analyzer := process.NewAnalyzer(0, false, false, "test")
	rowCnts, err := appendBuildBatchToSpillFiles(proc, bat, files, buffers, false, conditions, analyzer)
	require.NoError(t, err)

	totalRows := int64(0)
	for _, cnt := range rowCnts {
		totalRows += cnt
	}
	require.Equal(t, int64(0), totalRows)
}
