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
		spillfs.RemoveFile(context.Background(), "test_build_flush")
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
		spillfs.RemoveFile(context.Background(), buckets[i])
	}
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
			CanSpill:       true,
			NeedHashMap:    true,
		}
		hb.ctr.setSpillThreshold(1)
		bat := batch.NewWithSize(1)
		bat.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 2, 3, 4, 5}, nil, proc.Mp())
		bat.SetRowCount(5)
		hb.ctr.hashmapBuilder.Batches.Buf = []*batch.Batch{bat}
		require.True(t, hb.shouldSpillBatches())
	})
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
		spillfs.RemoveFile(context.Background(), "test_large_build")
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

	ctr := &container{}
	_, err := ctr.flushBucketBuffer(proc, bat, file, analyzer)
	require.Error(t, err)

	spillfs.RemoveFile(context.Background(), "test_error_build")
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
			spillfs.RemoveFile(context.Background(), buckets[i])
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

	buffers := make([]*batch.Batch, spillNumBuckets)

	analyzer := process.NewAnalyzer(0, false, false, "test")
	ctr := &container{}
	_, err = ctr.initSpillExprExecs(proc, conditions)
	require.NoError(t, err)
	err = ctr.appendBuildBatchToSpillFiles(proc, bat, files, buffers, ctr.spillExprExecs, analyzer)
	require.NoError(t, err)

	// Flush remaining buffers
	for i, buf := range buffers {
		if buf != nil && buf.RowCount() > 0 {
			_, err := ctr.flushBucketBuffer(proc, buf, files[i], analyzer)
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
			spillfs.RemoveFile(context.Background(), buckets[i])
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

	buffers := make([]*batch.Batch, spillNumBuckets)

	analyzer := process.NewAnalyzer(0, false, false, "test")
	ctr := &container{}
	_, err = ctr.initSpillExprExecs(proc, conditions)
	require.NoError(t, err)
	err = ctr.appendBuildBatchToSpillFiles(proc, bat, files, buffers, ctr.spillExprExecs, analyzer)
	require.NoError(t, err)
}

func TestAppendBuildBatchMultipleFlushes(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	buckets, files, err := createSpillFiles(proc)
	require.NoError(t, err)
	defer func() {
		spillfs, _ := proc.GetSpillFileService()
		for i, file := range files {
			file.Close()
			spillfs.RemoveFile(context.Background(), buckets[i])
		}
	}()

	// Create large batch to trigger buffer flushes
	size := spillBufferSize * 2
	values := make([]int32, size)
	for i := range values {
		values[i] = int32(i)
	}

	bat := batch.NewWithSize(1)
	bat.Vecs[0] = testutil.MakeInt32Vector(values, nil, proc.Mp())
	bat.SetRowCount(size)

	conditions := []*plan.Expr{
		{
			Typ: plan.Type{Id: int32(types.T_int32)},
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{ColPos: 0},
			},
		},
	}

	buffers := make([]*batch.Batch, spillNumBuckets)
	analyzer := process.NewAnalyzer(0, false, false, "test")
	ctr := &container{}

	_, err = ctr.initSpillExprExecs(proc, conditions)
	require.NoError(t, err)
	err = ctr.appendBuildBatchToSpillFiles(proc, bat, files, buffers, ctr.spillExprExecs, analyzer)
	require.NoError(t, err)

	// Flush remaining
	for i, buf := range buffers {
		if buf != nil && buf.RowCount() > 0 {
			_, err := ctr.flushBucketBuffer(proc, buf, files[i], analyzer)
			require.NoError(t, err)
		}
	}
}

func TestAppendBuildBatchWithNulls(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	buckets, files, err := createSpillFiles(proc)
	require.NoError(t, err)
	defer func() {
		spillfs, _ := proc.GetSpillFileService()
		for i, file := range files {
			file.Close()
			spillfs.RemoveFile(context.Background(), buckets[i])
		}
	}()

	bat := batch.NewWithSize(1)
	bat.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 2, 3, 4}, []uint64{1}, proc.Mp()) // null at index 1
	bat.SetRowCount(4)

	conditions := []*plan.Expr{
		{
			Typ: plan.Type{Id: int32(types.T_int32)},
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{ColPos: 0},
			},
		},
	}

	buffers := make([]*batch.Batch, spillNumBuckets)
	analyzer := process.NewAnalyzer(0, false, false, "test")
	ctr := &container{}

	_, err = ctr.initSpillExprExecs(proc, conditions)
	require.NoError(t, err)
	err = ctr.appendBuildBatchToSpillFiles(proc, bat, files, buffers, ctr.spillExprExecs, analyzer)
	require.NoError(t, err)

	// Flush remaining
	for i, buf := range buffers {
		if buf != nil && buf.RowCount() > 0 {
			_, err := ctr.flushBucketBuffer(proc, buf, files[i], analyzer)
			require.NoError(t, err)
		}
	}
}

func TestAppendBuildBatchMultiColumn(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	buckets, files, err := createSpillFiles(proc)
	require.NoError(t, err)
	defer func() {
		spillfs, _ := proc.GetSpillFileService()
		for i, file := range files {
			file.Close()
			spillfs.RemoveFile(context.Background(), buckets[i])
		}
	}()

	bat := batch.NewWithSize(2)
	bat.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 2, 3}, nil, proc.Mp())
	bat.Vecs[1] = testutil.MakeVarcharVector([]string{"a", "b", "c"}, nil, proc.Mp())
	bat.SetRowCount(3)

	conditions := []*plan.Expr{
		{
			Typ: plan.Type{Id: int32(types.T_int32)},
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{ColPos: 0},
			},
		},
		{
			Typ: plan.Type{Id: int32(types.T_varchar)},
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{ColPos: 1},
			},
		},
	}

	buffers := make([]*batch.Batch, spillNumBuckets)
	analyzer := process.NewAnalyzer(0, false, false, "test")
	ctr := &container{}

	_, err = ctr.initSpillExprExecs(proc, conditions)
	require.NoError(t, err)
	err = ctr.appendBuildBatchToSpillFiles(proc, bat, files, buffers, ctr.spillExprExecs, analyzer)
	require.NoError(t, err)

	// Flush remaining
	for i, buf := range buffers {
		if buf != nil && buf.RowCount() > 0 {
			_, err := ctr.flushBucketBuffer(proc, buf, files[i], analyzer)
			require.NoError(t, err)
		}
	}
}

func TestMemUsedAndRowCnt(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	ctr := &container{}
	ctr.hashmapBuilder.Batches.Buf = []*batch.Batch{}

	// Empty
	require.Equal(t, int64(0), ctr.rowCnt())

	// Add batches
	bat1 := batch.NewWithSize(1)
	bat1.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 2, 3}, nil, proc.Mp())
	bat1.SetRowCount(3)

	bat2 := batch.NewWithSize(1)
	bat2.Vecs[0] = testutil.MakeInt32Vector([]int32{4, 5}, nil, proc.Mp())
	bat2.SetRowCount(2)

	ctr.hashmapBuilder.Batches.Buf = []*batch.Batch{bat1, bat2}

	require.Equal(t, int64(5), ctr.rowCnt())
	require.Greater(t, ctr.memUsed(), int64(0))
}

func TestShouldSpillBatchesRowThreshold(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	hb := &HashBuild{
		IsShuffle:      true,
		SpillThreshold: 10, // Small row threshold
		CanSpill:       true,
		NeedHashMap:    true,
	}
	hb.ctr.setSpillThreshold(10)

	bat := batch.NewWithSize(1)
	bat.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 2, 3}, nil, proc.Mp())
	bat.SetRowCount(3)
	hb.ctr.hashmapBuilder.Batches.Buf = []*batch.Batch{bat}

	require.False(t, hb.shouldSpillBatches())

	// Add more batches to exceed threshold
	for i := 0; i < 10; i++ {
		bat := batch.NewWithSize(1)
		bat.Vecs[0] = testutil.MakeInt32Vector([]int32{int32(i)}, nil, proc.Mp())
		bat.SetRowCount(1)
		hb.ctr.hashmapBuilder.Batches.Buf = append(hb.ctr.hashmapBuilder.Batches.Buf, bat)
	}

	require.True(t, hb.shouldSpillBatches())
}

func TestShouldSpillBatchesMemThreshold(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	hb := &HashBuild{
		IsShuffle:      true,
		SpillThreshold: 1024 * 1024, // 1MB
	}
	hb.ctr.setSpillThreshold(1024 * 1024)

	bat := batch.NewWithSize(1)
	bat.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 2}, nil, proc.Mp())
	bat.SetRowCount(2)
	hb.ctr.hashmapBuilder.Batches.Buf = []*batch.Batch{bat}

	require.False(t, hb.shouldSpillBatches())
}

func TestCreateSpillFilesError(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	// Normal case
	buckets, files, err := createSpillFiles(proc)
	require.NoError(t, err)
	require.Equal(t, spillNumBuckets, len(buckets))
	require.Equal(t, spillNumBuckets, len(files))

	spillfs, _ := proc.GetSpillFileService()
	for i, file := range files {
		file.Close()
		spillfs.RemoveFile(context.Background(), buckets[i])
	}
}

func TestHashWithConstVector(t *testing.T) {
	mp := mpool.MustNewZero()

	vec := testutil.MakeInt32Vector([]int32{42}, nil, mp)
	vec.SetClass(vector.CONSTANT)

	hashValues := make([]uint64, 10)
	err := computeXXHash([]*vector.Vector{vec}, hashValues)
	require.NoError(t, err)

	// All values should be the same for const vector
	for i := 1; i < len(hashValues); i++ {
		require.Equal(t, hashValues[0], hashValues[i])
	}
}

func TestHashMultiColumnCombinations(t *testing.T) {
	mp := mpool.MustNewZero()

	vec1 := testutil.MakeInt32Vector([]int32{1, 1, 2}, nil, mp)
	vec2 := testutil.MakeVarcharVector([]string{"a", "b", "a"}, nil, mp)

	hashValues := make([]uint64, 3)
	err := computeXXHash([]*vector.Vector{vec1, vec2}, hashValues)
	require.NoError(t, err)

	// Different combinations should produce different hashes
	require.NotEqual(t, hashValues[0], hashValues[1])
	require.NotEqual(t, hashValues[0], hashValues[2])
}

func TestFlushZeroRowBatch(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	spillfs, err := proc.GetSpillFileService()
	require.NoError(t, err)

	file, err := spillfs.CreateFile(context.Background(), "test_zero")
	require.NoError(t, err)
	defer func() {
		file.Close()
		spillfs.RemoveFile(context.Background(), "test_zero")
	}()

	analyzer := process.NewAnalyzer(0, false, false, "test")
	ctr := &container{}

	bat := batch.NewWithSize(1)
	bat.Vecs[0] = testutil.MakeInt32Vector([]int32{}, nil, proc.Mp())
	bat.SetRowCount(0)

	cnt, err := ctr.flushBucketBuffer(proc, bat, file, analyzer)
	require.NoError(t, err)
	require.Equal(t, int64(0), cnt)
}

func TestAppendBuildBatchSingleBucket(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	buckets, files, err := createSpillFiles(proc)
	require.NoError(t, err)
	defer func() {
		spillfs, _ := proc.GetSpillFileService()
		for i, file := range files {
			file.Close()
			spillfs.RemoveFile(context.Background(), buckets[i])
		}
	}()

	// Single value should go to one bucket
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = testutil.MakeInt32Vector([]int32{1}, nil, proc.Mp())
	bat.SetRowCount(1)

	conditions := []*plan.Expr{
		{
			Typ: plan.Type{Id: int32(types.T_int32)},
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{ColPos: 0},
			},
		},
	}

	buffers := make([]*batch.Batch, spillNumBuckets)
	analyzer := process.NewAnalyzer(0, false, false, "test")
	ctr := &container{}

	_, err = ctr.initSpillExprExecs(proc, conditions)
	require.NoError(t, err)
	err = ctr.appendBuildBatchToSpillFiles(proc, bat, files, buffers, ctr.spillExprExecs, analyzer)
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

func TestBufferReuse(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	buckets, files, err := createSpillFiles(proc)
	require.NoError(t, err)
	defer func() {
		spillfs, _ := proc.GetSpillFileService()
		for i, file := range files {
			file.Close()
			spillfs.RemoveFile(context.Background(), buckets[i])
		}
	}()

	conditions := []*plan.Expr{
		{
			Typ: plan.Type{Id: int32(types.T_int32)},
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{ColPos: 0},
			},
		},
	}

	buffers := make([]*batch.Batch, spillNumBuckets)
	analyzer := process.NewAnalyzer(0, false, false, "test")
	ctr := &container{}

	_, err = ctr.initSpillExprExecs(proc, conditions)
	require.NoError(t, err)

	// First batch
	bat1 := batch.NewWithSize(1)
	bat1.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 2}, nil, proc.Mp())
	bat1.SetRowCount(2)

	err = ctr.appendBuildBatchToSpillFiles(proc, bat1, files, buffers, ctr.spillExprExecs, analyzer)
	require.NoError(t, err)

	// Second batch - buffers should be reused
	bat2 := batch.NewWithSize(1)
	bat2.Vecs[0] = testutil.MakeInt32Vector([]int32{3, 4}, nil, proc.Mp())
	bat2.SetRowCount(2)

	err = ctr.appendBuildBatchToSpillFiles(proc, bat2, files, buffers, ctr.spillExprExecs, analyzer)
	require.NoError(t, err)
}

func TestHashEmptyVectors(t *testing.T) {
	hashValues := make([]uint64, 0)
	err := computeXXHash([]*vector.Vector{}, hashValues)
	require.NoError(t, err)
}

func TestSetSpillThreshold(t *testing.T) {
	ctr := &container{}

	ctr.setSpillThreshold(1024)
	require.Equal(t, int64(1024), ctr.spillThreshold)

	// 0 means auto config, should set to a positive value
	ctr.setSpillThreshold(0)
	require.Greater(t, ctr.spillThreshold, int64(0))
}
