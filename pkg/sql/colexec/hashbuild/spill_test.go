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
	"bufio"
	"bytes"
	"context"
	"io"
	"math"
	"os"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestComputeXXHashBuild(t *testing.T) {
	mp := mpool.MustNewZero()

	t.Run("empty", func(t *testing.T) {
		computeXXHash(nil, nil)
	})

	t.Run("single_column", func(t *testing.T) {
		vec := testutil.MakeInt32Vector([]int32{1, 2, 3}, nil, mp)
		hashValues := make([]uint64, 3)
		computeXXHash([]*vector.Vector{vec}, hashValues)
		require.NotEqual(t, uint64(0), hashValues[0])
		require.NotEqual(t, hashValues[0], hashValues[1])
	})

	t.Run("multiple_columns", func(t *testing.T) {
		vec1 := testutil.MakeInt32Vector([]int32{1, 2}, nil, mp)
		vec2 := testutil.MakeVarcharVector([]string{"a", "b"}, nil, mp)
		hashValues := make([]uint64, 2)
		computeXXHash([]*vector.Vector{vec1, vec2}, hashValues)
		require.NotEqual(t, hashValues[0], hashValues[1])
	})

	t.Run("const_vector", func(t *testing.T) {
		vec := testutil.MakeInt32Vector([]int32{5}, nil, mp)
		vec.SetClass(vector.CONSTANT)
		hashValues := make([]uint64, 3)
		computeXXHash([]*vector.Vector{vec}, hashValues)
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
	ctr := &container{spillUUID: t.Name()}

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

func TestShouldSpillBatches(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	t.Run("not_shuffle", func(t *testing.T) {
		hb := &HashBuild{
			IsShuffle:   false,
			NeedHashMap: true,
		}
		hb.ctr.setSpillThreshold(1)
		bat := batch.NewWithSize(0)
		bat.SetRowCount(1)
		hb.ctr.hashmapBuilder.Batches.Buf = []*batch.Batch{bat}
		require.False(t, hb.shouldSpillBatches())
	})

	t.Run("no_hashmap", func(t *testing.T) {
		hb := &HashBuild{
			IsShuffle: true,
		}
		hb.ctr.setSpillThreshold(1)
		bat := batch.NewWithSize(0)
		bat.SetRowCount(1)
		hb.ctr.hashmapBuilder.Batches.Buf = []*batch.Batch{bat}
		require.False(t, hb.shouldSpillBatches())
	})

	t.Run("below_threshold", func(t *testing.T) {
		hb := &HashBuild{
			IsShuffle:      true,
			SpillThreshold: 1024 * 1024, // 1MB
			NeedHashMap:    true,
		}
		hb.ctr.setSpillThreshold(1024 * 1024)
		hb.ctr.hashmapBuilder.Batches.Buf = []*batch.Batch{
			{Vecs: []*vector.Vector{testutil.MakeInt32Vector([]int32{1, 2}, nil, proc.Mp())}},
		}
		require.False(t, hb.shouldSpillBatches())
	})

	t.Run("above_threshold", func(t *testing.T) {
		hb := &HashBuild{
			IsShuffle:      true,
			SpillThreshold: 1, // 1 byte
			NeedHashMap:    true,
		}
		hb.ctr.setSpillThreshold(1)
		bat := batch.NewWithSize(1)
		bat.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 2, 3, 4, 5}, nil, proc.Mp())
		bat.SetRowCount(5)
		hb.ctr.hashmapBuilder.Batches.Buf = []*batch.Batch{bat}
		hb.ctr.hashmapBuilder.InputBatchRowCount = bat.RowCount()
		require.True(t, hb.shouldSpillBatches())
	})
}

func TestShouldSpillBeforeRetain(t *testing.T) {
	t.Run("byte threshold predicts crossing batch", func(t *testing.T) {
		hb := &HashBuild{IsShuffle: true, NeedHashMap: true}
		hb.ctr.setSpillThreshold(100_001)
		hb.ctr.hashmapBuilder.Batches.MemSize = 60_000
		hb.ctr.hashmapBuilder.InputBatchRowCount = 2

		require.False(t, hb.shouldSpillBeforeRetain(40_001),
			"the byte convention spills only after the threshold")
		require.True(t, hb.shouldSpillBeforeRetain(40_002),
			"the crossing batch must be routed directly before it consumes headroom")
	})

	t.Run("row threshold already includes ingress batch", func(t *testing.T) {
		hb := &HashBuild{IsShuffle: true, NeedHashMap: true}
		hb.ctr.setSpillThreshold(10)
		hb.ctr.hashmapBuilder.InputBatchRowCount = 9
		require.False(t, hb.shouldSpillBeforeRetain(1))
		hb.ctr.hashmapBuilder.InputBatchRowCount = 10
		require.True(t, hb.shouldSpillBeforeRetain(1))
	})

	t.Run("ineligible topology stays resident", func(t *testing.T) {
		hb := &HashBuild{IsShuffle: false, NeedHashMap: true}
		hb.ctr.setSpillThreshold(1)
		hb.ctr.hashmapBuilder.InputBatchRowCount = 1
		require.False(t, hb.shouldSpillBeforeRetain(math.MaxInt64))
	})

	t.Run("size overflow fails toward spill", func(t *testing.T) {
		hb := &HashBuild{IsShuffle: true, NeedHashMap: true}
		hb.ctr.setSpillThreshold(100_001)
		hb.ctr.hashmapBuilder.Batches.MemSize = math.MaxInt64 - 1
		require.True(t, hb.shouldSpillBeforeRetain(2))
	})
}

func TestMemUsedIncludesPartialTailAfterFullBatches(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	full := batch.NewWithSize(0)
	full.SetRowCount(colexec.DefaultBatchSize)
	partial := batch.NewWithSize(1)
	partial.Vecs[0] = testutil.MakeVarcharVector([]string{"partial-tail"}, nil, proc.Mp())
	partial.SetRowCount(1)
	defer partial.Clean(proc.Mp())

	ctr := container{}
	ctr.hashmapBuilder.Batches.Buf = []*batch.Batch{full, partial}
	ctr.hashmapBuilder.Batches.MemSize = 60_000
	require.Equal(t, int64(60_000+partial.Size()), ctr.memUsed())
}

func TestHashDistributionBuild(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := testutil.MakeInt32Vector([]int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
		11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30}, nil, mp)

	hashValues := make([]uint64, 30)
	computeXXHash([]*vector.Vector{vec}, hashValues)

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

	ctr := &container{spillUUID: t.Name()}
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
			computeXXHash([]*vector.Vector{tt.vec}, hashValues)
			require.NotEqual(t, uint64(0), hashValues[0])
			require.NotEqual(t, hashValues[0], hashValues[1])
		})
	}
}

func TestNullValuesBuild(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := testutil.MakeInt32Vector([]int32{1, 2, 3}, []uint64{1}, mp)
	hashValues := make([]uint64, 3)
	computeXXHash([]*vector.Vector{vec}, hashValues)
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

	ctr := &container{spillUUID: t.Name()}
	_, err := ctr.flushBucketBuffer(proc, bat, file, analyzer)
	require.Error(t, err)

	spillfs.RemoveFile(context.Background(), "test_error_build")
}

func TestWriteSpillPayloadCancellationStopsBeforePhysicalWrite(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	ctx, cancel := context.WithCancelCause(proc.Ctx)
	process.ReplacePipelineCtx(proc, ctx, cancel)

	spillfs, err := proc.GetSpillFileService()
	require.NoError(t, err)
	file, err := spillfs.CreateFile(context.Background(), t.Name())
	require.NoError(t, err)
	defer func() {
		require.NoError(t, file.Close())
		require.NoError(t, spillfs.RemoveFile(context.Background(), t.Name()))
	}()

	proc.Cancel(context.Canceled)
	analyzer := process.NewAnalyzer(0, false, false, "test")
	err = (&container{}).writeSpillPayload(proc, file, []byte("stale spill payload"), 1, analyzer)
	require.ErrorIs(t, err, context.Canceled)

	info, err := file.Stat()
	require.NoError(t, err)
	require.Zero(t, info.Size(), "canceled spill must not start physical I/O")
	require.Zero(t, analyzer.GetOpStats().SpillSize)
	require.Zero(t, analyzer.GetOpStats().SpillRows)
}

func TestAppendBatchToSpillFilesPartitioning(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	files := make([]*os.File, spillNumBuckets)
	defer func() {
		for _, file := range files {
			if file != nil {
				file.Close()
			}
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
	ctr := &container{spillUUID: t.Name()}
	_, err := ctr.initSpillExprExecs(proc, conditions)
	require.NoError(t, err)
	err = ctr.appendBuildBatchToSpillFiles(proc, bat, files, buffers, ctr.spillExprExecs, analyzer)
	require.NoError(t, err)

	// Flush remaining buffers (lazy file creation via ensureSpillFile)
	for i, buf := range buffers {
		if buf != nil && buf.RowCount() > 0 {
			file, err := ctr.ensureSpillFile(proc, files, i)
			require.NoError(t, err)
			_, err = ctr.flushBucketBuffer(proc, buf, file, analyzer)
			require.NoError(t, err)
		}
	}
}

func TestEmptyBatchSpill(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	files := make([]*os.File, spillNumBuckets)
	defer func() {
		for _, file := range files {
			if file != nil {
				file.Close()
			}
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
	ctr := &container{spillUUID: t.Name()}
	_, err := ctr.initSpillExprExecs(proc, conditions)
	require.NoError(t, err)
	err = ctr.appendBuildBatchToSpillFiles(proc, bat, files, buffers, ctr.spillExprExecs, analyzer)
	require.NoError(t, err)
}

func TestAppendBuildBatchMultipleFlushes(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	files := make([]*os.File, spillNumBuckets)
	defer func() {
		for _, file := range files {
			if file != nil {
				file.Close()
			}
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
	ctr := &container{spillUUID: t.Name()}

	_, err := ctr.initSpillExprExecs(proc, conditions)
	require.NoError(t, err)
	err = ctr.appendBuildBatchToSpillFiles(proc, bat, files, buffers, ctr.spillExprExecs, analyzer)
	require.NoError(t, err)

	// Flush remaining (lazy file creation via ensureSpillFile)
	for i, buf := range buffers {
		if buf != nil && buf.RowCount() > 0 {
			file, err := ctr.ensureSpillFile(proc, files, i)
			require.NoError(t, err)
			_, err = ctr.flushBucketBuffer(proc, buf, file, analyzer)
			require.NoError(t, err)
		}
	}
}

func TestAppendBuildBatchWithNulls(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	files := make([]*os.File, spillNumBuckets)
	defer func() {
		for _, file := range files {
			if file != nil {
				file.Close()
			}
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
	ctr := &container{spillUUID: t.Name()}

	_, err := ctr.initSpillExprExecs(proc, conditions)
	require.NoError(t, err)
	err = ctr.appendBuildBatchToSpillFiles(proc, bat, files, buffers, ctr.spillExprExecs, analyzer)
	require.NoError(t, err)

	// Flush remaining (lazy file creation via ensureSpillFile)
	for i, buf := range buffers {
		if buf != nil && buf.RowCount() > 0 {
			file, err := ctr.ensureSpillFile(proc, files, i)
			require.NoError(t, err)
			_, err = ctr.flushBucketBuffer(proc, buf, file, analyzer)
			require.NoError(t, err)
		}
	}
}

func TestAppendBuildBatchMultiColumn(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	files := make([]*os.File, spillNumBuckets)
	defer func() {
		for _, file := range files {
			if file != nil {
				file.Close()
			}
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
	ctr := &container{spillUUID: t.Name()}

	_, err := ctr.initSpillExprExecs(proc, conditions)
	require.NoError(t, err)
	err = ctr.appendBuildBatchToSpillFiles(proc, bat, files, buffers, ctr.spillExprExecs, analyzer)
	require.NoError(t, err)

	// Flush remaining (lazy file creation via ensureSpillFile)
	for i, buf := range buffers {
		if buf != nil && buf.RowCount() > 0 {
			file, err := ctr.ensureSpillFile(proc, files, i)
			require.NoError(t, err)
			_, err = ctr.flushBucketBuffer(proc, buf, file, analyzer)
			require.NoError(t, err)
		}
	}
}

func TestShouldSpillBatchesRowThreshold(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	hb := &HashBuild{
		IsShuffle:      true,
		SpillThreshold: 10, // Small row threshold
		NeedHashMap:    true,
	}
	hb.ctr.setSpillThreshold(10)

	bat := batch.NewWithSize(1)
	bat.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 2, 3}, nil, proc.Mp())
	bat.SetRowCount(3)
	hb.ctr.hashmapBuilder.Batches.Buf = []*batch.Batch{bat}
	hb.ctr.hashmapBuilder.InputBatchRowCount = bat.RowCount()

	require.False(t, hb.shouldSpillBatches())

	// Add more batches to exceed threshold
	for i := 0; i < 10; i++ {
		bat := batch.NewWithSize(1)
		bat.Vecs[0] = testutil.MakeInt32Vector([]int32{int32(i)}, nil, proc.Mp())
		bat.SetRowCount(1)
		hb.ctr.hashmapBuilder.Batches.Buf = append(hb.ctr.hashmapBuilder.Batches.Buf, bat)
		hb.ctr.hashmapBuilder.InputBatchRowCount += bat.RowCount()
	}

	require.True(t, hb.shouldSpillBatches())
}

func TestShouldSpillBatchesMemThreshold(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	hb := &HashBuild{
		IsShuffle:      true,
		SpillThreshold: 1024 * 1024, // 1MB
		NeedHashMap:    true,
	}
	hb.ctr.setSpillThreshold(1024 * 1024)

	bat := batch.NewWithSize(1)
	bat.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 2}, nil, proc.Mp())
	bat.SetRowCount(2)
	hb.ctr.hashmapBuilder.Batches.Buf = []*batch.Batch{bat}

	require.False(t, hb.shouldSpillBatches())
}

func TestHashWithConstVector(t *testing.T) {
	mp := mpool.MustNewZero()

	vec := testutil.MakeInt32Vector([]int32{42}, nil, mp)
	vec.SetClass(vector.CONSTANT)

	hashValues := make([]uint64, 10)
	computeXXHash([]*vector.Vector{vec}, hashValues)

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
	computeXXHash([]*vector.Vector{vec1, vec2}, hashValues)

	// Different combinations should produce different hashes
	require.NotEqual(t, hashValues[0], hashValues[1])
	require.NotEqual(t, hashValues[0], hashValues[2])
}

func TestAppendBuildBatchSingleBucket(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	files := make([]*os.File, spillNumBuckets)
	defer func() {
		for _, file := range files {
			if file != nil {
				file.Close()
			}
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
	ctr := &container{spillUUID: t.Name()}

	_, err := ctr.initSpillExprExecs(proc, conditions)
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

	files := make([]*os.File, spillNumBuckets)
	defer func() {
		for _, file := range files {
			if file != nil {
				file.Close()
			}
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
	ctr := &container{spillUUID: t.Name()}

	_, err := ctr.initSpillExprExecs(proc, conditions)
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

func TestSpillExpressionLeaseRetainsLargeBatchHighWater(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	budget := process.MustNewHashBuildBudget(256<<20, 256<<20)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	defer generation.Close()

	files := make([]*os.File, spillNumBuckets)
	defer func() {
		for _, file := range files {
			if file != nil {
				_ = file.Close()
			}
		}
	}()
	expr := makeExpressionLeaseTestExpr(t, proc)
	ctr := &container{spillUUID: t.Name()}
	ctr.hashmapBuilder.setBudget(generation)
	executors, err := ctr.initSpillExprExecs(proc, []*plan.Expr{expr})
	require.NoError(t, err)
	require.NotNil(t, ctr.spillExprLease)
	defer ctr.freeSpillExprExecs()
	defer ctr.dropSpillScratchBuffers()
	defer ctr.releaseSpillScratchReservation()

	analyzer := process.NewAnalyzer(0, false, false, "test")
	large := makeExpressionLeaseTestBatch(proc, colexec.DefaultBatchSize)
	defer large.Clean(proc.Mp())
	require.NoError(t, ctr.spillBatchBounded(proc, large, files, executors, analyzer, false))
	largeReserved := ctr.spillExprLease.Reserved()
	require.Positive(t, largeReserved)

	small := makeExpressionLeaseTestBatch(proc, 1)
	defer small.Clean(proc.Mp())
	require.NoError(t, ctr.spillBatchBounded(proc, small, files, executors, analyzer, false))
	require.Equal(t, largeReserved, ctr.spillExprLease.Reserved(),
		"a small spill batch must not release retained executor headroom")
	retained, ok := ctr.spillExprLease.Retained()
	require.True(t, ok)
	require.LessOrEqual(t, retained, ctr.spillExprLease.Reserved())
}

func TestSpillWriteCoalescesAcrossBatches(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	budget := process.MustNewHashBuildBudget(8<<20, 8<<20)
	generation, err := budget.OpenGeneration(8 << 20)
	require.NoError(t, err)
	defer generation.Close()
	files := make([]*os.File, spillNumBuckets)
	defer func() {
		for _, file := range files {
			if file != nil {
				file.Close()
			}
		}
	}()
	conditions := []*plan.Expr{{
		Typ:  plan.Type{Id: int32(types.T_int32)},
		Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}},
	}}
	ctr := &container{spillUUID: t.Name()}
	ctr.hashmapBuilder.setBudget(generation)
	_, err = ctr.initSpillExprExecs(proc, conditions)
	require.NoError(t, err)
	analyzer := process.NewAnalyzer(0, false, false, "test")
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 1, 1}, nil, proc.Mp())
	bat.SetRowCount(3)
	defer bat.Clean(proc.Mp())
	for i := 0; i < 2; i++ {
		require.NoError(t, ctr.appendBuildBatchToSpillFiles(proc, bat, files, nil, ctr.spillExprExecs, analyzer))
	}
	var pending int
	for i := range ctr.spillBucketWriteBufs {
		pending += ctr.spillBucketWriteBufs[i].Len()
	}
	require.Positive(t, pending)
	var file *os.File
	for _, f := range files {
		if f != nil {
			file = f
			break
		}
	}
	require.NotNil(t, file)
	stat, err := file.Stat()
	require.NoError(t, err)
	require.Zero(t, stat.Size(), "records stay pending until the handoff flush")
	require.NoError(t, ctr.flushSpillBuffers(proc, files, analyzer))
	stat, err = file.Stat()
	require.NoError(t, err)
	require.Positive(t, stat.Size())
	for i := range ctr.spillBucketWriteBufs {
		require.Zero(t, ctr.spillBucketWriteBufs[i].Len())
	}
	_, err = file.Seek(0, io.SeekStart)
	require.NoError(t, err)
	reader := bufio.NewReader(file)
	var totalRows int64
	for {
		var header [16]byte
		_, err = io.ReadFull(reader, header[:])
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		cnt := types.DecodeInt64(header[:8])
		payload := types.DecodeInt64(header[8:])
		require.GreaterOrEqual(t, cnt, int64(0))
		require.GreaterOrEqual(t, payload, int64(0))
		_, err = io.CopyN(io.Discard, reader, payload)
		require.NoError(t, err)
		var magic [8]byte
		_, err = io.ReadFull(reader, magic[:])
		require.NoError(t, err)
		require.Equal(t, uint64(spillMagic), types.DecodeUint64(magic[:]))
		totalRows += cnt
	}
	require.Equal(t, int64(6), totalRows)
	scratchPeak := analyzer.GetOpStats().ExtraStats["HashBuildSpillScratchPeakBytes"]
	require.GreaterOrEqual(t, scratchPeak, hashBuildStatInt64(ctr.spillScratchReservation.Size()))
	require.Greater(t, scratchPeak, hashBuildStatInt64(ctr.spillScratchBase),
		"scratch peak must include retained coalesce buffers above the base lease")
	for _, f := range files {
		if f != nil {
			_ = f.Close()
		}
	}
	if ctr.spillBundle != nil {
		ctr.spillBundle.release()
		ctr.spillBundle = nil
	}
	ctr.dropSpillScratchBuffers()
	ctr.releaseSpillScratchReservation()
	require.Zero(t, generation.Used())
}

func TestSpillScratchBudgetDoesNotDoubleChargeRetainedSource(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	bat := batch.NewWithSize(1)
	values := make([]int32, colexec.DefaultBatchSize)
	for i := range values {
		values[i] = int32(i)
	}
	bat.Vecs[0] = testutil.MakeInt32Vector(values, nil, proc.Mp())
	bat.SetRowCount(len(values))
	defer bat.Clean(proc.Mp())

	fullNeed, err := spillScratchBudgetBytes(bat, false)
	require.NoError(t, err)
	retainedNeed, err := spillScratchBudgetBytes(bat, true)
	require.NoError(t, err)
	source := uint64(bat.Allocated())
	require.Equal(t, source, fullNeed-retainedNeed)
	require.Positive(t, retainedNeed)

	// The retained source has its own batch reservation. Only the incremental
	// scratch must be admitted when the real spill path starts.
	proofCap := source + fullNeed - 1
	proofBudget := process.MustNewHashBuildBudget(proofCap, proofCap)
	proofGeneration, err := proofBudget.OpenGeneration(1)
	require.NoError(t, err)
	proofSource, err := proofGeneration.Reserve(source)
	require.NoError(t, err)
	_, err = proofGeneration.Reserve(fullNeed)
	require.ErrorIs(t, err, process.ErrHashBuildBudgetAdmission)
	proofScratch, err := proofGeneration.Reserve(retainedNeed)
	require.NoError(t, err)
	proofScratch.Release()
	proofSource.Release()
	require.Zero(t, proofGeneration.Used())
}

func TestSpillScratchLazyGrowSucceeds(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	bat := batch.NewWithSize(1)
	values := make([]int32, colexec.DefaultBatchSize)
	for i := range values {
		values[i] = int32(i)
	}
	bat.Vecs[0] = testutil.MakeInt32Vector(values, nil, proc.Mp())
	bat.SetRowCount(len(values))
	defer bat.Clean(proc.Mp())

	need, err := spillScratchBudgetBytes(bat, true)
	require.NoError(t, err)
	require.Greater(t, need, uint64(1))
	source := uint64(bat.Allocated())
	const slack = uint64(2 << 20)
	capBytes := source + need + slack
	budget := process.MustNewHashBuildBudget(capBytes, capBytes)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	defer generation.Close()
	retainedToken, err := generation.Reserve(source)
	require.NoError(t, err)
	defer retainedToken.Release()
	scratchToken, err := generation.Reserve(need - 1)
	require.NoError(t, err)

	files := make([]*os.File, spillNumBuckets)
	defer func() {
		for _, file := range files {
			if file != nil {
				_ = file.Close()
			}
		}
	}()
	conditions := []*plan.Expr{{
		Typ:  plan.Type{Id: int32(types.T_int32)},
		Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}},
	}}
	ctr := &container{
		spillUUID:               t.Name(),
		spillScratchReservation: scratchToken,
		spillScratchBase:        need - 1,
	}
	ctr.hashmapBuilder.setBudget(generation)
	_, err = ctr.initSpillExprExecs(proc, conditions)
	require.NoError(t, err)
	defer ctr.freeSpillExprExecs()
	defer ctr.dropSpillScratchBuffers()
	defer ctr.releaseSpillScratchReservation()

	analyzer := process.NewAnalyzer(0, false, false, "test")
	require.NoError(t, ctr.spillBatchBounded(proc, bat, files, ctr.spillExprExecs, analyzer, true))
	require.Equal(t, need, ctr.spillScratchBase)
	require.GreaterOrEqual(t, scratchToken.Size(), need)
	require.Equal(t, int64(1), analyzer.GetOpStats().ExtraStats["HashBuildSpillScratchGrowCount"])
	require.Equal(t, int64(1), analyzer.GetOpStats().ExtraStats["HashBuildSpillScratchGrowBytes"])
	require.NoError(t, ctr.flushSpillBuffers(proc, files, analyzer))
}

func TestSpillScratchLazyGrowRejectPreservesRetainedSource(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	bat := batch.NewWithSize(1)
	bat.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 2, 3, 4}, nil, proc.Mp())
	bat.SetRowCount(4)
	defer bat.Clean(proc.Mp())
	originalAllocated := bat.Allocated()

	need, err := spillScratchBudgetBytes(bat, true)
	require.NoError(t, err)
	require.Greater(t, need, uint64(1))
	source := uint64(bat.Allocated())
	capBytes := source + need - 1
	budget := process.MustNewHashBuildBudget(capBytes, capBytes)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	defer generation.Close()
	sourceToken, err := generation.Reserve(source)
	require.NoError(t, err)
	scratchToken, err := generation.Reserve(need - 1)
	require.NoError(t, err)

	files := make([]*os.File, spillNumBuckets)
	ctr := &container{
		spillUUID:               t.Name(),
		spillScratchReservation: scratchToken,
		spillScratchBase:        need - 1,
	}
	ctr.hashmapBuilder.setBudget(generation)
	analyzer := process.NewAnalyzer(0, false, false, "lazy spill reject")
	err = ctr.spillBatchBounded(proc, bat, files, nil, analyzer, true)
	require.ErrorIs(t, err, process.ErrHashBuildBudgetAdmission)
	require.Equal(t, int64(1), analyzer.GetOpStats().ExtraStats["HashBuildSpillScratchGrowRejects"])
	require.Equal(t, need-1, scratchToken.Size())
	require.Equal(t, capBytes, generation.Used())
	require.Equal(t, 4, bat.RowCount())
	require.Equal(t, originalAllocated, bat.Allocated())
	require.Nil(t, ctr.spillHashValues)
	require.Nil(t, ctr.spillBucketRowIds)
	for _, file := range files {
		require.Nil(t, file)
	}

	ctr.releaseSpillScratchReservation()
	sourceToken.Release()
	require.Zero(t, generation.Used())
}

func TestFlushSpillBuffersCancellationDiscardsPendingWrites(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	ctx, cancel := context.WithCancelCause(proc.Ctx)
	process.ReplacePipelineCtx(proc, ctx, cancel)

	ctr := &container{}
	for _, bucket := range []int{0, spillNumBuckets - 1} {
		_, err := ctr.spillBucketWriteBufs[bucket].Write([]byte("pending"))
		require.NoError(t, err)
		ctr.spillBucketWriteRows[bucket] = 1
	}
	proc.Cancel(context.Canceled)

	err := ctr.flushSpillBuffers(proc, nil, process.NewAnalyzer(0, false, false, "test"))
	require.ErrorIs(t, err, context.Canceled)
	for bucket := 0; bucket < spillNumBuckets; bucket++ {
		require.Zero(t, ctr.spillBucketWriteBufs[bucket].Len())
		require.Zero(t, ctr.spillBucketWriteRows[bucket])
	}
}

func TestSpillMaterializedBytesDoesNotScaleShuffledConstVector(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	const sourceRows = 32 * 1024
	bat := batch.NewWithSize(2)
	values := make([]int32, sourceRows)
	for i := range values {
		values[i] = int32(i)
	}
	bat.Vecs[0] = testutil.MakeInt32Vector(values, nil, proc.Mp())
	var err error
	bat.Vecs[1], err = vector.NewConstBytes(
		types.T_varchar.ToType(),
		[]byte("test create big fulltext index"),
		sourceRows,
		proc.Mp(),
	)
	require.NoError(t, err)
	bat.SetRowCount(sourceRows)
	defer bat.Clean(proc.Mp())

	// Batch.Shuffle intentionally leaves a const vector untouched while it
	// changes the batch cardinality. This is the shape produced by the failed
	// generate_series + const-varchar BVT query.
	require.NoError(t, bat.Shuffle([]int64{0}, proc.Mp()))
	require.Equal(t, 1, bat.RowCount())
	require.Equal(t, sourceRows, bat.Vecs[1].Length())

	legacySource := uint64(bat.Allocated())
	if size := uint64(bat.Size()); size > legacySource {
		legacySource = size
	}
	legacyScaled := legacySource * uint64(colexec.DefaultBatchSize)
	legacyMetadata, ok := retainedMetadataAllowance(bat)
	require.True(t, ok)
	legacyScaled += legacyMetadata * uint64(colexec.DefaultBatchSize)
	legacyNeed, err := spillPeakBudgetFor(uint64(colexec.DefaultBatchSize), 0, legacyScaled, uint64(len(bat.Vecs)))
	require.NoError(t, err)
	require.Greater(t, legacyNeed, uint64(10<<30),
		"the old logical-size extrapolation must reproduce the false 10 GiB rejection")

	materialized, err := spillMaterializedBytes(bat)
	require.NoError(t, err)
	wantMaterialized := uint64(types.T_int32.ToType().TypeSize()) +
		uint64(types.T_varchar.ToType().TypeSize()) +
		uint64(len("test create big fulltext index"))
	require.Equal(t, wantMaterialized, materialized,
		"lazy admission must use the batch's live rows and const payload")

	directNeed, err := spillBudgetBytes(bat)
	require.NoError(t, err)
	require.Less(t, directNeed, uint64(16<<20),
		"lazy scratch admission must not scale stale logical length")
}

func TestSpillMaterializedBytesDoesNotScaleRetainedVectorCapacity(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	const sourceRows = 32 * 1024
	bat := batch.NewWithSize(2)
	values := make([]int32, sourceRows)
	strings := make([]string, sourceRows)
	for i := range values {
		values[i] = int32(i)
		strings[i] = "test create big fulltext index"
	}
	bat.Vecs[0] = testutil.MakeInt32Vector(values, nil, proc.Mp())
	bat.Vecs[1] = testutil.MakeVarcharVector(strings, nil, proc.Mp())
	bat.SetRowCount(sourceRows)
	defer bat.Clean(proc.Mp())

	// Reused shuffle and table-function batches keep their allocation while
	// publishing a tiny final batch. Only the first row is live, but Allocated
	// still describes the original 32K-row capacity.
	bat.Vecs[0].SetLength(1)
	bat.Vecs[1].SetLength(1)
	bat.SetRowCount(1)
	require.Greater(t, bat.Allocated(), 1<<20)

	materialized, err := spillMaterializedBytes(bat)
	require.NoError(t, err)
	wantMaterialized := uint64(types.T_int32.ToType().TypeSize()) +
		uint64(types.T_varchar.ToType().TypeSize()) +
		uint64(len(strings[0]))
	require.Equal(t, wantMaterialized, materialized)

	directNeed, err := spillBudgetBytes(bat)
	require.NoError(t, err)
	require.Less(t, directNeed, uint64(16<<20),
		"source capacity is charged once, never extrapolated per live row")
}

func TestSpillProjectedSourceSkipsStaleNullVarlenaPayload(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	bat := batch.NewWithSize(1)
	bat.Vecs[0] = testutil.MakeVarcharVector([]string{"x"}, []uint64{0}, proc.Mp())
	bat.SetRowCount(1)
	defer bat.Clean(proc.Mp())

	// A null append does not overwrite a reused varlen slot. Plant the stale
	// non-inline header that such a slot can retain; UnionInt32 skips the null
	// value, so this dead payload must not be projected into the spill batch.
	values, _ := vector.MustVarlenaRawData(bat.Vecs[0])
	const staleLen = uint32(1 << 20)
	values[0].SetOffsetLen(0, staleLen)

	source, err := spillMaterializedBytes(bat)
	require.NoError(t, err)
	require.Equal(t, uint64(bat.Vecs[0].GetType().TypeSize()), source)

	need, err := spillBudgetBytes(bat)
	require.NoError(t, err)
	require.Less(t, need, uint64(16<<20))
}

func TestSpillMaterializedBytesBoundaryInputs(t *testing.T) {
	source, err := spillMaterializedBytes(nil)
	require.NoError(t, err)
	require.Zero(t, source)

	invalid := batch.NewWithSize(1)
	invalid.SetRowCount(1)
	_, err = spillMaterializedBytes(invalid)
	require.ErrorIs(t, err, process.ErrHashBuildBudgetInvalid)
	_, err = spillBudgetBytes(invalid)
	require.ErrorIs(t, err, process.ErrHashBuildBudgetInvalid)

	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	short := batch.NewWithSize(1)
	short.Vecs[0] = testutil.MakeVarcharVector([]string{"x"}, nil, proc.Mp())
	short.SetRowCount(2)
	defer short.Clean(proc.Mp())
	_, err = spillMaterializedBytes(short)
	require.ErrorIs(t, err, process.ErrHashBuildBudgetInvalid)

	constNull := batch.NewWithSize(1)
	constNull.Vecs[0] = vector.NewConstNull(types.T_varchar.ToType(), 1, proc.Mp())
	constNull.SetRowCount(1)
	defer constNull.Clean(proc.Mp())

	source, err = spillMaterializedBytes(constNull)
	require.NoError(t, err)
	require.Equal(t, uint64(types.T_varchar.ToType().TypeSize()), source)
}

func TestSpillBudgetArithmeticFailsClosed(t *testing.T) {
	value, err := spillCheckedAdd(math.MaxUint64-1, 1)
	require.NoError(t, err)
	require.Equal(t, uint64(math.MaxUint64), value)
	_, err = spillCheckedAdd(math.MaxUint64, 1)
	require.ErrorIs(t, err, process.ErrHashBuildBudgetInvalid)

	value, err = spillCheckedMul(math.MaxUint64, 1)
	require.NoError(t, err)
	require.Equal(t, uint64(math.MaxUint64), value)
	_, err = spillCheckedMul(math.MaxUint64, 2)
	require.ErrorIs(t, err, process.ErrHashBuildBudgetInvalid)

	_, err = spillPeakBudgetFor(math.MaxUint64, 0, 0, 0)
	require.ErrorIs(t, err, process.ErrHashBuildBudgetInvalid)
	_, err = spillPeakBudgetFor(0, math.MaxUint64, 1, 0)
	require.ErrorIs(t, err, process.ErrHashBuildBudgetInvalid)
	_, err = spillPeakBudgetFor(0, 0, math.MaxUint64, 0)
	require.ErrorIs(t, err, process.ErrHashBuildBudgetInvalid)
}

func TestSpillCapacityReplacementOverlapChargesOldArrays(t *testing.T) {
	got, err := spillCapacityReplacementOverlap(16, 4, 8, 8, 2)
	require.NoError(t, err)
	require.Equal(t, uint64(8*8+8*4+2*8), got)

	got, err = spillCapacityReplacementOverlap(8, 2, 8, 8, 2)
	require.NoError(t, err)
	require.Zero(t, got)

	_, err = spillCapacityReplacementOverlap(-1, 0, 0, 0, 0)
	require.ErrorIs(t, err, process.ErrHashBuildBudgetInvalid)
	_, err = spillCapacityReplacementOverlap(math.MaxInt, 0, math.MaxInt-1, 0, 0)
	require.ErrorIs(t, err, process.ErrHashBuildBudgetInvalid)
}

func TestSpillReplacementPeakReusesHighWaterLease(t *testing.T) {
	budget := process.MustNewHashBuildBudget(120, 120)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	token, err := generation.Reserve(100)
	require.NoError(t, err)
	ctr := container{
		hashmapBuilder:          HashmapBuilder{budget: generation},
		spillScratchReservation: token,
		spillScratchBase:        100,
	}
	analyzer := process.NewAnalyzer(0, false, false, "replacement peak")

	oldSize, grew, err := ctr.growSpillScratchTransient(90, analyzer)
	require.NoError(t, err)
	require.False(t, grew)
	require.Zero(t, oldSize)
	require.Equal(t, uint64(100), generation.Used())

	oldSize, grew, err = ctr.growSpillScratchTransient(110, analyzer)
	require.NoError(t, err)
	require.True(t, grew)
	require.Equal(t, uint64(100), oldSize)
	require.Equal(t, uint64(110), generation.Used())
	require.Equal(t, int64(110), analyzer.GetOpStats().ExtraStats["HashBuildSpillScratchPeakBytes"])
	require.NoError(t, ctr.restoreSpillScratchTransient(oldSize, grew))
	require.Equal(t, uint64(100), generation.Used())
	require.NoError(t, ctr.restoreSpillScratchTransient(0, false))

	_, grew, err = ctr.growSpillScratchTransient(121, analyzer)
	require.ErrorIs(t, err, process.ErrHashBuildBudgetAdmission)
	require.False(t, grew)
	require.Equal(t, uint64(100), generation.Used())

	token.Release()
	require.Zero(t, generation.Used())
}

func TestSpillPeakChargesSerializedPayloadOnce(t *testing.T) {
	const (
		rows          = uint64(8192)
		inputBytes    = uint64(3 << 20)
		selectedBytes = uint64(5 << 20)
	)
	got, err := spillPeakBudgetFor(rows, inputBytes, selectedBytes, 0)
	require.NoError(t, err)
	want := rows*12 + inputBytes + selectedBytes + selectedBytes + 64*1024
	require.Equal(t, want, got)
}

func TestMarshalSpillRecordPreallocatesSinglePayload(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	bat := batch.NewWithSize(1)
	var err error
	bat.Vecs[0], err = vector.NewConstBytes(
		types.T_varchar.ToType(), make([]byte, 4<<20), 64, proc.Mp(),
	)
	require.NoError(t, err)
	bat.SetRowCount(64)
	defer bat.Clean(proc.Mp())

	buf := bytes.NewBuffer(make([]byte, 0, 1<<20))
	_, err = marshalSpillRecord(bat, buf)
	require.NoError(t, err)
	base := uint64(bat.Allocated())
	if size := uint64(bat.Size()); size > base {
		base = size
	}
	require.Equal(t, base+128+24, uint64(buf.Cap()))

	small := batch.NewWithSize(1)
	small.Vecs[0], err = vector.NewConstBytes(
		types.T_varchar.ToType(), make([]byte, 1024), 1, proc.Mp(),
	)
	require.NoError(t, err)
	small.SetRowCount(1)
	defer small.Clean(proc.Mp())
	_, err = marshalSpillRecord(small, buf)
	require.NoError(t, err, "a retained large serialization buffer must be reusable for a smaller batch")
}

func TestSpillLazyReservationBoundaryInputs(t *testing.T) {
	budget := process.MustNewHashBuildBudget(1, 1)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	defer generation.Close()
	ctr := &container{}
	ctr.hashmapBuilder.setBudget(generation)
	analyzer := process.NewAnalyzer(0, false, false, "spill reservation boundary")

	require.NoError(t, ctr.spillBatchBounded(nil, nil, nil, nil, analyzer, false))
	require.Nil(t, ctr.spillScratchReservation)

	invalid := batch.NewWithSize(1)
	invalid.SetRowCount(1)
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	err = ctr.spillBatchBounded(
		proc, invalid, make([]*os.File, spillNumBuckets), nil, analyzer, false)
	require.ErrorIs(t, err, process.ErrHashBuildBudgetInvalid)
	require.Nil(t, ctr.spillScratchReservation)
	require.Zero(t, generation.Used())
}

func TestSpillMaterializedBytesFollowsConstUnionSemantics(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	payload := make([]byte, 1<<20)
	for i := range payload {
		payload[i] = 'x'
	}
	const rows = 64
	source := batch.NewWithSize(1)
	var err error
	source.Vecs[0], err = vector.NewConstBytes(types.T_varchar.ToType(), payload, rows, proc.Mp())
	require.NoError(t, err)
	source.SetRowCount(rows)
	defer source.Clean(proc.Mp())

	directBytes, err := spillMaterializedBytes(source)
	require.NoError(t, err)
	require.Equal(t,
		uint64(rows*types.T_varchar.ToType().TypeSize()+len(payload)),
		directBytes,
		"direct UnionInt32 copies one payload and broadcasts its descriptor")

	selected := batch.NewWithSize(1)
	selected.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
	defer selected.Clean(proc.Mp())
	sels := make([]int32, rows)
	for i := range sels {
		sels[i] = int32(i)
	}
	require.NoError(t, selected.Vecs[0].PreExtend(rows, proc.Mp()))
	require.NoError(t, selected.Vecs[0].UnionInt32(source.Vecs[0], sels, proc.Mp()))
	selected.SetRowCount(rows)
	require.GreaterOrEqual(t, directBytes, uint64(selected.Allocated()))

	var retained colexec.Batches
	defer retained.Clean(proc.Mp())
	require.NoError(t, retained.CopyIntoBatches(source, proc))
	require.Len(t, retained.Buf, 1)
	require.False(t, retained.Buf[0].Vecs[0].IsConst())
	retainedBytes, err := spillMaterializedBytes(retained.Buf[0])
	require.NoError(t, err)
	require.Equal(t,
		uint64(rows)*(uint64(types.T_varchar.ToType().TypeSize())+uint64(len(payload))),
		retainedBytes,
		"the actual retained batch is non-const, so selection copies each value")
	require.Greater(t, retainedBytes, directBytes*32)
}

func TestSpillMaterializedEstimateCoversRetainedConstCopy(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	payload := make([]byte, 1<<10)
	for i := range payload {
		payload[i] = 'x'
	}
	const (
		inputRows = 4
		totalRows = inputRows * 2
	)
	source := batch.NewWithSize(1)
	var err error
	source.Vecs[0], err = vector.NewConstBytes(types.T_varchar.ToType(), payload, inputRows, proc.Mp())
	require.NoError(t, err)
	source.SetRowCount(inputRows)
	defer source.Clean(proc.Mp())

	var retained colexec.Batches
	defer retained.Clean(proc.Mp())
	require.NoError(t, retained.CopyIntoBatches(source, proc))
	require.NoError(t, retained.CopyIntoBatches(source, proc))
	require.Len(t, retained.Buf, 1)
	require.Equal(t, totalRows, retained.Buf[0].RowCount())
	require.False(t, retained.Buf[0].Vecs[0].IsConst(),
		"CopyIntoBatches materializes const ingress as retained row values")

	estimated, err := spillMaterializedBytes(retained.Buf[0])
	require.NoError(t, err)
	selected := batch.NewWithSize(1)
	selected.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
	defer selected.Clean(proc.Mp())
	sels := make([]int32, totalRows)
	for i := range sels {
		sels[i] = int32(i)
	}
	require.NoError(t, selected.Vecs[0].PreExtend(totalRows, proc.Mp()))
	require.NoError(t, selected.Vecs[0].UnionInt32(retained.Buf[0].Vecs[0], sels, proc.Mp()))
	selected.SetRowCount(totalRows)
	require.GreaterOrEqual(t, estimated, uint64(selected.Allocated()))
}

func TestSpillMaterializedEstimateFollowsFullBatchCloneToSemantics(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	makeConstBatch := func(payloadBytes int) *batch.Batch {
		payload := make([]byte, payloadBytes)
		for i := range payload {
			payload[i] = 'x'
		}
		source := batch.NewWithSize(1)
		var err error
		source.Vecs[0], err = vector.NewConstBytes(
			types.T_varchar.ToType(),
			payload,
			colexec.DefaultBatchSize,
			proc.Mp(),
		)
		require.NoError(t, err)
		source.SetRowCount(colexec.DefaultBatchSize)
		return source
	}

	large := makeConstBatch(1 << 20)
	defer large.Clean(proc.Mp())
	directNeed, err := spillBudgetBytes(large)
	require.NoError(t, err)
	require.Less(t, directNeed, uint64(16<<20))

	var retainedLarge colexec.Batches
	defer retainedLarge.Clean(proc.Mp())
	require.NoError(t, retainedLarge.CopyIntoBatches(large, proc))
	require.Len(t, retainedLarge.Buf, 1)
	require.False(t, retainedLarge.Buf[0].Vecs[0].IsConst(),
		"Batch.Dup delegates to Batch.CloneTo/UnionBatch and does not call Vector.Dup")
	require.Equal(t, 1<<20, len(retainedLarge.Buf[0].Vecs[0].GetArea()))
	retainedNeed, err := spillMaterializedBytes(retainedLarge.Buf[0])
	require.NoError(t, err)
	wantRetained := uint64(colexec.DefaultBatchSize) *
		(uint64(1<<20) + uint64(types.T_varchar.ToType().TypeSize()))
	require.Equal(t, wantRetained, retainedNeed,
		"the actual non-const retained batch materializes one MiB plus one descriptor per row")

	// Materialize a smaller exact-full-batch payload end-to-end without
	// allocating the MiB case's 8 GiB selected area.
	small := makeConstBatch(4 << 10)
	defer small.Clean(proc.Mp())
	var retainedSmall colexec.Batches
	defer retainedSmall.Clean(proc.Mp())
	require.NoError(t, retainedSmall.CopyIntoBatches(small, proc))
	require.False(t, retainedSmall.Buf[0].Vecs[0].IsConst())

	estimated, err := spillMaterializedBytes(retainedSmall.Buf[0])
	require.NoError(t, err)
	selected := batch.NewWithSize(1)
	selected.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
	defer selected.Clean(proc.Mp())
	sels := make([]int32, colexec.DefaultBatchSize)
	for i := range sels {
		sels[i] = int32(i)
	}
	require.NoError(t, selected.Vecs[0].PreExtend(colexec.DefaultBatchSize, proc.Mp()))
	require.NoError(t, selected.Vecs[0].UnionInt32(retainedSmall.Buf[0].Vecs[0], sels, proc.Mp()))
	selected.SetRowCount(colexec.DefaultBatchSize)
	require.Equal(t, colexec.DefaultBatchSize*(4<<10), len(selected.Vecs[0].GetArea()))
	require.GreaterOrEqual(t, estimated, uint64(selected.Allocated()))
}

func TestSpillBatchLazyReservationFailsClosed(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	bat := batch.NewWithSize(1)
	bat.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 2, 3, 4}, nil, proc.Mp())
	bat.SetRowCount(4)
	defer bat.Clean(proc.Mp())

	need, err := spillBudgetBytes(bat)
	require.NoError(t, err)
	require.Positive(t, need)
	budget := process.MustNewHashBuildBudget(need-1, need-1)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	defer generation.Close()

	ctr := &container{}
	ctr.hashmapBuilder.setBudget(generation)
	files := make([]*os.File, spillNumBuckets)
	err = ctr.spillBatchBounded(
		proc, bat, files, nil,
		process.NewAnalyzer(0, false, false, "direct spill reject"), false)
	require.ErrorIs(t, err, process.ErrHashBuildBudgetAdmission)
	require.Nil(t, ctr.spillScratchReservation)
	require.Nil(t, ctr.spillHashValues)
	require.Nil(t, ctr.spillBucketRowIds)
	require.Equal(t, 4, bat.RowCount())
	for _, file := range files {
		require.Nil(t, file)
	}
	require.Zero(t, generation.Used())
}

func TestEnsureSpillFile(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	ctr := &container{spillUUID: "test_ensure"}
	files := make([]*os.File, spillNumBuckets)

	// First call creates a file.
	f, err := ctr.ensureSpillFile(proc, files, 3)
	require.NoError(t, err)
	require.NotNil(t, f)
	require.Equal(t, f, files[3])
	defer f.Close()

	// Second call returns cached file.
	f2, err := ctr.ensureSpillFile(proc, files, 3)
	require.NoError(t, err)
	require.Same(t, f, f2, "should return the same file object")

	// Different bucket creates a different file.
	f3, err := ctr.ensureSpillFile(proc, files, 7)
	require.NoError(t, err)
	require.NotNil(t, f3)
	require.NotEqual(t, f.Fd(), f3.Fd())
	defer f3.Close()

	// Untouched buckets remain nil.
	require.Nil(t, files[0])
	require.Nil(t, files[1])
}

func TestCleanupSpillFiles(t *testing.T) {
	// Create temp files to simulate spill fds.
	var fds []*os.File
	for i := 0; i < 3; i++ {
		f, err := os.CreateTemp("", "test_cleanup_*")
		require.NoError(t, err)
		defer os.Remove(f.Name())
		fds = append(fds, f)
	}
	// Include a nil entry.
	fds = append(fds, nil)

	hb := &HashBuild{ctr: container{spilledFds: fds}}
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	hb.cleanupSpillFiles(proc)
	require.Nil(t, hb.ctr.spilledFds)

	// Verify all files are closed (writing should fail).
	for _, f := range fds[:3] {
		_, err := f.Write([]byte("x"))
		require.Error(t, err, "file should be closed")
	}
}

func TestAccountedInitialSpillConvertsHeadroomToPhysicalOwnership(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	const limit = uint64(8 << 20)
	budget := process.MustNewHashBuildBudget(limit, limit)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	registry, err := mpool.NewAllocationAccountRegistry(1, 128)
	require.NoError(t, err)
	account, err := registry.OpenWithController(limit, generation)
	require.NoError(t, err)
	var op HashBuild
	op.NeedHashMap = true
	require.NoError(t, op.SetAllocationAccount(account))
	ctr := &op.ctr
	ctr.hashmapBuilder.setBudget(generation)
	ctr.spillUUID = "accounted-initial-spill"
	exprs := []*plan.Expr{newExpr(0, types.T_int64.ToType())}
	executors, err := ctr.initSpillExprExecs(proc, exprs)
	require.NoError(t, err)
	require.True(t, ctr.spillExprAccounted)
	input := testutil.NewBatch(
		[]types.Type{types.T_int64.ToType(), types.T_varchar.ToType()},
		true,
		1_024,
		proc.Mp(),
	)
	defer input.Clean(proc.Mp())
	headroom, err := spillBudgetBytes(input)
	require.NoError(t, err)
	ctr.spillScratchReservation, err = generation.Reserve(headroom)
	require.NoError(t, err)
	ctr.spillScratchBase = headroom
	ctr.spillScratchEmergency = true
	files := make([]*os.File, spillNumBuckets)
	analyzer := process.NewAnalyzer(0, false, false, "test")
	require.NoError(t, ctr.spillBatchBounded(
		proc,
		input,
		files,
		executors,
		analyzer,
		false,
	))
	require.Nil(t, ctr.spillScratchReservation)
	snapshot := generation.Snapshot()
	require.Equal(t, snapshot.AllocationUsed, snapshot.Used,
		"external input ownership is transient and headroom is not stacked")
	require.Positive(t, snapshot.AllocationUsed)
	require.NotNil(t, ctr.spillAccountedWrite)
	require.NoError(t, ctr.flushSpillBuffers(proc, files, analyzer))

	ctr.dropSpillScratchBuffers()
	ctr.freeSpillExprExecs()
	for _, file := range files {
		if file != nil {
			_ = file.Close()
		}
	}
	if ctr.spillBundle != nil {
		ctr.spillBundle.release()
		ctr.spillBundle = nil
	}
	require.Zero(t, account.Snapshot().Used)
	require.Zero(t, generation.Used())
	require.NoError(t, op.ClearAllocationAccount(account))
	_, _, err = registry.CompleteTerminal(account)
	require.NoError(t, err)
}

func TestAccountedMinimumSpillHeadroomIsOneUnitNotWholeBatch(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	makeBatch := func(rows int) *batch.Batch {
		values := make([]string, rows)
		for i := range values {
			values[i] = strings.Repeat("x", 4<<10)
		}
		bat := batch.NewWithSize(1)
		bat.Vecs[0] = testutil.MakeVarcharVector(values, nil, proc.Mp())
		bat.SetRowCount(rows)
		return bat
	}
	one := makeBatch(1)
	many := makeBatch(128)
	defer one.Clean(proc.Mp())
	defer many.Clean(proc.Mp())
	exprs := []*plan.Expr{newExpr(0, types.T_varchar.ToType())}
	oneUnit, err := spillMinimumUnitBudgetBytes(one, exprs)
	require.NoError(t, err)
	manyUnits, err := spillMinimumUnitBudgetBytes(many, exprs)
	require.NoError(t, err)
	require.Equal(t, oneUnit, manyUnits)
	require.Positive(t, oneUnit)
	legacyWholeBatch, err := spillBudgetBytes(many)
	require.NoError(t, err)
	require.Greater(t, legacyWholeBatch, manyUnits)
}

func TestAccountedConcatSpillHeadroomUsesOnePhysicalRow(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	column := func(pos int32) *plan.Expr {
		return &plan.Expr{
			Typ:  plan.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen},
			Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: pos}},
		}
	}
	expr, err := plan2.BindFuncExprImplByPlanExpr(
		proc.Ctx,
		"concat",
		[]*plan.Expr{column(0), column(1)},
	)
	require.NoError(t, err)
	bat := batch.NewWithSize(2)
	bat.Vecs[0] = testutil.MakeVarcharVector(
		[]string{strings.Repeat("a", 4<<10), "a"}, nil, proc.Mp())
	bat.Vecs[1] = testutil.MakeVarcharVector(
		[]string{"b", strings.Repeat("b", 4<<10)}, nil, proc.Mp())
	bat.SetRowCount(2)
	defer bat.Clean(proc.Mp())

	payload, err := spillExpressionPayloadBytes(
		expr,
		expr.GetF().GetArgs(),
		bat,
	)
	require.NoError(t, err)
	require.Equal(t, uint64((4<<10)+1), payload)
	headroom, err := spillMinimumUnitBudgetBytes(bat, []*plan.Expr{expr})
	require.NoError(t, err)
	require.Positive(t, headroom)
}

func TestAccountedInitialSpillReducesUnpublishedInputAndPreservesRows(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	const limit = uint64(80 << 10)
	budget := process.MustNewHashBuildBudget(limit, limit)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	registry, err := mpool.NewAllocationAccountRegistry(1, 128)
	require.NoError(t, err)
	account, err := registry.OpenWithController(limit, generation)
	require.NoError(t, err)
	var op HashBuild
	op.NeedHashMap = true
	require.NoError(t, op.SetAllocationAccount(account))
	ctr := &op.ctr
	ctr.hashmapBuilder.setBudget(generation)
	ctr.spillUUID = "accounted-adaptive-spill"
	exprs := []*plan.Expr{newExpr(0, types.T_int64.ToType())}
	executors, err := ctr.initSpillExprExecs(proc, exprs)
	require.NoError(t, err)
	values := make([]int64, colexec.DefaultBatchSize)
	for i := range values {
		values[i] = int64(i)
	}
	input := batch.NewWithSize(1)
	input.Vecs[0] = testutil.MakeInt64Vector(values, nil, proc.Mp())
	input.SetRowCount(len(values))
	defer input.Clean(proc.Mp())
	files := make([]*os.File, spillNumBuckets)
	analyzer := process.NewAnalyzer(0, false, false, "test")
	require.NoError(t, ctr.spillBatchWithPressure(
		proc,
		input,
		files,
		executors,
		analyzer,
		false,
	))
	require.Positive(t,
		analyzer.GetOpStats().ExtraStats["HashBuildSpillInputReductions"])
	require.NoError(t, ctr.flushSpillBuffers(proc, files, analyzer))

	var totalRows int64
	for _, file := range files {
		if file == nil {
			continue
		}
		_, err = file.Seek(0, io.SeekStart)
		require.NoError(t, err)
		reader := bufio.NewReader(file)
		for {
			var header [16]byte
			_, err = io.ReadFull(reader, header[:])
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			rows := types.DecodeInt64(header[:8])
			payload := types.DecodeInt64(header[8:])
			require.NoError(t, func() error {
				_, copyErr := io.CopyN(io.Discard, reader, payload+8)
				return copyErr
			}())
			totalRows += rows
		}
	}
	require.Equal(t, int64(len(values)), totalRows)

	ctr.dropSpillScratchBuffers()
	ctr.freeSpillExprExecs()
	for _, file := range files {
		if file != nil {
			_ = file.Close()
		}
	}
	if ctr.spillBundle != nil {
		ctr.spillBundle.release()
		ctr.spillBundle = nil
	}
	require.Zero(t, account.Snapshot().Used)
	require.Zero(t, generation.Used())
	require.NoError(t, op.ClearAllocationAccount(account))
	_, _, err = registry.CompleteTerminal(account)
	require.NoError(t, err)
}
