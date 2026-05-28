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

package hashbuild

import (
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestBuildHashMap(t *testing.T) {
	var hb HashmapBuilder
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	err := hb.Prepare([]*plan.Expr{newExpr(0, types.T_int32.ToType())}, -1, -1, nil, proc)
	require.NoError(t, err)

	inputBatch := testutil.NewBatch([]types.Type{types.T_int32.ToType()}, true, int(100000), proc.Mp())
	err = hb.Batches.CopyIntoBatches(inputBatch, proc)
	hb.InputBatchRowCount = inputBatch.RowCount()
	inputBatch.Clean(proc.Mp())
	require.NoError(t, err)

	err = hb.BuildHashmap(false, true, true, proc)
	require.NoError(t, err)
	require.Less(t, int64(0), hb.GetSize())
	require.Less(t, uint64(0), hb.GetGroupCount())
	hb.Reset(proc, true)
	hb.Free(proc)
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestHashMapAllocAndFree(t *testing.T) {
	mp := mpool.MustNewZero()

	var hb HashmapBuilder
	var err error
	hb.IntHashMap, err = hashmap.NewIntHashMap(false, mp)
	require.NoError(t, err)
	err = hb.IntHashMap.PreAlloc(100)
	require.NoError(t, err)
	hb.IntHashMap.Free()
	hb.IntHashMap, err = hashmap.NewIntHashMap(false, mp)
	require.NoError(t, err)
	err = hb.IntHashMap.PreAlloc(10000)
	require.NoError(t, err)
	hb.IntHashMap.Free()
	hb.IntHashMap, err = hashmap.NewIntHashMap(false, mp)
	require.NoError(t, err)
	err = hb.IntHashMap.PreAlloc(1000000)
	require.NoError(t, err)
	hb.IntHashMap.Free()
	hb.IntHashMap, err = hashmap.NewIntHashMap(false, mp)
	require.NoError(t, err)
	err = hb.IntHashMap.PreAlloc(100000000)
	require.NoError(t, err)
	hb.IntHashMap.Free()
}

func TestIteratorReuseAcrossBuilds(t *testing.T) {
	var hb HashmapBuilder
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_int32.ToType())}, -1, -1, nil, proc))

	b := testutil.NewBatch([]types.Type{types.T_int32.ToType()}, true, 16, proc.Mp())
	defer b.Clean(proc.Mp())

	hb.InputBatchRowCount = b.RowCount()
	require.NoError(t, hb.Batches.CopyIntoBatches(b, proc))
	require.NoError(t, hb.BuildHashmap(false, false, false, proc))
	require.NotNil(t, hb.cachedIntIterator)
	itr1 := hb.cachedIntIterator

	// Reset should detach owner but keep iterator for reuse.
	hb.Reset(proc, true)
	require.NotNil(t, hb.cachedIntIterator)
	require.Same(t, itr1, hb.cachedIntIterator)

	// Next build should reuse the same iterator instance.
	hb.InputBatchRowCount = b.RowCount()
	require.NoError(t, hb.Batches.CopyIntoBatches(b, proc))
	require.NoError(t, hb.BuildHashmap(false, false, false, proc))
	require.Same(t, itr1, hb.cachedIntIterator)
}

func TestStrIteratorCapacityPrune(t *testing.T) {
	var hb HashmapBuilder
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_varchar.ToType())}, -1, -1, nil, proc))

	// Build with an oversized string to inflate iterator buffers beyond threshold.
	vec := vector.NewVec(types.T_varchar.ToType())
	large := strings.Repeat("x", hashmap.MaxStrIteratorCapacity+4096)
	require.NoError(t, vector.AppendBytes(vec, []byte(large), false, proc.Mp()))
	bat := batch.New([]string{"col"})
	bat.SetVector(0, vec)
	bat.SetRowCount(1)
	hb.InputBatchRowCount = bat.RowCount()
	require.NoError(t, hb.Batches.CopyIntoBatches(bat, proc))
	require.NoError(t, hb.BuildHashmap(false, false, false, proc))
	require.NotNil(t, hb.cachedStrIterator)
	require.Greater(t, hashmap.StrIteratorCapacity(hb.cachedStrIterator), hashmap.MaxStrIteratorCapacity)

	hb.detachAndPruneCachedIterators()
	require.Nil(t, hb.cachedStrIterator)
}

func TestStrIteratorBelowThresholdIsKept(t *testing.T) {
	var hb HashmapBuilder
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_varchar.ToType())}, -1, -1, nil, proc))

	// Build with small strings so iterator capacity stays below threshold.
	vec := vector.NewVec(types.T_varchar.ToType())
	for i := 0; i < 4; i++ {
		require.NoError(t, vector.AppendBytes(vec, []byte("small"), false, proc.Mp()))
	}
	bat := batch.New([]string{"col"})
	bat.SetVector(0, vec)
	bat.SetRowCount(vec.Length())
	hb.InputBatchRowCount = bat.RowCount()
	require.NoError(t, hb.Batches.CopyIntoBatches(bat, proc))
	require.NoError(t, hb.BuildHashmap(false, false, false, proc))
	require.NotNil(t, hb.cachedStrIterator)
	require.Less(t, hashmap.StrIteratorCapacity(hb.cachedStrIterator), hashmap.MaxStrIteratorCapacity)

	hb.detachAndPruneCachedIterators()
	require.NotNil(t, hb.cachedStrIterator, "iterator below threshold should be kept")
}

func TestResetWithHashTableSentKeepsCache(t *testing.T) {
	var hb HashmapBuilder
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_int32.ToType())}, -1, -1, nil, proc))

	// Build once to populate cachedIntIterator.
	b := testutil.NewBatch([]types.Type{types.T_int32.ToType()}, true, 8, proc.Mp())
	defer b.Clean(proc.Mp())
	hb.InputBatchRowCount = b.RowCount()
	require.NoError(t, hb.Batches.CopyIntoBatches(b, proc))
	require.NoError(t, hb.BuildHashmap(false, false, false, proc))
	require.NotNil(t, hb.cachedIntIterator)

	// hashTableHasNotSent=false should skip FreeHashMapAndBatches and keep cached iterator.
	hb.Reset(proc, false)
	require.NotNil(t, hb.cachedIntIterator)
}

func TestAlternateIntStrBuildsReuseIndependently(t *testing.T) {
	var hb HashmapBuilder
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())

	// First int build
	require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_int32.ToType())}, -1, -1, nil, proc))
	bInt := testutil.NewBatch([]types.Type{types.T_int32.ToType()}, true, 4, proc.Mp())
	defer bInt.Clean(proc.Mp())
	hb.InputBatchRowCount = bInt.RowCount()
	require.NoError(t, hb.Batches.CopyIntoBatches(bInt, proc))
	require.NoError(t, hb.BuildHashmap(false, false, false, proc))
	require.NotNil(t, hb.cachedIntIterator)
	require.Nil(t, hb.cachedStrIterator)

	hb.Reset(proc, true)
	// Simulate a new plan with different key types.
	hb.executors = nil

	// Then str build
	require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_varchar.ToType())}, -1, -1, nil, proc))
	vec := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(vec, []byte("a"), false, proc.Mp()))
	bat := batch.New([]string{"col"})
	bat.SetVector(0, vec)
	bat.SetRowCount(1)
	hb.InputBatchRowCount = bat.RowCount()
	require.NoError(t, hb.Batches.CopyIntoBatches(bat, proc))
	require.NoError(t, hb.BuildHashmap(false, false, false, proc))
	require.NotNil(t, hb.cachedStrIterator)

	hb.Reset(proc, true)
	// Simulate switching back to int keys in a new plan.
	hb.executors = nil

	// Build int again and ensure int cache still exists (reuse if retained).
	prevIntItr := hb.cachedIntIterator
	// Use a fresh int batch to avoid zero-row short-circuit.
	bInt2 := testutil.NewBatch([]types.Type{types.T_int32.ToType()}, true, 4, proc.Mp())
	defer bInt2.Clean(proc.Mp())
	hb.InputBatchRowCount = bInt2.RowCount()
	require.NoError(t, hb.Batches.CopyIntoBatches(bInt2, proc))
	require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_int32.ToType())}, -1, -1, nil, proc))
	require.NoError(t, hb.BuildHashmap(false, false, false, proc))
	require.NotNil(t, hb.cachedIntIterator)
	if prevIntItr != nil {
		require.Same(t, prevIntItr, hb.cachedIntIterator)
	}
}

func TestBuildHashmapWithZeroInputKeepsCachesUntouched(t *testing.T) {
	var hb HashmapBuilder
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())

	require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_int32.ToType())}, -1, -1, nil, proc))

	// No rows added.
	hb.InputBatchRowCount = 0
	require.NoError(t, hb.BuildHashmap(false, false, false, proc))

	require.Nil(t, hb.cachedIntIterator)
	require.Nil(t, hb.cachedStrIterator)
}

func TestDedupBuildDuplicateKeyStillFailsByDefault(t *testing.T) {
	var hb HashmapBuilder
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	hb.IsDedup = true
	hb.OnDuplicateAction = plan.Node_FAIL
	hb.DedupColName = "id"
	hb.DedupColTypes = []plan.Type{newExpr(0, types.T_int32.ToType()).Typ}
	defer func() {
		hb.Reset(proc, true)
		hb.Free(proc)
		require.Equal(t, int64(0), proc.Mp().CurrNB())
	}()

	require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_int32.ToType())}, -1, -1, nil, proc))
	bat := makeIntKeyValueBatch(proc, []int32{1, 1}, []int32{10, 20})
	require.NoError(t, hb.Batches.CopyIntoBatches(bat, proc))
	hb.InputBatchRowCount = bat.RowCount()
	bat.Clean(proc.Mp())

	err := hb.BuildHashmap(false, false, false, proc)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry))
}

func TestDedupBuildKeepLastForReplace(t *testing.T) {
	var hb HashmapBuilder
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	hb.IsDedup = true
	hb.DedupBuildKeepLast = true
	hb.OnDuplicateAction = plan.Node_FAIL
	hb.DedupColName = "id"
	hb.DedupColTypes = []plan.Type{newExpr(0, types.T_int32.ToType()).Typ}
	defer func() {
		hb.Reset(proc, true)
		hb.Free(proc)
		require.Equal(t, int64(0), proc.Mp().CurrNB())
	}()

	require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_int32.ToType())}, -1, -1, nil, proc))
	bat := makeIntKeyValueBatch(proc, []int32{1, 1, 2}, []int32{10, 20, 30})
	require.NoError(t, hb.Batches.CopyIntoBatches(bat, proc))
	hb.InputBatchRowCount = bat.RowCount()
	bat.Clean(proc.Mp())

	require.NoError(t, hb.BuildHashmap(false, false, false, proc))
	require.Equal(t, 2, hb.InputBatchRowCount)
	require.Equal(t, 2, hb.Batches.RowCount())
	require.Equal(t, uint64(2), hb.GetGroupCount())

	out := hb.Batches.Buf[0]
	require.Equal(t, 2, out.RowCount())
	keys := vector.MustFixedColNoTypeCheck[int32](out.Vecs[0])[:out.RowCount()]
	values := vector.MustFixedColNoTypeCheck[int32](out.Vecs[1])[:out.RowCount()]
	require.Equal(t, []int32{1, 2}, keys)
	require.Equal(t, []int32{20, 30}, values)
}

func TestDedupBuildKeepLastPreservesDeleteOnlyRows(t *testing.T) {
	var hb HashmapBuilder
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	hb.IsDedup = true
	hb.DedupBuildKeepLast = true
	hb.OnDuplicateAction = plan.Node_FAIL
	hb.DedupColName = "id"
	hb.DedupColTypes = []plan.Type{newExpr(0, types.T_int32.ToType()).Typ}
	defer func() {
		hb.Reset(proc, true)
		hb.Free(proc)
		require.Equal(t, int64(0), proc.Mp().CurrNB())
	}()

	require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_int32.ToType())}, -1, 2, []int32{2}, proc))
	bat := makeIntKeyValueBatchWithMarker(
		proc,
		[]int32{1, 1, 2},
		[]int32{10, 20, 30},
		[]int32{100, 0, 0},
		[]uint64{1, 2},
	)
	require.NoError(t, hb.Batches.CopyIntoBatches(bat, proc))
	hb.InputBatchRowCount = bat.RowCount()
	bat.Clean(proc.Mp())

	require.NoError(t, hb.BuildHashmap(false, false, false, proc))
	require.Equal(t, 3, hb.InputBatchRowCount)
	require.Equal(t, 3, hb.Batches.RowCount())
	require.Equal(t, uint64(2), hb.GetGroupCount())
	require.NotNil(t, hb.DelRows)
	require.True(t, hb.DelRows.Contains(2))

	out := hb.Batches.Buf[0]
	require.Equal(t, 3, out.RowCount())
	require.True(t, out.Vecs[0].IsNull(2))
	require.True(t, out.Vecs[1].IsNull(2))
	require.Falsef(t, out.Vecs[2].IsNull(2), "nulls=%v", out.Vecs[2].GetNulls().GetBitmap().String())
	markers := vector.MustFixedColNoTypeCheck[int32](out.Vecs[2])[:out.RowCount()]
	require.Equal(t, int32(100), markers[2])
}

// TestDedupBuildKeepLastDeleteOnlyRowsWithDelColIdx covers the keep-last rebuild
// when BOTH delColIdx>=0 and dedupDeleteMarkerColIdx>=0 are set (the real
// REPLACE/FAIL path when OldColList has 2+ entries, see operator.go). This is
// the combination that exercises the rebuild's delColIdx Find loop against a
// DelRows bitmap pre-sized by keepDiscardedRowsForDelete: the bitmap is sized
// for the full post-append row set, and group ids from Find never exceed
// activeCount, so Add stays in bounds (no index-out-of-range panic).
func TestDedupBuildKeepLastDeleteOnlyRowsWithDelColIdx(t *testing.T) {
	var hb HashmapBuilder
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	hb.IsDedup = true
	hb.DedupBuildKeepLast = true
	hb.OnDuplicateAction = plan.Node_FAIL
	hb.DedupColName = "id"
	hb.DedupColTypes = []plan.Type{newExpr(0, types.T_int32.ToType()).Typ}
	defer func() {
		hb.Reset(proc, true)
		hb.Free(proc)
		require.Equal(t, int64(0), proc.Mp().CurrNB())
	}()

	// delColIdx=0 (probe the id column against the build hashmap), markerColIdx=2,
	// keep col 2. Key 1 is duplicated; the discarded earlier row (idx 0) carries
	// a non-null delete marker, so it becomes a delete-only appended row.
	require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_int32.ToType())}, 0, 2, []int32{2}, proc))
	bat := makeIntKeyValueBatchWithMarker(
		proc,
		[]int32{1, 1, 2},
		[]int32{10, 20, 30},
		[]int32{100, 0, 0},
		[]uint64{1, 2}, // marker null for rows 1,2; non-null for row 0
	)
	require.NoError(t, hb.Batches.CopyIntoBatches(bat, proc))
	hb.InputBatchRowCount = bat.RowCount()
	bat.Clean(proc.Mp())

	require.NotPanics(t, func() {
		require.NoError(t, hb.BuildHashmap(false, false, false, proc))
	})

	require.Equal(t, 3, hb.InputBatchRowCount)
	require.Equal(t, 3, hb.Batches.RowCount())
	require.Equal(t, uint64(2), hb.GetGroupCount())

	// DelRows must be sized for the full post-append row set and contain both the
	// two active group ids (0,1) deleted via the delColIdx Find loop and the
	// appended delete-only row position (2).
	require.NotNil(t, hb.DelRows)
	require.Equal(t, int64(3), hb.DelRows.Len())
	require.True(t, hb.DelRows.Contains(0))
	require.True(t, hb.DelRows.Contains(1))
	require.True(t, hb.DelRows.Contains(2))

	// The appended row keeps only the marker column.
	out := hb.Batches.Buf[0]
	require.Equal(t, 3, out.RowCount())
	require.True(t, out.Vecs[0].IsNull(2))
	require.True(t, out.Vecs[1].IsNull(2))
	require.False(t, out.Vecs[2].IsNull(2))
	markers2 := vector.MustFixedColNoTypeCheck[int32](out.Vecs[2])[:out.RowCount()]
	require.Equal(t, int32(100), markers2[2])
}

// TestDedupBuildKeepLastMarksConflictBucketForDiscardedFanout reproduces the
// REPLACE multi-UK fan-out case (issue #24428) at the hashbuild layer: one new
// row (same new PK) fans out to several build rows that carry DIFFERENT old
// PKs. keep-last keeps one and turns the others into delete-only rows. The
// surviving bucket must still be marked deleted (DelRows) when a discarded
// row's old PK equals the surviving row's new key, otherwise the dedup-join
// probe side raises a false DuplicateEntry for the existing row REPLACE removes.
func TestDedupBuildKeepLastMarksConflictBucketForDiscardedFanout(t *testing.T) {
	var hb HashmapBuilder
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	hb.IsDedup = true
	hb.DedupBuildKeepLast = true
	hb.OnDuplicateAction = plan.Node_FAIL
	hb.DedupColName = "id"
	hb.DedupColTypes = []plan.Type{newExpr(0, types.T_int32.ToType()).Typ}
	defer func() {
		hb.Reset(proc, true)
		hb.Free(proc)
		require.Equal(t, int64(0), proc.Mp().CurrNB())
	}()

	// keyCols = col0 (new PK); delColIdx = col1 (old PK); marker = col2 (old
	// row id). Empty keep-col list so BuildHashmap also preserves the old-PK
	// column on the delete-only rows.
	require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_int32.ToType())}, 1, 2, nil, proc))
	// Three fan-out copies of new PK=1 that matched old rows with PK 1, 2, 3.
	bat := makeIntKeyValueBatchWithMarker(
		proc,
		[]int32{1, 1, 1},
		[]int32{1, 2, 3},
		[]int32{100, 200, 300},
		nil,
	)
	require.NoError(t, hb.Batches.CopyIntoBatches(bat, proc))
	hb.InputBatchRowCount = bat.RowCount()
	bat.Clean(proc.Mp())

	require.NoError(t, hb.BuildHashmap(false, false, false, proc))

	// One surviving row (index 0) plus two delete-only rows (index 1, 2).
	require.Equal(t, 3, hb.Batches.RowCount())
	require.NotNil(t, hb.DelRows)
	require.True(t, hb.DelRows.Contains(1), "delete-only row should be marked")
	require.True(t, hb.DelRows.Contains(2), "delete-only row should be marked")
	// The fix: the surviving bucket (index 0, new key = 1) is marked deleted
	// because a discarded fan-out row carried old PK = 1.
	require.True(t, hb.DelRows.Contains(0),
		"surviving bucket must be marked deleted via a discarded row's old PK")

	// Delete-only rows keep the old-PK column (col1) that drives that marking,
	// while their new-PK column (col0) is nulled so they only delete.
	out := hb.Batches.Buf[0]
	require.True(t, out.Vecs[0].IsNull(1))
	require.True(t, out.Vecs[0].IsNull(2))
	require.False(t, out.Vecs[1].IsNull(1))
	require.False(t, out.Vecs[1].IsNull(2))
	oldPks := vector.MustFixedColNoTypeCheck[int32](out.Vecs[1])[:out.RowCount()]
	require.ElementsMatch(t, []int32{1, 2}, []int32{oldPks[1], oldPks[2]})
}
func TestBuildHashmapErrorDoesNotLeakIterators(t *testing.T) {
	var hb HashmapBuilder
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)

	require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_int32.ToType())}, -1, -1, nil, proc))

	// Inject a failing executor to trigger an error during evalJoinCondition.
	hb.executors = []colexec.ExpressionExecutor{failingExecutor{}}
	hb.InputBatchRowCount = 1
	hb.Batches.Buf = []*batch.Batch{batch.NewWithSize(1)}
	hb.Batches.Buf[0].Vecs[0] = testutil.MakeInt32Vector([]int32{1}, nil, proc.Mp())
	require.Error(t, hb.BuildHashmap(false, false, false, proc))
	require.Nil(t, hb.cachedIntIterator)
	require.Nil(t, hb.cachedStrIterator)

	// After failure, a normal path should still succeed and populate cache.
	hb.executors = nil
	require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_int32.ToType())}, -1, -1, nil, proc))
	hb.InputBatchRowCount = 1
	hb.Batches.Reset()
	hb.Batches.Buf = nil
	intVec := testutil.MakeInt32Vector([]int32{1}, nil, proc.Mp())
	intBat := batch.New([]string{"col"})
	intBat.SetVector(0, intVec)
	intBat.SetRowCount(1)
	require.NoError(t, hb.Batches.CopyIntoBatches(intBat, proc))
	require.NoError(t, hb.BuildHashmap(false, false, false, proc))
	require.NotNil(t, hb.cachedIntIterator)
}

func TestBuildHashmapReuseUniqueSelsBuffer(t *testing.T) {
	var hb HashmapBuilder
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_int32.ToType())}, -1, -1, nil, proc))

	bat := makeIntBatch(t, 4, proc)
	defer bat.Clean(proc.Mp())

	// First build: should allocate uniqueSels
	hb.InputBatchRowCount = bat.RowCount()
	require.NoError(t, hb.Batches.CopyIntoBatches(bat, proc))
	require.NoError(t, hb.BuildHashmap(false, false, true, proc))
	require.NotNil(t, hb.uniqueSels)
	require.Greater(t, cap(hb.uniqueSels), 0)
	require.Greater(t, len(hb.uniqueSels), 0)
	firstPtr := &hb.uniqueSels[0]

	hb.Reset(proc, true)
	hb.executors = nil

	// Second build: reuse same buffer
	require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_int32.ToType())}, -1, -1, nil, proc))
	hb.InputBatchRowCount = bat.RowCount()
	hb.Batches.Reset()
	hb.Batches.Buf = nil
	require.NoError(t, hb.Batches.CopyIntoBatches(bat, proc))
	require.NoError(t, hb.BuildHashmap(false, false, true, proc))
	require.NotNil(t, hb.uniqueSels)
	require.Greater(t, len(hb.uniqueSels), 0)
	require.Equal(t, firstPtr, &hb.uniqueSels[0])
}

func TestBuildHashmapDoesNotCreateUniqueSelsWhenNotNeeded(t *testing.T) {
	var hb HashmapBuilder
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_int32.ToType())}, -1, -1, nil, proc))

	bat := makeIntBatch(t, 2, proc)
	defer bat.Clean(proc.Mp())

	hb.InputBatchRowCount = bat.RowCount()
	require.NoError(t, hb.Batches.CopyIntoBatches(bat, proc))
	require.NoError(t, hb.BuildHashmap(false, false, false, proc))
	require.Nil(t, hb.uniqueSels, "should not allocate uniqueSels when needUniqueVec is false")
}

func TestCachedStrIteratorOwnerClearedBeforeReuse(t *testing.T) {
	var hb HashmapBuilder
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())

	// Build once to create cached str iterator.
	require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_varchar.ToType())}, -1, -1, nil, proc))
	bat := makeStrBatch(t, 4, proc)
	defer bat.Clean(proc.Mp())
	hb.InputBatchRowCount = bat.RowCount()
	require.NoError(t, hb.Batches.CopyIntoBatches(bat, proc))
	require.NoError(t, hb.BuildHashmap(false, false, false, proc))
	require.NotNil(t, hb.cachedStrIterator)

	// Bind to a stale map.
	staleMap, err := hashmap.NewStrHashMap(false, proc.Mp())
	require.NoError(t, err)
	hashmap.IteratorChangeOwner(hb.cachedStrIterator, staleMap)

	// Next build should clear stale owner.
	hb.Reset(proc, true)
	hb.executors = nil
	require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_varchar.ToType())}, -1, -1, nil, proc))
	hb.InputBatchRowCount = bat.RowCount()
	hb.Batches.Reset()
	hb.Batches.Buf = nil
	require.NoError(t, hb.Batches.CopyIntoBatches(bat, proc))
	require.NoError(t, hb.BuildHashmap(false, false, false, proc))

	rv := reflect.ValueOf(hb.cachedStrIterator).Elem()
	mpField := rv.FieldByName("mp")
	require.False(t, mpField.IsNil())
	require.NotEqual(t, reflect.ValueOf(staleMap).Pointer(), mpField.Pointer())
}

func TestSwitchKeyTypeCreatesCorrectIterator(t *testing.T) {
	var hb HashmapBuilder
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())

	// Build int first.
	require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_int32.ToType())}, -1, -1, nil, proc))
	intBat := makeIntBatch(t, 2, proc)
	defer intBat.Clean(proc.Mp())
	hb.InputBatchRowCount = intBat.RowCount()
	require.NoError(t, hb.Batches.CopyIntoBatches(intBat, proc))
	require.NoError(t, hb.BuildHashmap(false, false, false, proc))
	require.NotNil(t, hb.cachedIntIterator)

	// Switch to varchar keys; cached str should be created and usable.
	hb.Reset(proc, true)
	hb.executors = nil
	require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_varchar.ToType())}, -1, -1, nil, proc))
	strBat := makeStrBatch(t, 2, proc)
	defer strBat.Clean(proc.Mp())
	hb.InputBatchRowCount = strBat.RowCount()
	hb.Batches.Reset()
	hb.Batches.Buf = nil
	require.NoError(t, hb.Batches.CopyIntoBatches(strBat, proc))
	require.NoError(t, hb.BuildHashmap(false, false, false, proc))
	require.NotNil(t, hb.cachedStrIterator)
}

func TestCachedIteratorOwnerClearedBeforeReuse(t *testing.T) {
	var hb HashmapBuilder
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())

	// Build once to create cached int iterator and bind to map A.
	require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_int32.ToType())}, -1, -1, nil, proc))
	bat := makeIntBatch(t, 4, proc)
	defer bat.Clean(proc.Mp())
	hb.InputBatchRowCount = bat.RowCount()
	require.NoError(t, hb.Batches.CopyIntoBatches(bat, proc))
	require.NoError(t, hb.BuildHashmap(false, false, false, proc))
	require.NotNil(t, hb.cachedIntIterator)

	// Manually bind iterator to a different map to simulate stale owner.
	staleMap, err := hashmap.NewIntHashMap(false, proc.Mp())
	require.NoError(t, err)
	hashmap.IteratorChangeOwner(hb.cachedIntIterator, staleMap)

	// Next build should clear stale owner and rebind to fresh map, not panic.
	hb.Reset(proc, true)
	hb.executors = nil
	require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_int32.ToType())}, -1, -1, nil, proc))
	hb.InputBatchRowCount = bat.RowCount()
	hb.Batches.Reset()
	hb.Batches.Buf = nil
	require.NoError(t, hb.Batches.CopyIntoBatches(bat, proc))
	require.NoError(t, hb.BuildHashmap(false, false, false, proc))

	// Owner should now be non-nil and point to the new map (i.e., not staleMap).
	rv := reflect.ValueOf(hb.cachedIntIterator).Elem()
	mpField := rv.FieldByName("mp")
	require.False(t, mpField.IsNil())
	require.NotEqual(t, reflect.ValueOf(staleMap).Pointer(), mpField.Pointer())
}

func TestFreeThenBuildRepopulatesCache(t *testing.T) {
	var hb HashmapBuilder
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())

	// First build to populate cache.
	require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_int32.ToType())}, -1, -1, nil, proc))
	intVec := testutil.MakeInt32Vector([]int32{1, 2}, nil, proc.Mp())
	intBat := batch.New([]string{"col"})
	intBat.SetVector(0, intVec)
	intBat.SetRowCount(2)
	hb.InputBatchRowCount = intBat.RowCount()
	require.NoError(t, hb.Batches.CopyIntoBatches(intBat, proc))
	require.NoError(t, hb.BuildHashmap(false, false, false, proc))
	require.NotNil(t, hb.cachedIntIterator)

	// Free should clear cache.
	hb.Free(proc)
	require.Nil(t, hb.cachedIntIterator)
	require.Nil(t, hb.cachedStrIterator)

	// Build again after Free should succeed and repopulate cache.
	require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_int32.ToType())}, -1, -1, nil, proc))
	hb.InputBatchRowCount = intBat.RowCount()
	require.NoError(t, hb.Batches.CopyIntoBatches(intBat, proc))
	require.NoError(t, hb.BuildHashmap(false, false, false, proc))
	require.NotNil(t, hb.cachedIntIterator)
}

// failingExecutor always returns an error; used to simulate BuildHashmap failure paths.
type failingExecutor struct{}

func (f failingExecutor) Eval(*process.Process, []*batch.Batch, []bool) (*vector.Vector, error) {
	return nil, moerr.NewInternalErrorNoCtx("exec failed")
}
func (f failingExecutor) EvalWithoutResultReusing(*process.Process, []*batch.Batch, []bool) (*vector.Vector, error) {
	return nil, moerr.NewInternalErrorNoCtx("exec failed")
}
func (f failingExecutor) IsColumnExpr() bool { return false }
func (f failingExecutor) TypeName() string   { return "failingExecutor" }
func (f failingExecutor) Free()              {}
func (f failingExecutor) ResetForNextQuery() {}

// Benchmarks: cached vs new iterator paths for int/str.
func BenchmarkBuildHashmapCachedInt(b *testing.B) {
	proc := testutil.NewProcessWithMPool(b, "", mpool.MustNewZero())
	hb := &HashmapBuilder{}
	require.NoError(b, hb.Prepare([]*plan.Expr{newExpr(0, types.T_int32.ToType())}, -1, -1, nil, proc))
	data := makeIntBatch(b, 1024, proc)
	defer data.Clean(proc.Mp())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hb.InputBatchRowCount = data.RowCount()
		require.NoError(b, hb.Batches.CopyIntoBatches(data, proc))
		require.NoError(b, hb.BuildHashmap(false, false, false, proc))
		hb.Reset(proc, true)
	}
}

func BenchmarkBuildHashmapCachedStr(b *testing.B) {
	proc := testutil.NewProcessWithMPool(b, "", mpool.MustNewZero())
	hb := &HashmapBuilder{}
	require.NoError(b, hb.Prepare([]*plan.Expr{newExpr(0, types.T_varchar.ToType())}, -1, -1, nil, proc))
	data := makeStrBatch(b, 1024, proc)
	defer data.Clean(proc.Mp())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hb.InputBatchRowCount = data.RowCount()
		require.NoError(b, hb.Batches.CopyIntoBatches(data, proc))
		require.NoError(b, hb.BuildHashmap(false, false, false, proc))
		hb.Reset(proc, true)
	}
}

func makeIntBatch(tb testing.TB, n int, proc *process.Process) *batch.Batch {
	ints := make([]int32, n)
	for i := 0; i < n; i++ {
		ints[i] = int32(i)
	}
	vec := testutil.MakeInt32Vector(ints, nil, proc.Mp())
	bat := batch.New([]string{"col"})
	bat.SetVector(0, vec)
	bat.SetRowCount(n)
	return bat
}

func makeStrBatch(tb testing.TB, n int, proc *process.Process) *batch.Batch {
	vec := vector.NewVec(types.T_varchar.ToType())
	for i := 0; i < n; i++ {
		require.NoError(tb, vector.AppendBytes(vec, []byte("v"+strconv.Itoa(i)), false, proc.Mp()))
	}
	bat := batch.New([]string{"col"})
	bat.SetVector(0, vec)
	bat.SetRowCount(n)
	return bat
}

func makeIntKeyValueBatch(proc *process.Process, keys []int32, values []int32) *batch.Batch {
	keyVec := testutil.MakeInt32Vector(keys, nil, proc.Mp())
	valueVec := testutil.MakeInt32Vector(values, nil, proc.Mp())
	bat := batch.New([]string{"id", "v"})
	bat.SetVector(0, keyVec)
	bat.SetVector(1, valueVec)
	bat.SetRowCount(len(keys))
	return bat
}

func makeIntKeyValueBatchWithMarker(
	proc *process.Process,
	keys []int32,
	values []int32,
	markers []int32,
	markerNulls []uint64,
) *batch.Batch {
	keyVec := testutil.MakeInt32Vector(keys, nil, proc.Mp())
	valueVec := testutil.MakeInt32Vector(values, nil, proc.Mp())
	markerVec := testutil.MakeInt32Vector(markers, markerNulls, proc.Mp())
	bat := batch.New([]string{"id", "v", "old_row_id"})
	bat.SetVector(0, keyVec)
	bat.SetVector(1, valueVec)
	bat.SetVector(2, markerVec)
	bat.SetRowCount(len(keys))
	return bat
}

// Cold path benchmarks: recreate builder each iteration (no cached iterator reuse).
func BenchmarkBuildHashmapColdInt(b *testing.B) {
	proc := testutil.NewProcessWithMPool(b, "", mpool.MustNewZero())
	data := makeIntBatch(b, 1024, proc)
	defer data.Clean(proc.Mp())
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hb := &HashmapBuilder{}
		require.NoError(b, hb.Prepare([]*plan.Expr{newExpr(0, types.T_int32.ToType())}, -1, -1, nil, proc))
		hb.InputBatchRowCount = data.RowCount()
		require.NoError(b, hb.Batches.CopyIntoBatches(data, proc))
		require.NoError(b, hb.BuildHashmap(false, false, false, proc))
		hb.Free(proc)
	}
}

func BenchmarkBuildHashmapColdStr(b *testing.B) {
	proc := testutil.NewProcessWithMPool(b, "", mpool.MustNewZero())
	data := makeStrBatch(b, 1024, proc)
	defer data.Clean(proc.Mp())
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hb := &HashmapBuilder{}
		require.NoError(b, hb.Prepare([]*plan.Expr{newExpr(0, types.T_varchar.ToType())}, -1, -1, nil, proc))
		hb.InputBatchRowCount = data.RowCount()
		require.NoError(b, hb.Batches.CopyIntoBatches(data, proc))
		require.NoError(b, hb.BuildHashmap(false, false, false, proc))
		hb.Free(proc)
	}
}

func TestExtractRestoreCachedIterators(t *testing.T) {
	var hb HashmapBuilder
	mp := mpool.MustNewZero()

	intMap, err := hashmap.NewIntHashMap(false, mp)
	require.NoError(t, err)
	strMap, err := hashmap.NewStrHashMap(false, mp)
	require.NoError(t, err)

	hb.cachedIntIterator = intMap.NewIterator()
	hb.cachedStrIterator = strMap.NewIterator()

	intItr, strItr := hb.ExtractCachedIteratorsForReuse()
	require.Nil(t, hb.cachedIntIterator)
	require.Nil(t, hb.cachedStrIterator)

	// Owners should be cleared after extraction.
	rvInt := reflect.ValueOf(intItr).Elem()
	require.True(t, rvInt.FieldByName("mp").IsNil())
	rvStr := reflect.ValueOf(strItr).Elem()
	require.True(t, rvStr.FieldByName("mp").IsNil())

	hb.RestoreCachedIterators(intItr, strItr)
	require.Same(t, intItr, hb.cachedIntIterator)
	require.Same(t, strItr, hb.cachedStrIterator)
}

func TestStrIteratorLargeStringTriggersPrune(t *testing.T) {
	var hb HashmapBuilder
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_varchar.ToType())}, -1, -1, nil, proc))

	// Build a batch with one very large string to bloat iterator buffers.
	vec := vector.NewVec(types.T_varchar.ToType())
	large := strings.Repeat("x", hashmap.MaxStrIteratorCapacity+2048)
	require.NoError(t, vector.AppendBytes(vec, []byte(large), false, proc.Mp()))
	bat := batch.New([]string{"col"})
	bat.SetVector(0, vec)
	bat.SetRowCount(1)

	hb.InputBatchRowCount = bat.RowCount()
	require.NoError(t, hb.Batches.CopyIntoBatches(bat, proc))

	require.NoError(t, hb.BuildHashmap(false, false, false, proc))
	require.NotNil(t, hb.cachedStrIterator)
	require.Greater(t, hashmap.StrIteratorCapacity(hb.cachedStrIterator), hashmap.MaxStrIteratorCapacity)

	hb.Reset(proc, true)
	require.Nil(t, hb.cachedStrIterator, "iterator with oversized buffers should be dropped")
}

// TestResetWithNilPointers tests that Reset() handles nil pointers gracefully
// This is a regression test for the panic fix where Reset() would crash when
// curVecs or UniqueJoinKeys contained nil pointers.
func TestResetWithNilPointers(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	var hb HashmapBuilder

	// Test case 1: curVecs with nil pointers and needDupVec = true
	hb.needDupVec = true
	hb.curVecs = make([]*vector.Vector, 2)
	hb.curVecs[0] = nil
	hb.curVecs[1] = nil

	// Test case 2: UniqueJoinKeys with nil pointers
	hb.UniqueJoinKeys = make([]*vector.Vector, 3)
	hb.UniqueJoinKeys[0] = nil
	hb.UniqueJoinKeys[1] = nil
	hb.UniqueJoinKeys[2] = nil

	// Reset should not panic
	hb.Reset(proc, true)
	require.Nil(t, hb.curVecs)
	require.Nil(t, hb.UniqueJoinKeys)
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

// TestResetWithMixedNilAndValidPointers tests Reset() with a mix of nil and valid vectors
func TestResetWithMixedNilAndValidPointers(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	var hb HashmapBuilder

	// Create some valid vectors
	vec1 := testutil.MakeInt32Vector([]int32{1, 2, 3}, nil, proc.Mp())
	vec2 := testutil.MakeInt32Vector([]int32{4, 5, 6}, nil, proc.Mp())

	// Test case: curVecs with mix of nil and valid vectors
	hb.needDupVec = true
	hb.curVecs = []*vector.Vector{vec1, nil, vec2}

	// Test case: UniqueJoinKeys with mix of nil and valid vectors
	hb.UniqueJoinKeys = []*vector.Vector{nil}

	// Reset should free valid vectors and not panic on nil
	hb.Reset(proc, true)
	require.Nil(t, hb.curVecs)
	require.Nil(t, hb.UniqueJoinKeys)
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

// TestFreeWithNilPointers tests that Free() handles nil pointers gracefully
func TestFreeWithNilPointers(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	var hb HashmapBuilder

	// Test case: UniqueJoinKeys with nil pointers
	hb.UniqueJoinKeys = make([]*vector.Vector, 3)
	hb.UniqueJoinKeys[0] = nil
	hb.UniqueJoinKeys[1] = nil
	hb.UniqueJoinKeys[2] = nil

	// Free should not panic
	hb.Free(proc)
	require.Nil(t, hb.UniqueJoinKeys)
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

// TestFreeWithMixedNilAndValidPointers tests Free() with a mix of nil and valid vectors
func TestFreeWithMixedNilAndValidPointers(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	var hb HashmapBuilder

	// Create some valid vectors
	vec1 := testutil.MakeInt32Vector([]int32{1, 2, 3}, nil, proc.Mp())
	vec2 := testutil.MakeInt32Vector([]int32{4, 5, 6}, nil, proc.Mp())

	// Test case: UniqueJoinKeys with mix of nil and valid vectors
	hb.UniqueJoinKeys = []*vector.Vector{vec1, nil, vec2}

	// Free should free valid vectors and not panic on nil
	hb.Free(proc)
	require.Nil(t, hb.UniqueJoinKeys)
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}
