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

package hashmap_util

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

func newExpr(pos int32, typ types.Type) *plan.Expr {
	return &plan.Expr{
		Typ: plan.Type{
			Id:    int32(typ.Oid),
			Width: typ.Width,
			Scale: typ.Scale,
		},
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				ColPos: pos,
			},
		},
	}
}

func TestBuildHashMap(t *testing.T) {
	var hb HashmapBuilder
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	err := hb.Prepare([]*plan.Expr{newExpr(0, types.T_int32.ToType())}, -1, proc)
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
	require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_int32.ToType())}, -1, proc))

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
	require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_varchar.ToType())}, -1, proc))

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
	require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_varchar.ToType())}, -1, proc))

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
	require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_int32.ToType())}, -1, proc))

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
	require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_int32.ToType())}, -1, proc))
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
	require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_varchar.ToType())}, -1, proc))
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
	require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_int32.ToType())}, -1, proc))
	require.NoError(t, hb.BuildHashmap(false, false, false, proc))
	require.NotNil(t, hb.cachedIntIterator)
	if prevIntItr != nil {
		require.Same(t, prevIntItr, hb.cachedIntIterator)
	}
}

func TestBuildHashmapWithZeroInputKeepsCachesUntouched(t *testing.T) {
	var hb HashmapBuilder
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())

	require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_int32.ToType())}, -1, proc))

	// No rows added.
	hb.InputBatchRowCount = 0
	require.NoError(t, hb.BuildHashmap(false, false, false, proc))

	require.Nil(t, hb.cachedIntIterator)
	require.Nil(t, hb.cachedStrIterator)
}

func TestBuildHashmapErrorDoesNotLeakIterators(t *testing.T) {
	var hb HashmapBuilder
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)

	require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_int32.ToType())}, -1, proc))

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
	require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_int32.ToType())}, -1, proc))
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

func TestFreeThenBuildRepopulatesCache(t *testing.T) {
	var hb HashmapBuilder
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())

	// First build to populate cache.
	require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_int32.ToType())}, -1, proc))
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
	require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_int32.ToType())}, -1, proc))
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
	require.NoError(b, hb.Prepare([]*plan.Expr{newExpr(0, types.T_int32.ToType())}, -1, proc))
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
	require.NoError(b, hb.Prepare([]*plan.Expr{newExpr(0, types.T_varchar.ToType())}, -1, proc))
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

// Cold path benchmarks: recreate builder each iteration (no cached iterator reuse).
func BenchmarkBuildHashmapColdInt(b *testing.B) {
	proc := testutil.NewProcessWithMPool(b, "", mpool.MustNewZero())
	data := makeIntBatch(b, 1024, proc)
	defer data.Clean(proc.Mp())
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hb := &HashmapBuilder{}
		require.NoError(b, hb.Prepare([]*plan.Expr{newExpr(0, types.T_int32.ToType())}, -1, proc))
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
		require.NoError(b, hb.Prepare([]*plan.Expr{newExpr(0, types.T_varchar.ToType())}, -1, proc))
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
	require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_varchar.ToType())}, -1, proc))

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
// vecs or UniqueJoinKeys contained nil pointers.
func TestResetWithNilPointers(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	var hb HashmapBuilder

	// Test case 1: vecs with nil pointers and needDupVec = true
	hb.needDupVec = true
	hb.vecs = make([][]*vector.Vector, 2)
	hb.vecs[0] = make([]*vector.Vector, 2)
	hb.vecs[1] = make([]*vector.Vector, 2)
	// Set some vectors to nil to simulate partial initialization
	hb.vecs[0][0] = nil
	hb.vecs[0][1] = nil
	hb.vecs[1][0] = nil
	hb.vecs[1][1] = nil

	// Test case 2: vecs with nil slice
	hb.vecs = append(hb.vecs, nil)

	// Test case 3: UniqueJoinKeys with nil pointers
	hb.UniqueJoinKeys = make([]*vector.Vector, 3)
	hb.UniqueJoinKeys[0] = nil
	hb.UniqueJoinKeys[1] = nil
	hb.UniqueJoinKeys[2] = nil

	// Reset should not panic
	hb.Reset(proc, true)
	require.Nil(t, hb.vecs)
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

	// Test case: vecs with mix of nil and valid vectors
	hb.needDupVec = true
	hb.vecs = make([][]*vector.Vector, 2)
	hb.vecs[0] = []*vector.Vector{vec1, nil}
	hb.vecs[1] = []*vector.Vector{nil, vec2}

	// Test case: UniqueJoinKeys with mix of nil and valid vectors
	hb.UniqueJoinKeys = []*vector.Vector{vec1, nil, vec2}

	// Reset should free valid vectors and not panic on nil
	hb.Reset(proc, true)
	require.Nil(t, hb.vecs)
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
