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
	"context"
	"errors"
	"math"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/hashtable"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/runtimefilter"
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

func TestBuildHashmapOptionalAuxClosedBudgetRemainsFatal(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	budget := process.MustNewHashBuildBudget(1<<20, 1<<20)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	generation.Close()

	hb := HashmapBuilder{InputBatchRowCount: 1}
	hb.setBudget(generation)
	err = hb.BuildHashmap(false, false, true, proc)
	require.Error(t, err)
	var budgetErr *process.HashBuildBudgetError
	require.ErrorAs(t, err, &budgetErr)
	require.Equal(t, process.HashBuildBudgetErrorClosed, budgetErr.Kind)
	fallback, _ := hb.runtimeFilterFallbackState()
	require.False(t, fallback)
	require.Nil(t, hb.UniqueJoinKeys)
	require.Zero(t, generation.Used())
	hb.Free(proc)
}

func TestBuildHashmapMandatoryAuxRetryFailureDoesNotRecordFallback(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	budget := process.MustNewHashBuildBudget(1, 1)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	defer generation.Close()

	hb := HashmapBuilder{InputBatchRowCount: 1}
	hb.setBudget(generation)
	err = hb.BuildHashmap(false, false, true, proc)
	require.Error(t, err)
	var budgetErr *process.HashBuildBudgetError
	require.ErrorAs(t, err, &budgetErr)
	require.Equal(t, process.HashBuildBudgetErrorAdmission, budgetErr.Kind)
	fallback, _ := hb.runtimeFilterFallbackState()
	require.False(t, fallback,
		"fatal mandatory retry must not be counted as an optional fallback")
	require.Equal(t, uint64(2), generation.RejectCount())
	require.Zero(t, generation.Used())
	hb.Free(proc)
}

func TestPrepareCanonicalRuntimeFilterCollectionClosedBudgetRemainsFatal(
	t *testing.T,
) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	input := testutil.NewBatch(
		[]types.Type{types.T_int32.ToType()}, true, 16, proc.Mp())
	defer input.Clean(proc.Mp())
	budget := process.MustNewHashBuildBudget(64<<20, 64<<20)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)

	hb := HashmapBuilder{
		Batches:            colexec.Batches{Buf: []*batch.Batch{input}},
		InputBatchRowCount: input.RowCount(),
	}
	hb.setBudget(generation)
	require.NoError(t, hb.reserveBuildAux(false))
	generation.Close()

	collect, err := hb.prepareCanonicalRuntimeFilterCollection(true)
	require.Error(t, err)
	require.False(t, collect)
	var budgetErr *process.HashBuildBudgetError
	require.ErrorAs(t, err, &budgetErr)
	require.Equal(t, process.HashBuildBudgetErrorClosed, budgetErr.Kind)
	fallback, _ := hb.runtimeFilterFallbackState()
	require.False(t, fallback)
	hb.Batches.Buf = nil
	hb.releaseReservations()
	require.Zero(t, generation.Used())
}

func TestOptionalRuntimeFilterCollectionCleanupFailureRemainsFatal(
	t *testing.T,
) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	key := testutil.MakeInt32Vector([]int32{1}, nil, proc.Mp())
	budget := process.MustNewHashBuildBudget(64<<20, 64<<20)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	defer generation.Close()

	hb := HashmapBuilder{
		InputBatchRowCount: 1,
		UniqueJoinKeys:     []*vector.Vector{key},
	}
	hb.setBudget(generation)
	require.NoError(t, hb.reserveBuildAux(true))
	require.True(t, hb.auxReservation.Release())

	err = hb.fallbackOptionalRuntimeFilterCollection(
		proc,
		runtimefilter.MarkOptionalAllocationError(
			errors.New("mpool allocation failed")),
	)
	require.ErrorIs(t, err, process.ErrHashBuildReservationInactive)
	fallback, _ := hb.runtimeFilterFallbackState()
	require.False(t, fallback)
	require.Nil(t, hb.UniqueJoinKeys)
	require.Zero(t, generation.Used())
	hb.releaseReservations()
}

func TestBuildHashmapUniqueUnionAllocationFailureFallsBack(t *testing.T) {
	for _, test := range []struct {
		name     string
		hashOnPK bool
	}{
		{name: "union"},
		{name: "union-batch", hashOnPK: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			testBuildHashmapUniqueUnionAllocationFailureFallsBack(
				t, test.hashOnPK)
		})
	}
}

func testBuildHashmapUniqueUnionAllocationFailureFallsBack(
	t *testing.T,
	hashOnPK bool,
) {
	mp, err := mpool.NewMPool(t.Name(), 8<<20, mpool.NoFixed)
	require.NoError(t, err)
	proc := testutil.NewProcessWithMPool(t, "", mp)

	var hb HashmapBuilder
	require.NoError(t, hb.Prepare(
		[]*plan.Expr{newExpr(0, types.T_int32.ToType())},
		-1, -1, nil, proc))
	input := testutil.NewBatch(
		[]types.Type{types.T_int32.ToType()}, true, 16, mp)
	hb.Batches.Buf = []*batch.Batch{input}
	hb.InputBatchRowCount = input.RowCount()

	budget := process.MustNewHashBuildBudget(64<<20, 64<<20)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	hb.setBudget(generation)

	var filler []byte
	defer func() {
		hb.Free(proc)
		require.Zero(t, generation.Used())
		if filler != nil {
			mp.Free(filler)
		}
		generation.Close()
		proc.Free()
		require.Zero(t, mp.CurrNB())
	}()

	// Calibrate the deterministic mandatory map footprint, then leave exactly
	// that much headroom. The second build can recreate its required map, while
	// the first optional-key Union allocation must fail at the mpool boundary.
	retainedBytes := mp.CurrNB()
	require.NoError(t, hb.BuildHashmap(hashOnPK, false, false, proc))
	mapBytes := mp.CurrNB() - retainedBytes
	require.Greater(t, mapBytes, int64(0))
	hb.FreeHashMapOnly(proc)
	require.Equal(t, retainedBytes, mp.CurrNB())

	fillerBytes := mp.Cap() - mp.CurrNB() - mapBytes
	require.Greater(t, fillerBytes, int64(0))
	filler, err = mp.Alloc(int(fillerBytes), true)
	require.NoError(t, err)

	require.NoError(t, hb.BuildHashmap(hashOnPK, false, true, proc))
	fallback, rebuildSafe := hb.runtimeFilterFallbackState()
	require.True(t, fallback)
	require.True(t, rebuildSafe)
	require.Nil(t, hb.UniqueJoinKeys)
	require.Greater(t, hb.GetGroupCount(), uint64(0))
	require.Zero(t, generation.RejectCount(),
		"mpool failure must not be misclassified as budget admission")
}

func TestBuildHashMapBudgetRejectsResizeAndReleasesOnReset(t *testing.T) {
	const budgetCap = uint64(1 << 20)
	budget, err := process.NewHashBuildBudget(budgetCap, budgetCap)
	require.NoError(t, err)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)

	var hb HashmapBuilder
	hb.setBudget(generation)
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_int32.ToType())}, -1, -1, nil, proc))

	input := testutil.NewBatch([]types.Type{types.T_int32.ToType()}, true, 10_000, proc.Mp())
	require.NoError(t, hb.copyBuildBatch(input, proc))
	hb.InputBatchRowCount = input.RowCount()
	input.Clean(proc.Mp())

	err = hb.BuildHashmap(false, false, false, proc)
	require.Error(t, err)
	require.True(t, errors.Is(err, process.ErrHashBuildBudgetAdmission))
	require.Greater(t, generation.Used(), uint64(0))

	hb.Reset(proc, true)
	require.Zero(t, generation.Used())
}

func TestBuildHashMapCancellationReleasesRetainedBudgetOnReset(t *testing.T) {
	const budgetCap = uint64(16 << 20)
	budget, err := process.NewHashBuildBudget(budgetCap, budgetCap)
	require.NoError(t, err)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)

	var hb HashmapBuilder
	hb.setBudget(generation)
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	ctx, cancel := context.WithCancelCause(proc.Ctx)
	process.ReplacePipelineCtx(proc, ctx, cancel)
	require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_int32.ToType())}, -1, -1, nil, proc))

	input := testutil.NewBatch([]types.Type{types.T_int32.ToType()}, true, 10_000, proc.Mp())
	require.NoError(t, hb.copyBuildBatch(input, proc))
	hb.InputBatchRowCount = input.RowCount()
	input.Clean(proc.Mp())
	require.Positive(t, generation.Used(), "retained build input must own budget before cancellation")

	proc.Cancel(context.Canceled)
	err = hb.BuildHashmap(false, false, false, proc)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, hb.IntHashMap)
	require.Nil(t, hb.StrHashMap)

	hb.Reset(proc, true)
	hb.Free(proc)
	require.Zero(t, generation.Used())
	require.Zero(t, generation.SpillDiskUsed())
	require.Zero(t, generation.SpillFDUsed())
	require.Zero(t, proc.Mp().CurrNB())
}

func TestPublishedJoinMapResizeKeepsReservationWithConsumer(t *testing.T) {
	const budgetCap = uint64(16 << 20)
	budget, err := process.NewHashBuildBudget(budgetCap, budgetCap)
	require.NoError(t, err)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)

	var hb HashmapBuilder
	hb.setBudget(generation)
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_int32.ToType())}, -1, -1, nil, proc))
	input := testutil.NewBatch([]types.Type{types.T_int32.ToType()}, true, 100, proc.Mp())
	require.NoError(t, hb.copyBuildBatch(input, proc))
	hb.InputBatchRowCount = input.RowCount()
	input.Clean(proc.Mp())
	require.NoError(t, hb.BuildHashmap(false, false, false, proc))

	jm := hb.GetJoinMap(proc.Mp())
	require.NotNil(t, jm)
	jm.IncRef(1)
	usedBeforeResize := generation.Used()
	secondGeneration, err := budget.OpenGeneration(2)
	require.NoError(t, err)
	hb.setBudget(secondGeneration)
	require.NoError(t, jm.PreAlloc(100_000))
	require.Greater(t, generation.Used(), usedBeforeResize)
	require.Zero(t, secondGeneration.Used(), "published map must retain its original generation")
	jm.Free()
	require.Zero(t, generation.Used())

	hb.Reset(proc, false)
}

func TestHashMapReservationOwnerRetainsSegmentedGrowthTokens(t *testing.T) {
	budget, err := process.NewHashBuildBudget(1<<20, 1<<20)
	require.NoError(t, err)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)

	initial, err := generation.Reserve(100)
	require.NoError(t, err)
	owner := &hashMapReservationOwner{tokens: []*process.HashBuildReservation{initial}}

	incremental, err := generation.Reserve(50)
	require.NoError(t, err)
	(&hashMapResizeReservation{owner: owner, token: incremental}).Commit(
		hashtable.ResizePlan{ReuseCurrentBlocks: true},
	)
	require.Equal(t, uint64(150), generation.Used())

	replacement, err := generation.Reserve(200)
	require.NoError(t, err)
	(&hashMapResizeReservation{owner: owner, token: replacement}).Commit(hashtable.ResizePlan{})
	require.Equal(t, uint64(200), generation.Used())

	owner.release()
	require.Zero(t, generation.Used())
}

func TestBudgetedEmptyJoinMapRejectsUnadmittedAllocationAndResize(t *testing.T) {
	for _, tc := range []struct {
		name         string
		keyWidth     int
		initialBytes uint64
	}{
		{name: "int", keyWidth: 4, initialBytes: hashtable.Int64HashMapInitialAllocationBytes()},
		{name: "string", keyWidth: 128, initialBytes: hashtable.StringHashMapInitialAllocationBytes()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()

			tooSmall := process.MustNewHashBuildBudget(tc.initialBytes-1, tc.initialBytes-1)
			tooSmallGeneration, err := tooSmall.OpenGeneration(1)
			require.NoError(t, err)
			jm, err := NewBudgetedEmptyJoinMap(tc.keyWidth, tooSmallGeneration, mp)
			require.Nil(t, jm)
			require.ErrorIs(t, err, process.ErrHashBuildBudgetAdmission)
			require.Zero(t, tooSmallGeneration.Used())
			require.Zero(t, mp.CurrNB())

			exact := process.MustNewHashBuildBudget(tc.initialBytes, tc.initialBytes)
			generation, err := exact.OpenGeneration(2)
			require.NoError(t, err)
			jm, err = NewBudgetedEmptyJoinMap(tc.keyWidth, generation, mp)
			require.NoError(t, err)
			require.Equal(t, tc.initialBytes, generation.Used())
			require.Equal(t, int64(tc.initialBytes), mp.CurrNB())

			err = jm.PreAlloc(10_000)
			require.ErrorIs(t, err, process.ErrHashBuildBudgetAdmission)
			require.Equal(t, tc.initialBytes, generation.Used(),
				"rejected growth must roll back its temporary reservation")

			jm.Free()
			require.Zero(t, generation.Used())
			require.Zero(t, generation.SpillDiskUsed())
			require.Zero(t, generation.SpillFDUsed())
			require.Zero(t, mp.CurrNB())
		})
	}
}

func TestCopyBuildBatchBudgetsSmallIngressAfterFullBatches(t *testing.T) {
	const budgetCap = uint64(32 << 20)
	budget, err := process.NewHashBuildBudget(budgetCap, budgetCap)
	require.NoError(t, err)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)

	var hb HashmapBuilder
	hb.setBudget(generation)
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	for _, rows := range []int{colexec.DefaultBatchSize, colexec.DefaultBatchSize} {
		input := testutil.NewBatch([]types.Type{types.T_int32.ToType()}, true, rows, proc.Mp())
		require.NoError(t, hb.copyBuildBatch(input, proc))
		input.Clean(proc.Mp())
	}
	input := testutil.NewBatch([]types.Type{types.T_int32.ToType()}, true, 100, proc.Mp())
	projected, err := hb.projectedBatchCopyBytes(input)
	require.NoError(t, err)
	destination, err := projectedNewDestinationBytes(input, 0, input.RowCount())
	require.NoError(t, err)
	metadata, ok := retainedMetadataAllowance(input)
	require.True(t, ok)
	require.Equal(t, destination+metadata+uint64(64<<10), projected,
		"a small ingress must not be projected as a full 8192-row allocation")
	require.NoError(t, hb.copyBuildBatch(input, proc))
	input.Clean(proc.Mp())

	require.Len(t, hb.Batches.Buf, 3)
	require.Equal(t, 100, hb.Batches.Buf[2].RowCount())
	hb.FreeHashMapAndBatches(proc)
	require.Zero(t, generation.Used())
}

func TestCopyBuildBatchBudgetsPartialTailGrowth(t *testing.T) {
	const budgetCap = uint64(32 << 20)
	budget, err := process.NewHashBuildBudget(budgetCap, budgetCap)
	require.NoError(t, err)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)

	var hb HashmapBuilder
	hb.setBudget(generation)
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	for range 1000 {
		// Deep spill partitions contain many tiny records. They coalesce into
		// one physical batch whose vector capacity grows geometrically.
		input := testutil.NewBatch([]types.Type{types.T_int32.ToType()}, true, 7, proc.Mp())
		require.NoError(t, hb.copyBuildBatch(input, proc))
		input.Clean(proc.Mp())
	}
	require.Len(t, hb.Batches.Buf, 1)
	require.Equal(t, 7000, hb.Batches.Buf[0].RowCount())
	hb.FreeHashMapAndBatches(proc)
	require.Zero(t, generation.Used())
}

func TestCopyBuildBatchBudgetsWideVarcharPartialTailReplacement(t *testing.T) {
	const budgetCap = uint64(128 << 20)
	budget, err := process.NewHashBuildBudget(budgetCap, budgetCap)
	require.NoError(t, err)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)

	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	values := make([]string, colexec.DefaultBatchSize/2)
	for i := range values {
		values[i] = strings.Repeat("x", 1024)
	}
	input := batch.NewWithSize(1)
	input.Vecs[0] = testutil.MakeVarcharVector(values, nil, proc.Mp())
	input.SetRowCount(len(values))
	defer input.Clean(proc.Mp())

	var hb HashmapBuilder
	hb.setBudget(generation)
	defer hb.FreeHashMapAndBatches(proc)
	require.NoError(t, hb.copyBuildBatch(input, proc))
	require.NoError(t, hb.copyBuildBatch(input, proc))
	require.Len(t, hb.Batches.Buf, 1)
	require.Equal(t, colexec.DefaultBatchSize, hb.Batches.Buf[0].RowCount())
}

func TestProjectedPartialTailReplacementMatchesUnionBatch(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	tail := batch.NewWithSize(2)
	tail.Vecs[0] = testutil.MakeInt32Vector([]int32{1}, nil, proc.Mp())
	tail.Vecs[1] = testutil.MakeVarcharVector([]string{strings.Repeat("a", 1024)}, nil, proc.Mp())
	tail.SetRowCount(1)
	defer tail.Clean(proc.Mp())

	src := batch.NewWithSize(2)
	src.Vecs[0] = testutil.MakeInt32Vector([]int32{2, 3}, nil, proc.Mp())
	constVec, err := vector.NewConstBytes(
		types.T_varchar.ToType(),
		[]byte(strings.Repeat("b", 1024)),
		2,
		proc.Mp(),
	)
	require.NoError(t, err)
	src.Vecs[1] = constVec
	src.SetRowCount(2)
	defer src.Clean(proc.Mp())

	peak, retained, err := projectedPartialTailReplacementBytes(tail, src, src.RowCount())
	require.NoError(t, err)
	require.GreaterOrEqual(t, peak, retained)
	before := tail.Allocated()
	for i := range tail.Vecs {
		require.NoError(t, tail.Vecs[i].UnionBatch(src.Vecs[i], 0, src.RowCount(), nil, proc.Mp()))
	}
	tail.AddRowCount(src.RowCount())
	require.Equal(t, uint64(tail.Allocated()-before), retained)

	inline := batch.NewWithSize(1)
	inline.Vecs[0] = testutil.MakeVarcharVector([]string{"small"}, nil, proc.Mp())
	inline.SetRowCount(1)
	defer inline.Clean(proc.Mp())
	preallocated, err := proc.NewBatchFromSrc(inline, colexec.DefaultBatchSize)
	require.NoError(t, err)
	defer preallocated.Clean(proc.Mp())
	require.NoError(t, preallocated.Vecs[0].UnionBatch(inline.Vecs[0], 0, 1, nil, proc.Mp()))
	preallocated.AddRowCount(1)
	peak, retained, err = projectedPartialTailReplacementBytes(preallocated, inline, 1)
	require.NoError(t, err)
	require.Zero(t, peak)
	require.Zero(t, retained)
}

func TestCopyBuildBatchBudgetsPartialTailWithRemainder(t *testing.T) {
	const budgetCap = uint64(16 << 20)
	budget := process.MustNewHashBuildBudget(budgetCap, budgetCap)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	var hb HashmapBuilder
	hb.setBudget(generation)
	defer hb.FreeHashMapAndBatches(proc)
	for _, rows := range []int{
		colexec.DefaultBatchSize,
		colexec.DefaultBatchSize,
		colexec.DefaultBatchSize - 1,
		2,
	} {
		input := testutil.NewBatch([]types.Type{types.T_int32.ToType()}, true, rows, proc.Mp())
		require.NoError(t, hb.copyBuildBatch(input, proc))
		input.Clean(proc.Mp())
	}
	require.Len(t, hb.Batches.Buf, 4)
	require.Equal(t, 1, hb.Batches.Buf[3].RowCount())
}

func TestProjectedPartialTailReplacementRejectsInvalidInputs(t *testing.T) {
	_, _, err := projectedPartialTailReplacementBytes(nil, nil, -1)
	require.ErrorIs(t, err, process.ErrHashBuildBudgetInvalid)
	_, err = (&HashmapBuilder{}).projectedBatchCopyBytes(nil)
	require.ErrorIs(t, err, process.ErrHashBuildBudgetInvalid)

	tail := batch.NewOffHeapWithSize(1)
	src := batch.NewOffHeapWithSize(1)
	_, _, err = projectedPartialTailReplacementBytes(tail, src, 1)
	require.ErrorIs(t, err, process.ErrHashBuildBudgetInvalid)

	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	src.Vecs[0] = testutil.MakeVarcharVector([]string{strings.Repeat("x", 32)}, nil, proc.Mp())
	src.SetRowCount(1)
	defer src.Clean(proc.Mp())
	tail.Vecs[0] = vector.NewOffHeapVecWithType(types.T_varchar.ToType())
	defer tail.Clean(proc.Mp())
	_, _, err = projectedPartialTailReplacementBytes(tail, src, 2)
	require.ErrorIs(t, err, process.ErrHashBuildBudgetInvalid)

	invalidTail := batch.NewOffHeapWithSize(1)
	invalidTail.SetRowCount(-1)
	hb := HashmapBuilder{}
	hb.Batches.Buf = []*batch.Batch{invalidTail}
	_, err = hb.projectedBatchCopyBytes(src)
	require.ErrorIs(t, err, process.ErrHashBuildBudgetInvalid)

	mismatchedTail := batch.NewOffHeapWithSize(0)
	hb.Batches.Buf = []*batch.Batch{mismatchedTail}
	_, err = hb.projectedBatchCopyBytes(src)
	require.ErrorIs(t, err, process.ErrHashBuildBudgetInvalid)
}

func TestCopyBuildBatchUsesProjectedDestinationCapacity(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	values := make([]string, 4096)
	for i := range values {
		values[i] = strings.Repeat("x", 1024)
	}
	input := batch.NewWithSize(1)
	input.Vecs[0] = testutil.MakeVarcharVector(values, nil, proc.Mp())
	input.SetRowCount(len(values))
	defer input.Clean(proc.Mp())

	var hb HashmapBuilder
	projected, err := hb.projectedBatchCopyBytes(input)
	require.NoError(t, err)
	destination, err := projectedNewDestinationBytes(input, 0, input.RowCount())
	require.NoError(t, err)
	metadata, ok := retainedMetadataAllowance(input)
	require.True(t, ok)
	const wantSlack = uint64(64 << 10)
	require.Equal(t, destination+metadata+wantSlack, projected)

	budget := process.MustNewHashBuildBudget(projected, projected)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	hb.setBudget(generation)
	require.NoError(t, hb.copyBuildBatch(input, proc))
	require.Equal(t, projected, generation.Peak())
	hb.FreeHashMapAndBatches(proc)
	require.Zero(t, generation.Used())
}

func TestCopyBuildBatchSplitsLargeIngressWithinProjection(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	const rows = 50_000
	input := testutil.NewBatch([]types.Type{types.T_uuid.ToType()}, true, rows, proc.Mp())
	part, err := vector.NewConstFixed[int32](types.T_int32.ToType(), 1, rows, proc.Mp())
	require.NoError(t, err)
	input.Vecs = append(input.Vecs, part)
	defer input.Clean(proc.Mp())

	budget := process.MustNewHashBuildBudget(1<<30, 1<<30)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)

	var hb HashmapBuilder
	hb.setBudget(generation)
	require.NoError(t, hb.copyBuildBatch(input, proc))
	require.NoError(t, hb.copyBuildBatch(input, proc))
	require.Equal(t, 2*rows, hb.Batches.RowCount())
	require.Len(t, hb.Batches.Buf, 13)
	hb.FreeHashMapAndBatches(proc)
	require.Zero(t, generation.Used())
}

func TestCopyBuildBatchSplitsLargeConstVarcharIngressWithinProjection(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	const rows = 50_000
	input := batch.NewWithSize(1)
	value := make([]byte, 1<<20)
	vec, err := vector.NewConstBytes(types.T_varchar.ToType(), value, rows, proc.Mp())
	require.NoError(t, err)
	input.Vecs[0] = vec
	input.SetRowCount(rows)
	defer input.Clean(proc.Mp())

	budget := process.MustNewHashBuildBudget(1<<30, 1<<30)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)

	var hb HashmapBuilder
	hb.setBudget(generation)
	require.NoError(t, hb.copyBuildBatch(input, proc))
	require.NoError(t, hb.copyBuildBatch(input, proc))
	require.Equal(t, 2*rows, hb.Batches.RowCount())
	require.Len(t, hb.Batches.Buf, 13)
	hb.FreeHashMapAndBatches(proc)
	require.Zero(t, generation.Used())
}

func TestCopyBuildBatchManyExactSegmentsAvoidsFalseAdmission(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	const rows = 64 * colexec.DefaultBatchSize
	input := testutil.NewBatch([]types.Type{types.T_int32.ToType()}, true, rows, proc.Mp())
	defer input.Clean(proc.Mp())

	destination, err := projectedNewDestinationBytes(input, 0, input.RowCount())
	require.NoError(t, err)
	metadata, ok := retainedMetadataAllowance(input)
	require.True(t, ok)
	budgetCap := 2*(destination+metadata) + uint64(512<<10)
	budget := process.MustNewHashBuildBudget(budgetCap, budgetCap)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)

	var hb HashmapBuilder
	hb.setBudget(generation)
	require.NoError(t, hb.copyBuildBatch(input, proc))
	require.NoError(t, hb.copyBuildBatch(input, proc))
	require.LessOrEqual(t, generation.Used(), budgetCap)
	hb.FreeHashMapAndBatches(proc)
	require.Zero(t, generation.Used())
}

func TestCopyBuildBatchSharedVarlenaRejectsBeforeAllocation(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	const rows = 50_000
	value := make([]byte, 1<<10)
	constVec, err := vector.NewConstBytes(types.T_varchar.ToType(), value, rows, proc.Mp())
	require.NoError(t, err)
	defer constVec.Free(proc.Mp())

	flat := vector.NewOffHeapVecWithType(types.T_varchar.ToType())
	require.NoError(t, flat.UnionBatch(constVec, 0, rows, nil, proc.Mp()))
	require.False(t, flat.IsConst())
	require.Equal(t, len(value), len(flat.GetArea()))
	input := batch.NewWithSize(1)
	input.Vecs[0] = flat
	input.SetRowCount(rows)
	defer input.Clean(proc.Mp())

	const budgetCap = uint64(10 << 20)
	budget := process.MustNewHashBuildBudget(budgetCap, budgetCap)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)

	var hb HashmapBuilder
	hb.setBudget(generation)
	projected, err := hb.projectedBatchCopyBytes(input)
	require.NoError(t, err)
	require.GreaterOrEqual(t, projected, uint64(rows*len(value)))
	err = hb.copyBuildBatch(input, proc)
	require.ErrorIs(t, err, process.ErrHashBuildBudgetAdmission)
	require.Empty(t, hb.Batches.Buf)
	require.Zero(t, generation.Used())
}

func TestCopyBuildBatchWholeSharedVarlenaAvoidsFalseAdmission(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	const rows = colexec.DefaultBatchSize
	value := make([]byte, 1<<10)
	constVec, err := vector.NewConstBytes(types.T_varchar.ToType(), value, rows, proc.Mp())
	require.NoError(t, err)
	defer constVec.Free(proc.Mp())

	flat := vector.NewOffHeapVecWithType(types.T_varchar.ToType())
	require.NoError(t, flat.UnionBatch(constVec, 0, rows, nil, proc.Mp()))
	require.False(t, flat.IsConst())
	input := batch.NewWithSize(1)
	input.Vecs[0] = flat
	input.SetRowCount(rows)
	defer input.Clean(proc.Mp())

	const budgetCap = uint64(2 << 20)
	budget := process.MustNewHashBuildBudget(budgetCap, budgetCap)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)

	var hb HashmapBuilder
	hb.setBudget(generation)
	require.NoError(t, hb.copyBuildBatch(input, proc))
	require.LessOrEqual(t, generation.Used(), budgetCap)
	hb.FreeHashMapAndBatches(proc)
	require.Zero(t, generation.Used())
}

func TestReserveBuildAuxChargesOneRetainedCopy(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	input := testutil.NewBatch([]types.Type{types.T_int32.ToType()}, true, colexec.DefaultBatchSize, proc.Mp())
	defer input.Clean(proc.Mp())

	var hb HashmapBuilder
	hb.Batches.Buf = []*batch.Batch{input}
	hb.InputBatchRowCount = input.RowCount()
	retained := batchesAllocated(hb.Batches.Buf)
	const iteratorScratch = uint64(640 << 10)
	want := retained + (retained+3)/4 + uint64(input.RowCount())*64 + iteratorScratch

	budget := process.MustNewHashBuildBudget(want, want)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	hb.setBudget(generation)
	require.NoError(t, hb.reserveBuildAux(true))
	require.Equal(t, want, generation.Used())
	hb.releaseReservations()
	require.Zero(t, generation.Used())
	// The batch belongs to the test rather than batchReservations.
	hb.Batches.Buf = nil
}

func TestReserveUniqueAppendOverlapChargesReplacedCapacity(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	values := make([]string, 4_096)
	for i := range values {
		values[i] = strings.Repeat("x", 1_024)
	}
	dst := testutil.MakeVarcharVector(values, nil, proc.Mp())
	defer dst.Free(proc.Mp())
	extraArea := cap(dst.GetArea()) - len(dst.GetArea()) + 1
	src := testutil.MakeVarcharVector([]string{strings.Repeat("y", extraArea)}, nil, proc.Mp())
	defer src.Free(proc.Mp())

	want := uint64(cap(dst.GetData()) + cap(dst.GetArea()))
	const budgetCap = uint64(64 << 20)
	budget := process.MustNewHashBuildBudget(budgetCap, budgetCap)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	hb := HashmapBuilder{
		budget:         generation,
		UniqueJoinKeys: []*vector.Vector{dst},
	}
	require.NoError(t, hb.reserveBuildAux(true))
	areaBytes, err := uniqueAppendAreaBytes(src, 0, 1, nil)
	require.NoError(t, err)
	token, err := hb.reserveUniqueAppendOverlap(dst, 1, areaBytes)
	require.NoError(t, err)
	require.NotNil(t, token)
	require.Equal(t, want, token.Size())
	token.Release()
	require.Equal(t, hb.auxReservation.Size(), generation.Used())
	hb.releaseReservations()
	require.Zero(t, generation.Used())

	largeValue := strings.Repeat("z", 100)
	selected := testutil.MakeVarcharVector([]string{"a", largeValue}, nil, proc.Mp())
	defer selected.Free(proc.Mp())
	selectedArea, err := uniqueAppendAreaBytes(selected, 0, 1, []int64{0})
	require.NoError(t, err)
	require.Zero(t, selectedArea)
	selectedArea, err = uniqueAppendAreaBytes(selected, 0, 1, []int64{1})
	require.NoError(t, err)
	require.Equal(t, len(largeValue), selectedArea)
	_, err = uniqueAppendAreaBytes(selected, -1, 1, nil)
	require.ErrorIs(t, err, process.ErrHashBuildBudgetInvalid)
	_, err = uniqueAppendAreaBytes(selected, 0, 1, []int64{int64(selected.Length())})
	require.ErrorIs(t, err, process.ErrHashBuildBudgetInvalid)
	_, err = uniqueAppendAreaBytes(selected, 0, 2, []int64{0})
	require.ErrorIs(t, err, process.ErrHashBuildBudgetInvalid)

	fixed := testutil.MakeInt32Vector([]int32{1}, nil, proc.Mp())
	defer fixed.Free(proc.Mp())
	selectedArea, err = uniqueAppendAreaBytes(fixed, math.MaxInt, math.MaxInt, nil)
	require.NoError(t, err)
	require.Zero(t, selectedArea)

	constValue, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte(largeValue), 2, proc.Mp())
	require.NoError(t, err)
	defer constValue.Free(proc.Mp())
	selectedArea, err = uniqueAppendAreaBytes(constValue, 0, 2, nil)
	require.NoError(t, err)
	require.Equal(t, 2*len(largeValue), selectedArea)

	noBudget := HashmapBuilder{}
	token, err = noBudget.reserveUniqueAppendOverlap(dst, 1, 1)
	require.NoError(t, err)
	require.Nil(t, token)
	token, err = hb.reserveUniqueAppendOverlap(nil, 1, 1)
	require.ErrorIs(t, err, process.ErrHashBuildBudgetInvalid)
	token, err = hb.reserveUniqueAppendOverlap(dst, -1, 1)
	require.ErrorIs(t, err, process.ErrHashBuildBudgetInvalid)
	token, err = hb.reserveUniqueAppendOverlap(dst, 1, -1)
	require.ErrorIs(t, err, process.ErrHashBuildBudgetInvalid)
}

func TestUniqueAppendBudgetIncludesDeadAreaCopiedByUnionBatch(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	src := testutil.MakeVarcharVector(
		[]string{"inline", strings.Repeat("d", 128<<10)}, nil, proc.Mp())
	defer src.Free(proc.Mp())
	// SetLength leaves the second value's area allocation behind. The sole live
	// row is inline, but UnionBatch's whole-vector fast path copies all of area.
	src.SetLength(1)
	liveArea, err := uniqueAppendAreaBytes(src, 0, 1, nil)
	require.NoError(t, err)
	require.Zero(t, liveArea)
	unionArea, err := unionBatchAreaBytes(src, 0, 1)
	require.NoError(t, err)
	require.Equal(t, len(src.GetArea()), unionArea)
	require.Greater(t, unionArea, 0)

	dst := vector.NewOffHeapVecWithType(types.T_varchar.ToType())
	defer dst.Free(proc.Mp())
	const mandatoryAux = uint64(640 << 10)
	budget := process.MustNewHashBuildBudget(mandatoryAux, mandatoryAux)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	hb := HashmapBuilder{
		budget:         generation,
		UniqueJoinKeys: []*vector.Vector{dst},
	}
	require.NoError(t, hb.reserveBuildAux(true))

	_, err = hb.reserveUniqueAppendOverlap(dst, 1, unionArea)
	require.ErrorIs(t, err, process.ErrHashBuildBudgetAdmission)
	require.Zero(t, dst.Length())
	require.Zero(t, dst.Allocated(),
		"admission must fail before UnionBatch allocates copied dead area")

	hb.releaseReservations()
	require.Zero(t, generation.Used())
	generation.Close()
}

func TestCleanCopiedBatchReleasesCoalescedIngressReservations(t *testing.T) {
	const budgetCap = uint64(4 << 20)
	budget, err := process.NewHashBuildBudget(budgetCap, budgetCap)
	require.NoError(t, err)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)

	var hb HashmapBuilder
	hb.setBudget(generation)
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	var emergencyNeed uint64
	for range 2 {
		input := testutil.NewBatch([]types.Type{types.T_int32.ToType()}, true, colexec.DefaultBatchSize/2, proc.Mp())
		if emergencyNeed == 0 {
			emergencyNeed, err = spillRetainedBudgetBytes(input)
			require.NoError(t, err)
		}
		require.NoError(t, hb.copyBuildBatch(input, proc))
		input.Clean(proc.Mp())
	}
	require.Len(t, hb.Batches.Buf, 1, "small ingress batches should coalesce")
	require.Len(t, hb.batchReservations, 2, "reservations follow ingress, not physical batches")
	require.Greater(t, generation.Used(), uint64(0))
	physicalNeed, err := spillScratchBudgetBytes(hb.Batches.Buf[0], true)
	require.NoError(t, err)
	require.GreaterOrEqual(t, emergencyNeed, physicalNeed)

	require.NoError(t, hb.CleanCopiedBatchAt(0, proc))
	require.Empty(t, hb.Batches.Buf)
	require.Empty(t, hb.batchReservations)
	require.Zero(t, generation.Used())
}

func TestDrainCopiedBatchesReleasesBeforeSubsequentAdmission(t *testing.T) {
	const budgetCap = uint64(4 << 20)
	budget, err := process.NewHashBuildBudget(budgetCap, budgetCap)
	require.NoError(t, err)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)

	var hb HashmapBuilder
	hb.setBudget(generation)
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	for range 2 {
		input := testutil.NewBatch(
			[]types.Type{types.T_int32.ToType()},
			true,
			colexec.DefaultBatchSize/2,
			proc.Mp(),
		)
		require.NoError(t, hb.copyBuildBatch(input, proc))
		input.Clean(proc.Mp())
	}
	require.Len(t, hb.Batches.Buf, 1, "small ingress batches should coalesce")
	require.Len(t, hb.batchReservations, 2, "reservations follow ingress, not physical batches")

	visits := 0
	require.NoError(t, hb.DrainCopiedBatches(proc, func(bat *batch.Batch) error {
		visits++
		require.NotNil(t, bat)
		require.Positive(t, generation.Used(), "physical batch must remain charged while visited")
		return nil
	}))
	require.Equal(t, 1, visits)
	require.Empty(t, hb.Batches.Buf)
	require.Empty(t, hb.batchReservations)
	require.Zero(t, generation.Used(), "the final physical batch must release every coalesced ingress charge")

	// Model the expression/scatter/read reservation that follows a re-spill
	// drain. It can consume the complete cap only after stale batch ownership
	// has been removed from the ledger.
	next, err := generation.Reserve(budgetCap)
	require.NoError(t, err)
	require.True(t, next.Release())
	require.Zero(t, generation.Used())
}

func TestDrainCopiedBatchesVisitFailureRetainsOwnership(t *testing.T) {
	const budgetCap = uint64(4 << 20)
	budget, err := process.NewHashBuildBudget(budgetCap, budgetCap)
	require.NoError(t, err)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)

	var hb HashmapBuilder
	hb.setBudget(generation)
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	for _, rows := range []int{
		colexec.DefaultBatchSize / 2,
		colexec.DefaultBatchSize / 2,
		1024,
	} {
		input := testutil.NewBatch([]types.Type{types.T_int32.ToType()}, true, rows, proc.Mp())
		require.NoError(t, hb.copyBuildBatch(input, proc))
		input.Clean(proc.Mp())
	}
	require.Len(t, hb.Batches.Buf, 2)
	require.Len(t, hb.batchReservations, 3)

	wantErr := errors.New("visit failed")
	visits := 0
	require.ErrorIs(t, hb.DrainCopiedBatches(proc, func(*batch.Batch) error {
		visits++
		if visits == 1 {
			return nil
		}
		return wantErr
	}), wantErr)
	require.Equal(t, 2, visits)
	require.Len(t, hb.Batches.Buf, 1, "the failed current batch remains owned after prior batches drain")
	require.Len(t, hb.batchReservations, 3,
		"coalesced ingress reservations stay conservative until terminal cleanup")
	require.Positive(t, generation.Used())

	hb.FreeHashMapAndBatches(proc)
	require.Zero(t, generation.Used())
}

func TestSpillExpressionHashKeyUsesBoundedAdmission(t *testing.T) {
	var ctr container
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	budget, err := process.NewHashBuildBudget(1<<20, 1<<20)
	require.NoError(t, err)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	ctr.hashmapBuilder.setBudget(generation)
	expr := makeExpressionLeaseTestExpr(t, proc)
	_, err = ctr.initSpillExprExecs(proc, []*plan.Expr{expr})
	require.NoError(t, err)
	require.NoError(t, ctr.spillExprLease.Run(proc, 8192, func(_ int) error { return nil }))
	require.Positive(t, ctr.spillExprLease.Reserved())
	require.Equal(t, ctr.spillExprLease.Reserved(), generation.Used())
	ctr.freeSpillExprExecs()
	require.Zero(t, generation.Used())
}

func TestExpressionHashKeyReservesDeclaredPeakBeforeEval(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	budget, err := process.NewHashBuildBudget(96<<10, 96<<10)
	require.NoError(t, err)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)

	var hb HashmapBuilder
	hb.setBudget(generation)
	require.NoError(t, hb.Prepare([]*plan.Expr{{
		Typ:  plan.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen},
		Expr: &plan.Expr_F{F: &plan.Function{}},
	}}, -1, -1, nil, proc))
	input := testutil.NewBatch([]types.Type{types.T_int32.ToType()}, true, 1, proc.Mp())
	require.NoError(t, hb.copyBuildBatch(input, proc))
	hb.InputBatchRowCount = input.RowCount()
	input.Clean(proc.Mp())
	err = hb.BuildHashmap(false, true, false, proc)
	require.ErrorIs(t, err, process.ErrHashBuildBudgetAdmission)
	hb.Free(proc)
	require.Zero(t, generation.Used())
}

func TestExpressionHashKeyAcceptsCastTargetType(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	expr := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_int32)},
		Expr: &plan.Expr_F{F: &plan.Function{Args: []*plan.Expr{
			newExpr(0, types.T_int64.ToType()),
			{
				Typ:  plan.Type{Id: int32(types.T_int32)},
				Expr: &plan.Expr_T{T: &plan.TargetType{}},
			},
		}}},
	}

	peak, err := expressionVectorPeak(proc, expr, 1024, false)
	require.NoError(t, err)
	require.Equal(t, uint64(204800), peak, "charge the target-type and cast result vectors")
}

func TestPreparedParamExpressionPeakUsesConstCardinality(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	params := vector.NewVec(types.T_text.ToType())
	defer params.Free(proc.Mp())
	proc.SetPrepareParams(params)
	require.NoError(t, vector.AppendBytes(params, []byte("prepared"), false, proc.Mp()))
	require.NoError(t, vector.AppendBytes(params, nil, true, proc.Mp()))

	paramExpr := func(pos int32) *plan.Expr {
		return &plan.Expr{
			Typ:  plan.Type{Id: int32(types.T_text), Width: types.MaxVarcharLen},
			Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: pos}},
		}
	}

	peakOne, err := expressionVectorPeak(proc, paramExpr(0), 1, false)
	require.NoError(t, err)
	peakBatch, err := expressionVectorPeak(proc, paramExpr(0), colexec.DefaultBatchSize, false)
	require.NoError(t, err)
	require.Equal(t, peakOne, peakBatch, "const parameter admission must not scale with input rows")
	peakNull, err := expressionVectorPeak(proc, paramExpr(1), colexec.DefaultBatchSize, false)
	require.NoError(t, err)
	require.Equal(t, peakOne, peakNull, "null parameter keeps the declared one-row type bound")
}

func TestPreparedParamExpressionExecutorRemainsConst(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value []byte
		null  bool
	}{
		{name: "non-null", value: []byte("prepared")},
		{name: "null", null: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			defer proc.Free()
			params := vector.NewVec(types.T_text.ToType())
			defer params.Free(proc.Mp())
			require.NoError(t, vector.AppendBytes(params, tc.value, tc.null, proc.Mp()))
			proc.SetPrepareParams(params)

			expr := &plan.Expr{
				Typ:  plan.Type{Id: int32(types.T_text)},
				Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: 0}},
			}
			executor, err := colexec.NewExpressionExecutor(proc, expr)
			require.NoError(t, err)
			defer executor.Free()
			input := testutil.NewBatch([]types.Type{types.T_int32.ToType()}, true, colexec.DefaultBatchSize, proc.Mp())
			defer input.Clean(proc.Mp())

			result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
			require.NoError(t, err)
			require.True(t, result.IsConst())
			require.Equal(t, 1, result.Length())
		})
	}
}

func TestPreparedParamExpressionPeakNestedFunctionCardinality(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	params := vector.NewVec(types.T_text.ToType())
	defer params.Free(proc.Mp())
	require.NoError(t, vector.AppendBytes(params, []byte("prepared"), false, proc.Mp()))
	proc.SetPrepareParams(params)

	param := &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_text), Width: types.MaxVarcharLen},
		Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: 0}},
	}
	cast := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_int64)},
		Expr: &plan.Expr_F{F: &plan.Function{Args: []*plan.Expr{
			param,
			{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_T{T: &plan.TargetType{}}},
		}}},
	}
	modulo := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_int64)},
		Expr: &plan.Expr_F{F: &plan.Function{Args: []*plan.Expr{
			{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: 0}}},
			cast,
		}}},
	}

	paramTotal, paramOutput, err := expressionTreePeak(proc, param, colexec.DefaultBatchSize)
	require.NoError(t, err)
	paramOne, _, err := expressionTreePeak(proc, param, 1)
	require.NoError(t, err)
	require.Equal(t, paramOne, paramTotal)
	_, rootOutput, err := expressionTreePeak(proc, modulo, colexec.DefaultBatchSize)
	require.NoError(t, err)
	rootTypePeak, err := expressionTypePeak(modulo.Typ, colexec.DefaultBatchSize)
	require.NoError(t, err)
	require.Equal(t, rootTypePeak, rootOutput, "function output remains sized for input rows")
	require.Greater(t, paramOutput, uint64(0))
}

func TestPreparedParamExpressionPeakRejectsInvalidPosition(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	params := vector.NewVec(types.T_text.ToType())
	defer params.Free(proc.Mp())
	proc.SetPrepareParams(params)
	require.NoError(t, vector.AppendBytes(params, []byte("prepared"), false, proc.Mp()))

	expr := &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_text)},
		Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: -1}},
	}
	_, err := expressionVectorPeak(proc, expr, colexec.DefaultBatchSize, false)
	require.Error(t, err)
}

func TestPreparedParamExpressionPeakAccountsLargePayload(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	params := vector.NewVec(types.T_text.ToType())
	defer params.Free(proc.Mp())
	payload := make([]byte, types.MaxBlobLen+1)
	require.NoError(t, vector.AppendBytes(params, payload, false, proc.Mp()))
	proc.SetPrepareParams(params)

	expr := &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen},
		Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: 0}},
	}
	peak, err := expressionVectorPeak(proc, expr, colexec.DefaultBatchSize, false)
	require.NoError(t, err)
	header, ok := mpool.GrowCapacity(0, int64(types.VarlenaSize))
	require.True(t, ok)
	area, ok := mpool.GrowCapacity(0, int64(len(payload)))
	require.True(t, ok)
	require.GreaterOrEqual(t, peak, uint64(header)+uint64(area))
	require.Greater(t, peak, uint64(types.MaxBlobLen))
}

func TestGetJoinMapTransfersGroupSels(t *testing.T) {
	var hb HashmapBuilder
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_int32.ToType())}, -1, -1, nil, proc))
	input := makeIntKeyValueBatch(proc, []int32{1, 1}, []int32{10, 20})
	require.NoError(t, hb.Batches.CopyIntoBatches(input, proc))
	hb.InputBatchRowCount = input.RowCount()
	input.Clean(proc.Mp())

	require.NoError(t, hb.BuildHashmap(false, true, false, proc))
	require.Greater(t, hb.Sels.Size(), int64(0))

	jm := hb.GetJoinMap(proc.Mp())
	require.NotNil(t, jm)
	jm.IncRef(1)
	require.Zero(t, hb.Sels.Size(), "builder must relinquish GroupSels ownership")
	require.Equal(t, []int32{0, 1}, jm.GetSels(0))

	// A subsequent empty build cleanup must not release the previous JoinMap's sels.
	hb.Reset(proc, false)
	hb.Reset(proc, false)
	require.Equal(t, []int32{0, 1}, jm.GetSels(0))

	hb.Free(proc)
	jm.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestDedupUpdateBuildGroupsNullKeysSeparately(t *testing.T) {
	var hb HashmapBuilder
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	hb.IsDedup = true
	hb.OnDuplicateAction = plan.Node_UPDATE
	defer func() {
		hb.Reset(proc, true)
		hb.Free(proc)
		proc.Free()
		require.Equal(t, int64(0), proc.Mp().CurrNB())
	}()

	require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_int32.ToType())}, -1, -1, nil, proc))

	rows := hashmap.UnitLimit * 2
	keys := make([]int32, rows)
	nulls := make([]uint64, hashmap.UnitLimit)
	for i := 0; i < hashmap.UnitLimit; i++ {
		keys[i] = int32(i + 1)
		nulls[i] = uint64(hashmap.UnitLimit + i)
	}
	keyVec := testutil.MakeInt32Vector(keys, nulls, proc.Mp())
	bat := batch.New([]string{"id"})
	bat.SetVector(0, keyVec)
	bat.SetRowCount(rows)
	require.NoError(t, hb.Batches.CopyIntoBatches(bat, proc))
	hb.InputBatchRowCount = bat.RowCount()
	bat.Clean(proc.Mp())

	require.NoError(t, hb.BuildHashmap(false, true, false, proc))
	require.Equal(t, uint64(hashmap.UnitLimit), hb.GetGroupCount())

	nullRows := hb.Sels.Get(0)
	require.Len(t, nullRows, hashmap.UnitLimit)
	for i, row := range nullRows {
		require.Equal(t, int32(hashmap.UnitLimit+i), row)
	}
	for group := 1; group <= hashmap.UnitLimit; group++ {
		require.Equal(t, []int32{int32(group - 1)}, hb.Sels.Get(int32(group)))
	}
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

func TestDedupBuildIgnoreOnlyMarksCandidateOwnOldKey(t *testing.T) {
	tests := []struct {
		name    string
		newKeys []int32
		oldKeys []int32
	}{
		{
			name:    "another candidate old key is not released",
			newKeys: []int32{4, 1},
			oldKeys: []int32{1, 3},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var hb HashmapBuilder
			proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
			hb.IsDedup = true
			hb.OnDuplicateAction = plan.Node_IGNORE
			defer func() {
				hb.Reset(proc, true)
				hb.Free(proc)
				require.Equal(t, int64(0), proc.Mp().CurrNB())
			}()

			require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_int32.ToType())}, 1, -1, nil, proc))
			bat := makeIntKeyValueBatch(proc, tt.newKeys, tt.oldKeys)
			require.NoError(t, hb.Batches.CopyIntoBatches(bat, proc))
			hb.InputBatchRowCount = bat.RowCount()
			bat.Clean(proc.Mp())

			require.NoError(t, hb.BuildHashmap(false, false, false, proc))
			require.NotNil(t, hb.DelRows)
			require.Zero(t, hb.DelRows.Count())
		})
	}
}

func TestDedupBuildIgnorePrefersOriginalKeyOwner(t *testing.T) {
	for _, oldKeys := range [][]int32{{1, 2}, {2, 1}} {
		t.Run(strings.Join([]string{strconv.Itoa(int(oldKeys[0])), strconv.Itoa(int(oldKeys[1]))}, "_"), func(t *testing.T) {
			var hb HashmapBuilder
			proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
			hb.IsDedup = true
			hb.OnDuplicateAction = plan.Node_IGNORE
			defer func() {
				hb.Reset(proc, true)
				hb.Free(proc)
				require.Equal(t, int64(0), proc.Mp().CurrNB())
			}()

			require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_int32.ToType())}, 1, -1, nil, proc))
			bat := makeIntKeyValueBatch(proc, []int32{2, 2}, oldKeys)
			require.NoError(t, hb.Batches.CopyIntoBatches(bat, proc))
			hb.InputBatchRowCount = bat.RowCount()
			bat.Clean(proc.Mp())

			require.NoError(t, hb.BuildHashmap(false, false, false, proc))
			require.Equal(t, 1, hb.Batches.RowCount())
			keptOldKeys := vector.MustFixedColNoTypeCheck[int32](hb.Batches.Buf[0].Vecs[1])
			require.Equal(t, []int32{2}, keptOldKeys[:hb.Batches.RowCount()])
			require.NotNil(t, hb.DelRows)
			require.True(t, hb.DelRows.Contains(0), "delRows=%s count=%d", hb.DelRows.String(), hb.DelRows.Count())
		})
	}
}

func TestDedupBuildIgnoreRebuildsAfterOwnerReplacement(t *testing.T) {
	var hb HashmapBuilder
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	hb.IsDedup = true
	hb.OnDuplicateAction = plan.Node_IGNORE
	defer func() {
		hb.Reset(proc, true)
		hb.Free(proc)
		require.Equal(t, int64(0), proc.Mp().CurrNB())
	}()

	require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, types.T_int32.ToType())}, 1, -1, nil, proc))
	bat := makeIntKeyValueBatch(proc, []int32{2, 1, 2}, []int32{1, 3, 2})
	require.NoError(t, hb.Batches.CopyIntoBatches(bat, proc))
	hb.InputBatchRowCount = bat.RowCount()
	bat.Clean(proc.Mp())

	require.NoError(t, hb.BuildHashmap(false, false, false, proc))
	require.Equal(t, 2, hb.Batches.RowCount())
	keys := vector.MustFixedColNoTypeCheck[int32](hb.Batches.Buf[0].Vecs[0])
	oldKeys := vector.MustFixedColNoTypeCheck[int32](hb.Batches.Buf[0].Vecs[1])
	require.Equal(t, []int32{1, 2}, keys[:hb.Batches.RowCount()])
	require.Equal(t, []int32{3, 2}, oldKeys[:hb.Batches.RowCount()])
	require.NotNil(t, hb.DelRows)
	require.False(t, hb.DelRows.Contains(0))
	require.True(t, hb.DelRows.Contains(1), "delRows=%s count=%d", hb.DelRows.String(), hb.DelRows.Count())
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
