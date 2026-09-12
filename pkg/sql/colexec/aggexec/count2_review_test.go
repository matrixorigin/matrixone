// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package aggexec

import (
	"encoding/binary"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/arenaskl"
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestCountDistinctBulkFillChunksBeyondUnitLimit(t *testing.T) {
	for _, tc := range []struct {
		name         string
		rows         int
		disableFixed bool
	}{
		{name: "fixed-over-two-units", rows: hashmap.UnitLimit*2 + 1},
		{name: "legacy-over-one-unit", rows: hashmap.UnitLimit + 1, disableFixed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			values := make([]int64, tc.rows)
			for i := range values {
				values[i] = int64(i)
			}
			vec := testutil.NewInt64Vector(
				tc.rows, types.T_int64.ToType(), mp, false, nil, values)
			exec := newCountColumnExec(
				mp, AggIdOfCountColumn, true,
				[]types.Type{types.T_int64.ToType()},
			).(*countColumnExec)
			require.NoError(t, exec.GroupGrow(1024))
			if tc.disableFixed {
				require.True(t, exec.disableEmptyDistinctFixedStates())
			}

			require.NoError(t, exec.BulkFill(0, []*vector.Vector{vec}))
			result, err := exec.Flush()
			require.NoError(t, err)
			require.Len(t, result, 1)
			require.Equal(t, int64(tc.rows),
				vector.GetFixedAtNoTypeCheck[int64](result[0], 0))
			for _, v := range result {
				v.Free(mp)
			}
			exec.Free()
			vec.Free(mp)
			require.Zero(t, mp.CurrNB())
		})
	}
}

func TestCountDistinctBatchFillChunksAndBoundaryDuplicates(t *testing.T) {
	for _, tc := range []struct {
		name         string
		rows         int
		disableFixed bool
	}{
		{name: "fixed-over-two-units", rows: hashmap.UnitLimit*2 + 1},
		{name: "legacy-over-one-unit", rows: hashmap.UnitLimit + 1, disableFixed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			values := make([]int64, tc.rows)
			for i := range values {
				values[i] = int64(i)
			}
			// Keep one duplicate across a unit boundary.  The direct BatchFill
			// wrapper must preserve the resident membership set while it splits
			// the input into bounded admission units.
			values[len(values)-1] = values[0]
			vec := testutil.NewInt64Vector(
				tc.rows, types.T_int64.ToType(), mp, false, nil, values)
			exec := newCountColumnExec(
				mp, AggIdOfCountColumn, true,
				[]types.Type{types.T_int64.ToType()},
			).(*countColumnExec)
			require.NoError(t, exec.GroupGrow(1024))
			if tc.disableFixed {
				require.True(t, exec.disableEmptyDistinctFixedStates())
			}
			groups := make([]uint64, tc.rows)
			for i := range groups {
				groups[i] = 1
			}

			require.NoError(t, exec.BatchFill(0, groups, []*vector.Vector{vec}))
			result, err := exec.Flush()
			require.NoError(t, err)
			require.Len(t, result, 1)
			require.Equal(t, int64(tc.rows-1),
				vector.GetFixedAtNoTypeCheck[int64](result[0], 0))
			result[0].Free(mp)
			exec.Free()
			vec.Free(mp)
			require.Zero(t, mp.CurrNB())
		})
	}
}

func TestDistinctFixedBatchProbeIsBounded(t *testing.T) {
	var batch distinctFixedBatch
	batch.reset()
	for i := 0; i < len(batch.groups); i++ {
		duplicate, err := batch.seenOrInsert(1, uint64(i))
		require.NoError(t, err)
		require.False(t, duplicate)
	}
	_, err := batch.seenOrInsert(1, uint64(len(batch.groups)))
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvariant)
}

func TestCountDistinctFixedAdmissionRetainsNonPublishingRows(t *testing.T) {
	mp := mpool.MustNewZero()
	registry, account, allocation := newTestAggregateAllocation(t)
	exec := newCountColumnExec(
		mp, AggIdOfCountColumn, true,
		[]types.Type{types.T_int64.ToType()},
	).(*countColumnExec)
	require.NoError(t, exec.SetAllocationAccount(allocation))
	require.NoError(t, exec.GroupGrow(1024))

	values := []int64{7, 7, 0, 8, 8, 9}
	nulls := []bool{false, false, true, false, false, false}
	groups := []uint64{1, 1, 1, 1, 1, GroupNotMatched}
	vec := testutil.NewInt64Vector(
		len(values), types.T_int64.ToType(), mp, false, nulls, values)
	require.NoError(t, exec.PreflightBatchFill(
		0, groups, []*vector.Vector{vec}))
	require.Equal(t, groups,
		exec.distinctFixedAdmission.groups[:len(groups)])
	require.Equal(t,
		[]bool{true, false, false, true, false, false},
		exec.distinctFixedAdmission.publish[:len(groups)])
	require.True(t, exec.distinctFixedAdmission.matches(0, groups, vec))
	require.NoError(t, exec.BatchFill(0, groups, []*vector.Vector{vec}))

	result, err := exec.Flush()
	require.NoError(t, err)
	require.Equal(t, int64(2),
		vector.GetFixedAtNoTypeCheck[int64](result[0], 0))
	result[0].Free(mp)
	vec.Free(mp)
	exec.Free()
	require.NoError(t, exec.ClearAllocationAccount(allocation))
	finishTestAggregateAllocation(t, registry, account)
	require.Zero(t, mp.CurrNB())
}

func TestCountDistinctFixedMergeFromLegacySource(t *testing.T) {
	mp := mpool.MustNewZero()
	registry, account, allocation := newReviewAggregateAllocation(t, 2<<20)
	makeExec := func() *countColumnExec {
		exec := newCountColumnExec(
			mp, AggIdOfCountColumn, true,
			[]types.Type{types.T_int64.ToType()},
		).(*countColumnExec)
		require.NoError(t, exec.SetAllocationAccount(allocation))
		require.NoError(t, exec.GroupGrow(1024))
		return exec
	}
	source := makeExec()
	target := makeExec()

	// Force the source into the compatibility skiplist representation while
	// retaining the target's fixed index.  This models a small-account/legacy
	// spill source being merged into a current execution state.
	require.True(t, source.disableEmptyDistinctFixedStates())
	sourceValues := make([]int64, hashmap.UnitLimit-6)
	for i := range sourceValues {
		sourceValues[i] = int64(i)
	}
	targetValues := []int64{0}
	fill := func(exec *countColumnExec, values []int64) {
		vec := testutil.NewInt64Vector(
			len(values), types.T_int64.ToType(), mp, false, nil, values)
		groups := make([]uint64, len(values))
		for i := range groups {
			groups[i] = 1
		}
		require.NoError(t, exec.PreflightBatchFill(
			0, groups, []*vector.Vector{vec}))
		require.NoError(t, exec.BatchFill(0, groups, []*vector.Vector{vec}))
		vec.Free(mp)
	}
	fill(source, sourceValues)
	fill(target, targetValues)
	require.False(t, source.state[0].distinctFixedDeferred)
	require.True(t, target.state[0].distinctFixedDeferred)
	require.Equal(t, 256, len(target.state[0].distinctIndex.slotKeys))

	mergeGroups := []uint64{1}
	require.NoError(t, target.PreflightBatchMerge(
		source, 0, mergeGroups))
	// The source has 250 candidates and the target already has one.  Fixed
	// admission must grow the target index before publication (70% load bound),
	// rather than reserving the compatibility skiplist arena.
	require.Equal(t, 512, len(target.state[0].distinctIndex.slotKeys))
	require.NoError(t, target.BatchMerge(source, 0, mergeGroups))

	result, err := target.Flush()
	require.NoError(t, err)
	require.Equal(t, int64(250),
		vector.GetFixedAtNoTypeCheck[int64](result[0], 0))
	result[0].Free(mp)
	source.Free()
	target.Free()
	require.NoError(t, source.ClearAllocationAccount(allocation))
	require.NoError(t, target.ClearAllocationAccount(allocation))
	finishTestAggregateAllocation(t, registry, account)
	require.Zero(t, mp.CurrNB())
}

func newReviewAggregateAllocation(
	t *testing.T,
	limit uint64,
) (*mpool.AllocationAccountRegistry, *mpool.AllocationAccount, *AllocationAccount) {
	t.Helper()
	registry, err := mpool.NewAllocationAccountRegistry(1, 512)
	require.NoError(t, err)
	account, err := registry.Open(limit)
	require.NoError(t, err)
	allocation, err := NewAllocationAccount(
		account,
		mpool.AllocationOwnerGroup,
		AllocationAccountSites{
			VectorData:     1,
			VectorArea:     2,
			VectorNulls:    3,
			VectorGrouping: 4,
			ArgumentCount:  5,
			ArgumentArena:  6,
		},
	)
	require.NoError(t, err)
	return registry, account, allocation
}

func TestCountDistinctLegacyMergeFromFixedSource(t *testing.T) {
	mp := mpool.MustNewZero()
	registry, account, allocation := newReviewAggregateAllocation(t, 2<<20)
	makeExec := func() *countColumnExec {
		exec := newCountColumnExec(
			mp, AggIdOfCountColumn, true,
			[]types.Type{types.T_int64.ToType()},
		).(*countColumnExec)
		require.NoError(t, exec.SetAllocationAccount(allocation))
		require.NoError(t, exec.GroupGrow(1024))
		return exec
	}
	source := makeExec()
	target := makeExec()
	require.True(t, target.disableEmptyDistinctFixedStates())

	fill := func(exec *countColumnExec, values []int64, groups []uint64) {
		require.Len(t, groups, len(values))
		vec := testutil.NewInt64Vector(
			len(values), types.T_int64.ToType(), mp, false, nil, values)
		for offset := 0; offset < len(groups); offset += hashmap.UnitLimit {
			end := min(offset+hashmap.UnitLimit, len(groups))
			require.NoError(t, exec.PreflightBatchFill(
				offset, groups[offset:end], []*vector.Vector{vec}))
			require.NoError(t, exec.BatchFill(
				offset, groups[offset:end], []*vector.Vector{vec}))
		}
		vec.Free(mp)
	}
	makeKey := func(value int64) []byte {
		key := make([]byte, kAggArgPrefixSz+8)
		binary.BigEndian.PutUint16(key[:kAggArgPrefixSz], 0)
		binary.LittleEndian.PutUint64(key[kAggArgPrefixSz:], uint64(value))
		return key
	}
	keyNeed := func(value int64) uint64 {
		plan := arenaskl.MakeAddPlan(makeKey(value))
		consumed, trailing, ok := plan.ArenaFootprint(
			kAggArgPrefixSz+8, 0)
		require.True(t, ok)
		return consumed + trailing
	}
	var targetValues []int64
	var missingValues [2]int64
	for value := int64(1); value < 10000; value++ {
		arena := target.state[0].argSkl.Arena()
		used := uint64(arena.Size())
		capacity := uint64(arena.Capacity())
		one := keyNeed(value)
		two := one + keyNeed(value+1)
		three := two + keyNeed(value+2)
		if used+two <= capacity && used+three > capacity {
			missingValues = [2]int64{value, value + 1}
			break
		}
		targetValues = append(targetValues, value)
		fill(target, []int64{value}, []uint64{1})
	}
	require.Greater(t, len(targetValues), 2)
	require.NotEqual(t, [2]int64{}, missingValues)
	missing := []int64{missingValues[0], missingValues[1]}
	sourceValues := make([]int64, 0, (len(targetValues)+len(missing))*2)
	sourceGroups := make([]uint64, 0, cap(sourceValues))
	for _, value := range targetValues {
		sourceValues = append(sourceValues, value)
		sourceGroups = append(sourceGroups, 1)
	}
	for _, value := range missing {
		sourceValues = append(sourceValues, value)
		sourceGroups = append(sourceGroups, 1)
	}
	for _, value := range targetValues {
		sourceValues = append(sourceValues, value)
		sourceGroups = append(sourceGroups, 2)
	}
	for _, value := range missing {
		sourceValues = append(sourceValues, value)
		sourceGroups = append(sourceGroups, 2)
	}
	fill(source, sourceValues, sourceGroups)
	require.True(t, source.state[0].distinctFixedDeferred)
	require.False(t, target.state[0].distinctFixedDeferred)

	mergeGroups := []uint64{1, 1}
	beforeCapacity := target.state[0].argSkl.Arena().Capacity()
	beforeSize := target.state[0].argSkl.Arena().Size()
	require.NoError(t, target.PreflightBatchMerge(
		source, 0, mergeGroups))
	// The fixed source is iterated in reverse insertion order and both source
	// groups map to the same legacy target group.  Only the two missing values
	// are new; the exact preflight must deduplicate the reversed overlap and
	// reserve exactly those two nodes without growing the already-boundary arena.
	require.Equal(t, beforeCapacity, target.state[0].argSkl.Arena().Capacity())
	require.Equal(t, beforeSize, target.state[0].argSkl.Arena().Size())
	require.NoError(t, target.BatchMerge(source, 0, mergeGroups))
	result, err := target.Flush()
	require.NoError(t, err)
	require.Equal(t, int64(len(targetValues)+len(missing)),
		vector.GetFixedAtNoTypeCheck[int64](result[0], 0))
	result[0].Free(mp)
	source.Free()
	target.Free()
	require.NoError(t, source.ClearAllocationAccount(allocation))
	require.NoError(t, target.ClearAllocationAccount(allocation))
	finishTestAggregateAllocation(t, registry, account)
	require.Zero(t, mp.CurrNB())
}
