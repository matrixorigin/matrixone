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
	"bytes"
	"context"
	"math"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/arenaskl"
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestCapacityPreflightPrimitiveBoundaries(t *testing.T) {
	mp := mpool.MustNewZero()
	registry, account, allocation := newTestAggregateAllocation(t)

	plain := &aggExec{}
	require.NoError(t, plain.PreflightBatchFill(-1, nil, nil))
	require.NoError(t, plain.PreflightBatchMerge(nil, -1, nil))

	plain.allocation = allocation
	plain.stateTypes = []types.Type{types.T_varchar.ToType()}
	mergeSource, err := MakeAgg(mp, AggIdOfSum, false, types.T_int64.ToType())
	require.NoError(t, err)
	require.ErrorIs(t, plain.PreflightBatchFill(0, nil, nil), mpool.ErrAllocationAccountInvariant)
	require.ErrorIs(t, plain.PreflightBatchMerge(mergeSource, 0, nil), mpool.ErrAllocationAccountInvariant)
	require.Error(t, plain.PreflightBatchMerge(nil, 0, nil))
	tooManyGroups := make([]uint64, hashmap.UnitLimit+1)
	require.ErrorIs(t, plain.PreflightBatchFill(0, tooManyGroups, nil), mpool.ErrAllocationAccountInvalid)
	require.ErrorIs(t, plain.PreflightBatchMerge(nil, 0, tooManyGroups), mpool.ErrAllocationAccountInvalid)
	plain.allocation = nil
	mergeSource.Free()

	var needs [hashmap.UnitLimit]argumentChunkCapacity
	needCount := 0
	require.ErrorIs(t,
		addArgumentChunkCapacityWithValue(&needs, &needCount, 0, []byte("k"), -1),
		mpool.ErrAllocationAccountInvalid)
	require.NoError(t, addArgumentChunkCapacity(&needs, &needCount, 3, []byte("first")))
	require.NoError(t, addArgumentChunkCapacity(&needs, &needCount, 3, []byte("second")))
	require.Equal(t, 1, needCount)
	require.Greater(t, needs[0].arenaRequired, needs[0].arenaConsumed)
	needs[0].arenaConsumed = math.MaxUint64
	require.ErrorIs(t,
		addArgumentChunkCapacity(&needs, &needCount, 3, []byte("overflow")),
		mpool.ErrAllocationAllocatorLimit)
	needCount = len(needs)
	require.ErrorIs(t,
		addArgumentChunkCapacity(&needs, &needCount, 99, []byte("full")),
		mpool.ErrAllocationAccountInvalid)

	var progress [hashmap.UnitLimit]argumentTargetProgress
	progressCount := 0
	ordinal, err := nextArgumentOrdinal(&progress, &progressCount, 1, 2, 7)
	require.NoError(t, err)
	require.Equal(t, uint32(7), ordinal)
	ordinal, err = nextArgumentOrdinal(&progress, &progressCount, 1, 2, 7)
	require.NoError(t, err)
	require.Equal(t, uint32(8), ordinal)
	progress[0].added = 0
	_, err = nextArgumentOrdinal(&progress, &progressCount, 1, 2, math.MaxUint32)
	require.ErrorIs(t, err, mpool.ErrAllocationAllocatorLimit)
	progressCount = len(progress)
	_, err = nextArgumentOrdinal(&progress, &progressCount, 9, 9, 0)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvalid)

	normal := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(normal, []byte("a"), false, mp))
	require.NoError(t, vector.AppendBytes(normal, nil, true, mp))
	require.NoError(t, vector.AppendBytes(normal, []byte("b"), false, mp))
	constant, err := vector.NewConstFixed(types.T_int64.ToType(), int64(5), 1, mp)
	require.NoError(t, err)
	require.ErrorIs(t, validatePreflightVectors([]*vector.Vector{normal}, -1, 1), mpool.ErrAllocationAccountInvalid)
	require.ErrorIs(t, validatePreflightVectors([]*vector.Vector{normal}, 0, hashmap.UnitLimit+1), mpool.ErrAllocationAccountInvalid)
	require.ErrorIs(t, validatePreflightVectors([]*vector.Vector{nil}, 0, 1), mpool.ErrAllocationAccountInvalid)
	require.NoError(t, validatePreflightVectors([]*vector.Vector{constant}, 7, 3))
	_, err = preflightPhysicalRow(nil, 0)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvalid)
	_, err = preflightPhysicalRow(normal, -1)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvalid)
	physical, err := preflightPhysicalRow(constant, 8)
	require.NoError(t, err)
	require.Zero(t, physical)

	equal, err := preflightArgumentRowsEqual([]*vector.Vector{normal}, 0, 2)
	require.NoError(t, err)
	require.False(t, equal)
	equal, err = preflightArgumentRowsEqual([]*vector.Vector{normal}, 1, 1)
	require.NoError(t, err)
	require.True(t, equal)
	batch := distinctArgumentBatch{}
	duplicate, err := batch.seenOrInsert(
		[]uint64{1, 2, 1}, []*vector.Vector{normal}, 0, 0)
	require.NoError(t, err)
	require.False(t, duplicate)
	duplicate, err = batch.seenOrInsert(
		[]uint64{1, 2, 1}, []*vector.Vector{normal}, 0, 2)
	require.NoError(t, err)
	require.False(t, duplicate)
	batch = distinctArgumentBatch{}
	duplicate, err = batch.seenOrInsert(
		[]uint64{1, 2, 1}, []*vector.Vector{constant}, 5, 0)
	require.NoError(t, err)
	require.False(t, duplicate)
	duplicate, err = batch.seenOrInsert(
		[]uint64{1, 2, 1}, []*vector.Vector{constant}, 5, 2)
	require.NoError(t, err)
	require.True(t, duplicate)

	stateInfo := aggInfo{
		argTypes: []types.Type{types.T_varchar.ToType()},
		saveArg:  true,
	}
	state := aggState{}
	require.NoError(t, state.initWithAllocation(mp, 1, 1, &stateInfo, false, allocation))
	exec := &aggExec{
		mp:         mp,
		aggInfo:    stateInfo,
		chunkSize:  AggBatchSize,
		state:      []aggState{state},
		allocation: allocation,
	}
	require.Nil(t, (*aggExec)(nil).preflightStateAt(0))
	require.Nil(t, exec.preflightStateAt(-1))
	require.Same(t, &exec.state[0], exec.preflightStateAt(0))
	exec.standby = []aggState{{}}
	require.Same(t, &exec.standby[0], exec.preflightStateAt(1))
	require.Nil(t, exec.preflightStateAt(2))
	_, _, stateAt, err := exec.validatePreflightTarget(GroupNotMatched)
	require.NoError(t, err)
	require.Nil(t, stateAt)
	_, _, _, err = exec.validatePreflightTarget(2)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvariant)

	key, err := exec.state[0].preparePreflightArgumentKey(
		mp, 0, []*vector.Vector{normal}, 0, len(normal.GetRawBytesAt(0)), false, 11)
	require.NoError(t, err)
	require.Len(t, key, kAggArgPrefixSz+kAggArgOrdinalSz+1)

	// Exercise every key-copy mode used by the DISTINCT preflight path. In
	// particular, signed zero must be canonicalized in-place in the accounted
	// scratch key, while non-distinct and non-zero values retain their bytes.
	floatValues := vector.NewVec(types.T_float64.ToType())
	require.NoError(t, vector.AppendFixed(floatValues, math.Copysign(0, -1), false, mp))
	require.NoError(t, vector.AppendFixed(floatValues, float64(3), false, mp))
	intValues := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(intValues, int64(7), false, mp))
	key, err = exec.state[0].preparePreflightArgumentKey(
		mp, 0, []*vector.Vector{floatValues}, 0,
		len(floatValues.GetRawBytesAt(0)), true, 0)
	require.NoError(t, err)
	require.Equal(t, make([]byte, 8), key[kAggArgPrefixSz:])
	key, err = exec.state[0].preparePreflightArgumentKey(
		mp, 0, []*vector.Vector{floatValues, intValues}, 0,
		4+len(floatValues.GetRawBytesAt(0))+4+len(intValues.GetRawBytesAt(0)), true, 0)
	require.NoError(t, err)
	require.Len(t, key, kAggArgPrefixSz+4+8+4+8)
	key, err = exec.state[0].preparePreflightArgumentKey(
		mp, 0, []*vector.Vector{normal, intValues}, 0,
		4+len(normal.GetRawBytesAt(0))+4+len(intValues.GetRawBytesAt(0)), false, 11)
	require.NoError(t, err)
	require.Len(t, key, kAggArgPrefixSz+kAggArgOrdinalSz+4+1+4+8)
	_, err = exec.state[0].preparePreflightArgumentKey(
		mp, 0, []*vector.Vector{normal}, 99, 0, true, 0)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvalid)
	_, err = (*aggState)(nil).preparePreflightArgumentKey(
		mp, 0, []*vector.Vector{normal}, 0, 1, true, 0)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvalid)

	exec.state[0].free(mp)
	exec.state = nil
	exec.standby = nil
	exec.allocation = nil
	normal.Free(mp)
	constant.Free(mp)
	floatValues.Free(mp)
	intValues.Free(mp)
	finishTestAggregateAllocation(t, registry, account)
	require.Zero(t, mp.CurrNB())
}

func TestDistinctArgumentBatchDeduplicatesFullWorkUnit(t *testing.T) {
	const offset = 17
	mp := mpool.MustNewZero()
	input := vector.NewVec(types.T_int64.ToType())
	for i := 0; i < offset; i++ {
		require.NoError(t, vector.AppendFixed(input, int64(-1), false, mp))
	}
	groups := make([]uint64, hashmap.UnitLimit)
	for i := range groups {
		groups[i] = 1
		require.NoError(t, vector.AppendFixed(input, int64(i), false, mp))
	}
	defer input.Free(mp)

	batch := distinctArgumentBatch{}
	for row := range groups {
		duplicate, err := batch.seenOrInsert(
			groups, []*vector.Vector{input}, offset, row)
		require.NoError(t, err)
		require.False(t, duplicate, "unique row %d", row)
	}
	for row := range groups {
		duplicate, err := batch.seenOrInsert(
			groups, []*vector.Vector{input}, offset, row)
		require.NoError(t, err)
		require.True(t, duplicate, "second visit to row %d", row)
	}

	_, err := batch.seenOrInsert(groups, []*vector.Vector{input}, offset, -1)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvalid)
	_, err = batch.seenOrInsert(
		make([]uint64, hashmap.UnitLimit+1), []*vector.Vector{input}, offset, 0)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvalid)
}

func TestDistinctArgumentLinearBatchTransitionsOnlyAfterUniqueLimit(t *testing.T) {
	const offset = 3
	mp := mpool.MustNewZero()
	input := vector.NewVec(types.T_int64.ToType())
	for range offset {
		require.NoError(t, vector.AppendFixed(input, int64(-1), false, mp))
	}
	for i := range distinctArgumentLinearLimit {
		require.NoError(t, vector.AppendFixed(input, int64(i), false, mp))
	}
	require.NoError(t, vector.AppendFixed(input, int64(0), false, mp))
	require.NoError(t, vector.AppendFixed(input, int64(distinctArgumentLinearLimit), false, mp))
	defer input.Free(mp)

	groups := make([]uint64, distinctArgumentLinearLimit+2)
	for i := range groups {
		groups[i] = 1
	}
	batch := distinctArgumentLinearBatch{}
	for row := range distinctArgumentLinearLimit {
		duplicate, err := batch.seen(
			groups, []*vector.Vector{input}, offset, row)
		require.NoError(t, err)
		require.False(t, duplicate)
		require.True(t, batch.insertUnique(row))
	}
	duplicate, err := batch.seen(
		groups, []*vector.Vector{input}, offset, distinctArgumentLinearLimit)
	require.NoError(t, err)
	require.True(t, duplicate,
		"a duplicate stays on the exact linear path")

	duplicate, err = batch.seen(
		groups, []*vector.Vector{input}, offset, distinctArgumentLinearLimit+1)
	require.NoError(t, err)
	require.False(t, duplicate)
	require.False(t, batch.insertUnique(distinctArgumentLinearLimit+1),
		"the ninth unique tuple switches to hashed admission")
}

func TestDistinctArgumentAdaptivePreflightPreservesMultiColumnSemantics(t *testing.T) {
	const offset = 2
	mp := mpool.MustNewZero()
	registry, account, allocation := newTestAggregateAllocation(t)
	exec, err := MakeAgg(
		mp, AggIdOfCountColumn, true,
		types.T_varchar.ToType(), types.T_int64.ToType())
	require.NoError(t, err)
	owner := exec.(AllocationAccountOwner)
	require.NoError(t, owner.SetAllocationAccount(allocation))
	require.NoError(t, exec.GroupGrow(2))

	stringsInput := vector.NewVec(types.T_varchar.ToType())
	intsInput := vector.NewVec(types.T_int64.ToType())
	for range offset {
		require.NoError(t, vector.AppendBytes(stringsInput, []byte("ignored"), false, mp))
		require.NoError(t, vector.AppendFixed(intsInput, int64(-1), false, mp))
	}
	groups := make([]uint64, hashmap.UnitLimit)
	want := [2]map[string]struct{}{{}, {}}
	for row := range groups {
		group := uint64(row%2 + 1)
		if row == 0 {
			group = GroupNotMatched
		}
		groups[row] = group
		value := []byte("value-" + string(rune('a'+row%32)))
		null := row == 19
		require.NoError(t, vector.AppendBytes(stringsInput, value, null, mp))
		require.NoError(t, vector.AppendFixed(intsInput, int64(row%32), false, mp))
		if group != GroupNotMatched && !null {
			want[group-1][string(value)] = struct{}{}
		}
	}
	vectors := []*vector.Vector{stringsInput, intsInput}
	preflight := exec.(BatchCapacityPreflight)
	require.NoError(t, preflight.PreflightBatchFill(offset, groups, vectors))
	admitted := account.Snapshot().Used
	require.NoError(t, exec.BatchFill(offset, groups, vectors))
	require.Equal(t, admitted, account.Snapshot().Used,
		"publication must stay within the adaptively admitted capacity")

	// The second pass exercises both the in-batch hashed duplicate path and
	// keys already present in the published skiplist.
	require.NoError(t, preflight.PreflightBatchFill(offset, groups, vectors))
	require.NoError(t, exec.BatchFill(offset, groups, vectors))
	require.Equal(t, admitted, account.Snapshot().Used)
	results, err := exec.Flush()
	require.NoError(t, err)
	for group := range want {
		require.Equal(t, int64(len(want[group])),
			vector.GetFixedAtNoTypeCheck[int64](results[0], group))
	}
	results[0].Free(mp)
	stringsInput.Free(mp)
	intsInput.Free(mp)
	exec.Free()
	require.NoError(t, owner.ClearAllocationAccount(allocation))
	finishTestAggregateAllocation(t, registry, account)
	require.Zero(t, mp.CurrNB())
}

func TestNonDistinctMultiArgumentPreflightRemainsPublicationFree(t *testing.T) {
	mp := mpool.MustNewZero()
	registry, account, allocation := newTestAggregateAllocation(t)
	info := aggInfo{
		argTypes: []types.Type{
			types.T_varchar.ToType(), types.T_int64.ToType(),
		},
		saveArg: true,
	}
	state := aggState{}
	require.NoError(t, state.initWithAllocation(
		mp, 1, 1, &info, false, allocation))
	exec := aggExec{
		mp:         mp,
		aggInfo:    info,
		chunkSize:  AggBatchSize,
		state:      []aggState{state},
		allocation: allocation,
	}
	stringsInput := buildVarlenVec(t, mp, types.T_varchar.ToType(),
		[]string{"ignored", "first", "null", "second"})
	intsInput := buildFixedVec(t, mp, types.T_int64.ToType(),
		[]int64{0, 1, 2, 3})
	stringsInput.SetNull(2)
	require.NoError(t, exec.preflightBatchFillArgs(
		0,
		[]uint64{GroupNotMatched, 1, 1, 1},
		[]*vector.Vector{stringsInput, intsInput},
		false,
	))
	require.Zero(t, exec.state[0].argCnt[0],
		"capacity admission must not publish aggregate arguments")
	require.Positive(t, len(exec.state[0].argScratch))

	stringsInput.Free(mp)
	intsInput.Free(mp)
	exec.state[0].free(mp)
	exec.state = nil
	exec.allocation = nil
	finishTestAggregateAllocation(t, registry, account)
	require.Zero(t, mp.CurrNB())
}

func TestNextArgumentArenaCapacityUsesGeometricGrowth(t *testing.T) {
	capacity, err := nextArgumentArenaCapacity(1024, 1024)
	require.NoError(t, err)
	require.Equal(t, uint64(1024), capacity)

	capacity, err = nextArgumentArenaCapacity(16<<10, 16<<10+1)
	require.NoError(t, err)
	require.Equal(t, uint64(16<<10+kAggArgArenaSize), capacity)

	capacity, err = nextArgumentArenaCapacity(kAggArgArenaSize, kAggArgArenaSize+1)
	require.NoError(t, err)
	require.Equal(t, uint64(2*kAggArgArenaSize), capacity)

	capacity, err = nextArgumentArenaCapacity(math.MaxUint32-1, math.MaxUint32)
	require.NoError(t, err)
	require.Equal(t, uint64(math.MaxUint32), capacity)

	_, err = nextArgumentArenaCapacity(math.MaxUint32, uint64(math.MaxUint32)+1)
	require.ErrorIs(t, err, mpool.ErrAllocationAllocatorLimit)

	capacity, err = nextLinearArgumentArenaCapacity(
		2*kAggArgArenaSize, 2*kAggArgArenaSize+1)
	require.NoError(t, err)
	require.Equal(t, uint64(3*kAggArgArenaSize), capacity)

	capacity, err = nextLinearArgumentArenaCapacity(
		math.MaxUint32-1, math.MaxUint32)
	require.NoError(t, err)
	require.Equal(t, uint64(math.MaxUint32), capacity)

	_, err = nextLinearArgumentArenaCapacity(
		math.MaxUint32, uint64(math.MaxUint32)+1)
	require.ErrorIs(t, err, mpool.ErrAllocationAllocatorLimit)
}

func TestArgumentArenaGrowthFallsBackUnderCapacityPressure(t *testing.T) {
	const current = 2 * kAggArgArenaSize
	preferred, err := nextArgumentArenaCapacity(current, current+1)
	require.NoError(t, err)
	fallback, err := nextLinearArgumentArenaCapacity(current, current+1)
	require.NoError(t, err)
	require.Equal(t, uint64(2*current), preferred)
	require.Equal(t, uint64(current+kAggArgArenaSize), fallback)

	mp := mpool.MustNewZero()
	registry, err := mpool.NewAllocationAccountRegistry(1, 8)
	require.NoError(t, err)
	// The old arena and the linear fallback fit together exactly. The preferred
	// geometric arena does not, proving that retry preserves availability rather
	// than merely selecting the same capacity through another branch.
	account, err := registry.Open(current + fallback)
	require.NoError(t, err)
	allocation, err := NewAllocationAccount(
		account, mpool.AllocationOwnerGroup, AllocationAccountSites{
			VectorData: 1, VectorArea: 2, VectorNulls: 3,
			VectorGrouping: 4, ArgumentCount: 5, ArgumentArena: 6,
		})
	require.NoError(t, err)

	buf, err := allocation.allocArgumentArena(mp, current)
	require.NoError(t, err)
	state := aggState{
		allocation: allocation,
		argbuf:     buf,
		argSkl:     arenaskl.NewSkiplist(arenaskl.NewArena(buf), bytes.Compare),
	}
	require.NoError(t, state.argSkl.Add([]byte("kept"), nil))
	used := uint64(state.argSkl.Arena().Size())
	require.Less(t, used, uint64(current+1))
	require.NoError(t, state.preflightArgumentCapacity(
		mp, uint64(current+1)-used, 0))
	require.Equal(t, int(fallback), len(state.argbuf))
	require.True(t, state.argSkl.Contains([]byte("kept")))
	require.Equal(t, fallback, account.Snapshot().Used)

	mp.Free(state.argbuf)
	require.Zero(t, account.Snapshot().Used)
	account.Seal()
	_, err = registry.Finalize(account)
	require.NoError(t, err)
	require.Zero(t, mp.CurrNB())
}

func TestArgumentArenaGrowthFallsBackAtAllocatorLimit(t *testing.T) {
	const current = 2 * kAggArgArenaSize
	preferred, err := nextArgumentArenaCapacity(current, current+1)
	require.NoError(t, err)
	fallback, err := nextLinearArgumentArenaCapacity(current, current+1)
	require.NoError(t, err)
	require.Greater(t, preferred, fallback)

	oldCapLimit := mpool.CapLimit
	mpool.CapLimit = int(preferred)
	t.Cleanup(func() {
		mpool.CapLimit = oldCapLimit
	})

	mp := mpool.MustNewZero()
	registry, err := mpool.NewAllocationAccountRegistry(1, 8)
	require.NoError(t, err)
	account, err := registry.Open(current + fallback)
	require.NoError(t, err)
	state := aggState{}
	t.Cleanup(func() {
		if state.argbuf != nil {
			mp.Free(state.argbuf)
			state.argbuf = nil
		}
		account.Seal()
		_, finalizeErr := registry.Finalize(account)
		require.NoError(t, finalizeErr)
		require.Zero(t, mp.CurrNB())
	})
	allocation, err := NewAllocationAccount(
		account, mpool.AllocationOwnerGroup, AllocationAccountSites{
			VectorData: 1, VectorArea: 2, VectorNulls: 3,
			VectorGrouping: 4, ArgumentCount: 5, ArgumentArena: 6,
		})
	require.NoError(t, err)

	buf, err := allocation.allocArgumentArena(mp, current)
	require.NoError(t, err)
	state = aggState{
		allocation: allocation,
		argbuf:     buf,
		argSkl:     arenaskl.NewSkiplist(arenaskl.NewArena(buf), bytes.Compare),
	}

	require.NoError(t, state.argSkl.Add([]byte("kept"), nil))
	used := uint64(state.argSkl.Arena().Size())
	require.Less(t, used, uint64(current+1))
	require.NoError(t, state.preflightArgumentCapacity(
		mp, uint64(current+1)-used, 0))
	require.Equal(t, int(fallback), len(state.argbuf))
	require.True(t, state.argSkl.Contains([]byte("kept")))
	require.Equal(t, fallback, account.Snapshot().Used)
}

func TestAccountedArgumentGrowthFailurePreservesPublishedState(t *testing.T) {
	const capacity = 16 << 10

	mp := mpool.MustNewZero()
	registry, err := mpool.NewAllocationAccountRegistry(1, 8)
	require.NoError(t, err)
	account, err := registry.Open(capacity)
	require.NoError(t, err)
	allocation, err := NewAllocationAccount(
		account, mpool.AllocationOwnerGroup, AllocationAccountSites{
			VectorData: 1, VectorArea: 2, VectorNulls: 3,
			VectorGrouping: 4, ArgumentCount: 5, ArgumentArena: 6,
		})
	require.NoError(t, err)

	buf, err := allocation.allocArgumentArena(mp, capacity)
	require.NoError(t, err)
	state := aggState{
		allocation: allocation,
		argbuf:     buf,
		argSkl:     arenaskl.NewSkiplist(arenaskl.NewArena(buf), bytes.Compare),
	}
	defer func() {
		state.free(mp)
		account.Seal()
		_, finalizeErr := registry.Finalize(account)
		require.NoError(t, finalizeErr)
		require.Zero(t, mp.CurrNB())
	}()

	var inserter arenaskl.Inserter
	require.NoError(t,
		state.insertArgValueWithInserter(mp, []byte("kept"), nil, &inserter))
	originalArena := state.argSkl.Arena()
	originalSize := originalArena.Size()

	// The account is exactly full, so a key that requires growth must fail
	// before arena publication. The existing arena and the caller's cached
	// inserter must both remain usable after that rejection.
	err = state.insertArgValueWithInserter(
		mp, make([]byte, capacity), nil, &inserter)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
	require.Same(t, originalArena, state.argSkl.Arena())
	require.Equal(t, originalSize, state.argSkl.Arena().Size())
	require.True(t, state.argSkl.Contains([]byte("kept")))
	require.Equal(t, uint64(capacity), account.Snapshot().Used)

	require.NoError(t,
		state.insertArgValueWithInserter(mp, []byte("kept-next"), nil, &inserter))
	require.True(t, state.argSkl.Contains([]byte("kept-next")))
}

func TestConcreteAggregatePreflightsRejectOversizedWorkUnits(t *testing.T) {
	tests := []aggregateAllocationTestCase{
		{name: "any", id: AggIdOfAny, params: []types.Type{types.T_varchar.ToType()}},
		{name: "bit-fixed", id: AggIdOfBitAnd, params: []types.Type{types.T_int64.ToType()}},
		{name: "bit-bytes", id: AggIdOfBitOr, params: []types.Type{types.T_varbinary.ToType()}},
		{name: "min-fixed", id: AggIdOfMin, params: []types.Type{types.T_int64.ToType()}},
		{name: "max-by", id: AggIdOfMaxBy, params: []types.Type{
			types.T_varchar.ToType(), types.T_int64.ToType(), types.T_int64.ToType(),
		}},
		{name: "group-concat", id: AggIdOfGroupConcat, params: []types.Type{types.T_varchar.ToType()}},
		{name: "bitmap-construct", id: AggIdOfBitmapConstruct, params: []types.Type{types.T_uint64.ToType()}},
		{name: "bitmap-or", id: AggIdOfBitmapOr, params: []types.Type{types.T_varbinary.ToType()}},
		{name: "json-array", id: AggIdOfJsonArrayAgg, params: []types.Type{types.T_json.ToType()}},
		{name: "json-object", id: AggIdOfJsonObjectAgg, params: []types.Type{
			types.T_varchar.ToType(), types.T_json.ToType(),
		}},
		{name: "sum-distinct", id: AggIdOfSum, distinct: true, params: []types.Type{types.T_int64.ToType()}},
		{name: "count-column", id: AggIdOfCountColumn, params: []types.Type{types.T_int64.ToType()}},
		{name: "count-distinct", id: AggIdOfCountColumn, distinct: true, params: []types.Type{types.T_int64.ToType()}},
		{name: "median", id: AggIdOfMedian, params: []types.Type{types.T_int64.ToType()}},
		{name: "percentile-cont", id: AggIdOfPercentileCont, params: []types.Type{types.T_int64.ToType()}},
		{name: "percentile-disc", id: AggIdOfPercentileDisc, params: []types.Type{types.T_int64.ToType()}},
		{name: "approx-percentile", id: AggIdOfApproxPercentile, params: []types.Type{types.T_int64.ToType()}},
		{name: "approx-count", id: AggIdOfApproxCount, params: []types.Type{types.T_int64.ToType()}},
		{name: "hll-add", id: AggIdOfHllAdd, params: []types.Type{types.T_int64.ToType()}},
	}
	groups := make([]uint64, hashmap.UnitLimit+1)
	buildOneRow := func(t *testing.T, mp *mpool.MPool, params []types.Type) []*vector.Vector {
		t.Helper()
		vectors := make([]*vector.Vector, len(params))
		for i, param := range params {
			vec := vector.NewVec(param)
			switch param.Oid {
			case types.T_int64:
				require.NoError(t, vector.AppendFixed(vec, int64(1), false, mp))
			case types.T_uint64:
				require.NoError(t, vector.AppendFixed(vec, uint64(1), false, mp))
			case types.T_varchar, types.T_varbinary:
				require.NoError(t, vector.AppendBytes(vec, []byte("one-row-value"), false, mp))
			case types.T_json:
				value, err := bytejson.CreateByteJSONWithCheck(int64(1))
				require.NoError(t, err)
				encoded, err := value.Marshal()
				require.NoError(t, err)
				require.NoError(t, vector.AppendBytes(vec, encoded, false, mp))
			default:
				t.Fatalf("unsupported preflight test type %s", param)
			}
			vectors[i] = vec
		}
		return vectors
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			registry, account, allocation := newTestAggregateAllocation(t)
			var extra any
			if tc.id == AggIdOfPercentileCont || tc.id == AggIdOfPercentileDisc ||
				tc.id == AggIdOfApproxPercentile {
				extra = []byte("0.5")
			}
			exec, err := MakeGroupAgg(mp, tc.id, tc.distinct, allocation, extra, tc.params...)
			require.NoError(t, err)
			SyncAggregatorsToChunkSize([]AggFuncExec{exec}, AggBatchSize)
			preflight := exec.(BatchCapacityPreflight)
			require.Error(t, preflight.PreflightBatchFill(0, groups, nil))
			require.Error(t, preflight.PreflightBatchMerge(exec, 0, groups))
			require.NoError(t, exec.GroupGrow(1))
			invalidVectors := make([]*vector.Vector, len(tc.params))
			fillErr := preflight.PreflightBatchFill(-1, []uint64{1}, invalidVectors)
			if tc.name == "bit-fixed" || tc.name == "count-column" {
				require.NoError(t, fillErr)
			} else {
				require.Error(t, fillErr)
			}
			mergeErr := preflight.PreflightBatchMerge(exec, -1, []uint64{1})
			if tc.name == "bit-fixed" || tc.name == "count-column" {
				require.NoError(t, mergeErr)
			} else {
				require.Error(t, mergeErr)
			}
			vectors := buildOneRow(t, mp, tc.params)
			invalidGroup := uint64(AggBatchSize + 1)
			invalidTargetFill := preflight.PreflightBatchFill(0, []uint64{invalidGroup}, vectors)
			invalidTargetMerge := preflight.PreflightBatchMerge(exec, 0, []uint64{invalidGroup})
			if tc.name == "bit-fixed" || tc.name == "count-column" {
				require.NoError(t, invalidTargetFill)
			} else {
				require.Error(t, invalidTargetFill)
			}
			if tc.name == "bit-fixed" || tc.name == "count-column" ||
				tc.name == "min-fixed" || tc.name == "max-by" ||
				tc.name == "bitmap-construct" || tc.name == "bitmap-or" ||
				tc.name == "approx-percentile" {
				require.NoError(t, invalidTargetMerge)
			} else {
				require.Error(t, invalidTargetMerge)
			}
			for _, vec := range vectors {
				vec.Free(mp)
			}
			exec.Free()
			require.NoError(t, exec.ClearAllocationAccount(allocation))
			finishTestAggregateAllocation(t, registry, account)
			require.Zero(t, mp.CurrNB())
		})
	}
}

func TestConcreteAggregatePreflightWinnerAndMergePaths(t *testing.T) {
	long := strings.Repeat("x", types.VarlenaInlineSize+8)
	jsonValue := func(t *testing.T, value any) []byte {
		t.Helper()
		bj, err := bytejson.CreateByteJSONWithCheck(value)
		require.NoError(t, err)
		data, err := bj.Marshal()
		require.NoError(t, err)
		return data
	}
	tests := []struct {
		name     string
		id       int64
		distinct bool
		extra    any
		params   []types.Type
		build    func(*testing.T, *mpool.MPool) []*vector.Vector
	}{
		{
			name: "bit-fixed", id: AggIdOfBitAnd,
			params: []types.Type{types.T_int64.ToType()},
			build: func(t *testing.T, mp *mpool.MPool) []*vector.Vector {
				return []*vector.Vector{buildFixedVec(t, mp,
					types.T_int64.ToType(), []int64{0, 7, 3, 1})}
			},
		},
		{
			name: "sum-distinct", id: AggIdOfSum, distinct: true,
			params: []types.Type{types.T_int64.ToType()},
			build: func(t *testing.T, mp *mpool.MPool) []*vector.Vector {
				return []*vector.Vector{buildFixedVec(t, mp,
					types.T_int64.ToType(), []int64{0, 7, 7, 3})}
			},
		},
		{
			name: "avg-distinct", id: AggIdOfAvg, distinct: true,
			params: []types.Type{types.T_float64.ToType()},
			build: func(t *testing.T, mp *mpool.MPool) []*vector.Vector {
				return []*vector.Vector{buildFixedVec(t, mp,
					types.T_float64.ToType(), []float64{0, 7, 7, 3})}
			},
		},
		{
			name: "count-column", id: AggIdOfCountColumn,
			params: []types.Type{types.T_int64.ToType()},
			build: func(t *testing.T, mp *mpool.MPool) []*vector.Vector {
				return []*vector.Vector{buildFixedVec(t, mp,
					types.T_int64.ToType(), []int64{0, 7, 7, 3})}
			},
		},
		{
			name: "count-column-distinct", id: AggIdOfCountColumn, distinct: true,
			params: []types.Type{types.T_int64.ToType()},
			build: func(t *testing.T, mp *mpool.MPool) []*vector.Vector {
				return []*vector.Vector{buildFixedVec(t, mp,
					types.T_int64.ToType(), []int64{0, 7, 7, 3})}
			},
		},
		{
			name: "median", id: AggIdOfMedian,
			params: []types.Type{types.T_int64.ToType()},
			build: func(t *testing.T, mp *mpool.MPool) []*vector.Vector {
				return []*vector.Vector{buildFixedVec(t, mp,
					types.T_int64.ToType(), []int64{0, 7, 3, 1})}
			},
		},
		{
			name: "percentile-cont", id: AggIdOfPercentileCont, extra: []byte("0.5"),
			params: []types.Type{types.T_int64.ToType()},
			build: func(t *testing.T, mp *mpool.MPool) []*vector.Vector {
				return []*vector.Vector{buildFixedVec(t, mp,
					types.T_int64.ToType(), []int64{0, 7, 3, 1})}
			},
		},
		{
			name: "percentile-disc", id: AggIdOfPercentileDisc, extra: []byte("0.5"),
			params: []types.Type{types.T_int64.ToType()},
			build: func(t *testing.T, mp *mpool.MPool) []*vector.Vector {
				return []*vector.Vector{buildFixedVec(t, mp,
					types.T_int64.ToType(), []int64{0, 7, 3, 1})}
			},
		},
		{
			name: "approx-percentile", id: AggIdOfApproxPercentile, extra: []byte("0.5"),
			params: []types.Type{types.T_int64.ToType()},
			build: func(t *testing.T, mp *mpool.MPool) []*vector.Vector {
				return []*vector.Vector{buildFixedVec(t, mp,
					types.T_int64.ToType(), []int64{0, 7, 3, 1})}
			},
		},
		{
			name: "approx-count", id: AggIdOfApproxCount,
			params: []types.Type{types.T_int64.ToType()},
			build: func(t *testing.T, mp *mpool.MPool) []*vector.Vector {
				return []*vector.Vector{buildFixedVec(t, mp,
					types.T_int64.ToType(), []int64{0, 7, 3, 1})}
			},
		},
		{
			name: "approx-count-distinct", id: AggIdOfApproxCountDistinct,
			params: []types.Type{types.T_int64.ToType()},
			build: func(t *testing.T, mp *mpool.MPool) []*vector.Vector {
				return []*vector.Vector{buildFixedVec(t, mp,
					types.T_int64.ToType(), []int64{0, 7, 3, 1})}
			},
		},
		{
			name: "hll-add", id: AggIdOfHllAdd,
			params: []types.Type{types.T_int64.ToType()},
			build: func(t *testing.T, mp *mpool.MPool) []*vector.Vector {
				return []*vector.Vector{buildFixedVec(t, mp,
					types.T_int64.ToType(), []int64{0, 7, 3, 1})}
			},
		},
		{
			name: "avg-tw-cache", id: AggIdOfAvgTwCache,
			params: []types.Type{types.T_int64.ToType()},
			build: func(t *testing.T, mp *mpool.MPool) []*vector.Vector {
				return []*vector.Vector{buildFixedVec(t, mp,
					types.T_int64.ToType(), []int64{0, 7, 3, 1})}
			},
		},
		{
			name: "any-varlen", id: AggIdOfAny,
			params: []types.Type{types.T_varchar.ToType()},
			build: func(t *testing.T, mp *mpool.MPool) []*vector.Vector {
				vec := buildVarlenVec(t, mp, types.T_varchar.ToType(),
					[]string{"ignored", long + "-a", long + "-b", "tail"})
				return []*vector.Vector{vec}
			},
		},
		{
			name: "bit-bytes", id: AggIdOfBitOr,
			params: []types.Type{types.T_varbinary.ToType()},
			build: func(t *testing.T, mp *mpool.MPool) []*vector.Vector {
				vec := vector.NewVec(types.T_varbinary.ToType())
				for _, value := range [][]byte{{0}, {1, 2}, {2, 1}, {3, 3}} {
					require.NoError(t, vector.AppendBytes(vec, value, false, mp))
				}
				return []*vector.Vector{vec}
			},
		},
		{
			name: "min-fixed", id: AggIdOfMin,
			params: []types.Type{types.T_int64.ToType()},
			build: func(t *testing.T, mp *mpool.MPool) []*vector.Vector {
				return []*vector.Vector{buildFixedVec(t, mp,
					types.T_int64.ToType(), []int64{99, 7, 3, 5})}
			},
		},
		{
			name: "min-varlen", id: AggIdOfMin,
			params: []types.Type{types.T_varchar.ToType()},
			build: func(t *testing.T, mp *mpool.MPool) []*vector.Vector {
				return []*vector.Vector{buildVarlenVec(t, mp,
					types.T_varchar.ToType(), []string{"ignored", long + "-z", long + "-a", "tail"})}
			},
		},
		{
			name: "max-by", id: AggIdOfMaxBy,
			params: []types.Type{
				types.T_varchar.ToType(), types.T_int64.ToType(), types.T_int64.ToType(),
			},
			build: func(t *testing.T, mp *mpool.MPool) []*vector.Vector {
				return []*vector.Vector{
					buildVarlenVec(t, mp, types.T_varchar.ToType(),
						[]string{"ignored", long + "-low", long + "-high", "tail"}),
					buildFixedVec(t, mp, types.T_int64.ToType(), []int64{0, 1, 2, 3}),
					buildFixedVec(t, mp, types.T_int64.ToType(), []int64{0, 1, 1, 1}),
				}
			},
		},
		{
			name: "group-concat", id: AggIdOfGroupConcat,
			params: []types.Type{types.T_varchar.ToType()},
			build: func(t *testing.T, mp *mpool.MPool) []*vector.Vector {
				return []*vector.Vector{buildVarlenVec(t, mp,
					types.T_varchar.ToType(), []string{"ignored", "a", "b", "c"})}
			},
		},
		{
			name: "bitmap-construct", id: AggIdOfBitmapConstruct,
			params: []types.Type{types.T_uint64.ToType()},
			build: func(t *testing.T, mp *mpool.MPool) []*vector.Vector {
				return []*vector.Vector{buildFixedVec(t, mp,
					types.T_uint64.ToType(), []uint64{0, 1, 2, 3})}
			},
		},
		{
			name: "json-array", id: AggIdOfJsonArrayAgg,
			params: []types.Type{types.T_json.ToType()},
			build: func(t *testing.T, mp *mpool.MPool) []*vector.Vector {
				vec := vector.NewVec(types.T_json.ToType())
				for _, value := range []any{int64(0), long + "-a", long + "-b", nil} {
					require.NoError(t, vector.AppendBytes(vec, jsonValue(t, value), false, mp))
				}
				return []*vector.Vector{vec}
			},
		},
		{
			name: "json-object", id: AggIdOfJsonObjectAgg,
			params: []types.Type{types.T_varchar.ToType(), types.T_json.ToType()},
			build: func(t *testing.T, mp *mpool.MPool) []*vector.Vector {
				keys := buildVarlenVec(t, mp, types.T_varchar.ToType(),
					[]string{"ignored", "a", "b", "c"})
				values := vector.NewVec(types.T_json.ToType())
				for _, value := range []any{int64(0), long + "-a", long + "-b", nil} {
					require.NoError(t, vector.AppendBytes(values, jsonValue(t, value), false, mp))
				}
				return []*vector.Vector{keys, values}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			registry, account, allocation := newTestAggregateAllocation(t)
			makeExec := func() GroupAggFuncExec {
				exec, err := MakeGroupAgg(
					mp, tc.id, tc.distinct, allocation, tc.extra, tc.params...)
				require.NoError(t, err)
				require.NoError(t, exec.GroupGrow(2))
				return exec
			}
			left, right, mergeTarget := makeExec(), makeExec(), makeExec()
			vectors := tc.build(t, mp)
			groups := []uint64{GroupNotMatched, 1, 1, 2}
			for _, exec := range []GroupAggFuncExec{left, right} {
				preflight := exec.(BatchCapacityPreflight)
				require.NoError(t, preflight.PreflightBatchFill(0, nil, vectors))
				require.NoError(t, preflight.PreflightBatchFill(0, groups, vectors))
				require.NoError(t, exec.BatchFill(0, groups, vectors))
				// Re-admitting the same candidates exercises the existing-winner,
				// duplicate, and already-sized capacity paths without publishing a
				// second update.
				require.NoError(t, preflight.PreflightBatchFill(0, groups, vectors))
				vectors[0].SetNull(3)
				preflightErr := preflight.PreflightBatchFill(3, []uint64{2}, vectors)
				if tc.id == AggIdOfJsonObjectAgg {
					require.Error(t, preflightErr)
				} else {
					require.NoError(t, preflightErr)
				}
				vectors[0].GetNulls().Del(3)
			}
			preflight := mergeTarget.(BatchCapacityPreflight)
			require.NoError(t, preflight.PreflightBatchMerge(right, 0, nil))
			require.NoError(t, preflight.PreflightBatchMerge(right, 0, []uint64{1, 1}))
			require.NoError(t, mergeTarget.BatchMerge(right, 0, []uint64{1, 1}))
			require.NoError(t, preflight.PreflightBatchMerge(right, 0, []uint64{1, 1}))

			for _, vec := range vectors {
				vec.Free(mp)
			}
			for _, exec := range []GroupAggFuncExec{left, right, mergeTarget} {
				exec.Free()
				require.NoError(t, exec.ClearAllocationAccount(allocation))
			}
			finishTestAggregateAllocation(t, registry, account)
			require.Zero(t, mp.CurrNB())
		})
	}
}

func TestArgumentArenaPreflightMigratesRetainedRows(t *testing.T) {
	mp := mpool.MustNewZero()
	registry, account, allocation := newTestAggregateAllocation(t)
	exec, err := MakeGroupAgg(
		mp, AggIdOfGroupConcat, false, allocation, nil, types.T_varchar.ToType())
	require.NoError(t, err)
	SyncAggregatorsToChunkSize([]AggFuncExec{exec}, AggBatchSize)
	require.NoError(t, exec.GroupGrow(1))

	input := vector.NewVec(types.T_varchar.ToType())
	// GroupGrow reserves the regular 512 KiB argument arena. Cross that
	// boundary so the test proves migration preserves already-published rows,
	// rather than only exercising the in-place admission path.
	const rows = hashmap.UnitLimit * 6
	for row := 0; row < rows; row++ {
		value := strings.Repeat(string(rune('a'+row%26)), 512)
		require.NoError(t, vector.AppendBytes(input, []byte(value), false, mp))
	}
	groups := make([]uint64, hashmap.UnitLimit)
	for i := range groups {
		groups[i] = 1
	}
	for offset := 0; offset < input.Length(); offset += hashmap.UnitLimit {
		require.NoError(t, exec.PreflightBatchFill(offset, groups, []*vector.Vector{input}))
		require.NoError(t, exec.BatchFill(offset, groups, []*vector.Vector{input}))
	}
	base := exec.(aggregateBaseCarrier).aggregateBase()
	require.Equal(t, uint32(rows), base.state[0].argCnt[0])
	require.Greater(t, len(base.state[0].argbuf), kAggArgArenaSize)

	input.Free(mp)
	exec.Free()
	require.NoError(t, exec.ClearAllocationAccount(allocation))
	finishTestAggregateAllocation(t, registry, account)
	require.Zero(t, mp.CurrNB())
}

func TestAccountedOrderedGroupConcatPreflightAndResult(t *testing.T) {
	for _, distinct := range []bool{false, true} {
		t.Run(map[bool]string{false: "all", true: "distinct"}[distinct], func(t *testing.T) {
			mp := mpool.MustNewZero()
			registry, account, allocation := newTestAggregateAllocation(t)
			info := multiAggInfo{
				aggID:     AggIdOfGroupConcat,
				distinct:  distinct,
				argTypes:  []types.Type{types.T_varchar.ToType(), types.T_int64.ToType()},
				retType:   types.T_text.ToType(),
				emptyNull: true,
			}
			exec := newGroupConcatExec(mp, info, ",").(*groupConcatExec)
			require.NoError(t, exec.SetAllocationAccount(allocation))
			require.NoError(t, exec.SetExtraInformation(
				testGroupConcatOrderConfig(1, []byte{groupConcatOrderAsc | groupConcatOrderNullsLast}, "|"),
				0,
			))
			SyncAggregatorsToChunkSize([]AggFuncExec{exec}, AggBatchSize)
			require.NoError(t, exec.GroupGrow(2))

			values := buildVarlenVec(t, mp, types.T_varchar.ToType(),
				[]string{"b", "a", "a", "c", "ignored"})
			values.SetNull(4)
			orderKeys := vector.NewVec(types.T_int64.ToType())
			require.NoError(t, vector.AppendFixedList(orderKeys,
				[]int64{2, 1, 3, 0, 4},
				[]bool{false, false, false, true, false}, mp))
			vectors := []*vector.Vector{values, orderKeys}
			groups := []uint64{1, 1, 1, 2, 2}

			require.NoError(t, exec.PreflightBatchFill(0, groups, vectors))
			require.NoError(t, exec.BatchFill(0, groups, vectors))
			// A second preflight must account only for candidates that would be
			// newly published; DISTINCT suppresses the repeated concat value even
			// though its order key differs.
			require.NoError(t, exec.PreflightBatchFill(0, groups, vectors))

			result, err := exec.FlushWithContext(context.Background())
			require.NoError(t, err)
			want := "a|b|a"
			if distinct {
				want = "a|b"
			}
			require.Equal(t, want, string(result[0].GetBytesAt(0)))
			require.Equal(t, "c", string(result[0].GetBytesAt(1)))
			result[0].Free(mp)
			values.Free(mp)
			orderKeys.Free(mp)
			exec.Free()
			require.NoError(t, exec.ClearAllocationAccount(allocation))
			finishTestAggregateAllocation(t, registry, account)
			require.Zero(t, mp.CurrNB())
		})
	}
}
