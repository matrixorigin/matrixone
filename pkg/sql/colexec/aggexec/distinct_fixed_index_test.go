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
	"encoding/binary"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/arenaskl"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/stretchr/testify/require"
)

func fixedIndexKey(group uint16, value uint64) []byte {
	key := make([]byte, kAggArgPrefixSz+8)
	binary.BigEndian.PutUint16(key, group)
	binary.LittleEndian.PutUint64(key[kAggArgPrefixSz:], value)
	return key
}

func TestDistinctFixedIndexExactMembershipAndGrowth(t *testing.T) {
	mp := mpool.MustNewZero()
	index := distinctFixedIndex{}

	const count = distinctFixedIndexInitialSlots * 3
	for i := 0; i < count; i++ {
		key := fixedIndexKey(uint16(i%7), uint64(i/7))
		group, value, ok := decodeDistinctFixedKey(key, 8)
		require.True(t, ok)
		duplicate, err := index.prepare(mp, nil, group, value)
		require.NoError(t, err)
		require.False(t, duplicate)
		require.NoError(t, index.insert(group, value))
	}

	require.GreaterOrEqual(t, len(index.slotKeys), count)
	require.Equal(t, len(index.slotKeys), len(index.slotGroups))
	require.Equal(t, uint32(count), index.count)
	for i := 0; i < count; i++ {
		key := fixedIndexKey(uint16(i%7), uint64(i/7))
		group, value, ok := decodeDistinctFixedKey(key, 8)
		require.True(t, ok)
		require.True(t, index.lookup(group, value))
		duplicate, err := index.prepare(mp, nil, group, value)
		require.NoError(t, err)
		require.True(t, duplicate)
	}
	require.False(t, index.lookup(0, ^uint64(0)))

	index.free(mp)
	require.Empty(t, index.slotKeys)
	require.Empty(t, index.slotGroups)
}

func TestDistinctFixedIndexDoesNotChangeSkiplistDuplicateContract(t *testing.T) {
	mp := mpool.MustNewZero()
	buf, err := mp.Alloc(16*1024, true)
	require.NoError(t, err)
	list := arenaskl.NewSkiplist(arenaskl.NewArena(buf), bytes.Compare)
	require.NoError(t, list.Add(fixedIndexKey(1, 42), nil))
	require.ErrorIs(t, list.Add(fixedIndexKey(1, 42), nil), arenaskl.ErrRecordExists)
	mp.Free(buf)
}

func TestDistinctFixedIndexMergeIntoDeduplicatesPerGroup(t *testing.T) {
	mp := mpool.MustNewZero()
	source := distinctFixedIndex{groupLimit: 4}
	target := distinctFixedIndex{groupLimit: 4}
	insert := func(index *distinctFixedIndex, group uint16, value uint64) {
		t.Helper()
		duplicate, err := index.prepare(mp, nil, group, value)
		require.NoError(t, err)
		if duplicate {
			return
		}
		require.NoError(t, index.insert(group, value))
	}

	insert(&source, 1, 10)
	insert(&source, 1, 20)
	insert(&source, 2, 30)
	insert(&target, 3, 20)

	added, err := target.mergeInto(mp, nil, 3, &source, 1)
	require.NoError(t, err)
	require.Equal(t, uint32(1), added,
		"the value already present in the destination group is a duplicate")
	added, err = target.mergeInto(mp, nil, 3, &source, 2)
	require.NoError(t, err)
	require.Equal(t, uint32(1), added,
		"a value from another source group is new for the destination")

	var got []uint64
	require.NoError(t, target.forEach(3, func(value uint64) error {
		got = append(got, value)
		return nil
	}))
	require.ElementsMatch(t, []uint64{10, 20, 30}, got)
	target.free(mp)
	source.free(mp)
}

func TestDistinctFixedIndexProbeStopsOnFullMalformedTable(t *testing.T) {
	// A valid table is kept below the 70% load factor.  A corrupt restored state
	// can violate that invariant, so a miss must still terminate instead of
	// looping around a full table forever.
	const slots = 16
	index := distinctFixedIndex{
		slotKeys:   make([]uint64, slots),
		slotGroups: make([]uint16, slots),
		groupHead:  make([]int32, 1),
		logKeys:    make([]uint64, slots),
		logNext:    make([]int32, slots),
		groupLimit: 1,
	}
	for i := range index.slotGroups {
		index.slotGroups[i] = 0
	}
	require.False(t, index.lookup(0, 99))
	require.ErrorIs(t, index.insert(0, 99), mpool.ErrAllocationAccountInvariant)
}
