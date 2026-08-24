// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package disttae

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

func testBlockID(block uint16) types.Blockid {
	var objectID types.Objectid
	return types.NewBlockidWithObjectID(&objectID, block)
}

func testTombstoneStats(minBlock, maxBlock uint16) objectio.ObjectStats {
	var objectID types.Objectid
	minRow := types.NewRowIDWithObjectIDBlkNumAndRowID(objectID, minBlock, 0)
	maxRow := types.NewRowIDWithObjectIDBlkNumAndRowID(objectID, maxBlock, 100)
	zm := index.NewZM(types.T_Rowid, 0)
	if err := zm.Update(minRow); err != nil {
		panic(err)
	}
	if err := zm.Update(maxRow); err != nil {
		panic(err)
	}
	stats := objectio.NewObjectStats()
	if err := objectio.SetObjectStatsSortKeyZoneMap(stats, zm); err != nil {
		panic(err)
	}
	return *stats
}

func TestTombstoneObjectIndexSelectCandidates(t *testing.T) {
	objects := []objectio.ObjectStats{
		testTombstoneStats(10, 20),
		testTombstoneStats(0, 5),
		testTombstoneStats(15, 30),
		{},
	}
	idx := newTombstoneObjectIndex(objects)
	var scratch []int

	tests := []struct {
		block uint16
		want  []int
	}{
		{block: 3, want: []int{1, 3}},
		{block: 12, want: []int{0, 3}},
		{block: 17, want: []int{0, 2, 3}},
		{block: 31, want: []int{3}},
	}
	for _, test := range tests {
		block := testBlockID(test.block)
		scratch = idx.selectCandidates(&block, scratch)
		require.Equal(t, test.want, scratch)
	}
}

func TestTombstoneObjectIndexMatchesExhaustiveScan(t *testing.T) {
	objects := make([]objectio.ObjectStats, 0, 701)
	for n := range 700 {
		minBlock := uint16((n * 17) % 1000)
		maxBlock := minBlock + uint16(n%23)
		objects = append(objects, testTombstoneStats(minBlock, maxBlock))
	}
	objects = append(objects, objectio.ObjectStats{})
	idx := newTombstoneObjectIndex(objects)
	var scratch []int

	for blockNumber := range 1100 {
		block := testBlockID(uint16(blockNumber))
		var exhaustive []int
		for ordinal := range objects {
			zm := objects[ordinal].SortKeyZoneMap()
			if !zm.IsInited() || zm.RowidPrefixEq(block[:]) {
				exhaustive = append(exhaustive, ordinal)
			}
		}
		scratch = idx.selectCandidates(&block, scratch)
		require.True(t, slices.Equal(exhaustive, scratch), "block %d", blockNumber)
	}
}

func TestIndexedObjectStatsIter(t *testing.T) {
	objects := []objectio.ObjectStats{
		testTombstoneStats(0, 5),
		testTombstoneStats(10, 20),
		testTombstoneStats(30, 40),
	}
	idx := newTombstoneObjectIndex(objects)
	iter := indexedObjectStatsIter{
		index:      &idx,
		candidates: []int{2, 0},
	}

	stats, err := iter.next()
	require.NoError(t, err)
	require.Same(t, &idx.objects[2], stats)

	stats, err = iter.next()
	require.NoError(t, err)
	require.Same(t, &idx.objects[0], stats)

	stats, err = iter.next()
	require.NoError(t, err)
	require.Nil(t, stats)
}

func BenchmarkTombstoneObjectIndexQA(b *testing.B) {
	objects := make([]objectio.ObjectStats, 700)
	for n := range objects {
		minBlock := uint16((n * 17) % 4000)
		objects[n] = testTombstoneStats(minBlock, minBlock+uint16(n%11))
	}
	blocks := make([]types.Blockid, 132)
	for n := range blocks {
		blocks[n] = testBlockID(uint16(n * 29))
	}
	idx := newTombstoneObjectIndex(objects)

	b.Run("build", func(b *testing.B) {
		for b.Loop() {
			_ = newTombstoneObjectIndex(objects)
		}
	})
	b.Run("exhaustive", func(b *testing.B) {
		var matches int
		for b.Loop() {
			for block := range blocks {
				for object := range objects {
					if objects[object].SortKeyZoneMap().RowidPrefixEq(blocks[block][:]) {
						matches++
					}
				}
			}
		}
		_ = matches
	})
	b.Run("range-index", func(b *testing.B) {
		scratch := make([]int, 0, len(objects))
		var matches int
		for b.Loop() {
			for block := range blocks {
				scratch = idx.selectCandidates(&blocks[block], scratch)
				matches += len(scratch)
			}
		}
		_ = matches
	})
}
