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

package hashtable

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/stretchr/testify/require"
)

func TestEstimateHashMapSizeFollowsRuntimeGrowth(t *testing.T) {
	require.Equal(t, uint64(16*1024), EstimateInt64HashMapSize(0))
	require.Equal(t, uint64(16*1024), EstimateInt64HashMapSize(512))
	require.Equal(t, uint64(32*1024), EstimateInt64HashMapSize(513))

	require.Equal(t, uint64(32*1024), EstimateStringHashMapSize(0))
	require.Equal(t, uint64(32*1024), EstimateStringHashMapSize(512))
	require.Equal(t, uint64(64*1024), EstimateStringHashMapSize(513))

	require.Equal(t, ^uint64(0), EstimateInt64HashMapSize(^uint64(0)))
	require.Equal(t, ^uint64(0), EstimateStringHashMapSize(^uint64(0)))
}

func TestInt64HashMapSegmentedGrowthKeepsExistingBlocks(t *testing.T) {
	mp := mpool.MustNewZero()
	ht := new(Int64HashMap)
	require.NoError(t, ht.Init(mp))

	firstTarget := maxElemCnt(maxIntCellCntPerBlock*2, intCellSize)
	require.NoError(t, ht.ResizeOnDemand(int(firstTarget)))
	require.Len(t, ht.cells, 2)
	require.Equal(t, maxIntCellCntPerBlock, ht.blockCellCnt)

	hashes := []uint64{
		1,
		maxIntCellCntPerBlock + 1,
	}
	values := make([]uint64, len(hashes))
	require.NoError(t, ht.InsertBatch(len(hashes), hashes, nil, values))
	firstBlock := &ht.cells[0][0]

	secondTarget := maxElemCnt(maxIntCellCntPerBlock*4, intCellSize)
	require.NoError(t, ht.ResizeOnDemand(int(secondTarget-ht.elemCnt)))
	require.Len(t, ht.cells, 4)
	require.True(t, firstBlock == &ht.cells[0][0])

	found := make([]uint64, len(hashes))
	ht.FindBatch(len(hashes), hashes, nil, found)
	require.Equal(t, values, found)

	cellsAt64MiB := uint64(64*MB) / intCellSize
	maxAt32MiB := maxElemCnt(cellsAt64MiB/2, intCellSize)
	require.NoError(t, ht.ResizeOnDemand(int(maxAt32MiB-ht.elemCnt)))
	require.Equal(t, cellsAt64MiB/2, ht.cellCnt)
	require.NoError(t, ht.ResizeOnDemand(int(maxAt32MiB+1-ht.elemCnt)))
	require.Equal(t, cellsAt64MiB, ht.cellCnt)

	// At 64 MiB the total-table load factor becomes 2/3. Segmenting must not
	// accidentally keep applying the 4 MiB block's 1/2 load factor.
	aboveHalfAt64MiB := cellsAt64MiB/2 + 1
	require.NoError(t, ht.ResizeOnDemand(int(aboveHalfAt64MiB-ht.elemCnt)))
	require.Equal(t, cellsAt64MiB, ht.cellCnt)

	ht.Free()
	require.Zero(t, mp.CurrNB())
}

func TestStringHashMapSegmentedGrowthKeepsExistingBlocks(t *testing.T) {
	mp := mpool.MustNewZero()
	ht := new(StringHashMap)
	require.NoError(t, ht.Init(mp))

	firstTarget := maxElemCnt(maxStrCellCntPerBlock*2, strCellSize)
	require.NoError(t, ht.ResizeOnDemand(firstTarget))
	require.Len(t, ht.cells, 2)
	require.Equal(t, maxStrCellCntPerBlock, ht.blockCellCnt)

	keys := [][]byte{[]byte("first"), []byte("second")}
	states := make([][3]uint64, len(keys))
	values := make([]uint64, len(keys))
	require.NoError(t, ht.InsertStringBatch(states, keys, values))
	firstBlock := &ht.cells[0][0]

	secondTarget := maxElemCnt(maxStrCellCntPerBlock*4, strCellSize)
	require.NoError(t, ht.ResizeOnDemand(secondTarget-ht.elemCnt))
	require.Len(t, ht.cells, 4)
	require.True(t, firstBlock == &ht.cells[0][0])

	found := make([]uint64, len(keys))
	ht.FindStringBatch(states, keys, found)
	require.Equal(t, values, found)

	cellsAt64MiB := uint64(64*MB) / strCellSize
	maxAt32MiB := maxElemCnt(cellsAt64MiB/2, strCellSize)
	require.NoError(t, ht.ResizeOnDemand(maxAt32MiB-ht.elemCnt))
	require.Equal(t, cellsAt64MiB/2, ht.cellCnt)
	require.NoError(t, ht.ResizeOnDemand(maxAt32MiB+1-ht.elemCnt))
	require.Equal(t, cellsAt64MiB, ht.cellCnt)

	// At 64 MiB the total-table load factor becomes 2/3. Segmenting must not
	// accidentally keep applying the 4 MiB block's 1/2 load factor.
	aboveHalfAt64MiB := cellsAt64MiB/2 + 1
	require.NoError(t, ht.ResizeOnDemand(aboveHalfAt64MiB-ht.elemCnt))
	require.Equal(t, cellsAt64MiB, ht.cellCnt)

	ht.Free()
	require.Zero(t, mp.CurrNB())
}
