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
	"strconv"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/stretchr/testify/require"
)

func padStringHashMapKey(key []byte) []byte {
	if len(key) < len(StrKeyPadding) {
		key = append(key, StrKeyPadding[len(key):]...)
	}
	return key
}

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

func TestPowerOfTwoBits(t *testing.T) {
	require.Equal(t, uint8(kInitialCellCntBits), powerOfTwoBits(kInitialCellCnt))
	require.Panics(t, func() {
		powerOfTwoBits(3)
	})
}

func TestEmptySegmentedHashMapGrowth(t *testing.T) {
	t.Run("int64", func(t *testing.T) {
		mp := mpool.MustNewZero()
		ht := new(Int64HashMap)
		require.NoError(t, ht.Init(mp))

		require.NoError(t, ht.ResizeOnDemand(
			int(maxElemCnt(maxIntCellCntPerBlock*2, intCellSize)),
		))
		firstBlock := &ht.cells[0][0]
		require.NoError(t, ht.ResizeOnDemand(
			int(maxElemCnt(maxIntCellCntPerBlock*4, intCellSize)),
		))
		require.Len(t, ht.cells, 4)
		require.True(t, firstBlock == &ht.cells[0][0])
		require.Zero(t, ht.Cardinality())

		ht.Free()
		require.Zero(t, mp.CurrNB())
		mpool.DeleteMPool(mp)
	})

	t.Run("string", func(t *testing.T) {
		mp := mpool.MustNewZero()
		ht := new(StringHashMap)
		require.NoError(t, ht.Init(mp))

		require.NoError(t, ht.ResizeOnDemand(
			maxElemCnt(maxStrCellCntPerBlock*2, strCellSize),
		))
		firstBlock := &ht.cells[0][0]
		require.NoError(t, ht.ResizeOnDemand(
			maxElemCnt(maxStrCellCntPerBlock*4, strCellSize),
		))
		require.Len(t, ht.cells, 4)
		require.True(t, firstBlock == &ht.cells[0][0])
		require.Zero(t, ht.elemCnt)

		ht.Free()
		require.Zero(t, mp.CurrNB())
		mpool.DeleteMPool(mp)
	})
}

func TestInt64HashMapInPlaceRehashExhaustive(t *testing.T) {
	const (
		oldCellCnt   = uint64(8)
		newCellCnt   = uint64(32)
		blockCellCnt = uint64(4)
	)

	for first := uint64(0); first < 16; first++ {
		for second := uint64(0); second < 16; second++ {
			for third := uint64(0); third < 16; third++ {
				for fourth := uint64(0); fourth < 16; fourth++ {
					hashes := [...]uint64{first, second, third, fourth}
					if first == second || first == third || first == fourth ||
						second == third || second == fourth || third == fourth {
						continue
					}

					ht := &Int64HashMap{
						blockCellCntBits: powerOfTwoBits(blockCellCnt),
						cellCntMask:      oldCellCnt - 1,
						cellCnt:          oldCellCnt,
						cells:            make([][]Int64HashMapCell, oldCellCnt/blockCellCnt),
					}
					for i := range ht.cells {
						ht.cells[i] = make([]Int64HashMapCell, blockCellCnt)
					}
					for i, hash := range hashes {
						cell := ht.findEmptyCell(hash)
						cell.Key = hash
						cell.Mapped = uint64(i + 1)
						ht.elemCnt++
					}

					for len(ht.cells) < int(newCellCnt/blockCellCnt) {
						ht.cells = append(ht.cells, make([]Int64HashMapCell, blockCellCnt))
					}
					ht.cellCnt = newCellCnt
					ht.cellCntMask = newCellCnt - 1
					ht.rehashInPlace(oldCellCnt)

					for i, hash := range hashes {
						require.Equalf(
							t,
							uint64(i+1),
							ht.findCell(hash).Mapped,
							"hashes %v",
							hashes,
						)
					}
				}
			}
		}
	}
}

func TestInt64HashMapSegmentedGrowthKeepsExistingBlocks(t *testing.T) {
	mp := mpool.MustNewZero()
	ht := new(Int64HashMap)
	require.NoError(t, ht.Init(mp))

	firstTarget := maxElemCnt(maxIntCellCntPerBlock*2, intCellSize)
	require.NoError(t, ht.ResizeOnDemand(int(firstTarget)))
	require.Len(t, ht.cells, 2)
	require.Equal(t, maxIntCellCntPerBlock, ht.blockCellCnt())

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
	for i := range values {
		require.Equalf(t, values[i], found[i], "hash index %d", i)
	}

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
	mpool.DeleteMPool(mp)
}

func TestStringHashMapSegmentedGrowthKeepsExistingBlocks(t *testing.T) {
	mp := mpool.MustNewZero()
	ht := new(StringHashMap)
	require.NoError(t, ht.Init(mp))

	firstTarget := maxElemCnt(maxStrCellCntPerBlock*2, strCellSize)
	require.NoError(t, ht.ResizeOnDemand(firstTarget))
	require.Len(t, ht.cells, 2)
	require.Equal(t, maxStrCellCntPerBlock, ht.blockCellCnt())

	keys := [][]byte{
		padStringHashMapKey([]byte("first")),
		padStringHashMapKey([]byte("second")),
	}
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
	mpool.DeleteMPool(mp)
}

func TestInt64HashMapFailedSegmentedGrowthIsAtomic(t *testing.T) {
	testCases := []struct {
		name           string
		capacity       int64
		initialCells   uint64
		requestedCells uint64
	}{
		{
			name:           "transition to segments",
			capacity:       5 * MB,
			initialCells:   maxIntCellCntPerBlock / 2,
			requestedCells: maxIntCellCntPerBlock * 2,
		},
		{
			name:           "partial segment allocation",
			capacity:       9 * MB,
			initialCells:   maxIntCellCntPerBlock,
			requestedCells: maxIntCellCntPerBlock * 4,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			mp, err := mpool.NewMPool(t.Name(), testCase.capacity, mpool.NoFixed)
			require.NoError(t, err)
			ht := new(Int64HashMap)
			require.NoError(t, ht.Init(mp))
			t.Cleanup(func() {
				ht.Free()
				require.Zero(t, mp.CurrNB())
				mpool.DeleteMPool(mp)
			})

			require.NoError(t, ht.ResizeOnDemand(int(maxElemCnt(testCase.initialCells, intCellSize))))
			hashes := []uint64{1, testCase.initialCells + 1}
			values := make([]uint64, len(hashes))
			require.NoError(t, ht.InsertBatch(len(hashes), hashes, nil, values))

			oldCellCnt := ht.cellCnt
			oldBlockCellCnt := ht.blockCellCnt()
			oldBlockCount := len(ht.cells)
			oldFirstBlock := &ht.cells[0][0]
			oldBytes := mp.CurrNB()

			target := maxElemCnt(testCase.requestedCells, intCellSize)
			require.Error(t, ht.ResizeOnDemand(int(target-ht.elemCnt)))
			require.Equal(t, oldCellCnt, ht.cellCnt)
			require.Equal(t, oldBlockCellCnt, ht.blockCellCnt())
			require.Len(t, ht.cells, oldBlockCount)
			require.True(t, oldFirstBlock == &ht.cells[0][0])
			require.Equal(t, oldBytes, mp.CurrNB())

			found := make([]uint64, len(hashes))
			ht.FindBatch(len(hashes), hashes, nil, found)
			require.Equal(t, values, found)
		})
	}
}

func TestStringHashMapFailedSegmentedGrowthIsAtomic(t *testing.T) {
	testCases := []struct {
		name           string
		capacity       int64
		initialCells   uint64
		requestedCells uint64
	}{
		{
			name:           "transition to segments",
			capacity:       5 * MB,
			initialCells:   maxStrCellCntPerBlock / 2,
			requestedCells: maxStrCellCntPerBlock * 2,
		},
		{
			name:           "partial segment allocation",
			capacity:       9 * MB,
			initialCells:   maxStrCellCntPerBlock,
			requestedCells: maxStrCellCntPerBlock * 4,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			mp, err := mpool.NewMPool(t.Name(), testCase.capacity, mpool.NoFixed)
			require.NoError(t, err)
			ht := new(StringHashMap)
			require.NoError(t, ht.Init(mp))
			t.Cleanup(func() {
				ht.Free()
				require.Zero(t, mp.CurrNB())
				mpool.DeleteMPool(mp)
			})

			require.NoError(t, ht.ResizeOnDemand(maxElemCnt(testCase.initialCells, strCellSize)))
			keys := [][]byte{
				padStringHashMapKey([]byte("first")),
				padStringHashMapKey([]byte("second")),
			}
			states := make([][3]uint64, len(keys))
			values := make([]uint64, len(keys))
			require.NoError(t, ht.InsertStringBatch(states, keys, values))

			oldCellCnt := ht.cellCnt
			oldBlockCellCnt := ht.blockCellCnt()
			oldBlockCount := len(ht.cells)
			oldFirstBlock := &ht.cells[0][0]
			oldBytes := mp.CurrNB()

			target := maxElemCnt(testCase.requestedCells, strCellSize)
			require.Error(t, ht.ResizeOnDemand(target-ht.elemCnt))
			require.Equal(t, oldCellCnt, ht.cellCnt)
			require.Equal(t, oldBlockCellCnt, ht.blockCellCnt())
			require.Len(t, ht.cells, oldBlockCount)
			require.True(t, oldFirstBlock == &ht.cells[0][0])
			require.Equal(t, oldBytes, mp.CurrNB())

			found := make([]uint64, len(keys))
			ht.FindStringBatch(states, keys, found)
			require.Equal(t, values, found)
		})
	}
}

func TestInt64HashMapFailedPopulatedGrowthIsAtomic(t *testing.T) {
	const entryCount = 400_000

	mp, err := mpool.NewMPool(t.Name(), 21*MB, mpool.NoFixed)
	require.NoError(t, err)
	ht := new(Int64HashMap)
	require.NoError(t, ht.Init(mp))
	t.Cleanup(func() {
		ht.Free()
		require.Zero(t, mp.CurrNB())
		mpool.DeleteMPool(mp)
	})

	initialTarget := maxElemCnt(maxIntCellCntPerBlock*4, intCellSize)
	require.NoError(t, ht.ResizeOnDemand(int(initialTarget)))
	hashes := make([]uint64, entryCount)
	values := make([]uint64, entryCount)
	for i := range hashes {
		hashes[i] = uint64(i+1) * 0x9e3779b97f4a7c15
	}
	require.NoError(t, ht.InsertBatch(len(hashes), hashes, nil, values))

	oldCellCnt := ht.cellCnt
	oldBlockCount := len(ht.cells)
	oldFirstBlock := &ht.cells[0][0]
	oldBytes := mp.CurrNB()
	target := maxElemCnt(maxIntCellCntPerBlock*8, intCellSize)
	require.Error(t, ht.ResizeOnDemand(int(target-ht.elemCnt)))
	require.Equal(t, oldCellCnt, ht.cellCnt)
	require.Len(t, ht.cells, oldBlockCount)
	require.True(t, oldFirstBlock == &ht.cells[0][0])
	require.Equal(t, oldBytes, mp.CurrNB())

	found := make([]uint64, len(hashes))
	ht.FindBatch(len(hashes), hashes, nil, found)
	for i := range values {
		require.Equalf(t, values[i], found[i], "hash index %d", i)
	}
}

func TestStringHashMapFailedPopulatedGrowthIsAtomic(t *testing.T) {
	const entryCount = 200_000

	mp, err := mpool.NewMPool(t.Name(), 21*MB, mpool.NoFixed)
	require.NoError(t, err)
	ht := new(StringHashMap)
	require.NoError(t, ht.Init(mp))
	t.Cleanup(func() {
		ht.Free()
		require.Zero(t, mp.CurrNB())
		mpool.DeleteMPool(mp)
	})

	initialTarget := maxElemCnt(maxStrCellCntPerBlock*4, strCellSize)
	require.NoError(t, ht.ResizeOnDemand(initialTarget))
	keys := make([][]byte, entryCount)
	for i := range keys {
		keys[i] = padStringHashMapKey(strconv.AppendInt(nil, int64(i), 10))
	}
	states := make([][3]uint64, len(keys))
	values := make([]uint64, len(keys))
	require.NoError(t, ht.InsertStringBatch(states, keys, values))

	oldCellCnt := ht.cellCnt
	oldBlockCount := len(ht.cells)
	oldFirstBlock := &ht.cells[0][0]
	oldBytes := mp.CurrNB()
	target := maxElemCnt(maxStrCellCntPerBlock*8, strCellSize)
	require.Error(t, ht.ResizeOnDemand(target-ht.elemCnt))
	require.Equal(t, oldCellCnt, ht.cellCnt)
	require.Len(t, ht.cells, oldBlockCount)
	require.True(t, oldFirstBlock == &ht.cells[0][0])
	require.Equal(t, oldBytes, mp.CurrNB())

	foundStates := make([][3]uint64, len(keys))
	found := make([]uint64, len(keys))
	ht.FindStringBatch(foundStates, keys, found)
	for i := range values {
		require.Equalf(t, values[i], found[i], "key index %d", i)
	}
}

func TestInt64HashMapMultiSegmentGrowthPreservesEntries(t *testing.T) {
	const entryCount = 100_000

	mp := mpool.MustNewZero()
	ht := new(Int64HashMap)
	require.NoError(t, ht.Init(mp))
	t.Cleanup(func() {
		ht.Free()
		require.Zero(t, mp.CurrNB())
		mpool.DeleteMPool(mp)
	})

	hashes := make([]uint64, entryCount)
	values := make([]uint64, entryCount)
	for i := range hashes {
		// Form short collision chains under the old mask while distributing the
		// same entries across distinct segments under the larger mask.
		hashes[i] = uint64(i/8)*8 + 1 +
			uint64(i%8)*maxIntCellCntPerBlock
	}
	require.NoError(t, ht.InsertBatch(len(hashes), hashes, nil, values))
	require.Equal(t, maxIntCellCntPerBlock, ht.cellCnt)

	target := maxElemCnt(maxIntCellCntPerBlock*8, intCellSize)
	require.NoError(t, ht.ResizeOnDemand(int(target-ht.elemCnt)))
	require.Len(t, ht.cells, 8)

	found := make([]uint64, len(hashes))
	ht.FindBatch(len(hashes), hashes, nil, found)
	for i := range values {
		require.Equalf(t, values[i], found[i], "hash index %d", i)
	}
}

func TestStringHashMapMultiSegmentGrowthPreservesEntries(t *testing.T) {
	const entryCount = 50_000

	mp := mpool.MustNewZero()
	ht := new(StringHashMap)
	require.NoError(t, ht.Init(mp))
	t.Cleanup(func() {
		ht.Free()
		require.Zero(t, mp.CurrNB())
		mpool.DeleteMPool(mp)
	})

	keys := make([][]byte, entryCount)
	for i := range keys {
		// The vectorized AES hash path requires a full lane, matching production
		// vector storage and TestHashFn.
		keys[i] = padStringHashMapKey(strconv.AppendInt(nil, int64(i), 10))
	}
	states := make([][3]uint64, len(keys))
	values := make([]uint64, len(keys))
	require.NoError(t, ht.InsertStringBatch(states, keys, values))
	require.Equal(t, maxStrCellCntPerBlock, ht.cellCnt)
	beforeGrowth := make([]uint64, len(keys))
	beforeGrowthStates := make([][3]uint64, len(keys))
	ht.FindStringBatch(beforeGrowthStates, keys, beforeGrowth)
	for i := range values {
		require.Equalf(t, states[i], beforeGrowthStates[i], "hash state %d before growth", i)
		require.Equalf(t, values[i], beforeGrowth[i], "key index %d before growth", i)
	}

	target := maxElemCnt(maxStrCellCntPerBlock*8, strCellSize)
	require.NoError(t, ht.ResizeOnDemand(target-ht.elemCnt))
	require.Len(t, ht.cells, 8)
	seen := make([]bool, len(keys)+1)
	for _, block := range ht.cells {
		for _, cell := range block {
			if cell.Mapped != 0 {
				require.LessOrEqual(t, cell.Mapped, uint64(len(keys)))
				require.False(t, seen[cell.Mapped])
				seen[cell.Mapped] = true
			}
		}
	}
	for mapped := 1; mapped <= len(keys); mapped++ {
		require.Truef(t, seen[mapped], "missing mapped value %d", mapped)
	}

	found := make([]uint64, len(keys))
	afterGrowthStates := make([][3]uint64, len(keys))
	ht.FindStringBatch(afterGrowthStates, keys, found)
	for i := range values {
		require.Equalf(t, states[i], afterGrowthStates[i], "hash state %d after growth", i)
		require.Equalf(t, values[i], found[i], "key index %d", i)
	}
}
