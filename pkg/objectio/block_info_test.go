// Copyright 2022 Matrix Origin
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

package objectio

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestEncodeInfoHeader(t *testing.T) {
	h := InfoHeader{
		Type:    BlockInfoType,
		Version: V1,
	}
	require.Equal(t, h, DecodeInfoHeader(EncodeInfoHeader(h)))
}

func FuzzEncodeInfoHeader(f *testing.F) {
	f.Fuzz(func(t *testing.T, typ, v uint16) {
		h := InfoHeader{
			Type:    typ,
			Version: v,
		}
		require.Equal(t, h, DecodeInfoHeader(EncodeInfoHeader(h)))
	})
}

func TestBlockInfoSlice_Append(t *testing.T) {
	var s BlockInfoSlice
	s.AppendBlockInfo(&BlockInfo{BlockID: types.Blockid{1}})
	require.Equal(t, 1, s.Len())
	require.Equal(t, BlockInfoSize, s.Size())
	require.Equal(t, &BlockInfo{BlockID: types.Blockid{1}}, s.Get(0))

	var s2 BlockInfoSlice
	s2.AppendBlockInfo(&BlockInfo{BlockID: types.Blockid{1}})
	s2.AppendBlockInfo(&BlockInfo{BlockID: types.Blockid{2}})
	require.Equal(t, 2, s2.Len())
	require.Equal(t, BlockInfoSize*2, s2.Size())
	require.Equal(t, &BlockInfo{BlockID: types.Blockid{1}}, s2.Get(0))
	require.Equal(t, &BlockInfo{BlockID: types.Blockid{2}}, s2.Get(1))

	var s3 BlockInfoSlice
	s3.Append(EncodeBlockInfo(&BlockInfo{BlockID: types.Blockid{1}}))
	s3.Append(EncodeBlockInfo(&BlockInfo{BlockID: types.Blockid{2}}))
	require.Equal(t, 2, s3.Len())
	require.Equal(t, BlockInfoSize*2, s3.Size())
	require.Equal(t, &BlockInfo{BlockID: types.Blockid{1}}, s3.Get(0))
	require.Equal(t, &BlockInfo{BlockID: types.Blockid{2}}, s3.Get(1))
}

func intToBlockid(i int32) types.Blockid {
	return types.Blockid{
		byte(i >> 24),
		byte(i >> 16),
		byte(i >> 8),
		byte(i),
	}
}

func TestBlockInfoSliceTraverse(t *testing.T) {
	var s BlockInfoSlice
	for i := int32(0); i < 1000; i++ {
		s.AppendBlockInfo(&BlockInfo{BlockID: intToBlockid(i)})
	}
	require.Equal(t, 1000, s.Len())

	for i := 0; i < s.Len(); i++ {
		blkInfo := s.Get(i)
		require.Equal(t, intToBlockid(int32(i)), blkInfo.BlockID)
		require.Equal(t, false, blkInfo.IsAppendable())
		blkInfo.ObjectFlags |= ObjectFlag_Appendable
	}

	for i := 0; i < s.Len(); i++ {
		require.Equal(t, true, s.Get(i).IsAppendable())
	}

	blk := BlockInfo{BlockID: intToBlockid(1000)}
	blk.ObjectFlags |= ObjectFlag_Appendable

	s.AppendBlockInfo(&blk)

	for i := 0; i < s.Len(); i++ {
		require.Equal(t, true, s.Get(i).IsAppendable())
	}
}

func TestBytesToBlockInfoSlice(t *testing.T) {
	bs := make([]byte, 0)
	for i := 0; i < 1000; i++ {
		bs = append(bs, EncodeBlockInfo(&BlockInfo{BlockID: intToBlockid(int32(i))})...)
	}

	s := BlockInfoSlice(bs)
	require.Equal(t, 1000, s.Len())

	for i := 0; i < s.Len(); i++ {
		blkInfo := s.Get(i)
		require.Equal(t, intToBlockid(int32(i)), blkInfo.BlockID)
		require.Equal(t, false, blkInfo.IsAppendable())
		blkInfo.ObjectFlags |= ObjectFlag_Appendable
	}

	blk := BlockInfo{BlockID: intToBlockid(1000)}
	blk.ObjectFlags |= ObjectFlag_Appendable
	s.AppendBlockInfo(&blk)

	for i := 0; i < s.Len(); i++ {
		require.Equal(t, true, s.Get(i).IsAppendable())
	}

	require.Equal(t, 1000*BlockInfoSize, len(bs))
	require.Equal(t, s.Size(), len(bs)+BlockInfoSize)
	bs = s
	require.Equal(t, 1001*BlockInfoSize, len(bs))
	require.Equal(t, s.GetAllBytes(), bs)

	s.Get(999).ObjectFlags &= ^ObjectFlag_Appendable
	require.Equal(t, false, s.Get(999).IsAppendable())
	blkInfo := DecodeBlockInfo(bs[999*BlockInfoSize:])
	require.Equal(t, false, blkInfo.IsAppendable())
}

func TestBlockInfoSlice_Slice(t *testing.T) {
	s := make(BlockInfoSlice, 0)
	s.AppendBlockInfo(&BlockInfo{BlockID: intToBlockid(0)})
	// Get BlockInfoSlice[:1]
	require.Equal(t, s.GetBytes(0), []byte(s.Slice(0, 1)))
	// Get BlockInfoSlice[1:]
	require.Equal(t, []byte{}, []byte(s.Slice(1, s.Len())))

	s = s.Slice(1, s.Len())
	require.Equal(t, 0, len(s))
	require.Equal(t, 0, s.Len())

	s.AppendBlockInfo(&BlockInfo{BlockID: intToBlockid(1)})
	s.AppendBlockInfo(&BlockInfo{BlockID: intToBlockid(2)})
	require.Equal(t, s.GetBytes(0), []byte(s.Slice(0, 1)))
	require.Equal(t, s.GetBytes(1), []byte(s.Slice(1, s.Len())))
}

func TestBlockInfoSlice_GetBytes(t *testing.T) {
	s := make(BlockInfoSlice, 0, 10)
	for i := 0; i < 10; i++ {
		s.AppendBlockInfo(&BlockInfo{BlockID: intToBlockid(int32(i))})
	}

	for i := 0; i < 10; i++ {
		require.Equal(t, EncodeBlockInfo(&BlockInfo{BlockID: intToBlockid(int32(i))}), s.GetBytes(i))
		require.Equal(t, &BlockInfo{BlockID: intToBlockid(int32(i))}, DecodeBlockInfo(s.GetBytes(i)))
		require.Equal(t, &BlockInfo{BlockID: intToBlockid(int32(i))}, s.Get(i))
		require.Equal(t, EncodeBlockInfo(&BlockInfo{BlockID: intToBlockid(int32(i))}), EncodeBlockInfo(s.Get(i)))
	}
}

func TestBlockInfoSlice_Remove(t *testing.T) {
	s := make(BlockInfoSlice, 0, 10)
	for i := 0; i < 10; i++ {
		s.AppendBlockInfo(&BlockInfo{BlockID: intToBlockid(int32(i))})
	}

	curr := 0
	for i := 0; i < 10; i++ {
		blk := s.Get(i)
		if blk.BlockID == intToBlockid(0) {
			// remove the first element
			continue
		}
		s.Set(curr, blk)
		curr++
	}

	s = s.Slice(0, curr)
	require.Equal(t, 9, s.Len())
	for i := 0; i < 9; i++ {
		require.Equal(t, intToBlockid(int32(i+1)), s.Get(i).BlockID)
	}
}

func TestObjectStatsToBlockInfoSlice(t *testing.T) {
	obj1 := NewObjectid()
	stats := NewObjectStatsWithObjectID(obj1, true, true, true)
	extent := NewExtent(0x1f, 0x2f, 0x3f, 0x4f)
	SetObjectStatsExtent(stats, extent)
	blkCnt := uint16(99)
	SetObjectStatsBlkCnt(stats, uint32(blkCnt))
	rowCnt := uint32(BlockMaxRows*98) + uint32(1)
	SetObjectStatsRowCnt(stats, rowCnt)

	slice := ObjectStatsToBlockInfoSlice(stats, false)
	require.Equal(t, int(blkCnt), slice.Len())
	for i := 0; i < int(blkCnt); i++ {
		blk := slice.Get(i)
		require.True(t, blk.IsAppendable())
		require.True(t, blk.IsSorted())
		require.True(t, blk.IsCNCreated())
		require.True(t, blk.BlockID.Object().EQ(obj1))
		require.Equal(t, uint16(i), blk.BlockID.Sequence())
		if i == int(blkCnt)-1 {
			require.Equal(t, uint32(1), blk.MetaLocation().Rows())
		} else {
			require.Equal(t, uint32(BlockMaxRows), blk.MetaLocation().Rows())
		}
	}

	slice = ObjectStatsToBlockInfoSlice(stats, true)
	require.Equal(t, int(blkCnt)+1, slice.Len())
	for i := 0; i < int(blkCnt)+1; i++ {
		blk := slice.Get(i)
		if i == 0 {
			require.Equal(t, EmptyBlockInfo, *blk)
			continue
		}
		require.True(t, blk.IsAppendable())
		require.True(t, blk.IsSorted())
		require.True(t, blk.IsCNCreated())
		require.True(t, blk.BlockID.Object().EQ(obj1))
		require.Equal(t, uint16(i-1), blk.BlockID.Sequence())
		if i == int(blkCnt) {
			require.Equal(t, uint32(1), blk.MetaLocation().Rows())
		} else {
			require.Equal(t, uint32(BlockMaxRows), blk.MetaLocation().Rows())
		}
	}
}

func BenchmarkObjectStatsRelatedUtils(b *testing.B) {
	stats := NewObjectStats()
	b.Run("stats-to-blockinfo", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			stats.ConstructBlockInfo(uint16(0))
		}
	})
	b.Run("stats-to-blockinfo2", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var blk BlockInfo
			stats.ConstructBlockInfoTo(uint16(0), &blk)
		}
	})
	b.Run("stats-to-blockinfo3", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			loc := stats.BlockLocation(uint16(0), BlockMaxRows)
			blk := BlockInfo{
				BlockID: *BuildObjectBlockid(stats.ObjectName(), uint16(0)),
				MetaLoc: ObjectLocation(loc),
			}

			blk.SetFlagByObjStats(stats)
		}
	})
	var blk1 BlockInfo
	b.Run("encode-block-info", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			EncodeBlockInfo(&blk1)
		}
	})

	name := stats.ObjectName()
	b.Run("BuildObjectBlockid", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			BuildObjectBlockid(name, uint16(1))
		}
	})
	b.Run("ConstructBlockId", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			stats.ConstructBlockId(uint16(1))
		}
	})
}
