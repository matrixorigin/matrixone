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
	s.AppendBlockInfo(BlockInfo{BlockID: types.Blockid{1}})
	require.Equal(t, 1, s.Len())
	require.Equal(t, BlockInfoSize, s.Size())
	require.Equal(t, &BlockInfo{BlockID: types.Blockid{1}}, s.Get(0))

	var s2 BlockInfoSlice
	s2.AppendBlockInfo(BlockInfo{BlockID: types.Blockid{1}})
	s2.AppendBlockInfo(BlockInfo{BlockID: types.Blockid{2}})
	require.Equal(t, 2, s2.Len())
	require.Equal(t, BlockInfoSize*2, s2.Size())
	require.Equal(t, &BlockInfo{BlockID: types.Blockid{1}}, s2.Get(0))
	require.Equal(t, &BlockInfo{BlockID: types.Blockid{2}}, s2.Get(1))

	var s3 BlockInfoSlice
	s3.Append(EncodeBlockInfo(BlockInfo{BlockID: types.Blockid{1}}))
	s3.Append(EncodeBlockInfo(BlockInfo{BlockID: types.Blockid{2}}))
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
		s.AppendBlockInfo(BlockInfo{BlockID: intToBlockid(i)})
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

	s.AppendBlockInfo(blk)

	for i := 0; i < s.Len(); i++ {
		require.Equal(t, true, s.Get(i).IsAppendable())
	}
}

func TestBytesToBlockInfoSlice(t *testing.T) {
	bs := make([]byte, 0)
	for i := 0; i < 1000; i++ {
		bs = append(bs, EncodeBlockInfo(BlockInfo{BlockID: intToBlockid(int32(i))})...)
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
	s.AppendBlockInfo(blk)

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
	s.AppendBlockInfo(BlockInfo{BlockID: intToBlockid(0)})
	// Get BlockInfoSlice[:1]
	require.Equal(t, s.GetBytes(0), []byte(s.Slice(0, 1)))
	// Get BlockInfoSlice[1:]
	require.Equal(t, []byte{}, []byte(s.Slice(1, s.Len())))

	s = s.Slice(1, s.Len())
	require.Equal(t, 0, len(s))
	require.Equal(t, 0, s.Len())

	s.AppendBlockInfo(BlockInfo{BlockID: intToBlockid(1)})
	s.AppendBlockInfo(BlockInfo{BlockID: intToBlockid(2)})
	require.Equal(t, s.GetBytes(0), []byte(s.Slice(0, 1)))
	require.Equal(t, s.GetBytes(1), []byte(s.Slice(1, s.Len())))
}

func TestBlockInfoSlice_GetBytes(t *testing.T) {
	s := make(BlockInfoSlice, 0, 10)
	for i := 0; i < 10; i++ {
		s.AppendBlockInfo(BlockInfo{BlockID: intToBlockid(int32(i))})
	}

	for i := 0; i < 10; i++ {
		require.Equal(t, EncodeBlockInfo(BlockInfo{BlockID: intToBlockid(int32(i))}), s.GetBytes(i))
		require.Equal(t, &BlockInfo{BlockID: intToBlockid(int32(i))}, DecodeBlockInfo(s.GetBytes(i)))
		require.Equal(t, &BlockInfo{BlockID: intToBlockid(int32(i))}, s.Get(i))
		require.Equal(t, EncodeBlockInfo(BlockInfo{BlockID: intToBlockid(int32(i))}), EncodeBlockInfo(*s.Get(i)))
	}
}

func TestBlockInfoSlice_Remove(t *testing.T) {
	s := make(BlockInfoSlice, 0, 10)
	for i := 0; i < 10; i++ {
		s.AppendBlockInfo(BlockInfo{BlockID: intToBlockid(int32(i))})
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
