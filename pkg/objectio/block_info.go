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
	"bytes"
	"fmt"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

type ObjectLocation [LocationLen]byte

const (
	BlockInfoType = uint16(1)

	V1 = uint16(1)
)

type InfoHeader struct {
	Type, Version uint16
}

func EncodeInfoHeader(h InfoHeader) uint32 {
	return uint32(h.Type)<<16 | uint32(h.Version)
}

func DecodeInfoHeader(h uint32) InfoHeader {
	return InfoHeader{
		Type:    uint16(h >> 16),
		Version: uint16(h),
	}
}

var (
	EmptyBlockInfo      = BlockInfo{}
	EmptyBlockInfoBytes = EncodeBlockInfo(&EmptyBlockInfo)
)

const (
	BlockInfoSize = int(unsafe.Sizeof(EmptyBlockInfo))
)

type BlockInfo struct {
	BlockID     types.Blockid
	MetaLoc     ObjectLocation
	ObjectFlags int8

	//TODO:: remove it.
	PartitionNum int16
}

func (b *BlockInfo) SetFlagByObjStats(stats *ObjectStats) {
	b.ObjectFlags = stats.GetFlag()
}

func (b *BlockInfo) ConstructBlockID(name ObjectName, sequence uint16) {
	BuildObjectBlockidTo(name, sequence, b.BlockID[:])
}

func (b *BlockInfo) IsAppendable() bool {
	return b.ObjectFlags&ObjectFlag_Appendable != 0
}

func (b *BlockInfo) IsSorted() bool {
	return b.ObjectFlags&ObjectFlag_Sorted != 0
}

func (b *BlockInfo) IsCNCreated() bool {
	return b.ObjectFlags&ObjectFlag_CNCreated != 0
}

func (b *BlockInfo) String() string {
	flag := ""
	if b.IsAppendable() {
		flag = flag + "A"
	}
	if b.IsSorted() {
		flag = flag + "S"
	}
	if b.IsCNCreated() {
		flag = flag + "C"
	}

	return fmt.Sprintf(
		"[%s]ID-%s, MetaLoc: %s, PartitionNum: %d",
		flag,
		b.BlockID.ShortStringEx(),
		b.MetaLocation().String(),
		b.PartitionNum)
}

func (b *BlockInfo) MarshalWithBuf(w *bytes.Buffer) (uint32, error) {
	var space uint32
	if _, err := w.Write(types.EncodeFixed(b.BlockID)); err != nil {
		return 0, err
	}
	space += uint32(types.BlockidSize)

	if _, err := w.Write(types.EncodeSlice(b.MetaLoc[:])); err != nil {
		return 0, err
	}
	space += uint32(LocationLen)

	if _, err := w.Write(types.EncodeInt8(&b.ObjectFlags)); err != nil {
		return 0, err
	}
	space++

	if _, err := w.Write(types.EncodeInt16(&b.PartitionNum)); err != nil {
		return 0, err
	}
	space += 2

	return space, nil
}

func (b *BlockInfo) Unmarshal(buf []byte) error {
	b.BlockID = types.DecodeFixed[types.Blockid](buf[:types.BlockidSize])
	buf = buf[types.BlockidSize:]

	copy(b.MetaLoc[:], buf[:LocationLen])
	buf = buf[LocationLen:]

	b.ObjectFlags = types.DecodeInt8(buf[:1])
	buf = buf[1:]

	b.PartitionNum = types.DecodeFixed[int16](buf[:2])
	return nil
}

func (b *BlockInfo) MetaLocation() Location {
	return b.MetaLoc[:]
}

func (b *BlockInfo) SetMetaLocation(metaLoc Location) {
	b.MetaLoc = *(*[LocationLen]byte)(unsafe.Pointer(&metaLoc[0]))
}

func (b *BlockInfo) IsMemBlk() bool {
	return bytes.Equal(EncodeBlockInfo(b), EmptyBlockInfoBytes)
}

func EncodeBlockInfo(info *BlockInfo) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(info)), BlockInfoSize)
}

func DecodeBlockInfo(buf []byte) *BlockInfo {
	return (*BlockInfo)(unsafe.Pointer(&buf[0]))
}

type BlockInfoSlice []byte

func (s *BlockInfoSlice) Get(i int) *BlockInfo {
	return DecodeBlockInfo((*s)[i*BlockInfoSize:])
}

func (s *BlockInfoSlice) GetBytes(i int) []byte {
	return (*s)[i*BlockInfoSize : (i+1)*BlockInfoSize]
}

func (s *BlockInfoSlice) Set(i int, info *BlockInfo) {
	copy((*s)[i*BlockInfoSize:], EncodeBlockInfo(info))
}

func (s *BlockInfoSlice) Len() int {
	return len(*s) / BlockInfoSize
}

func (s *BlockInfoSlice) Size() int {
	return len(*s)
}

func (s *BlockInfoSlice) Slice(i, j int) []byte {
	return (*s)[i*BlockInfoSize : j*BlockInfoSize]
}

func (s *BlockInfoSlice) Append(bs []byte) {
	*s = append(*s, bs...)
}

func (s *BlockInfoSlice) AppendBlockInfo(info *BlockInfo) {
	*s = append(*s, EncodeBlockInfo(info)...)
}

func (s *BlockInfoSlice) SetBytes(bs []byte) {
	*s = bs
}

func (s *BlockInfoSlice) GetAllBytes() []byte {
	return *s
}

func (s *BlockInfoSlice) String() string {
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("BlockInfoSlice[Len=%d]:\n", s.Len()))
	for i := 0; i < s.Len(); i++ {
		buf.WriteString(s.Get(i).BlockID.String())
		buf.WriteByte('\n')
	}
	return buf.String()
}

type BackupObject struct {
	Location Location
	CrateTS  types.TS
	DropTS   types.TS
	NeedCopy bool
}

func MakeBlockInfoSlice(cnt int) BlockInfoSlice {
	return make([]byte, cnt*BlockInfoSize)
}

func MultiObjectStatsToBlockInfoSlice(objs []ObjectStats, withFirstEmpty bool) BlockInfoSlice {
	offset := 0
	var ret BlockInfoSlice
	cnt := 0
	for _, obj := range objs {
		cnt += int(obj.BlkCnt())
	}
	if withFirstEmpty {
		ret = MakeBlockInfoSlice(cnt + 1)
		offset = 1
	} else {
		ret = MakeBlockInfoSlice(cnt)
	}
	idx := 0
	for _, obj := range objs {
		for i := 0; i < int(obj.BlkCnt()); i++ {
			blk := ret.Get(idx + offset)
			obj.BlockLocationTo(uint16(i), BlockMaxRows, blk.MetaLoc[:])
			blk.ConstructBlockID(obj.ObjectName(), uint16(i))
			blk.SetFlagByObjStats(&obj)
			idx++
		}
	}
	return ret
}

func ObjectStatsToBlockInfoSlice(stats *ObjectStats, withFirstEmpty bool) BlockInfoSlice {
	offset := 0
	var ret BlockInfoSlice
	cnt := int(stats.BlkCnt())
	if withFirstEmpty {
		ret = MakeBlockInfoSlice(cnt + 1)
		offset = 1
	} else {
		ret = MakeBlockInfoSlice(cnt)
	}
	for i := 0; i < cnt; i++ {
		blk := ret.Get(i + offset)
		stats.BlockLocationTo(uint16(i), BlockMaxRows, blk.MetaLoc[:])
		blk.ConstructBlockID(stats.ObjectName(), uint16(i))
		blk.SetFlagByObjStats(stats)
	}
	return ret
}
