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
	EmptyBlockInfoInProgress      = BlockInfoInProgress{}
	EmptyBlockInfoInProgressBytes = EncodeBlockInfoInProgress(EmptyBlockInfoInProgress)
)

const (
	BlockInfoSizeInProgress = int(unsafe.Sizeof(EmptyBlockInfoInProgress))
)

type BlockInfoInProgress struct {
	BlockID types.Blockid
	//It's used to indicate whether the block is appendable block or non-appendable blk for reader.
	// for appendable block, the data visibility in the block is determined by the commit ts and abort ts.
	EntryState bool
	Sorted     bool
	MetaLoc    ObjectLocation
	CommitTs   types.TS

	//TODO:: remove it.
	PartitionNum int16
}

func (b *BlockInfoInProgress) String() string {
	return fmt.Sprintf("[A-%v]blk-%s", b.EntryState, b.BlockID.ShortStringEx())
}

func (b *BlockInfoInProgress) MarshalWithBuf(w *bytes.Buffer) (uint32, error) {
	var space uint32
	if _, err := w.Write(types.EncodeFixed(b.BlockID)); err != nil {
		return 0, err
	}
	space += uint32(types.BlockidSize)

	if _, err := w.Write(types.EncodeBool(&b.EntryState)); err != nil {
		return 0, err
	}
	space++

	if _, err := w.Write(types.EncodeBool(&b.Sorted)); err != nil {
		return 0, err
	}
	space++

	if _, err := w.Write(types.EncodeSlice(b.MetaLoc[:])); err != nil {
		return 0, err
	}
	space += uint32(LocationLen)

	if _, err := w.Write(types.EncodeFixed(b.CommitTs)); err != nil {
		return 0, err
	}
	space += types.TxnTsSize

	if _, err := w.Write(types.EncodeInt16(&b.PartitionNum)); err != nil {
		return 0, err
	}
	space += 2

	return space, nil
}

func (b *BlockInfoInProgress) Unmarshal(buf []byte) error {
	b.BlockID = types.DecodeFixed[types.Blockid](buf[:types.BlockidSize])
	buf = buf[types.BlockidSize:]
	b.EntryState = types.DecodeFixed[bool](buf)
	buf = buf[1:]
	b.Sorted = types.DecodeFixed[bool](buf)
	buf = buf[1:]

	copy(b.MetaLoc[:], buf[:LocationLen])
	buf = buf[LocationLen:]

	b.CommitTs = types.DecodeFixed[types.TS](buf[:types.TxnTsSize])
	buf = buf[types.TxnTsSize:]
	b.PartitionNum = types.DecodeFixed[int16](buf[:2])
	return nil
}

func (b *BlockInfoInProgress) MetaLocation() Location {
	return b.MetaLoc[:]
}

func (b *BlockInfoInProgress) SetMetaLocation(metaLoc Location) {
	b.MetaLoc = *(*[LocationLen]byte)(unsafe.Pointer(&metaLoc[0]))
}

func (b *BlockInfoInProgress) IsMemBlk() bool {
	return bytes.Equal(EncodeBlockInfoInProgress(*b), EmptyBlockInfoInProgressBytes)
}

func EncodeBlockInfoInProgress(info BlockInfoInProgress) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(&info)), BlockInfoSizeInProgress)
}

func DecodeBlockInfoInProgress(buf []byte) *BlockInfoInProgress {
	return (*BlockInfoInProgress)(unsafe.Pointer(&buf[0]))
}

type BlockInfoSliceInProgress []byte

func (s *BlockInfoSliceInProgress) Get(i int) *BlockInfoInProgress {
	return DecodeBlockInfoInProgress((*s)[i*BlockInfoSizeInProgress:])
}

func (s *BlockInfoSliceInProgress) GetBytes(i int) []byte {
	return (*s)[i*BlockInfoSizeInProgress : (i+1)*BlockInfoSizeInProgress]
}

func (s *BlockInfoSliceInProgress) Set(i int, info *BlockInfoInProgress) {
	copy((*s)[i*BlockInfoSizeInProgress:], EncodeBlockInfoInProgress(*info))
}

func (s *BlockInfoSliceInProgress) Len() int {
	return len(*s) / BlockInfoSizeInProgress
}

func (s *BlockInfoSliceInProgress) Size() int {
	return len(*s)
}

func (s *BlockInfoSliceInProgress) Slice(i, j int) []byte {
	return (*s)[i*BlockInfoSizeInProgress : j*BlockInfoSizeInProgress]
}

func (s *BlockInfoSliceInProgress) Append(bs []byte) {
	*s = append(*s, bs...)
}

func (s *BlockInfoSliceInProgress) AppendBlockInfo(info BlockInfoInProgress) {
	*s = append(*s, EncodeBlockInfoInProgress(info)...)
}

func (s *BlockInfoSliceInProgress) SetBytes(bs []byte) {
	*s = bs
}

func (s *BlockInfoSliceInProgress) GetAllBytes() []byte {
	return *s
}

func (s *BlockInfoSliceInProgress) String() string {
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("BlockInfoSlice[Len=%d]:\n", s.Len()))
	for i := 0; i < s.Len(); i++ {
		buf.WriteString(s.Get(i).BlockID.String())
		buf.WriteByte('\n')
	}
	return buf.String()
}

// It's deprecated
type BlockInfoSlice []byte

func (s *BlockInfoSlice) Get(i int) *BlockInfoInProgress {
	return DecodeBlockInfoInProgress((*s)[i*BlockInfoSizeInProgress:])
}

func (s *BlockInfoSlice) GetBytes(i int) []byte {
	return (*s)[i*BlockInfoSizeInProgress : (i+1)*BlockInfoSizeInProgress]
}

func (s *BlockInfoSlice) Set(i int, info *BlockInfoInProgress) {
	copy((*s)[i*BlockInfoSizeInProgress:], EncodeBlockInfoInProgress(*info))
}

func (s *BlockInfoSlice) Len() int {
	return len(*s) / BlockInfoSizeInProgress
}

func (s *BlockInfoSlice) Size() int {
	return len(*s)
}

func (s *BlockInfoSlice) Slice(i, j int) []byte {
	return (*s)[i*BlockInfoSizeInProgress : j*BlockInfoSizeInProgress]
}

func (s *BlockInfoSlice) Append(bs []byte) {
	*s = append(*s, bs...)
}

func (s *BlockInfoSlice) AppendBlockInfo(info BlockInfoInProgress) {
	*s = append(*s, EncodeBlockInfoInProgress(info)...)
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
