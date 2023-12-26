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
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

type ObjectLocation [LocationLen]byte

// ProtoSize is used by gogoproto.
func (m *ObjectLocation) ProtoSize() int {
	return LocationLen
}

// MarshalTo is used by gogoproto.
func (m *ObjectLocation) MarshalTo(data []byte) (int, error) {
	size := m.ProtoSize()
	return m.MarshalToSizedBuffer(data[:size])
}

// MarshalToSizedBuffer is used by gogoproto.
func (m *ObjectLocation) MarshalToSizedBuffer(data []byte) (int, error) {
	if len(data) < m.ProtoSize() {
		panic("invalid byte slice")
	}
	n := copy(data, m[:])
	return n, nil
}

// Marshal is used by gogoproto.
func (m *ObjectLocation) Marshal() ([]byte, error) {
	data := make([]byte, m.ProtoSize())
	n, err := m.MarshalToSizedBuffer(data)
	if err != nil {
		return nil, err
	}
	return data[:n], err
}

// Unmarshal is used by gogoproto.
func (m *ObjectLocation) Unmarshal(data []byte) error {
	if len(data) < m.ProtoSize() {
		panic("invalid byte slice")
	}
	copy(m[:], data)
	return nil
}

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
	EmptyBlockInfoBytes = EncodeBlockInfo(EmptyBlockInfo)
)

const (
	BlockInfoSize = int(unsafe.Sizeof(EmptyBlockInfo))
)

type BlockInfo struct {
	BlockID    types.Blockid
	EntryState bool
	Sorted     bool
	MetaLoc    ObjectLocation
	DeltaLoc   ObjectLocation
	CommitTs   types.TS
	SegmentID  types.Uuid

	//TODO:: putting them here is a bad idea, remove
	//this block can be distributed to remote nodes.
	CanRemote    bool
	PartitionNum int
}

func (b *BlockInfo) MetaLocation() Location {
	return b.MetaLoc[:]
}

func (b *BlockInfo) SetMetaLocation(metaLoc Location) {
	b.MetaLoc = *(*[LocationLen]byte)(unsafe.Pointer(&metaLoc[0]))
}

func (b *BlockInfo) DeltaLocation() Location {
	return b.DeltaLoc[:]
}

func (b *BlockInfo) SetDeltaLocation(deltaLoc Location) {
	b.DeltaLoc = *(*[LocationLen]byte)(unsafe.Pointer(&deltaLoc[0]))
}

// XXX info is passed in by value.   The use of unsafe here will cost
// an allocation and copy.  BlockInfo is not small therefore this is
// not exactly cheap.   However, caller of this function will keep a
// reference to the buffer.  See txnTable.rangesOnePart.
// ranges is *[][]byte.
func EncodeBlockInfo(info BlockInfo) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(&info)), BlockInfoSize)
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
	copy((*s)[i*BlockInfoSize:], EncodeBlockInfo(*info))
}

func (s *BlockInfoSlice) Len() int {
	return len(*s) / BlockInfoSize
}

func (s *BlockInfoSlice) Slice(i, j int) BlockInfoSlice {
	return (*s)[i*BlockInfoSize : j*BlockInfoSize]
}

func (s *BlockInfoSlice) Append(info BlockInfo) {
	*s = append(*s, EncodeBlockInfo(info)...)
}

func (s *BlockInfoSlice) Clear() {
	for i := range *s {
		(*s)[i] = 0
	}
}
