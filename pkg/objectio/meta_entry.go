// Copyright 2021 Matrix Origin
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

import "github.com/matrixorigin/matrixone/pkg/container/types"

type ObjectMeta []byte

func (e ObjectMeta) MustGetMeta(metaType DataMetaType) ObjectDataMeta {
	return objectMetaV3(e[IOEntryHeaderSize:]).MustGetMeta(metaType)
}

func (e ObjectMeta) DataMetaCount() uint16 {
	return objectMetaV3(e[IOEntryHeaderSize:]).DataMetaCount()
}

func (e ObjectMeta) DataMeta() (ObjectDataMeta, bool) {
	return objectMetaV3(e[IOEntryHeaderSize:]).DataMeta()
}

func (e ObjectMeta) MustDataMeta() ObjectDataMeta {
	return objectMetaV3(e[IOEntryHeaderSize:]).MustDataMeta()
}

func (e ObjectMeta) MustTombstoneMeta() ObjectDataMeta {
	return objectMetaV3(e[IOEntryHeaderSize:]).MustTombstoneMeta()
}

func (e ObjectMeta) SubMeta(pos uint16) (ObjectDataMeta, bool) {
	return objectMetaV3(e[IOEntryHeaderSize:]).SubMeta(pos)
}

// CheckedDataBlocks returns borrowed metadata for the expected zero-based data
// blocks. It validates the block index and column metadata bounds before slicing.
func (e ObjectMeta) CheckedDataBlocks(expected uint16) ([]BlockObject, bool) {
	if len(e) < IOEntryHeaderSize+metaHeaderLen || DecodeIOEntryHeader(e).Type != IOET_ObjMeta || expected == 0 {
		return nil, false
	}
	payload := e[IOEntryHeaderSize:]
	offset := uint64(types.DecodeUint32(payload[dataMetaCount:tombstoneMetaCountOff]))
	if offset < metaHeaderLen || offset+headerLen > uint64(len(payload)) || e.DataMetaCount() != expected {
		return nil, false
	}
	data := ObjectDataMeta(payload[offset:])
	if data.BlockCount() != uint32(expected) || data.BlockHeader().StartID() != 0 {
		return nil, false
	}
	indexSize := blockCountLen + uint32(expected)*posLen
	indexEnd := data.Length() + indexSize
	if uint64(indexEnd) > uint64(len(data)) {
		return nil, false
	}
	idx := data.BlockIndex()[:indexSize]
	if idx.BlockCount() != uint32(expected) {
		return nil, false
	}

	blocks := make([]BlockObject, expected)
	previousEnd := uint64(indexEnd)
	for i := range blocks {
		offset, length := idx.BlockMetaPos(uint32(i))
		end := uint64(offset) + uint64(length)
		if uint64(offset) < previousEnd || length < headerLen || end > uint64(len(data)) {
			return nil, false
		}
		block := BlockObject(data[offset:end])
		if block.GetID() != uint16(i) || headerLen+uint32(block.GetMetaColumnCount())*colMetaLen > length {
			return nil, false
		}
		blocks[i] = block
		previousEnd = end
	}
	return blocks, true
}
