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

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// Block is the organizational structure of a batch in objectio
// Write one batch at a time, and batch and block correspond one-to-one
type Block struct {
	meta BlockMeta
	// id is the serial number of the block in the object
	id uint32

	// extent is the location of the block's metadata on the fileservice
	extent Extent

	object *Object
	// name is the file name or object name of the block
	name ObjectName
}

func NewBlock(colCnt uint16, object *Object, name ObjectName) BlockObject {
	header := BuildBlockHeader()
	header.SetColumnCount(colCnt)
	blockMeta := BuildBlockMeta(colCnt)
	blockMeta.SetBlockMetaHeader(header)
	for i := uint16(0); i < colCnt; i++ {
		col := BuildColumnMeta()
		col.setIdx(i)
		blockMeta.AddColumnMeta(i, col)
	}
	block := &Block{
		meta:   blockMeta,
		object: object,
		name:   name,
	}
	return block
}

func (b *Block) GetExtent() Extent {
	return b.extent
}

func (b *Block) GetName() ObjectName {
	return b.name
}

func (b *Block) GetColumn(idx uint16) (ColumnObject, error) {
	if idx >= b.meta.BlockHeader().ColumnCount() {
		return nil, moerr.NewInternalErrorNoCtx("ObjectIO: bad index: %d, "+
			"block: %v, column count: %d",
			idx, b.name,
			b.meta.BlockHeader().ColumnCount())
	}
	return NewColumnBlock(b.meta.ColumnMeta(idx), b.object), nil
}

func (b *Block) GetRows() (uint32, error) {
	panic(any("implement me"))
}

func (b *Block) GetMeta() BlockMeta {
	return b.meta
}

func (b *Block) GetID() uint32 {
	return b.id
}

func (b *Block) GetColumnCount() uint16 {
	return b.meta.BlockHeader().ColumnCount()
}

func (b *Block) MarshalMeta() []byte {
	return b.meta
}

func (b *Block) UnmarshalMeta(data []byte, ZMUnmarshalFunc ZoneMapUnmarshalFunc) (uint32, error) {
	var err error
	header := BlockHeader(data[:headerLen])
	metaLen := headerLen + header.ColumnCount()*colMetaLen
	b.meta = data[:metaLen]
	return uint32(metaLen), err
}
