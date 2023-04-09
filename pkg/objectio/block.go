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

func NewBlock(colCnt uint16) BlockMeta {
	header := BuildBlockHeader()
	header.SetColumnCount(colCnt)
	blockMeta := BuildBlockMeta(colCnt)
	blockMeta.SetBlockMetaHeader(header)
	for i := uint16(0); i < colCnt; i++ {
		col := BuildColumnMeta()
		col.setIdx(i)
		blockMeta.AddColumnMeta(i, col)
	}
	return blockMeta
}

func (bm BlockMeta) GetExtent() Extent {
	return Extent{}
}

func (bm BlockMeta) GetName() ObjectName {
	return ObjectName{}
}

func (bm BlockMeta) GetColumn(idx uint16) (ColumnMeta, error) {
	if idx >= bm.BlockHeader().ColumnCount() {
		return nil, moerr.NewInternalErrorNoCtx("ObjectIO: bad index: %d, "+
			"block: %v, column count: %d",
			idx, bm.GetName().String(),
			bm.BlockHeader().ColumnCount())
	}
	return bm.ColumnMeta(idx), nil
}

func (bm BlockMeta) GetRows() (uint32, error) {
	panic(any("implement me"))
}

func (bm BlockMeta) GetMeta() BlockMeta {
	return bm
}

func (bm BlockMeta) GetID() uint32 {
	return uint32(bm.BlockHeader().BlockID())
}

func (bm BlockMeta) GetColumnCount() uint16 {
	return bm.BlockHeader().ColumnCount()
}

func (bm BlockMeta) MarshalMeta() []byte {
	return bm
}

func (bm BlockMeta) UnmarshalMeta(data []byte) (uint32, error) {
	var err error
	header := BlockHeader(data[:headerLen])
	metaLen := headerLen + header.ColumnCount()*colMetaLen
	bm = data[:metaLen]
	return uint32(metaLen), err
}
