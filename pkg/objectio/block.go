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

func NewBlock(seqnums *Seqnums) BlockObject {
	header := BuildBlockHeader()
	header.SetColumnCount(uint16(len(seqnums.Seqs)))
	metaColCnt := seqnums.MetaColCnt
	header.SetMetaColumnCount(metaColCnt)
	header.SetMaxSeqnum(seqnums.MaxSeq)
	blockMeta := BuildBlockMeta(metaColCnt)
	blockMeta.SetBlockMetaHeader(header)
	// create redundant columns to make reading O(1)
	for i := uint16(0); i < metaColCnt; i++ {
		col := BuildColumnMeta()
		blockMeta.AddColumnMeta(i, col)
	}

	for i, seq := range seqnums.Seqs {
		blockMeta.ColumnMeta(seq).setIdx(uint16(i))
	}
	return blockMeta
}

func (bm BlockObject) GetExtent() Extent {
	return bm.BlockHeader().MetaLocation()
}

func (bm BlockObject) MustGetColumn(seqnum uint16) ColumnMeta {
	meta, err := bm.GetColumn(seqnum)
	if err != nil {
		panic(err)
	}
	return meta
}

func (bm BlockObject) GetColumn(seqnum uint16) (ColumnMeta, error) {
	if seqnum >= bm.BlockHeader().MetaColumnCount() {
		return nil, moerr.NewInternalErrorNoCtx("ObjectIO: bad index: %d, "+
			"block: %d, column count: %d",
			seqnum, bm.BlockHeader().Sequence(),
			bm.BlockHeader().MetaColumnCount())
	}
	return bm.ColumnMeta(seqnum), nil
}

func (bm BlockObject) GetRows() uint32 {
	return bm.BlockHeader().Rows()
}

func (bm BlockObject) GetMeta() BlockObject {
	return bm
}

func (bm BlockObject) GetID() uint16 {
	return bm.BlockHeader().Sequence()
}

func (bm BlockObject) GetColumnCount() uint16 {
	return bm.BlockHeader().ColumnCount()
}

func (bm BlockObject) GetMetaColumnCount() uint16 {
	return bm.BlockHeader().MetaColumnCount()
}

func (bm BlockObject) GetMaxSeqnum() uint16 {
	return bm.BlockHeader().MaxSeqnum()
}
