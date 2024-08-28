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
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

type BlockObject []byte

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

func BuildBlockMeta(count uint16) BlockObject {
	length := headerLen + uint32(count)*colMetaLen
	buf := make([]byte, length)
	meta := BlockObject(buf)
	return meta
}

func (bm BlockObject) BlockHeader() BlockHeader {
	return BlockHeader(bm[:headerLen])
}

func (bm BlockObject) SetBlockMetaHeader(header BlockHeader) {
	copy(bm[:headerLen], header)
}

// ColumnMeta is for internal use only, it didn't consider the block does not
// contain the seqnum
func (bm BlockObject) ColumnMeta(seqnum uint16) ColumnMeta {
	return GetColumnMeta(seqnum, bm)
}

func (bm BlockObject) AddColumnMeta(idx uint16, col ColumnMeta) {
	offset := headerLen + uint32(idx)*colMetaLen
	copy(bm[offset:offset+colMetaLen], col)
}

func (bm BlockObject) IsEmpty() bool {
	return len(bm) == 0
}

func (bm BlockObject) ToColumnZoneMaps(seqnums []uint16) []ZoneMap {
	maxseq := bm.GetMaxSeqnum()
	zms := make([]ZoneMap, len(seqnums))
	for i, idx := range seqnums {
		if idx >= SEQNUM_UPPER {
			panic(fmt.Sprintf("do not read special %d", idx))
		}
		if idx > maxseq {
			zms[i] = index.DecodeZM(EmptyZm[:])
		}
		column := bm.MustGetColumn(idx)
		zms[i] = index.DecodeZM(column.ZoneMap())
	}
	return zms
}

func (bm BlockObject) GetExtent() Extent {
	return bm.BlockHeader().MetaLocation()
}

// MustGetColumn is for general use. it return a empty ColumnMeta if the block does not
// contain the seqnum
func (bm BlockObject) MustGetColumn(seqnum uint16) ColumnMeta {
	if seqnum >= bm.BlockHeader().MetaColumnCount() {
		h := bm.BlockHeader()
		logutil.Infof("ObjectIO: blk-%d genernate empty ColumnMeta for big seqnum %d, maxseq: %d, column count: %d",
			h.Sequence(), seqnum, h.MaxSeqnum(), h.MetaColumnCount())
		return BuildColumnMeta()
	}
	return bm.ColumnMeta(seqnum)
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

func (bm BlockObject) GetBlockID(name ObjectName) *Blockid {
	segmentId := name.SegmentId()
	num := name.Num()
	return NewBlockid(&segmentId, num, bm.BlockHeader().Sequence())
}

func (bm BlockObject) GenerateBlockInfo(objName ObjectName, sorted bool) BlockInfo {
	location := BuildLocation(
		objName,
		bm.GetExtent(),
		bm.GetRows(),
		bm.GetID(),
	)

	sid := location.Name().SegmentId()
	blkInfo := BlockInfo{
		BlockID: *NewBlockid(
			&sid,
			location.Name().Num(),
			location.ID()),
		//non-appendable block
		Appendable: false,
		Sorted:     sorted,
	}
	blkInfo.SetMetaLocation(location)
	return blkInfo
}
