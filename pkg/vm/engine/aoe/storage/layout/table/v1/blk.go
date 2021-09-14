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

package table

import (
	"bytes"
	"errors"
	"fmt"
	ro "matrixone/pkg/container/vector"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/container/batch"
	"matrixone/pkg/vm/engine/aoe/storage/container/vector"
	"matrixone/pkg/vm/engine/aoe/storage/dbi"
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/col"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/iface"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/wrapper"
	"matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
)

type block struct {
	common.BaseMvcc
	baseBlock
	data struct {
		cols  []col.IColumnBlock
		sizes []uint64
	}
}

func newBlock(host iface.ISegment, meta *metadata.Block) (iface.IBlock, error) {
	blk := &block{
		baseBlock: *newBaseBlock(host, meta),
	}
	blk.data.cols = make([]col.IColumnBlock, 0)
	blk.OnZeroCB = blk.close
	blk.Ref()

	if err := blk.initColumns(); err != nil {
		return nil, err
	}

	blk.GetObject = func() interface{} { return blk }
	blk.Pin = func(o interface{}) { o.(iface.IBlock).Ref() }
	blk.Unpin = func(o interface{}) { o.(iface.IBlock).Unref() }

	if blk.indexholder != nil {
		blk.indexholder.Init(blk.GetSegmentFile())
	}
	return blk, nil
}

func (blk *block) initColumns() error {
	blk.data.sizes = make([]uint64, 0)
	for idx, _ := range blk.meta.Segment.Table.Schema.ColDefs {
		blk.Ref()
		colBlk := col.NewStdColumnBlock(blk, idx)
		blk.data.cols = append(blk.data.cols, colBlk)
		if blk.typ >= base.PERSISTENT_BLK {
			blk.data.sizes = append(blk.data.sizes, colBlk.Size())
		}
	}
	return nil
}

func (blk *block) close() {
	blk.baseBlock.release()
	for _, colBlk := range blk.data.cols {
		colBlk.Unref()
	}
	blk.OnVersionStale()
}

func (blk *block) Size(attr string) uint64 {
	idx := blk.meta.Segment.Table.Schema.GetColIdx(attr)
	if idx < 0 {
		panic(fmt.Sprintf("Specified attr %s not found", attr))
	}
	if !blk.IsMutable() {
		return blk.data.sizes[idx]
	}
	return blk.data.cols[idx].Size()
}

func (blk *block) GetSegmentedIndex() (id uint64, ok bool) {
	if blk.typ == base.TRANSIENT_BLK {
		return id, ok
	}
	return blk.meta.GetAppliedIndex()
}

func (blk *block) cloneWithUpgradeColumns(upgraded *block) {
	for idx, colBlk := range blk.data.cols {
		upgraded.Ref()
		upgradedCol := colBlk.CloneWithUpgrade(upgraded)
		upgraded.data.cols[idx] = upgradedCol
		upgraded.data.sizes = append(upgraded.data.sizes, upgradedCol.Size())
	}
}

func (blk *block) CloneWithUpgrade(host iface.ISegment, meta *md.Block) (iface.IBlock, error) {
	defer host.Unref()
	newBase, err := blk.upgrade(host, meta)
	if err != nil {
		return nil, err
	}

	upgraded := &block{
		baseBlock: *newBase,
	}

	upgraded.data.cols = make([]col.IColumnBlock, len(blk.data.cols))
	blk.cloneWithUpgradeColumns(upgraded)

	upgraded.OnZeroCB = upgraded.close

	upgraded.Ref()

	upgraded.GetObject = func() interface{} { return upgraded }
	upgraded.Pin = func(o interface{}) { o.(iface.IBlock).Ref() }
	upgraded.Unpin = func(o interface{}) { o.(iface.IBlock).Unref() }

	if upgraded.indexholder != nil {
		upgraded.indexholder.Init(upgraded.GetSegmentFile())
	}

	return upgraded, nil
}

func (blk *block) String() string {
	s := fmt.Sprintf("<Blk[%d]>(ColBlk=%d)(Refs=%d)", blk.meta.ID, len(blk.data.cols), blk.RefCount())
	// for _, colBlk := range blk.data.cols {
	// 	s = fmt.Sprintf("%s\n\t%s", s, colBlk.String())
	// }
	return s
}

func (blk *block) GetVectorWrapper(attrid int) (*vector.VectorWrapper, error) {
	vec, err := blk.data.cols[attrid].LoadVectorWrapper()
	if err != nil {
		return nil, err
	}
	return vec, nil
}

func (blk *block) GetVectorCopy(attr string, compressed, deCompressed *bytes.Buffer) (*ro.Vector, error) {
	colIdx := blk.meta.Segment.Table.Schema.GetColIdx(attr)
	vec, err := blk.data.cols[colIdx].ForceLoad(compressed, deCompressed)
	if err != nil {
		return nil, err
	}
	return vec, nil
}

func (blk *block) Prefetch(attr string) error {
	colIdx := blk.meta.Segment.Table.Schema.GetColIdx(attr)
	if colIdx == -1 {
		return errors.New("column not found")
	}
	return blk.data.cols[colIdx].Prefetch()
}

func (blk *block) GetFullBatch() batch.IBatch {
	vecs := make([]vector.IVector, len(blk.data.cols))
	attrs := make([]int, len(blk.data.cols))
	for idx, colBlk := range blk.data.cols {
		vecs[idx] = colBlk.GetVector()
		attrs[idx] = colBlk.GetColIdx()
	}
	blk.Ref()
	return wrapper.NewBatch(blk, attrs, vecs)
}

func (blk *block) GetBatch(attrs []int) dbi.IBatchReader {
	// TODO: check attrs validity
	vecs := make([]vector.IVector, len(attrs))
	clonedAttrs := make([]int, len(attrs))
	for idx, attr := range attrs {
		clonedAttrs[idx] = attr
		vecs[idx] = blk.data.cols[attr].GetVector()
	}
	blk.Ref()
	return wrapper.NewBatch(blk, attrs, vecs).(dbi.IBatchReader)
}
