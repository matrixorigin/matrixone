package col

import (
	"bytes"
	"fmt"
	"matrixone/pkg/container/types"
	ro "matrixone/pkg/container/vector"
	logutil2 "matrixone/pkg/logutil"
	"matrixone/pkg/vm/engine/aoe/storage/container/vector"
	"matrixone/pkg/vm/engine/aoe/storage/dbi"
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/iface"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"time"
)

type stdColumnBlock struct {
	columnBlock
	part IColumnPart
}

func EstimateStdColumnCapacity(colIdx int, meta *md.Block) uint64 {
	switch meta.Segment.Table.Schema.ColDefs[colIdx].Type.Oid {
	case types.T_json, types.T_char, types.T_varchar:
		return meta.Segment.Table.Conf.BlockMaxRows * 2 * 4
	default:
		return meta.Segment.Table.Conf.BlockMaxRows * uint64(meta.Segment.Table.Schema.ColDefs[colIdx].Type.Size)
	}
}

func NewStdColumnBlock(host iface.IBlock, colIdx int) IColumnBlock {
	defer host.Unref()
	blk := &stdColumnBlock{
		columnBlock: columnBlock{
			colIdx:  colIdx,
			meta:    host.GetMeta(),
			segFile: host.GetSegmentFile(),
			typ:     host.GetType(),
		},
	}
	capacity := EstimateStdColumnCapacity(colIdx, blk.meta)
	host.Ref()
	blk.Ref()
	part := NewColumnPart(host, blk, capacity)
	for part == nil {
		blk.Ref()
		host.Ref()
		part = NewColumnPart(host, blk, capacity)
		time.Sleep(time.Duration(1) * time.Millisecond)
	}
	part.Unref()
	blk.OnZeroCB = blk.close
	blk.Ref()
	return blk
}

func (blk *stdColumnBlock) CloneWithUpgrade(host iface.IBlock) IColumnBlock {
	defer host.Unref()
	if blk.typ == base.PERSISTENT_SORTED_BLK {
		panic("logic error")
	}
	if host.GetMeta().DataState != md.FULL {
		panic(fmt.Sprintf("logic error: blk %s DataState=%d", host.GetMeta().AsCommonID().BlockString(), host.GetMeta().DataState))
	}
	cloned := &stdColumnBlock{
		columnBlock: columnBlock{
			typ:         host.GetType(),
			colIdx:      blk.colIdx,
			meta:        host.GetMeta(),
			indexHolder: host.GetIndexHolder(),
			segFile:     host.GetSegmentFile(),
		},
	}
	cloned.Ref()
	blk.RLock()
	part := blk.part.CloneWithUpgrade(cloned, host.GetSSTBufMgr())
	blk.RUnlock()
	if part == nil {
		logutil2.Error("logic error")
		panic("logic error")
	}
	cloned.part = part
	cloned.OnZeroCB = cloned.close
	cloned.Ref()
	return cloned
}

func (blk *stdColumnBlock) RegisterPart(part IColumnPart) {
	blk.Lock()
	defer blk.Unlock()
	if blk.meta.ID != part.GetID() || blk.part != nil {
		panic("logic error")
	}
	blk.part = part
}

func (blk *stdColumnBlock) close() {
	if blk.indexHolder != nil {
		blk.indexHolder.Unref()
		blk.indexHolder = nil
	}
	if blk.part != nil {
		blk.part.Close()
	}
	blk.part = nil
	// log.Infof("destroy colblk %d, colidx %d", blk.meta.ID, blk.colIdx)
}

func (blk *stdColumnBlock) LoadVectorWrapper() (*vector.VectorWrapper, error) {
	return blk.part.LoadVectorWrapper()
}

func (blk *stdColumnBlock) ForceLoad(compressed, deCompressed *bytes.Buffer) (*ro.Vector, error) {
	return blk.part.ForceLoad(compressed, deCompressed)
}

func (blk *stdColumnBlock) Prefetch() error {
	return blk.part.Prefetch()
}

func (blk *stdColumnBlock) GetVector() vector.IVector {
	return blk.part.GetVector()
}

func (blk *stdColumnBlock) GetVectorReader() dbi.IVectorReader {
	return blk.part.GetVector().(dbi.IVectorReader)
}

func (blk *stdColumnBlock) Size() uint64 {
	return blk.part.Size()
}

func (blk *stdColumnBlock) String() string {
	s := fmt.Sprintf("<Std[%s](T=%s)(Refs=%d)(Size=%d)>", blk.meta.String(), blk.typ.String(), blk.RefCount(), blk.meta.Count)
	return s
}
