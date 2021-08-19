package col

import (
	"fmt"
	"matrixone/pkg/container/types"
	ro "matrixone/pkg/container/vector"
	"matrixone/pkg/vm/engine/aoe/storage/container/vector"
	"matrixone/pkg/vm/engine/aoe/storage/dbi"
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v2/iface"
	"matrixone/pkg/vm/engine/aoe/storage/logutil"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"matrixone/pkg/vm/process"
	"time"
)

type StdColumnBlock struct {
	ColumnBlock
	Part IColumnPart
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
	blk := &StdColumnBlock{
		ColumnBlock: ColumnBlock{
			ColIdx:      colIdx,
			Meta:        host.GetMeta(),
			SegmentFile: host.GetSegmentFile(),
			Type:        host.GetType(),
		},
	}
	capacity := EstimateStdColumnCapacity(colIdx, blk.Meta)
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

func (blk *StdColumnBlock) CloneWithUpgrade(host iface.IBlock) IColumnBlock {
	defer host.Unref()
	if blk.Type == base.PERSISTENT_SORTED_BLK {
		panic("logic error")
	}
	if host.GetMeta().DataState != md.FULL {
		panic(fmt.Sprintf("logic error: blk %s DataState=%d", host.GetMeta().AsCommonID().BlockString(), host.GetMeta().DataState))
	}
	cloned := &StdColumnBlock{
		ColumnBlock: ColumnBlock{
			Type:        host.GetType(),
			ColIdx:      blk.ColIdx,
			Meta:        host.GetMeta(),
			IndexHolder: host.GetIndexHolder(),
			SegmentFile: host.GetSegmentFile(),
		},
	}
	cloned.Ref()
	blk.RLock()
	part := blk.Part.CloneWithUpgrade(cloned, host.GetSSTBufMgr())
	blk.RUnlock()
	if part == nil {
		logutil.Error("logic error")
		panic("logic error")
	}
	cloned.Part = part
	cloned.OnZeroCB = cloned.close
	cloned.Ref()
	return cloned
}

func (blk *StdColumnBlock) RegisterPart(part IColumnPart) {
	blk.Lock()
	defer blk.Unlock()
	if blk.Meta.ID != part.GetID() || blk.Part != nil {
		panic("logic error")
	}
	blk.Part = part
}

func (blk *StdColumnBlock) close() {
	if blk.IndexHolder != nil {
		blk.IndexHolder.Unref()
		blk.IndexHolder = nil
	}
	if blk.Part != nil {
		blk.Part.Close()
	}
	blk.Part = nil
	// log.Infof("destroy colblk %d, colidx %d", blk.Meta.ID, blk.ColIdx)
}

func (blk *StdColumnBlock) LoadVectorWrapper() (*vector.VectorWrapper, error) {
	return blk.Part.LoadVectorWrapper()
}

func (blk *StdColumnBlock) ForceLoad(ref uint64, proc *process.Process) (*ro.Vector, error) {
	return blk.Part.ForceLoad(ref, proc)
}

func (blk *StdColumnBlock) GetVector() vector.IVector {
	return blk.Part.GetVector()
}

func (blk *StdColumnBlock) GetVectorReader() dbi.IVectorReader {
	return blk.Part.GetVector().(dbi.IVectorReader)
}

func (blk *StdColumnBlock) Size() uint64 {
	return blk.Part.Size()
}

func (blk *StdColumnBlock) String() string {
	s := fmt.Sprintf("<Std[%s](T=%s)(Refs=%d)(Size=%d)>", blk.Meta.String(), blk.Type.String(), blk.RefCount(), blk.Meta.Count)
	return s
}
