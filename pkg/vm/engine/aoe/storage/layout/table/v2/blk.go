package table

import (
	"fmt"
	ro "matrixone/pkg/container/vector"
	bmgrif "matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/container/batch"
	"matrixone/pkg/vm/engine/aoe/storage/container/vector"
	"matrixone/pkg/vm/engine/aoe/storage/dbi"
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"matrixone/pkg/vm/engine/aoe/storage/layout/index"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v2/col"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v2/iface"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v2/wrapper"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata"
	"matrixone/pkg/vm/process"
	"sync"
	// log "github.com/sirupsen/logrus"
)

type Block struct {
	common.BaseMvcc
	common.RefHelper
	data struct {
		sync.RWMutex
		Columns  []col.IColumnBlock
		Helper   map[string]int
		AttrSize []uint64
		Next     iface.IBlock
	}
	Meta        *md.Block
	MTBufMgr    bmgrif.IBufferManager
	SSTBufMgr   bmgrif.IBufferManager
	IndexHolder *index.BlockHolder
	FsMgr       base.IManager
	SegmentFile base.ISegmentFile
	Type        base.BlockType
}

func NewBlock(host iface.ISegment, meta *md.Block) (iface.IBlock, error) {
	blk := &Block{
		Meta:      meta,
		MTBufMgr:  host.GetMTBufMgr(),
		SSTBufMgr: host.GetSSTBufMgr(),
		FsMgr:     host.GetFsManager(),
	}

	blk.data.Columns = make([]col.IColumnBlock, 0)
	blk.data.Helper = make(map[string]int)
	blk.OnZeroCB = blk.close

	var blkType base.BlockType
	if meta.DataState < md.FULL {
		blkType = base.TRANSIENT_BLK
	} else if host.GetType() == base.UNSORTED_SEG {
		blkType = base.PERSISTENT_BLK
	} else {
		blkType = base.PERSISTENT_SORTED_BLK
	}
	blk.Type = blkType
	indexHolder := host.GetIndexHolder().RegisterBlock(meta.AsCommonID().AsBlockID(), blkType, nil)

	blk.SegmentFile = host.GetSegmentFile()
	blk.IndexHolder = indexHolder
	blk.Ref()
	err := blk.initColumns()
	if err != nil {
		return nil, err
	}
	blk.GetObject = func() interface{} {
		return blk
	}
	blk.Pin = func(o interface{}) {
		o.(*Block).Ref()
	}
	blk.Unpin = func(o interface{}) {
		o.(*Block).Unref()
	}

	return blk, nil
}

func (blk *Block) initColumns() error {
	blk.data.AttrSize = make([]uint64, 0)
	for idx, colDef := range blk.Meta.Segment.Schema.ColDefs {
		blk.Ref()
		colBlk := col.NewStdColumnBlock(blk, idx)
		blk.data.Helper[colDef.Name] = len(blk.data.Columns)
		blk.data.Columns = append(blk.data.Columns, colBlk)
		if blk.Type >= base.PERSISTENT_BLK {
			blk.data.AttrSize = append(blk.data.AttrSize, colBlk.Size())
		}
	}
	return nil
}

func (blk *Block) GetType() base.BlockType {
	return blk.Type
}

func (blk *Block) GetRowCount() uint64 {
	return blk.Meta.GetCount()
}

func (blk *Block) Size(attr string) uint64 {
	idx, ok := blk.data.Helper[attr]
	if !ok {
		panic("logic error")
	}
	if blk.Type >= base.PERSISTENT_BLK {
		return blk.data.AttrSize[idx]
	}
	return blk.data.Columns[idx].Size()
}

func (blk *Block) close() {
	if blk.IndexHolder != nil {
		blk.IndexHolder.Unref()
	}
	for _, colBlk := range blk.data.Columns {
		// log.Infof("destroy blk %d, col %d, refs %d", blk.Meta.ID, idx, colBlk.RefCount())
		colBlk.Unref()
	}
	if blk.data.Next != nil {
		blk.data.Next.Unref()
		blk.data.Next = nil
	}

	blk.OnVersionStale()
}

func (blk *Block) GetMTBufMgr() bmgrif.IBufferManager {
	return blk.MTBufMgr
}

func (blk *Block) GetSSTBufMgr() bmgrif.IBufferManager {
	return blk.SSTBufMgr
}

func (blk *Block) GetFsManager() base.IManager {
	return blk.FsMgr
}

func (blk *Block) GetIndexHolder() *index.BlockHolder {
	blk.IndexHolder.Ref()
	return blk.IndexHolder
}

func (blk *Block) GetMeta() *md.Block {
	return blk.Meta
}

func (blk *Block) CloneWithUpgrade(host iface.ISegment, meta *md.Block) (iface.IBlock, error) {
	if blk.Type == base.PERSISTENT_SORTED_BLK {
		panic("logic error")
	}
	if meta.DataState != md.FULL {
		panic("logic error")
	}

	blkId := meta.AsCommonID().AsBlockID()
	var (
		newType base.BlockType
	)
	indexHolder := host.GetIndexHolder().StrongRefBlock(meta.ID)
	newIndexHolder := false

	switch blk.Type {
	case base.TRANSIENT_BLK:
		newType = base.PERSISTENT_BLK
		if indexHolder == nil {
			indexHolder = host.GetIndexHolder().RegisterBlock(blkId, newType, nil)
			newIndexHolder = true
		} else if indexHolder.Type < newType {
			indexHolder = host.GetIndexHolder().UpgradeBlock(meta.ID, newType)
			newIndexHolder = true
		}
	case base.PERSISTENT_BLK:
		newType = base.PERSISTENT_SORTED_BLK
		if indexHolder == nil {
			indexHolder = host.GetIndexHolder().RegisterBlock(blkId, newType, nil)
			newIndexHolder = true
		} else if indexHolder.Type < newType {
			indexHolder = host.GetIndexHolder().UpgradeBlock(meta.ID, newType)
			newIndexHolder = true
		}
	default:
		panic("logic error")
	}
	cloned := &Block{
		Meta:        meta,
		MTBufMgr:    blk.MTBufMgr,
		SSTBufMgr:   blk.SSTBufMgr,
		FsMgr:       blk.FsMgr,
		IndexHolder: indexHolder,
		Type:        newType,
		SegmentFile: host.GetSegmentFile(),
	}
	cloned.data.Columns = make([]col.IColumnBlock, len(blk.data.Columns))
	cloned.data.Helper = make(map[string]int)
	if newIndexHolder {
		indexHolder.Init(cloned.SegmentFile)
	}
	blk.cloneWithUpgradeColumns(cloned)
	cloned.OnZeroCB = cloned.close
	cloned.Ref()
	cloned.GetObject = func() interface{} {
		return cloned
	}
	cloned.Pin = func(o interface{}) {
		o.(*Block).Ref()
	}
	cloned.Unpin = func(o interface{}) {
		o.(*Block).Unref()
	}
	host.Unref()
	return cloned, nil
}

func (blk *Block) cloneWithUpgradeColumns(cloned *Block) {
	for name, idx := range blk.data.Helper {
		colBlk := blk.data.Columns[idx]
		cloned.Ref()
		clonedCol := colBlk.CloneWithUpgrade(cloned)
		cloned.data.Helper[name] = idx
		cloned.data.Columns[idx] = clonedCol
		cloned.data.AttrSize = append(cloned.data.AttrSize, clonedCol.Size())
	}
}

func (blk *Block) GetSegmentFile() base.ISegmentFile {
	return blk.SegmentFile
}

func (blk *Block) String() string {
	s := fmt.Sprintf("<Blk[%d]>(ColBlk=%d)(Refs=%d)", blk.Meta.ID, len(blk.data.Columns), blk.RefCount())
	for _, colBlk := range blk.data.Columns {
		s = fmt.Sprintf("%s/n\t%s", s, colBlk.String())
	}
	return s
}

func (blk *Block) GetVectorCopy(attr string, ref uint64, proc *process.Process) (*ro.Vector, error) {
	colIdx := blk.Meta.Segment.Schema.GetColIdx(attr)
	vec, err := blk.data.Columns[colIdx].ForceLoad(ref, proc)
	if err != nil {
		return nil, err
	}
	return vec, nil
}

func (blk *Block) GetFullBatch() batch.IBatch {
	vecs := make([]vector.IVector, len(blk.data.Columns))
	attrs := make([]int, len(blk.data.Columns))
	for idx, colBlk := range blk.data.Columns {
		vecs[idx] = colBlk.GetVector()
		attrs[idx] = colBlk.GetColIdx()
	}
	blk.Ref()
	return wrapper.NewBatch(blk, attrs, vecs)
}

func (blk *Block) GetBatch(attrs []int) dbi.IBatchReader {
	// TODO: check attrs validity
	vecs := make([]vector.IVector, len(attrs))
	clonedAttrs := make([]int, len(attrs))
	for idx, attr := range attrs {
		clonedAttrs[idx] = attr
		vecs[idx] = blk.data.Columns[attr].GetVector()
	}
	blk.Ref()
	return wrapper.NewBatch(blk, attrs, vecs).(dbi.IBatchReader)
}

func (blk *Block) SetNext(next iface.IBlock) {
	blk.data.Lock()
	defer blk.data.Unlock()
	if blk.data.Next != nil {
		blk.data.Next.Unref()
	}
	blk.data.Next = next
}

func (blk *Block) GetNext() iface.IBlock {
	blk.data.RLock()
	if blk.data.Next != nil {
		blk.data.Next.Ref()
	}
	r := blk.data.Next
	blk.data.RUnlock()
	return r
}
