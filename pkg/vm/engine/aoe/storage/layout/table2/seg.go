package table

import (
	"fmt"
	bmgrif "matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/index"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table2/iface"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata"
	"sync"
	"sync/atomic"

	log "github.com/sirupsen/logrus"
)

type Segment struct {
	common.RefHelper
	Type base.SegmentType
	tree struct {
		sync.RWMutex
		Blocks   []iface.IBlock
		Helper   map[uint64]int
		BlockCnt uint32
	}
	MTBufMgr    bmgrif.IBufferManager
	SSTBufMgr   bmgrif.IBufferManager
	Meta        *md.Segment
	IndexHolder *index.SegmentHolder
	FsMgr       base.IManager
	SegmentFile base.ISegmentFile
}

func NewSegment(host iface.ITableData, meta *md.Segment) (iface.ISegment, error) {
	var err error
	segType := base.UNSORTED_SEG
	if meta.DataState == md.SORTED {
		segType = base.SORTED_SEG
	}
	seg := &Segment{
		Type:      segType,
		MTBufMgr:  host.GetMTBufMgr(),
		SSTBufMgr: host.GetSSTBufMgr(),
		FsMgr:     host.GetFsManager(),
		Meta:      meta,
	}

	segId := meta.AsCommonID().AsSegmentID()
	indexHolder := host.GetIndexHolder().RegisterSegment(segId, segType, nil)
	seg.IndexHolder = indexHolder
	segFile := seg.FsMgr.GetUnsortedFile(segId)
	if segType == base.UNSORTED_SEG {
		if segFile == nil {
			segFile, err = seg.FsMgr.RegisterUnsortedFiles(segId)
			if err != nil {
				panic(err)
			}
		}
		seg.IndexHolder.Init(segFile)
	} else {
		if segFile != nil {
			seg.FsMgr.UpgradeFile(segId)
		} else {
			segFile = seg.FsMgr.GetSortedFile(segId)
			if segFile == nil {
				segFile, err = seg.FsMgr.RegisterSortedFiles(segId)
				if err != nil {
					panic(err)
				}
			}
		}
		seg.IndexHolder.Init(segFile)
	}

	seg.tree.Blocks = make([]iface.IBlock, 0)
	seg.tree.Helper = make(map[uint64]int)
	seg.OnZeroCB = seg.noRefCB
	seg.SegmentFile = segFile
	seg.Ref()
	return seg, nil
}

func (seg *Segment) noRefCB() {
	for _, blk := range seg.tree.Blocks {
		blk.Unref()
		// log.Infof("blk refs=%d", blk.RefCount())
	}
	// log.Infof("destroy seg %d", seg.Meta.ID)
}

func (seg *Segment) GetMeta() *md.Segment {
	return seg.Meta
}

func (seg *Segment) GetSegmentFile() base.ISegmentFile {
	return seg.SegmentFile
}

func (seg *Segment) GetType() base.SegmentType {
	return seg.Type
}

func (seg *Segment) GetMTBufMgr() bmgrif.IBufferManager {
	return seg.MTBufMgr
}

func (seg *Segment) GetSSTBufMgr() bmgrif.IBufferManager {
	return seg.SSTBufMgr
}

func (seg *Segment) GetFsManager() base.IManager {
	return seg.FsMgr
}

func (seg *Segment) GetIndexHolder() *index.SegmentHolder {
	return seg.IndexHolder
}

func (seg *Segment) String() string {
	seg.tree.RLock()
	defer seg.tree.RUnlock()
	s := fmt.Sprintf("<Segment[%d]>(BlkCnt=%d)(Refs=%d)", seg.Meta.ID, seg.tree.BlockCnt, seg.RefCount())
	for _, blk := range seg.tree.Blocks {
		s = fmt.Sprintf("%s\n\t%s", s, blk.String())
	}
	return s
}

func (seg *Segment) RegisterBlock(blkMeta *md.Block) (blk iface.IBlock, err error) {
	blk, err = NewBlock(seg, blkMeta)
	if err != nil {
		return nil, err
	}
	seg.tree.Lock()
	defer seg.tree.Unlock()
	seg.tree.Blocks = append(seg.tree.Blocks, blk)
	seg.tree.Helper[blkMeta.ID] = int(seg.tree.BlockCnt)
	atomic.AddUint32(&seg.tree.BlockCnt, uint32(1))
	blk.Ref()
	return blk, err
}

func (seg *Segment) WeakRefBlock(id uint64) iface.IBlock {
	seg.tree.RLock()
	defer seg.tree.RUnlock()
	idx, ok := seg.tree.Helper[id]
	if !ok {
		return nil
	}
	return seg.tree.Blocks[idx]
}

func (seg *Segment) StrongRefBlock(id uint64) iface.IBlock {
	seg.tree.RLock()
	defer seg.tree.RUnlock()
	idx, ok := seg.tree.Helper[id]
	if !ok {
		return nil
	}
	blk := seg.tree.Blocks[idx]
	blk.Ref()
	return blk
}
func (seg *Segment) CloneWithUpgrade(td iface.ITableData, meta *md.Segment) (iface.ISegment, error) {
	if seg.Type != base.UNSORTED_SEG {
		panic("logic error")
	}
	cloned := &Segment{
		Type:      base.SORTED_SEG,
		MTBufMgr:  seg.MTBufMgr,
		SSTBufMgr: seg.SSTBufMgr,
		FsMgr:     seg.FsMgr,
		Meta:      meta,
	}

	indexHolder := td.GetIndexHolder().GetSegment(seg.Meta.ID)
	if indexHolder == nil {
		panic("logic error")
	}

	id := seg.Meta.AsCommonID().AsSegmentID()
	segFile := seg.FsMgr.UpgradeFile(id)
	if segFile == nil {
		panic("logic error")
	}
	if indexHolder.Type == base.UNSORTED_SEG {
		indexHolder = td.GetIndexHolder().UpgradeSegment(seg.Meta.ID, base.SORTED_SEG)
		seg.IndexHolder.Init(segFile)
	}
	cloned.IndexHolder = indexHolder
	cloned.SegmentFile = segFile
	for _, blk := range seg.tree.Blocks {
		newBlkMeta, err := cloned.Meta.ReferenceBlock(blk.GetMeta().ID)
		if err != nil {
			panic(err)
		}
		cloned.Ref()
		cur, err := blk.CloneWithUpgrade(cloned, newBlkMeta)
		if err != nil {
			panic(err)
		}
		cloned.tree.Blocks = append(cloned.tree.Blocks, cur)
	}

	cloned.Ref()
	return cloned, nil
}

func (seg *Segment) UpgradeBlock(meta *md.Block) (iface.IBlock, error) {
	if seg.Type != base.UNSORTED_SEG {
		panic("logic error")
	}
	if seg.Meta.ID != meta.Segment.ID {
		panic("logic error")
	}
	idx, ok := seg.tree.Helper[meta.ID]
	if !ok {
		log.Errorf("")
		panic("logic error")
	}
	old := seg.tree.Blocks[idx]
	seg.Ref()
	upgradeBlk, err := old.CloneWithUpgrade(seg, meta)
	if err != nil {
		return nil, err
	}
	seg.tree.Lock()
	defer seg.tree.Unlock()
	seg.tree.Blocks[idx] = upgradeBlk
	upgradeBlk.Ref()
	old.Unref()
	return upgradeBlk, nil
}
