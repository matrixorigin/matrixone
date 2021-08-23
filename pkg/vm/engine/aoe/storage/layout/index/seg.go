package index

import (
	"fmt"
	mgrif "matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"sync"
	"sync/atomic"
)

type PostCloseCB = func(interface{})

type SegmentHolder struct {
	common.RefHelper
	ID     common.ID
	BufMgr mgrif.IBufferManager
	Inited bool
	self   struct {
		sync.RWMutex
		Indices    []*Node
		ColIndices map[int][]int
	}
	tree struct {
		sync.RWMutex
		Blocks   []*BlockHolder
		IdMap    map[uint64]int
		BlockCnt int32
	}
	Type        base.SegmentType
	PostCloseCB PostCloseCB
}

func newSegmentHolder(bufMgr mgrif.IBufferManager, id common.ID, segType base.SegmentType, cb PostCloseCB) *SegmentHolder {
	holder := &SegmentHolder{ID: id, Type: segType, BufMgr: bufMgr}
	holder.tree.Blocks = make([]*BlockHolder, 0)
	holder.tree.IdMap = make(map[uint64]int)
	holder.self.ColIndices = make(map[int][]int)
	holder.self.Indices = make([]*Node, 0)
	holder.OnZeroCB = holder.close
	holder.PostCloseCB = cb
	holder.Ref()
	return holder
}

func (holder *SegmentHolder) Init(segFile base.ISegmentFile) {
	if holder.Inited {
		panic("logic error")
	}
	indicesMeta := segFile.GetIndicesMeta()
	if indicesMeta == nil {
		return
	}
	for _, meta := range indicesMeta.Data {
		vf := segFile.MakeVirtualIndexFile(meta)
		col := int(meta.Cols.ToArray()[0])
		var node *Node
		switch meta.Type {
		case base.ZoneMap:
			node = newNode(holder.BufMgr, vf, false, SegmentZoneMapIndexConstructor, meta.Cols, nil)
		case base.NumBsi:
			node = newNode(holder.BufMgr, vf, false, NumericBsiIndexConstructor, meta.Cols, nil)
			//log.Info(node.GetManagedNode().DataNode.(*NumericBsiIndex).Get(uint64(39)))
			//node.Close()
		default:
			// todo: str bsi
			panic("unsupported index type")
		}
		idxes, ok := holder.self.ColIndices[col]
		if !ok {
			idxes = make([]int, 0)
			holder.self.ColIndices[col] = idxes
		}
		holder.self.ColIndices[col] = append(holder.self.ColIndices[col], len(holder.self.Indices))
		holder.self.Indices = append(holder.self.Indices, node)
	}
	holder.Inited = true
}

func (holder *SegmentHolder) close() {
	for _, blk := range holder.tree.Blocks {
		blk.Unref()
	}

	for _, colIndex := range holder.self.Indices {
		colIndex.Unref()
	}
	if holder.PostCloseCB != nil {
		holder.PostCloseCB(holder)
	}
}

func (holder *SegmentHolder) EvalFilter(colIdx int, ctx *FilterCtx) error {
	idxes, ok := holder.self.ColIndices[colIdx]
	if !ok {
		// TODO
		ctx.BoolRes = true
		return nil
	}
	var err error
	for _, idx := range idxes {
		node := holder.self.Indices[idx].GetManagedNode()
		err = node.DataNode.(Index).Eval(ctx)
		if err != nil {
			node.Close()
			return err
		}
		node.Close()
	}
	return nil
}

func (holder *SegmentHolder) CollectMinMax(colIdx int) (min []interface{}, max []interface{}, err error) {
	idxes, ok := holder.self.ColIndices[colIdx]
	if !ok {
		return nil, nil, nil
	}

	for _, idx := range idxes {
		node := holder.self.Indices[idx].GetManagedNode()
		if node.DataNode.(Index).Type() != base.ZoneMap {
			err = node.Close()
			if err != nil {
				return
			}
			continue
		}
		index := node.DataNode.(*SegmentZoneMapIndex)
		min = make([]interface{}, len(index.BlkMin))
		max = make([]interface{}, len(index.BlkMax))
		for i := 0; i < len(min); i++ {
			min[i] = index.BlkMin[i]
			max[i] = index.BlkMax[i]
		}
		err = node.Close()
		if err != nil {
			return
		}
	}
	return
}

func (holder *SegmentHolder) stringNoLock() string {
	s := fmt.Sprintf("<IndexSegmentHolder[%s]>[Ty=%v](Cnt=%d)(RefCount=%d)", holder.ID.SegmentString(), holder.Type,
		holder.tree.BlockCnt, holder.RefCount())
	for _, blk := range holder.tree.Blocks {
		s = fmt.Sprintf("%s\n\t%s", s, blk.stringNoLock())
	}
	return s
}

func (holder *SegmentHolder) StrongRefBlock(id uint64) (blk *BlockHolder) {
	holder.tree.RLock()
	idx, ok := holder.tree.IdMap[id]
	if !ok {
		holder.tree.RUnlock()
		return nil
	}
	blk = holder.tree.Blocks[idx]
	blk.Ref()
	holder.tree.RUnlock()
	return blk
}

func (holder *SegmentHolder) RegisterBlock(id common.ID, blkType base.BlockType, cb PostCloseCB) *BlockHolder {
	blk := newBlockHolder(holder.BufMgr, id, blkType, cb)
	holder.addBlock(blk)
	blk.Ref()
	return blk
}

func (holder *SegmentHolder) addBlock(blk *BlockHolder) {
	holder.tree.Lock()
	defer holder.tree.Unlock()
	_, ok := holder.tree.IdMap[blk.ID.BlockID]
	if ok {
		panic(fmt.Sprintf("Duplicate blk %s for seg %s", blk.ID.BlockString(), holder.ID.SegmentString()))
	}
	holder.tree.IdMap[blk.ID.BlockID] = len(holder.tree.Blocks)
	holder.tree.Blocks = append(holder.tree.Blocks, blk)
	atomic.AddInt32(&holder.tree.BlockCnt, int32(1))
}

func (holder *SegmentHolder) DropBlock(id uint64) *BlockHolder {
	holder.tree.Lock()
	defer holder.tree.Unlock()
	idx, ok := holder.tree.IdMap[id]
	if !ok {
		panic(fmt.Sprintf("Specified blk %d not found in seg %d", id, holder.ID))
	}
	dropped := holder.tree.Blocks[idx]
	delete(holder.tree.IdMap, id)
	holder.tree.Blocks = append(holder.tree.Blocks[:idx], holder.tree.Blocks[idx+1:]...)
	atomic.AddInt32(&holder.tree.BlockCnt, int32(-1))
	return dropped
}

func (holder *SegmentHolder) GetBlockCount() int32 {
	return atomic.LoadInt32(&holder.tree.BlockCnt)
}

func (holder *SegmentHolder) UpgradeBlock(id uint64, blkType base.BlockType) *BlockHolder {
	holder.tree.Lock()
	defer holder.tree.Unlock()
	idx, ok := holder.tree.IdMap[id]
	if !ok {
		panic(fmt.Sprintf("specified blk %d not found in %d", id, holder.ID))
	}
	stale := holder.tree.Blocks[idx]
	if stale.Type >= blkType {
		panic(fmt.Sprintf("Cannot upgrade blk %d, type %d", id, blkType))
	}
	blk := newBlockHolder(holder.BufMgr, stale.ID, blkType, stale.PostCloseCB)
	holder.tree.Blocks[idx] = blk
	blk.Ref()
	stale.Unref()
	return blk
}
