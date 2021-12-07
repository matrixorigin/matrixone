package index

import (
	"fmt"
	roaring "github.com/RoaringBitmap/roaring/roaring64"
	mgrif "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"sync"
	"sync/atomic"
)

type unsortedSegmentHolder struct {
	common.RefHelper
	ID     common.ID
	BufMgr mgrif.IBufferManager
	tree struct {
		sync.RWMutex
		Blocks   []*BlockHolder
		IdMap    map[uint64]int
		BlockCnt int32
	}
	PostCloseCB PostCloseCB
}

func newUnsortedSegmentHolder(bufMgr mgrif.IBufferManager, id common.ID, cb PostCloseCB) SegmentIndexHolder {
	holder := &unsortedSegmentHolder{BufMgr: bufMgr, ID: id, PostCloseCB: cb}
	holder.tree.Blocks = make([]*BlockHolder, 0)
	holder.tree.IdMap = make(map[uint64]int)
	holder.OnZeroCB = holder.close
	holder.Ref()
	return holder
}

func (holder *unsortedSegmentHolder) HolderType() base.SegmentType {
	return base.UNSORTED_SEG
}

func (holder *unsortedSegmentHolder) GetID() common.ID {
	return holder.ID
}

func (holder *unsortedSegmentHolder) GetCB() PostCloseCB {
	return holder.PostCloseCB
}

func (holder *unsortedSegmentHolder) Init(file base.ISegmentFile) {
	// TODO(zzl)
}

func (holder *unsortedSegmentHolder) EvalFilter(colIdx int, ctx *FilterCtx) error {
	// TODO(zzl)
	return nil
}

func (holder *unsortedSegmentHolder) CollectMinMax(colIdx int) ([]interface{}, []interface{}, error) {
	// TODO(zzl)
	return nil, nil, nil
}

func (holder *unsortedSegmentHolder) Count(colIdx int, filter *roaring.Bitmap) (uint64, error) {
	// TODO(zzl)
	return 0, nil
}

func (holder *unsortedSegmentHolder) NullCount(colIdx int, filter *roaring.Bitmap) (uint64, error) {
	// TODO(zzl)
	return 0, nil
}

func (holder *unsortedSegmentHolder) Min(colIdx int, filter *roaring.Bitmap) (interface{}, error) {
	// TODO(zzl)
	return nil, nil
}

func (holder *unsortedSegmentHolder) Max(colIdx int, filter *roaring.Bitmap) (interface{}, error) {
	// TODO(zzl)
	return nil, nil
}

func (holder *unsortedSegmentHolder) Sum(colIdx int, filter *roaring.Bitmap) (int64, uint64, error) {
	// TODO(zzl)
	return 0, 0, nil
}

func (holder *unsortedSegmentHolder) StrongRefBlock(id uint64) *BlockHolder {
	holder.tree.RLock()
	idx, ok := holder.tree.IdMap[id]
	if !ok {
		holder.tree.RUnlock()
		return nil
	}
	blk := holder.tree.Blocks[idx]
	blk.Ref()
	holder.tree.RUnlock()
	return blk
}

func (holder *unsortedSegmentHolder) RegisterBlock(id common.ID, blockType base.BlockType, cb PostCloseCB) *BlockHolder {
	blk := newBlockHolder(holder.BufMgr, id, blockType, cb)
	holder.addBlock(blk)
	blk.Ref()
	return blk
}

func (holder *unsortedSegmentHolder) DropBlock(id uint64) *BlockHolder {
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

func (holder *unsortedSegmentHolder) GetBlockCount() int32 {
	return atomic.LoadInt32(&holder.tree.BlockCnt)
}

func (holder *unsortedSegmentHolder) UpgradeBlock(id uint64, blockType base.BlockType) *BlockHolder {
	holder.tree.Lock()
	defer holder.tree.Unlock()
	idx, ok := holder.tree.IdMap[id]
	if !ok {
		panic(fmt.Sprintf("specified blk %d not found in %d", id, holder.ID))
	}
	stale := holder.tree.Blocks[idx]
	if stale.Type >= blockType {
		panic(fmt.Sprintf("Cannot upgrade blk %d, type %d", id, blockType))
	}
	blk := newBlockHolder(holder.BufMgr, stale.ID, blockType, stale.PostCloseCB)
	holder.tree.Blocks[idx] = blk
	blk.Ref()
	stale.Unref()
	return blk
}

func (holder *unsortedSegmentHolder) addBlock(blk *BlockHolder) {
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

func (holder *unsortedSegmentHolder) stringNoLock() string {
	s := fmt.Sprintf("<IndexSegmentHolder[%s]>[Ty=%v](Cnt=%d)(RefCount=%d)", holder.ID.SegmentString(), base.UNSORTED_SEG,
		holder.tree.BlockCnt, holder.RefCount())
	for _, blk := range holder.tree.Blocks {
		s = fmt.Sprintf("%s\n\t%s", s, blk.stringNoLock())
	}
	return s
}

// AllocateVersion is not supported in unsortedSegmentHolder
func (holder *unsortedSegmentHolder) AllocateVersion(colIdx int) uint64 {
	panic("unsupported")
}

// VersionAllocater is not supported in unsortedSegmentHolder
func (holder *unsortedSegmentHolder) VersionAllocater() *ColumnsAllocator {
	panic("unsupported")
}

// IndicesCount is not supported in unsortedSegmentHolder
func (holder *unsortedSegmentHolder) IndicesCount() int {
	panic("unsupported")
}

// DropIndex is not supported in unsortedSegmentHolder
func (holder *unsortedSegmentHolder) DropIndex(filename string) {
	panic("unsupported")
}

// LoadIndex is not supported in unsortedSegmentHolder
func (holder *unsortedSegmentHolder) LoadIndex(file base.ISegmentFile, filename string) {
	panic("unsupported")
}

// StringIndicesRefsNoLock is not supported in unsortedSegmentHolder
func (holder *unsortedSegmentHolder) StringIndicesRefsNoLock() string {
	panic("unsupported")
}

func (holder *unsortedSegmentHolder) close() {
	for _, blk := range holder.tree.Blocks {
		blk.Unref()
	}
	if holder.PostCloseCB != nil {
		holder.PostCloseCB(holder)
	}
}