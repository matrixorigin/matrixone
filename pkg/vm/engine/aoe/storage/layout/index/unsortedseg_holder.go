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
		blockHolders map[uint64]*BlockIndexHolder
		BlockCnt int32
	}
	PostCloseCB PostCloseCB
}

func newUnsortedSegmentHolder(bufMgr mgrif.IBufferManager, id common.ID, cb PostCloseCB) SegmentIndexHolder {
	holder := &unsortedSegmentHolder{BufMgr: bufMgr, ID: id, PostCloseCB: cb}
	holder.tree.blockHolders = make(map[uint64]*BlockIndexHolder, 0)
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

func (holder *unsortedSegmentHolder) StrongRefBlock(id uint64) *BlockIndexHolder {
	holder.tree.RLock()
	defer holder.tree.RUnlock()
	blk, ok := holder.tree.blockHolders[id]
	if !ok {
		return nil
	}
	blk.Ref()
	return blk
}

func (holder *unsortedSegmentHolder) RegisterBlock(id common.ID, blockType base.BlockType, cb PostCloseCB) *BlockIndexHolder {
	blk := newBlockHolder(holder.BufMgr, id, blockType, cb)
	holder.addBlock(blk)
	blk.Ref()
	return blk
}

func (holder *unsortedSegmentHolder) DropBlock(id uint64) *BlockIndexHolder {
	holder.tree.Lock()
	defer holder.tree.Unlock()
	idx, ok := holder.tree.blockHolders[id]
	if !ok {
		panic("block not found")
	}
	delete(holder.tree.blockHolders, id)
	atomic.AddInt32(&holder.tree.BlockCnt, int32(-1))
	return idx
}

func (holder *unsortedSegmentHolder) GetBlockCount() int32 {
	return atomic.LoadInt32(&holder.tree.BlockCnt)
}

//func (holder *unsortedSegmentHolder) UpgradeBlock(id uint64, blockType base.BlockType) *BlockHolder {
//	holder.tree.Lock()
//	defer holder.tree.Unlock()
//	idx, ok := holder.tree.IdMap[id]
//	if !ok {
//		panic(fmt.Sprintf("specified blk %d not found in %d", id, holder.ID))
//	}
//	stale := holder.tree.Blocks[idx]
//	if stale.Type >= blockType {
//		panic(fmt.Sprintf("Cannot upgrade blk %d, type %d", id, blockType))
//	}
//	blk := newBlockHolder(holder.BufMgr, stale.ID, blockType, stale.PostCloseCB)
//	holder.tree.Blocks[idx] = blk
//	blk.Ref()
//	stale.Unref()
//	return blk
//}

func (holder *unsortedSegmentHolder) addBlock(blk *BlockIndexHolder) {
	holder.tree.Lock()
	defer holder.tree.Unlock()
	if _, ok := holder.tree.blockHolders[blk.ID.BlockID]; ok {
		panic("duplicate block")
	}
	holder.tree.blockHolders[blk.ID.BlockID] = blk
	atomic.AddInt32(&holder.tree.BlockCnt, int32(1))
}

func (holder *unsortedSegmentHolder) stringNoLock() string {
	s := fmt.Sprintf("<IndexSegmentHolder[%s]>[Ty=%v](Cnt=%d)(RefCount=%d)", holder.ID.SegmentString(), base.UNSORTED_SEG,
		holder.tree.BlockCnt, holder.RefCount())
	for _, blk := range holder.tree.blockHolders {
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
	for _, blk := range holder.tree.blockHolders {
		blk.Unref()
	}
	if holder.PostCloseCB != nil {
		holder.PostCloseCB(holder)
	}
}