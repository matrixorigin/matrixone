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
	roaring2 "github.com/RoaringBitmap/roaring"
	roaring "github.com/RoaringBitmap/roaring/roaring64"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	mgrif "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"path/filepath"
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
	// do nothing
}

func (holder *unsortedSegmentHolder) EvalFilter(colIdx int, ctx *FilterCtx) error {
	holder.tree.RLock()
	defer holder.tree.RUnlock()
	blkSet := make([]uint64, 0)
	res := roaring2.NewBitmap()
	for _, blkHolder := range holder.tree.blockHolders {
		if ctx.BsiRequired {
			subCtx := &FilterCtx{Op: ctx.Op, Val: ctx.Val, ValMax: ctx.ValMax, ValMin: ctx.ValMin, BMRes: ctx.BMRes.Clone(), BsiRequired: true}
			if err := blkHolder.EvalFilter(colIdx, subCtx); err != nil {
				return err
			}
			if !subCtx.BMRes.IsEmpty() {
				//logutil.Infof("...... %+v", subCtx.BMRes.ToArray())
				res.Or(subCtx.BMRes)
			}
		} else {
			subCtx := &FilterCtx{Op: ctx.Op, Val: ctx.Val, ValMax: ctx.ValMax, ValMin: ctx.ValMin}
			if err := blkHolder.EvalFilter(colIdx, subCtx); err != nil {
				return err
			}
			if subCtx.BoolRes {
				blkSet = append(blkSet, blkHolder.ID.BlockID)
				ctx.BoolRes = true
			}
		}
	}
	ctx.BlockSet = blkSet
	ctx.BMRes = res
	if !ctx.BMRes.IsEmpty() {
		ctx.BoolRes = true
	}
	return nil
}

// CollectMinMax is not needed in unsortedSegmentHolder
func (holder *unsortedSegmentHolder) CollectMinMax(colIdx int) ([]interface{}, []interface{}, error) {
	panic("unsupported")
}

func (holder *unsortedSegmentHolder) Count(colIdx int, filter *roaring.Bitmap) (uint64, error) {
	holder.tree.RLock()
	defer holder.tree.RUnlock()
	total := uint64(0)
	for _, blkHolder := range holder.tree.blockHolders {
		cnt, err := blkHolder.Count(colIdx, filter)
		if err != nil {
			return 0, err
		}
		total += cnt
	}
	return total, nil
}

func (holder *unsortedSegmentHolder) NullCount(colIdx int, maxRows uint64, filter *roaring.Bitmap) (uint64, error) {
	holder.tree.RLock()
	defer holder.tree.RUnlock()
	total := uint64(0)
	for _, blkHolder := range holder.tree.blockHolders {
		cnt, err := blkHolder.NullCount(colIdx, maxRows, filter)
		if err != nil {
			return 0, err
		}
		total += cnt
	}
	return total, nil
}

func (holder *unsortedSegmentHolder) Min(colIdx int, filter *roaring.Bitmap) (interface{}, error) {
	holder.tree.RLock()
	defer holder.tree.RUnlock()
	var gmin interface{}
	flag := true
	for _, blkHolder := range holder.tree.blockHolders {
		min, err := blkHolder.Min(colIdx, filter)
		if err != nil {
			return 0, err
		}
		if min == nil {
			continue
		}
		if flag {
			gmin = min
			flag = false
		}
		if common.CompareInterface(gmin, min) > 0 {
			gmin = min
		}
	}
	return gmin, nil
}

func (holder *unsortedSegmentHolder) Max(colIdx int, filter *roaring.Bitmap) (interface{}, error) {
	holder.tree.RLock()
	defer holder.tree.RUnlock()
	var gmax interface{}
	flag := true
	for _, blkHolder := range holder.tree.blockHolders {
		max, err := blkHolder.Max(colIdx, filter)
		if err != nil {
			return 0, err
		}
		if max == nil {
			continue
		}
		if flag {
			gmax = max
			flag = false
		}
		if common.CompareInterface(max, gmax) > 0 {
			gmax = max
		}
	}
	return gmax, nil
}

func (holder *unsortedSegmentHolder) Sum(colIdx int, filter *roaring.Bitmap) (int64, uint64, error) {
	holder.tree.RLock()
	defer holder.tree.RUnlock()
	gsum := int64(0)
	gcnt := uint64(0)
	for _, blkHolder := range holder.tree.blockHolders {
		sum, cnt, err := blkHolder.Sum(colIdx, filter)
		if err != nil {
			return 0, 0, err
		}
		gsum += sum
		gcnt += cnt
	}
	return gsum, gcnt, nil
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
	blk := newBlockIndexHolder(holder.BufMgr, id, blockType, cb)
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

func (holder *unsortedSegmentHolder) FetchCurrentVersion(col uint16, blkId uint64) uint64 {
	holder.tree.RLock()
	defer holder.tree.RUnlock()
	blk, ok := holder.tree.blockHolders[blkId]
	if !ok {
		logutil.Infof("%d\n%+v\n", blkId, holder.tree.blockHolders)
		panic("logic error")
		//return uint64(1)
	}
	return blk.fetchCurrentVersion(col)
}

// IndicesCount is not supported in unsortedSegmentHolder
func (holder *unsortedSegmentHolder) IndicesCount() int {
	panic("unsupported")
}

// DropIndex is not supported in unsortedSegmentHolder
func (holder *unsortedSegmentHolder) DropIndex(filename string) {
	if name, ok := common.ParseBlockBitSlicedIndexFileName(filepath.Base(filename)); ok {
		_, tid, sid, bid, _, ok := common.ParseBlockBitSlicedIndexFileNameToInfo(name)
		if !ok {
			panic("unexpected error")
		}
		if sid != holder.ID.SegmentID || tid != holder.ID.TableID {
			panic("unexpected error")
		}
		holder.tree.RLock()
		blk, ok := holder.tree.blockHolders[bid]
		if !ok {
			panic("unexpected error")
		}
		blk.DropIndex(filename)
		holder.tree.RUnlock()
		return
	}
	panic("unexpected error")
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