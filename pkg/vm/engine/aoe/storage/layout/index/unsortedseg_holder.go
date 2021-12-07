package index

import (
	roaring "github.com/RoaringBitmap/roaring/roaring64"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"sync"
)

type unsortedSegmentHolder struct {
	common.RefHelper
	ID     common.ID
	tree struct {
		sync.RWMutex
		Blocks   []*BlockHolder
		IdMap    map[uint64]int
		BlockCnt int32
	}
	PostCloseCB PostCloseCB
}

func (holder *unsortedSegmentHolder) Init(file base.ISegmentFile) {
	panic("implement me")
}

func (holder *unsortedSegmentHolder) EvalFilter(colIdx int, ctx *FilterCtx) error {
	panic("implement me")
}

func (holder *unsortedSegmentHolder) CollectMinMax(colIdx int) ([]interface{}, []interface{}, error) {
	panic("implement me")
}

func (holder *unsortedSegmentHolder) Count(colIdx int, filter *roaring.Bitmap) (uint64, error) {
	panic("implement me")
}

func (holder *unsortedSegmentHolder) NullCount(colIdx int, filter *roaring.Bitmap) (uint64, error) {
	panic("implement me")
}

func (holder *unsortedSegmentHolder) Min(colIdx int, filter *roaring.Bitmap) (interface{}, error) {
	panic("implement me")
}

func (holder *unsortedSegmentHolder) Max(colIdx int, filter *roaring.Bitmap) (interface{}, error) {
	panic("implement me")
}

func (holder *unsortedSegmentHolder) Sum(colIdx int, filter *roaring.Bitmap) (int64, uint64, error) {
	panic("implement me")
}

func (holder *unsortedSegmentHolder) StrongRefBlock(id uint64) *BlockHolder {
	panic("implement me")
}

func (holder *unsortedSegmentHolder) RegisterBlock(id common.ID, blockType base.BlockType, cb PostCloseCB) *BlockHolder {
	panic("implement me")
}

func (holder *unsortedSegmentHolder) DropBlock(id uint64) *BlockHolder {
	panic("implement me")
}

func (holder *unsortedSegmentHolder) GetBlockCount() int32 {
	panic("implement me")
}

func (holder *unsortedSegmentHolder) UpgradeBlock(id uint64, blockType base.BlockType) *BlockHolder {
	panic("implement me")
}

func (holder *unsortedSegmentHolder) stringNoLock() string {
	panic("implement me")
}

// AllocateVersion is not supported in unsortedSegmentHolder
func (holder *unsortedSegmentHolder) AllocateVersion(colIdx int) uint64 {
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
		blk.close()
	}
	if holder.PostCloseCB != nil {
		holder.PostCloseCB(holder)
	}
}