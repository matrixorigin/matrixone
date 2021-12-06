package index

import (
	roaring "github.com/RoaringBitmap/roaring/roaring64"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/base"
)

type sortedSegmentHolder struct {
	
}

func (holder *sortedSegmentHolder) Init(file base.ISegmentFile) {
	panic("implement me")
}

func (holder *sortedSegmentHolder) EvalFilter(colIdx int, ctx *FilterCtx) error {
	panic("implement me")
}

func (holder *sortedSegmentHolder) CollectMinMax(colIdx int) ([]interface{}, []interface{}, error) {
	panic("implement me")
}

func (holder *sortedSegmentHolder) Count(colIdx int, filter *roaring.Bitmap) (uint64, error) {
	panic("implement me")
}

func (holder *sortedSegmentHolder) NullCount(colIdx int, filter *roaring.Bitmap) (uint64, error) {
	panic("implement me")
}

func (holder *sortedSegmentHolder) Min(colIdx int, filter *roaring.Bitmap) (interface{}, error) {
	panic("implement me")
}

func (holder *sortedSegmentHolder) Max(colIdx int, filter *roaring.Bitmap) (interface{}, error) {
	panic("implement me")
}

func (holder *sortedSegmentHolder) Sum(colIdx int, filter *roaring.Bitmap) (int64, uint64, error) {
	panic("implement me")
}

func (holder *sortedSegmentHolder) StrongRefBlock(id uint64) *BlockHolder {
	panic("implement me")
}

func (holder *sortedSegmentHolder) RegisterBlock(id common.ID, blockType base.BlockType, cb PostCloseCB) *BlockHolder {
	panic("implement me")
}

func (holder *sortedSegmentHolder) DropBlock(id uint64) *BlockHolder {
	panic("implement me")
}

func (holder *sortedSegmentHolder) GetBlockCount() int32 {
	panic("implement me")
}

func (holder *sortedSegmentHolder) UpgradeBlock(id uint64, blockType base.BlockType) *BlockHolder {
	panic("implement me")
}

func (holder *sortedSegmentHolder) stringNoLock() string {
	panic("implement me")
}

func (holder *sortedSegmentHolder) AllocateVersion(colIdx int) uint64 {
	panic("implement me")
}

func (holder *sortedSegmentHolder) IndicesCount() int {
	panic("implement me")
}

func (holder *sortedSegmentHolder) DropIndex(filename string) {
	panic("implement me")
}

func (holder *sortedSegmentHolder) LoadIndex(file base.ISegmentFile, filename string) {
	panic("implement me")
}

func (holder *sortedSegmentHolder) StringIndicesRefsNoLock() string {
	panic("implement me")
}

func (holder *sortedSegmentHolder) close() {
	panic("implement me")
}

