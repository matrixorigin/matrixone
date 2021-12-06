package index

import (
	roaring "github.com/RoaringBitmap/roaring/roaring64"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/base"
)

type unsortedSegmentHolder struct {

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

func (holder *unsortedSegmentHolder) AllocateVersion(colIdx int) uint64 {
	panic("implement me")
}

func (holder *unsortedSegmentHolder) IndicesCount() int {
	panic("implement me")
}

func (holder *unsortedSegmentHolder) DropIndex(filename string) {
	panic("implement me")
}

func (holder *unsortedSegmentHolder) LoadIndex(file base.ISegmentFile, filename string) {
	panic("implement me")
}

func (holder *unsortedSegmentHolder) StringIndicesRefsNoLock() string {
	panic("implement me")
}

func (holder *unsortedSegmentHolder) close() {
	panic("implement me")
}