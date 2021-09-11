package base

import (
	"matrixone/pkg/vm/engine/aoe/storage/container/batch"
	"matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
	"matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"matrixone/pkg/vm/engine/aoe/storage/mutation/buffer/base"
)

type BlockFlusher = func(base.INode, batch.IBatch, *metadata.Block, *dataio.TransientBlockFile) error

type MockSize struct {
	size uint64
}

func NewMockSize(size uint64) *MockSize {
	s := &MockSize{size: size}
	return s
}

func (mz *MockSize) Size() uint64 {
	return mz.size
}

type IMutableBlock interface {
	base.INode
	GetData() batch.IBatch
	GetFile() *dataio.TransientBlockFile
	GetSegmentedIndex() (uint64, bool)
	GetMeta() *metadata.Block
	SetStale()
	Flush() error
}
