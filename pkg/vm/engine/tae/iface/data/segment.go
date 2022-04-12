package data

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio"
)

type Segment interface {
	GetAppender() (*common.ID, BlockAppender, error)
	SetAppender(uint64) (BlockAppender, error)
	GetID() uint64
	IsAppendable() bool
	GetSegmentFile() dataio.SegmentFile
}
