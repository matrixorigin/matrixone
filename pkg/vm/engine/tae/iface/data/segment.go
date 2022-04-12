package data

import (
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

type Segment interface {
	GetAppender() (*common.ID, BlockAppender, error)
	SetAppender(uint64) (BlockAppender, error)
	GetID() uint64
	IsAppendable() bool
	GetSegmentFile() dataio.SegmentFile
	BatchDedup(txn txnif.AsyncTxn, pks *vector.Vector) error
}
