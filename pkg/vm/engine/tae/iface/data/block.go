package data

import (
	"bytes"
	"io"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

type CheckpointUnit interface {
	GetID() uint64
	TryCheckpoint()
	IsDirty() bool
}

type BlockAppender interface {
	io.Closer
	GetID() *common.ID
	PrepareAppend(rows uint32) (n uint32, err error)
	ApplyAppend(bat *batch.Batch, offset, length uint32, txn txnif.AsyncTxn) (uint32, error)
}

type Block interface {
	CheckpointUnit
	MakeAppender() (BlockAppender, error)
	IsAppendable() bool
	Rows(txn txnif.AsyncTxn, coarse bool) int
	GetVectorCopy(txn txnif.AsyncTxn, attr string, compressed, decompressed *bytes.Buffer) (*vector.Vector, *roaring.Bitmap, error)
	RangeDelete(txn txnif.AsyncTxn, start, end uint32) (txnif.DeleteNode, error)
	Update(txn txnif.AsyncTxn, row uint32, colIdx uint16, v interface{}) (txnif.UpdateNode, error)

	// GetUpdateChain() txnif.UpdateChain
	BatchDedup(txn txnif.AsyncTxn, pks *vector.Vector) error
	GetByFilter(txn txnif.AsyncTxn, filter *handle.Filter) (uint32, error)
	GetValue(txn txnif.AsyncTxn, row uint32, col uint16) (interface{}, error)
	PPString(level common.PPLevel, depth int, prefix string) string
}
