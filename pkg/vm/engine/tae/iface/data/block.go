package data

import (
	"bytes"
	"io"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

type UpdateChain interface {
	sync.Locker
	RLock()
	RUnlock()
	GetID() *common.ID
	TryDeleteRowsLocked(start, end uint32, txn txnif.AsyncTxn) error
	TryUpdateColLocked(row uint32, colIdx uint16, txn txnif.AsyncTxn) error
}

type UpdateNode interface {
	sync.Locker
	RLock()
	RUnlock()
	GetID() *common.ID
	String() string
	GetChain() UpdateChain
	PrepareCommit() error
	ApplyDeleteRowsLocked(start, end uint32)
	ApplyUpdateColLocked(row uint32, colIdx uint16, v interface{})
	MakeCommand(id uint32, forceFlush bool) (txnif.TxnCmd, txnbase.NodeEntry, error)
}

type BlockAppender interface {
	io.Closer
	GetID() *common.ID
	PrepareAppend(rows uint32) (n uint32, err error)
	ApplyAppend(bat *batch.Batch, offset, length uint32, ctx interface{}) (uint32, error)
}

type Block interface {
	MakeAppender() (BlockAppender, error)
	IsAppendable() bool
	Rows(txn txnif.AsyncTxn, coarse bool) int
	GetVectorCopy(txn txnif.AsyncTxn, attr string, compressed, decompressed *bytes.Buffer) (*vector.Vector, error)
	RangeDelete(txn txnif.AsyncTxn, start, end uint32) (UpdateNode, error)
}
