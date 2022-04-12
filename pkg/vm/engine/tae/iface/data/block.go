package data

import (
	"bytes"
	"io"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

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
	// CopyBatch(cs []uint64, attrs []string, compressed []*bytes.Buffer, deCompressed []*bytes.Buffer) (*batch.Batch, error)
}
