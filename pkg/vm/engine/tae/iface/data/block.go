package data

import (
	"bytes"
	"io"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/file"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

type CheckpointUnit interface {
	MutationInfo() string
	RunCalibration()
	EstimateScore() int
	BuildCompactionTaskFactory() (tasks.TxnTaskFactory, tasks.TaskType, []common.ID, error)
}

type BlockAppender interface {
	io.Closer
	GetID() *common.ID
	GetMeta() interface{}
	PrepareAppend(rows uint32) (n uint32, err error)
	ApplyAppend(bat *batch.Batch, offset, length uint32, txn txnif.AsyncTxn) (txnif.AppendNode, uint32, error)
	IsAppendable() bool
}

type Block interface {
	CheckpointUnit
	GetID() uint64
	MutationInfo() string
	MakeAppender() (BlockAppender, error)
	IsAppendable() bool
	Rows(txn txnif.AsyncTxn, coarse bool) int
	GetColumnDataByName(txn txnif.AsyncTxn, attr string, compressed, decompressed *bytes.Buffer) (*vector.Vector, *roaring.Bitmap, error)
	GetColumnDataById(txn txnif.AsyncTxn, colIdx int, compressed, decompressed *bytes.Buffer) (*vector.Vector, *roaring.Bitmap, error)
	RangeDelete(txn txnif.AsyncTxn, start, end uint32) (txnif.DeleteNode, error)
	Update(txn txnif.AsyncTxn, row uint32, colIdx uint16, v interface{}) (txnif.UpdateNode, error)

	CollectChangesInRange(startTs, endTs uint64) interface{}
	CollectAppendLogIndexes(startTs, endTs uint64) []*wal.Index
	// CollectColumnUpdateLogIndexes(colIdx int, startTs, endTs uint64) []*wal.Index

	// GetUpdateChain() txnif.UpdateChain
	BatchDedup(txn txnif.AsyncTxn, pks *vector.Vector) error
	GetByFilter(txn txnif.AsyncTxn, filter *handle.Filter) (uint32, error)
	GetValue(txn txnif.AsyncTxn, row uint32, col uint16) (interface{}, error)
	PPString(level common.PPLevel, depth int, prefix string) string
	GetBlockFile() file.Block
	GetTotalChanges() int
	RefreshIndex() error
	Destroy() error

	SetMaxCheckpointTS(ts uint64)
	GetMaxCheckpointTS() uint64
	GetMaxVisibleTS() uint64

	ForceCompact() error

	CheckpointWALClosure(endTs uint64) tasks.FuncT
	SyncBlockDataClosure(ts uint64, rows uint32) tasks.FuncT
	FlushColumnDataClosure(ts uint64, colIdx int, colData *vector.Vector, sync bool) tasks.FuncT
}
