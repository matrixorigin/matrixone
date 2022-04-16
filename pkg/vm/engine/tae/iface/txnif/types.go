package txnif

import (
	"io"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
)

type Txn2PC interface {
	PreCommit() error
	PrepareRollback() error
	PrepareCommit() error
	ApplyRollback() error
	ApplyCommit() error
}

type TxnReader interface {
	RLock()
	RUnlock()
	GetID() uint64
	GetStartTS() uint64
	GetCommitTS() uint64
	GetInfo() []byte
	IsTerminated(bool) bool
	IsVisible(o TxnReader) bool
	GetTxnState(waitIfcommitting bool) int32
	GetError() error
	GetStore() TxnStore
	String() string
	Repr() string
}

type TxnHandle interface {
	CreateDatabase(name string) (handle.Database, error)
	DropDatabase(name string) (handle.Database, error)
	GetDatabase(name string) (handle.Database, error)
	UseDatabase(name string) error
}

type TxnChanger interface {
	sync.Locker
	RLock()
	RUnlock()
	ToCommittedLocked() error
	ToCommittingLocked(ts uint64) error
	ToRollbackedLocked() error
	ToRollbackingLocked(ts uint64) error
	Commit() error
	Rollback() error
	SetError(error)
	SetPrepareCommitFn(func(interface{}) error)
}

type TxnWriter interface {
}

type TxnAsyncer interface {
	WaitDone() error
}

type TxnTest interface {
	MockSetCommitTSLocked(ts uint64)
}

type AsyncTxn interface {
	TxnTest
	Txn2PC
	TxnHandle
	TxnAsyncer
	TxnReader
	TxnWriter
	TxnChanger
}

type SyncTxn interface {
	TxnReader
	TxnWriter
	TxnChanger
}

type UpdateChain interface {
	sync.Locker
	RLock()
	RUnlock()
	GetID() *common.ID
	CheckDeletedLocked(start, end uint32, txn AsyncTxn) error
	CheckColumnUpdatedLocked(row uint32, colIdx uint16, txn AsyncTxn) error
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
	// MakeCommand(id uint32, forceFlush bool) (txnif.TxnCmd, txnbase.NodeEntry, error)
}

type TxnStore interface {
	Txn2PC
	io.Closer
	BindTxn(AsyncTxn)

	BatchDedup(id uint64, pks *vector.Vector) error
	LogSegmentID(tid, sid uint64)
	LogBlockID(tid, bid uint64)

	Append(id uint64, data *batch.Batch) error
	RangeDeleteLocalRows(id uint64, start, end uint32) error
	UpdateLocalValue(id uint64, row uint32, col uint16, v interface{}) error
	AddUpdateNode(id uint64, node UpdateNode) error

	RangeDelete(id *common.ID, start, end uint32) error
	Update(id *common.ID, row uint32, col uint16, v interface{}) error
	GetByFilter(id uint64, filter *handle.Filter) (*common.ID, uint32, error)
	GetValue(id *common.ID, row uint32, col uint16) (interface{}, error)

	CreateRelation(def interface{}) (handle.Relation, error)
	DropRelationByName(name string) (handle.Relation, error)
	GetRelationByName(name string) (handle.Relation, error)

	CreateDatabase(name string) (handle.Database, error)
	GetDatabase(name string) (handle.Database, error)
	DropDatabase(name string) (handle.Database, error)
	UseDatabase(name string) error

	CreateSegment(tid uint64) (handle.Segment, error)
	CreateBlock(tid, sid uint64) (handle.Block, error)

	AddTxnEntry(TxnEntryType, TxnEntry)
}

type TxnEntryType int16

type TxnEntry interface {
	sync.Locker
	RLock()
	RUnlock()
	PrepareCommit() error
	PrepareRollback() error
	ApplyCommit() error
	ApplyRollback() error
	MakeCommand(uint32) (TxnCmd, error)
}
