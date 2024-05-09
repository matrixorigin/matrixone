// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package client

import (
	"context"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
)

// TxnOption options for setup transaction
// FIXME(fagongzi): refactor TxnOption to avoid mem alloc
type TxnOption func(*txnOperator)

// TxnClientCreateOption options for create txn
type TxnClientCreateOption func(*txnClient)

// TxnTimestampAware transaction timestamp aware
type TxnTimestampAware interface {
	// Minimum Active Transaction Timestamp
	MinTimestamp() timestamp.Timestamp
	// WaitLogTailAppliedAt wait log tail applied at ts
	WaitLogTailAppliedAt(ctx context.Context, ts timestamp.Timestamp) (timestamp.Timestamp, error)
	// GetLatestCommitTS get latest commit timestamp
	GetLatestCommitTS() timestamp.Timestamp
	// SyncLatestCommitTS sync latest commit timestamp
	SyncLatestCommitTS(timestamp.Timestamp)
	// GetSyncLatestCommitTSTimes returns times of sync latest commit ts
	GetSyncLatestCommitTSTimes() uint64
}

type TxnAction interface {
	// AbortAllRunningTxn rollback all running transactions. but still keep their workspace to avoid panic.
	AbortAllRunningTxn()
	// Pause the txn client to prevent new txn from being created.
	Pause()
	// Resume the txn client to allow new txn to be created.
	Resume()
}

// TxnClient transaction client, the operational entry point for transactions.
// Each CN node holds one instance of TxnClient.
type TxnClient interface {
	TxnTimestampAware
	TxnAction

	// New returns a TxnOperator to handle read and write operation for a
	// transaction.
	New(ctx context.Context, commitTS timestamp.Timestamp, options ...TxnOption) (TxnOperator, error)
	// NewWithSnapshot create a txn operator from a snapshot. The snapshot must
	// be from a CN coordinator txn operator.
	NewWithSnapshot(snapshot []byte) (TxnOperator, error)
	// Close closes client.sender
	Close() error
	// RefreshExpressionEnabled return true if refresh expression feature enabled
	RefreshExpressionEnabled() bool
	// CNBasedConsistencyEnabled return true if cn based consistency feature enabled
	CNBasedConsistencyEnabled() bool
	// IterTxns iter all txns
	IterTxns(func(TxnOverview) bool)
	// GetState returns the current state of txn client.
	GetState() TxnState
}

type TxnState struct {
	State int
	// user active txns
	Users int
	// all active txns
	ActiveTxns []string
	// FIFO queue for ready to active txn
	WaitActiveTxns []string
	// LatestTS is the latest timestamp of the txn client.
	LatestTS timestamp.Timestamp
}

// TxnOperator operator for transaction clients, handling read and write
// requests for transactions, and handling distributed transactions across DN
// nodes.
// Note: For Error returned by Read/Write/WriteAndCommit/Commit/Rollback, need
// to check if it is a moerr.ErrDNShardNotFound error, if so, the TN information
// held is out of date and needs to be reloaded by HAKeeper.
type TxnOperator interface {
	// GetOverview returns txn overview
	GetOverview() TxnOverview

	// CloneSnapshotOp clone a read-only snapshot op from parent txn operator
	CloneSnapshotOp(snapshot timestamp.Timestamp) TxnOperator
	IsSnapOp() bool

	// Txn returns the current txn metadata
	Txn() txn.TxnMeta
	// TxnOptions returns the current txn options
	TxnOptions() txn.TxnOptions
	// TxnRef returns pointer of current txn metadata. In RC mode, txn's snapshot ts
	// will updated before statement executed.
	TxnRef() *txn.TxnMeta
	// Snapshot a snapshot of the transaction handle that can be passed around the
	// network. In some scenarios, operations of a transaction are executed on multiple
	// CN nodes for performance acceleration. But with only one CN coordinator, Snapshot
	// can be used to recover the transaction operation handle at a non-CN coordinator
	// node, or it can be used to pass information back to the transaction coordinator
	// after the non-CN coordinator completes the transaction operation.
	Snapshot() ([]byte, error)
	// UpdateSnapshot in some scenarios, we need to boost the snapshotTimestamp to eliminate
	// the w-w conflict.
	// If ts is empty, it will use the latest commit timestamp which is received from DN.
	UpdateSnapshot(ctx context.Context, ts timestamp.Timestamp) error
	// SnapshotTS returns the snapshot timestamp of the transaction.
	SnapshotTS() timestamp.Timestamp
	// CreateTS returns the creation timestamp of the txnOperator.
	CreateTS() timestamp.Timestamp
	// Status returns the current transaction status.
	Status() txn.TxnStatus
	// ApplySnapshot CN coordinator applies a snapshot of the non-coordinator's transaction
	// operation information.
	ApplySnapshot(data []byte) error
	// Read transaction read operation, the operator routes the message based
	// on the given TN node information and waits for the read data synchronously.
	// The transaction has been aborted if ErrTxnAborted returned.
	// After use, SendResult needs to call the Release method
	Read(ctx context.Context, ops []txn.TxnRequest) (*rpc.SendResult, error)
	// Write transaction write operation, and the operator will record the DN
	// nodes written by the current transaction, and when it finds that multiple
	// TN nodes are written, it will start distributed transaction processing.
	// The transaction has been aborted if ErrTxnAborted returned.
	// After use, SendResult needs to call the Release method
	Write(ctx context.Context, ops []txn.TxnRequest) (*rpc.SendResult, error)
	// WriteAndCommit is similar to Write, but commit the transaction after write.
	// After use, SendResult needs to call the Release method
	WriteAndCommit(ctx context.Context, ops []txn.TxnRequest) (*rpc.SendResult, error)
	// Commit the transaction. If data has been written to multiple TN nodes, a
	// 2pc distributed transaction commit process is used.
	Commit(ctx context.Context) error
	// Rollback the transaction.
	Rollback(ctx context.Context) error

	// AddLockTable for pessimistic transactions, if the current transaction is successfully
	// locked, the metadata corresponding to the lockservice needs to be recorded to the txn, and
	// at transaction commit time, the metadata of all lock services accessed by the transaction
	// will be committed to tn to check. If the metadata of the lockservice changes in [lock, commit],
	// the transaction will be rolled back.
	AddLockTable(locktable lock.LockTable) error
	// AddWaitLock add wait lock for current txn
	AddWaitLock(tableID uint64, rows [][]byte, opt lock.LockOptions) uint64
	// RemoveWaitLock remove wait lock for current txn
	RemoveWaitLock(key uint64)
	// LockSkipped return true if lock need skipped.
	LockSkipped(tableID uint64, mode lock.LockMode) bool

	GetWaitActiveCost() time.Duration

	// AddWorkspace for the transaction
	AddWorkspace(workspace Workspace)
	// GetWorkspace from the transaction
	GetWorkspace() Workspace

	ResetRetry(bool)
	IsRetry() bool

	// AppendEventCallback append callback. All append callbacks will be called sequentially
	// if event happen.
	AppendEventCallback(event EventType, callbacks ...func(TxnEvent))

	// Debug send debug request to DN, after use, SendResult needs to call the Release
	// method.
	Debug(ctx context.Context, ops []txn.TxnRequest) (*rpc.SendResult, error)

	NextSequence() uint64

	EnterRunSql()
	ExitRunSql()
}

// TxnIDGenerator txn id generator
type TxnIDGenerator interface {
	// Generate returns a unique transaction id
	Generate() []byte
}

// SetupRuntimeTxnOptions setup runtime based txn options
func SetupRuntimeTxnOptions(
	rt runtime.Runtime,
	m txn.TxnMode,
	iso txn.TxnIsolation) {
	rt.SetGlobalVariables(runtime.TxnIsolation, iso)
	rt.SetGlobalVariables(runtime.TxnMode, m)
}

// TimestampWaiter is used to wait for the timestamp to reach a specified timestamp.
// In the Push mode of LogTail's Event, the TN pushes the logtail to the subscribed
// CN once a transaction has been Committed. So there is a actual wait (last push commit
// ts >= start ts). This is unfriendly to TP, so we can lose some freshness and use the
// latest commit ts received from the current TN push as the start ts of the transaction,
// which eliminates this physical wait.
type TimestampWaiter interface {
	// GetTimestamp get the latest commit ts as snapshot ts of the new txn. It will keep
	// blocking if latest commit timestamp received from TN is less than the given value.
	GetTimestamp(context.Context, timestamp.Timestamp) (timestamp.Timestamp, error)
	// NotifyLatestCommitTS notify the latest timestamp that received from DN. A applied logtail
	// commit ts is corresponds to an epoch. Whenever the connection of logtail of cn and tn is
	// reset, the epoch will be reset and all the ts of the old epoch should be invalidated.
	NotifyLatestCommitTS(appliedTS timestamp.Timestamp)
	// Pause pauses the timestamp waiter and cancel all waiters in timestamp waiter.
	// They will not wait for the newer timestamp anymore.
	Pause()
	// Resume resumes the cancel channel in the timestamp waiter after all transactions are
	// aborted.
	Resume()
	// CancelC returns the cancel channel of timestamp waiter. If it is nil, means that
	// the logtail consumer is reconnecting to logtail server and is aborting all transaction.
	// At this time, we cannot open new transactions.
	CancelC() chan struct{}
	// Close close the timestamp waiter
	Close()
	// LatestTS returns the latest timestamp of the waiter.
	LatestTS() timestamp.Timestamp
}

type Workspace interface {
	// StartStatement tag a statement is running
	StartStatement()
	// EndStatement tag end a statement is completed
	EndStatement()

	// IncrStatementID incr the execute statement id. It maintains the statement id, first statement is 1,
	// second is 2, and so on. If in rc mode, snapshot will updated to latest applied commit ts from dn. And
	// workspace will update snapshot data for later read request.
	IncrStatementID(ctx context.Context, commit bool) error
	// RollbackLastStatement rollback the last statement.
	RollbackLastStatement(ctx context.Context) error

	UpdateSnapshotWriteOffset()
	GetSnapshotWriteOffset() int

	// Adjust adjust workspace, adjust update's delete+insert to correct order and merge workspace.
	Adjust(writeOffset uint64) error

	Commit(ctx context.Context) ([]txn.TxnRequest, error)
	Rollback(ctx context.Context) error

	IncrSQLCount()
	GetSQLCount() uint64

	CloneSnapshotWS() Workspace

	BindTxnOp(op TxnOperator)
}

// TxnOverview txn overview include meta and status
type TxnOverview struct {
	// CreateAt create at
	CreateAt time.Time
	// Meta txn metadata
	Meta txn.TxnMeta
	// UserTxn true if is a user transaction
	UserTxn bool
	// WaitLocks wait locks
	WaitLocks []Lock
}

// Lock wait locks
type Lock struct {
	// TableID table id
	TableID uint64
	// Rows lock rows. If granularity is row, rows contains all point lock keys, otherwise rows
	// is range1start, range1end, range2start, range2end, ...
	Rows [][]byte
	// Options lock options, include lock type(row|range) and lock mode
	Options lock.LockOptions
}

type TxnEvent struct {
	Event     EventType
	Txn       txn.TxnMeta
	TableID   uint64
	Err       error
	Sequence  uint64
	Cost      time.Duration
	CostEvent bool
}
