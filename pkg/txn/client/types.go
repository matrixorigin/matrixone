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

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
)

// TxnOption options for setup transaction
type TxnOption func(*txnOperator)

// TxnClientCreateOption options for create txn
type TxnClientCreateOption func(*txnClient)

// TxnClient transaction client, the operational entry point for transactions.
// Each CN node holds one instance of TxnClient.
type TxnClient interface {
	// Minimum Active Transaction Timestamp
	MinTimestamp() timestamp.Timestamp
	// New returns a TxnOperator to handle read and write operation for a
	// transaction.
	New(ctx context.Context, commitTS timestamp.Timestamp, options ...TxnOption) (TxnOperator, error)
	// NewWithSnapshot create a txn operator from a snapshot. The snapshot must
	// be from a CN coordinator txn operator.
	NewWithSnapshot(snapshot []byte) (TxnOperator, error)
	// Close closes client.sender
	Close() error
}

// TxnOperator operator for transaction clients, handling read and write
// requests for transactions, and handling distributed transactions across DN
// nodes.
// Note: For Error returned by Read/Write/WriteAndCommit/Commit/Rollback, need
// to check if it is a moerr.ErrDNShardNotFound error, if so, the DN information
// held is out of date and needs to be reloaded by HAKeeper.
type TxnOperator interface {
	// Txn returns the current txn metadata
	Txn() txn.TxnMeta
	// Snapshot a snapshot of the transaction handle that can be passed around the
	// network. In some scenarios, operations of a transaction are executed on multiple
	// CN nodes for performance acceleration. But with only one CN coordinator, Snapshot
	// can be used to recover the transaction operation handle at a non-CN coordinator
	// node, or it can be used to pass information back to the transaction coordinator
	// after the non-CN coordinator completes the transaction operation.
	Snapshot() ([]byte, error)
	// UpdateSnapshot in some scenarios, we need to boost the snapshotTimestamp to eliminate
	// the w-w conflict
	UpdateSnapshot(ts timestamp.Timestamp) error
	// ApplySnapshot CN coordinator applies a snapshot of the non-coordinator's transaction
	// operation information.
	ApplySnapshot(data []byte) error
	// Read transaction read operation, the operator routes the message based
	// on the given DN node information and waits for the read data synchronously.
	// The transaction has been aborted if ErrTxnAborted returned.
	// After use, SendResult needs to call the Release method
	Read(ctx context.Context, ops []txn.TxnRequest) (*rpc.SendResult, error)
	// Write transaction write operation, and the operator will record the DN
	// nodes written by the current transaction, and when it finds that multiple
	// DN nodes are written, it will start distributed transaction processing.
	// The transaction has been aborted if ErrTxnAborted returned.
	// After use, SendResult needs to call the Release method
	Write(ctx context.Context, ops []txn.TxnRequest) (*rpc.SendResult, error)
	// WriteAndCommit is similar to Write, but commit the transaction after write.
	// After use, SendResult needs to call the Release method
	WriteAndCommit(ctx context.Context, ops []txn.TxnRequest) (*rpc.SendResult, error)
	// Commit the transaction. If data has been written to multiple DN nodes, a
	// 2pc distributed transaction commit process is used.
	Commit(ctx context.Context) error
	// Rollback the transaction.
	Rollback(ctx context.Context) error

	// AddLockTable for pessimistic transactions, if the current transaction is successfully
	// locked, the metadata corresponding to the lockservice needs to be recorded to the txn, and
	// at transaction commit time, the metadata of all lockservices accessed by the transaction
	// will be committed to dn to check. If the metadata of the lockservice changes in [lock, commit],
	// the transaction will be rolled back.
	AddLockTable(locktable lock.LockTable) error
}

// DebugableTxnOperator debugable txn operator
type DebugableTxnOperator interface {
	TxnOperator

	// Debug send debug request to DN, after use, SendResult needs to call the Release
	// method.
	Debug(ctx context.Context, ops []txn.TxnRequest) (*rpc.SendResult, error)
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
// In the Push mode of LogTail's Event, the DN pushes the logtail to the subscribed
// CN once a transaction has been Committed. So there is a actual wait (last push commit
// ts >= start ts). This is unfriendly to TP, so we can lose some freshness and use the
// latest commit ts received from the current DN pushg as the start ts of the transaction,
// which eliminates this physical wait.
type TimestampWaiter interface {
	// GetTimestamp get the latest commit ts as snapshot ts of the new txn. It will keep
	// blocking if latest commit timestamp received from DN is less than the given value.
	GetTimestamp(context.Context, timestamp.Timestamp) (timestamp.Timestamp, error)
	// NotifyLatestCommitTS notify the latest timestamp that received from DN
	NotifyLatestCommitTS(timestamp.Timestamp)
	// Close close the timestamp waiter
	Close()
}
