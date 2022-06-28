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

	"github.com/matrixorigin/matrixone/pkg/pb/txn"
)

// TxnOption options for setup transaction
type TxnOption func(*txnOperator)

// TxnClientCreateOption options for create txn
type TxnClientCreateOption func(*txnClient)

// TxnClient transaction client, the operational entry point for transactions.
// Each CN node holds one instance of TxnClient.
type TxnClient interface {
	// New returns a TxnOperator to handle read and write operation for a
	// transaction.
	New(options ...TxnOption) TxnOperator
	// NewWithSnapshot create a txn operator from a snapshot. The snapshot must
	// be from a CN coordinator txn operator.
	NewWithSnapshot(snapshot []byte) (TxnOperator, error)
}

// TxnOperator operator for transaction clients, handling read and write
// requests for transactions, and handling distributed transactions across DN
// nodes.
type TxnOperator interface {
	// Snapshot a snapshot of the transaction handle that can be passed around the
	// network. In some scenarios, operations of a transaction are executed on multiple
	// CN nodes for performance acceleration. But with only one CN coordinator, Snapshot
	// can be used to recover the transaction operation handle at a non-CN coordinator
	// node, or it can be used to pass information back to the transaction coordinator
	// after the non-CN coordinator completes the transaction operation.
	Snapshot() ([]byte, error)
	// ApplySnapshot CN coordinator applies a snapshot of the non-coordinator's transaction
	// operation information.
	ApplySnapshot(data []byte) error
	// Read transaction read operation, the operator routes the message based
	// on the given DN node information and waits for the read data synchronously.
	// The transaction has been aborted if ErrTxnAborted returned.
	Read(ctx context.Context, ops []txn.TxnRequest) ([]txn.TxnResponse, error)
	// Write transaction write operation, and the operator will record the DN
	// nodes written by the current transaction, and when it finds that multiple
	// DN nodes are written, it will start distributed transaction processing.
	// The transaction has been aborted if ErrTxnAborted returned.
	Write(ctx context.Context, ops []txn.TxnRequest) ([]txn.TxnResponse, error)
	// WriteAndCommit is similar to Write, but commit the transaction after write.
	WriteAndCommit(ctx context.Context, ops []txn.TxnRequest) ([]txn.TxnResponse, error)
	// Commit the transaction. If data has been written to multiple DN nodes, a
	// 2pc distributed transaction commit process is used.
	Commit(ctx context.Context) error
	// Rollback the transaction.
	Rollback(ctx context.Context) error
}

// TxnSender is used to send transaction requests to the DN nodes.
type TxnSender interface {
	// Send send request to the specified DN node, and wait for response synchronously.
	// For any reason, if no response is received, the internal will keep retrying until
	// the Context times out.
	Send(context.Context, []txn.TxnRequest) ([]txn.TxnResponse, error)
}

// TxnIDGenerator txn id generator
type TxnIDGenerator interface {
	// Generate returns a unique transaction id
	Generate() []byte
}
