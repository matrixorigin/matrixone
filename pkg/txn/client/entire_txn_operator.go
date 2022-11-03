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
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
)

type EntireTxnOperator struct {
	txnOperator  TxnOperator
	tempOperator TxnOperator
}

// Txn returns the current txn metadata
func (eto *EntireTxnOperator) Txn() txn.TxnMeta {
	return eto.txnOperator.Txn()
}

func (eto *EntireTxnOperator) GetTemp() TxnOperator {
	return eto.tempOperator
}

func (eto *EntireTxnOperator) SetTemp(tc TxnOperator) {
	eto.tempOperator = tc
}

// Snapshot a snapshot of the transaction handle that can be passed around the
// network. In some scenarios, operations of a transaction are executed on multiple
// CN nodes for performance acceleration. But with only one CN coordinator, Snapshot
// can be used to recover the transaction operation handle at a non-CN coordinator
// node, or it can be used to pass information back to the transaction coordinator
// after the non-CN coordinator completes the transaction operation.
func (eto *EntireTxnOperator) Snapshot() ([]byte, error) {
	return eto.txnOperator.Snapshot()
}

// ApplySnapshot CN coordinator applies a snapshot of the non-coordinator's transaction
// operation information.
func (eto *EntireTxnOperator) ApplySnapshot(data []byte) error {
	return eto.txnOperator.ApplySnapshot(data)
}

// Read transaction read operation, the operator routes the message based
// on the given DN node information and waits for the read data synchronously.
// The transaction has been aborted if ErrTxnAborted returned.
// After use, SendResult needs to call the Release method
func (eto *EntireTxnOperator) Read(ctx context.Context, ops []txn.TxnRequest) (*rpc.SendResult, error) {
	return eto.txnOperator.Read(ctx, ops)
}

// Write transaction write operation, and the operator will record the DN
// nodes written by the current transaction, and when it finds that multiple
// DN nodes are written, it will start distributed transaction processing.
// The transaction has been aborted if ErrTxnAborted returned.
// After use, SendResult needs to call the Release method
func (eto *EntireTxnOperator) Write(ctx context.Context, ops []txn.TxnRequest) (*rpc.SendResult, error) {
	return eto.txnOperator.Write(ctx, ops)
}

// WriteAndCommit is similar to Write, but commit the transaction after write.
// After use, SendResult needs to call the Release method
func (eto *EntireTxnOperator) WriteAndCommit(ctx context.Context, ops []txn.TxnRequest) (*rpc.SendResult, error) {
	return eto.txnOperator.WriteAndCommit(ctx, ops)
}

// Commit the transaction. If data has been written to multiple DN nodes, a
// 2pc distributed transaction commit process is used.
func (eto *EntireTxnOperator) Commit(ctx context.Context) error {
	if eto.tempOperator != nil {
		eto.tempOperator.Commit(ctx)
	}
	return eto.txnOperator.Commit(ctx)
}

// Rollback the transaction.
func (eto *EntireTxnOperator) Rollback(ctx context.Context) error {
	if eto.tempOperator != nil {
		eto.tempOperator.Rollback(ctx)
	}
	return eto.txnOperator.Rollback(ctx)
}
