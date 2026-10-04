// Copyright 2021 - 2022 Matrix Origin
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

package storage

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
)

// TxnStorage defines the storage operations used by TxnService.
type TxnStorage interface {
	// Start starts txnStorage if necessary.
	Start() error
	// Close close the txn storage.
	Close(context.Context) error
	// Destroy is similar to Close, but perform remove all related data and resources.
	Destroy(context.Context) error

	// Read execute read requests sent by CN.
	//
	// The Payload parameter is unsafe and should no longer be held by Storage
	// after the return of the current call.
	//
	// If any of the data in the current read has been written by other transactions,
	// these write transaction IDs need to be returned. The transaction IDs that need to be returned include
	// the following:
	// case1. Txn.Status == Committing && CurrentTxn.SnapshotTimestamp > Txn.CommitTimestamp
	// case2. Txn.Status == Prepared && CurrentTxn.SnapshotTimestamp > Txn.PreparedTimestamp
	Read(ctx context.Context, txnMeta txn.TxnMeta, op uint32, payload []byte) (ReadResult, error)
	// Write execute write requests sent by CN.
	// TODO: Handle spec error by storage.
	Write(ctx context.Context, txnMeta txn.TxnMeta, op uint32, payload []byte) ([]byte, error)
	// Commit commit the transaction. TxnStorage needs to do conflict locally.
	Commit(
		ctx context.Context,
		txnMeta txn.TxnMeta,
		response *txn.TxnResponse,
		commitRequests *txn.TxnCommitRequest) (timestamp.Timestamp, error)
	// Rollback rollback the transaction.
	Rollback(ctx context.Context, txnMeta txn.TxnMeta) error

	// Debug handle debug request
	Debug(ctx context.Context, txnMeta txn.TxnMeta, op uint32, payload []byte) ([]byte, error)
}

// ReadResult read result from TxnStorage. When a read operation encounters any concurrent write transaction,
// it is necessary to wait for the write transaction to complete to confirm that the latest write is visible
// to the current transaction.
//
// To avoid the read Payload being parsed multiple times, TxnStorage can store the parsed state in the ReadResult
// and continue to use it while the ReadResult continues.
type ReadResult interface {
	// WaitTxns returns the ID of the concurrent write transaction encountered.
	WaitTxns() [][]byte
	// Read perform a read operation really. There is a TxnService to ensure that the transaction to be waited for
	// has finished(Committed or Aborted).
	Read() ([]byte, error)
	// Release release the ReadResult. TxnStorage can resuse the response data and the ReadResult.
	Release()
}
