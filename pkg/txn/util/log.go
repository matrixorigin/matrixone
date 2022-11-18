// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package util

import (
	"bytes"
	"encoding/hex"

	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"go.uber.org/zap"
)

// LogTxnRead log txn read
func LogTxnRead(
	logger *zap.Logger,
	txnMeta txn.TxnMeta) {
	if ce := logger.Check(zap.DebugLevel, "txn read"); ce != nil {
		ce.Write(zap.String("txn", txnMeta.DebugString()))
	}
}

// LogTxnWrite log txn write
func LogTxnWrite(
	logger *zap.Logger,
	txnMeta txn.TxnMeta) {
	if ce := logger.Check(zap.DebugLevel, "txn write"); ce != nil {
		ce.Write(zap.String("txn", txnMeta.DebugString()))
	}
}

// LogTxnCommit log txn commit
func LogTxnCommit(
	logger *zap.Logger,
	txnMeta txn.TxnMeta) {
	if ce := logger.Check(zap.DebugLevel, "txn commit"); ce != nil {
		ce.Write(zap.String("txn", txnMeta.DebugString()))
	}
}

// LogTxnRollback log txn rollback
func LogTxnRollback(
	logger *zap.Logger,
	txnMeta txn.TxnMeta) {
	if ce := logger.Check(zap.DebugLevel, "txn rollback"); ce != nil {
		ce.Write(zap.String("txn", txnMeta.DebugString()))
	}
}

// LogTxnCreated log txn created
func LogTxnCreated(
	logger *zap.Logger,
	txnMeta txn.TxnMeta) {
	if ce := logger.Check(zap.DebugLevel, "txn created"); ce != nil {
		ce.Write(zap.String("txn", txnMeta.DebugString()))
	}
}

// LogTxnUpdated log txn updated
func LogTxnUpdated(
	logger *zap.Logger,
	txnMeta txn.TxnMeta) {
	if ce := logger.Check(zap.DebugLevel, "txn updated"); ce != nil {
		ce.Write(zap.String("txn", txnMeta.DebugString()))
	}
}

// LogTxnWaiterAdded log txn waiter added
func LogTxnWaiterAdded(
	logger *zap.Logger,
	txnMeta txn.TxnMeta,
	waitStatus txn.TxnStatus) {
	if ce := logger.Check(zap.DebugLevel, "txn waiter added"); ce != nil {
		ce.Write(TxnIDFieldWithID(txnMeta.ID),
			zap.String("wait-status", waitStatus.String()))
	}
}

// LogTxnHandleRequest log txn handle request
func LogTxnHandleRequest(
	logger *zap.Logger,
	request *txn.TxnRequest) {
	if ce := logger.Check(zap.DebugLevel, "txn handle request"); ce != nil {
		ce.Write(TxnIDFieldWithID(request.Txn.ID),
			zap.String("request", request.DebugString()))
	}
}

// LogTxnHandleResult log txn handle request
func LogTxnHandleResult(
	logger *zap.Logger,
	response *txn.TxnResponse) {
	if ce := logger.Check(zap.DebugLevel, "txn handle result"); ce != nil {
		ce.Write(zap.String("response", response.DebugString()))
	}
}

// LogTxnSendRequests log txn send txn requests
func LogTxnSendRequests(
	logger *zap.Logger,
	requests []txn.TxnRequest) {
	if ce := logger.Check(zap.DebugLevel, "txn send requests"); ce != nil {
		ce.Write(zap.String("requests", txn.RequestsDebugString(requests, true)))
	}
}

// LogTxnSendRequestsFailed log txn send txn requests failed
func LogTxnSendRequestsFailed(
	logger *zap.Logger,
	requests []txn.TxnRequest,
	err error) {
	// The payload cannot be recorded here because reading the payload field would
	// cause a DATA RACE, as it is possible that morpc was still processing the send
	// at the time of the error and would have manipulated the payload field. And logging
	// the error, the payload field does not need to be logged to the log either, you can
	// find the previous log to view the paylaod based on the request-id.
	logger.Error("txn send requests failed",
		zap.String("requests", txn.RequestsDebugString(requests, false)),
		zap.Error(err))
}

// LogTxnReceivedResponses log received txn responses
func LogTxnReceivedResponses(
	logger *zap.Logger,
	responses []txn.TxnResponse) {
	if ce := logger.Check(zap.DebugLevel, "txn received responses"); ce != nil {
		ce.Write(zap.String("responses", txn.ResponsesDebugString(responses)))
	}
}

// LogTxnCreateOn log Txn create on dn shard.
func LogTxnCreateOn(
	logger *zap.Logger,
	txnMeta txn.TxnMeta,
	dn metadata.DNShard) {
	if ce := logger.Check(zap.DebugLevel, "txn created on DNShard"); ce != nil {
		ce.Write(TxnField(txnMeta),
			TxnDNShardField(dn))
	}
}

// LogTxnReadBlockedByUncommittedTxns log Txn read blocked by other txns
func LogTxnReadBlockedByUncommittedTxns(
	logger *zap.Logger,
	txnMeta txn.TxnMeta,
	waitTxns [][]byte) {
	if ce := logger.Check(zap.DebugLevel, "txn read blocked by other uncommitted txns"); ce != nil {
		ce.Write(TxnField(txnMeta),
			TxnIDsField(waitTxns))
	}
}

// LogTxnWaitUncommittedTxnsFailed log Txn wait other uncommitted txns change to committed or abortted
// failed.
func LogTxnWaitUncommittedTxnsFailed(
	logger *zap.Logger,
	txnMeta txn.TxnMeta,
	waitTxns [][]byte,
	err error) {
	logger.Error("txn wait other uncommitted txns failed",
		TxnField(txnMeta),
		TxnIDsField(waitTxns),
		zap.Error(err))
}

// LogTxnReadFailed log Txn read failed.
func LogTxnNotFoundOn(
	logger *zap.Logger,
	txnMeta txn.TxnMeta,
	dn metadata.DNShard) {
	if ce := logger.Check(zap.DebugLevel, "txn not found on DNShard"); ce != nil {
		ce.Write(TxnField(txnMeta),
			TxnDNShardField(dn))
	}
}

// LogTxnWriteOnInvalidStatus log Txn write on invalid txn status.
func LogTxnWriteOnInvalidStatus(
	logger *zap.Logger,
	txnMeta txn.TxnMeta) {
	if ce := logger.Check(zap.DebugLevel, "txn write on invalid status"); ce != nil {
		ce.Write(TxnField(txnMeta))
	}
}

// LogTxnCommitOnInvalidStatus log Txn commit on invalid txn status.
func LogTxnCommitOnInvalidStatus(
	logger *zap.Logger,
	txnMeta txn.TxnMeta) {
	if ce := logger.Check(zap.DebugLevel, "txn commit on invalid status"); ce != nil {
		ce.Write(TxnField(txnMeta))
	}
}

// LogTxnReadFailed log Txn read failed.
func LogTxnReadFailed(
	logger *zap.Logger,
	txnMeta txn.TxnMeta,
	err error) {
	logger.Error("txn read failed",
		TxnField(txnMeta),
		zap.Error(err))
}

// LogTxnWriteFailed log Txn write failed.
func LogTxnWriteFailed(
	logger *zap.Logger,
	txnMeta txn.TxnMeta,
	err error) {
	logger.Error("txn write failed",
		TxnField(txnMeta),
		zap.Error(err))
}

// LogTxnParallelPrepareFailed log Txn parallel prepare failed
func LogTxnParallelPrepareFailed(
	logger *zap.Logger,
	txnMeta txn.TxnMeta,
	err error) {
	logger.Error("txn parallel prepare failed",
		TxnField(txnMeta),
		zap.Error(err))
}

// LogTxnParallelPrepareCompleted log Txn parallel prepare completed
func LogTxnParallelPrepareCompleted(
	logger *zap.Logger,
	txnMeta txn.TxnMeta) {
	if ce := logger.Check(zap.DebugLevel, "txn parallel prepare completed"); ce != nil {
		ce.Write(TxnField(txnMeta))
	}
}

// LogTxnPrepareFailedOn log Tx prepare failed on DNShard
func LogTxnPrepareFailedOn(
	logger *zap.Logger,
	txnMeta txn.TxnMeta,
	dn metadata.DNShard,
	err *txn.TxnError) {
	logger.Error("txn prepare failed on DNShard",
		TxnField(txnMeta),
		TxnDNShardField(dn),
		zap.String("error", err.DebugString()))
}

// LogTxnPrepareCompletedOn log Tx prepare completed on DNShard
func LogTxnPrepareCompletedOn(
	logger *zap.Logger,
	txnMeta txn.TxnMeta,
	dn metadata.DNShard,
	preparedTS timestamp.Timestamp) {
	if ce := logger.Check(zap.DebugLevel, "txn prepare completed on DNShard"); ce != nil {
		ce.Write(TxnField(txnMeta),
			TxnDNShardField(dn),
			zap.String("prepared-ts", preparedTS.DebugString()))
	}
}

// LogTxnStartAsyncCommit log start async commit distributed txn task
func LogTxnStartAsyncCommit(
	logger *zap.Logger,
	txnMeta txn.TxnMeta) {
	if ce := logger.Check(zap.DebugLevel, "async commit task started"); ce != nil {
		ce.Write(TxnField(txnMeta))
	}
}

// LogTxnStartAsyncRollback log start async rollback txn task
func LogTxnStartAsyncRollback(
	logger *zap.Logger,
	txnMeta txn.TxnMeta) {
	if ce := logger.Check(zap.DebugLevel, "async rollback task started"); ce != nil {
		ce.Write(TxnField(txnMeta))
	}
}

// LogTxnRollbackCompleted log Txn rollback completed
func LogTxnRollbackCompleted(
	logger *zap.Logger,
	txnMeta txn.TxnMeta) {
	if ce := logger.Check(zap.DebugLevel, "txn rollback completed"); ce != nil {
		ce.Write(TxnField(txnMeta))
	}
}

// LogTxnCommittingFailed log Txn Committing failed on coordinator failed
func LogTxnCommittingFailed(
	logger *zap.Logger,
	txnMeta txn.TxnMeta,
	err error) {
	logger.Error("txn committing failed, retry later",
		TxnDNShardField(txnMeta.DNShards[0]),
		TxnField(txnMeta),
		zap.Error(err))
}

// LogTxnStart1PCCommit log Txn start 1pc commit
func LogTxnStart1PCCommit(
	logger *zap.Logger,
	txnMeta txn.TxnMeta) {
	if ce := logger.Check(zap.DebugLevel, "txn commit with 1 PC"); ce != nil {
		ce.Write(TxnField(txnMeta))
	}
}

// LogTxn1PCCommitCompleted log Txn 1pc commit completed
func LogTxn1PCCommitCompleted(
	logger *zap.Logger,
	txnMeta txn.TxnMeta) {
	if ce := logger.Check(zap.DebugLevel, "txn commit with 1 PC completed"); ce != nil {
		ce.Write(TxnField(txnMeta))
	}
}

// LogTxnStart1PCCommitFailed log Txn 1pc commit failed
func LogTxnStart1PCCommitFailed(
	logger *zap.Logger,
	txnMeta txn.TxnMeta,
	err error) {
	logger.Error("txn commit with 1 PC failed",
		TxnField(txnMeta),
		zap.Error(err))
}

// LogTxnStart2PCCommit log Txn start 2pc commit
func LogTxnStart2PCCommit(
	logger *zap.Logger,
	txnMeta txn.TxnMeta) {
	if ce := logger.Check(zap.DebugLevel, "txn commit with 2 PC"); ce != nil {
		ce.Write(TxnField(txnMeta))
	}
}

// LogTxnCommittingCompleted log Txn Committing completed on coordinator failed
func LogTxnCommittingCompleted(
	logger *zap.Logger,
	txnMeta txn.TxnMeta) {
	if ce := logger.Check(zap.DebugLevel, "txn committing completed"); ce != nil {
		ce.Write(TxnDNShardField(txnMeta.DNShards[0]),
			TxnField(txnMeta))
	}
}

// TxnIDField returns a txn id field
func TxnIDField(txnMeta txn.TxnMeta) zap.Field {
	return TxnIDFieldWithID(txnMeta.ID)
}

// TxnIDsField returns a txn ids field
func TxnIDsField(txnIDs [][]byte) zap.Field {
	var buf bytes.Buffer
	n := len(txnIDs) - 1
	buf.WriteString("[")
	for idx, id := range txnIDs {
		buf.WriteString(hex.EncodeToString(id))
		if idx < n {
			buf.WriteString(", ")
		}
	}
	buf.WriteString("[")
	return zap.String("txn-ids", buf.String())
}

// TxnIDFieldWithID returns a txn id field
func TxnIDFieldWithID(id []byte) zap.Field {
	return zap.String("txn-id", hex.EncodeToString(id))
}

// TxnDNShardField returns a dn shard zap field
func TxnDNShardField(dn metadata.DNShard) zap.Field {
	return zap.String("dn-shard", dn.DebugString())
}

// TxnField returns a txn zap field
func TxnField(txnMeta txn.TxnMeta) zap.Field {
	return zap.String("txn", txnMeta.DebugString())
}
