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

	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"go.uber.org/zap"
)

// LogTxnRead log txn read
func LogTxnRead(
	logger *log.MOLogger,
	txnMeta txn.TxnMeta) {
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("txn read", zap.String("txn", txnMeta.DebugString()))
	}
}

// LogTxnWrite log txn write
func LogTxnWrite(
	logger *log.MOLogger,
	txnMeta txn.TxnMeta) {
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("txn write", zap.String("txn", txnMeta.DebugString()))
	}
}

// LogTxnCommit log txn commit
func LogTxnCommit(
	logger *log.MOLogger,
	txnMeta txn.TxnMeta) {
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("txn commit", zap.String("txn", txnMeta.DebugString()))
	}
}

// LogTxnRollback log txn rollback
func LogTxnRollback(
	logger *log.MOLogger,
	txnMeta txn.TxnMeta) {
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("txn rollback", zap.String("txn", txnMeta.DebugString()))
	}
}

// LogTxnCreated log txn created
func LogTxnCreated(
	logger *log.MOLogger,
	txnMeta txn.TxnMeta) {
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("txn created", zap.String("txn", txnMeta.DebugString()))
	}
}

// LogTxnUpdated log txn updated
func LogTxnUpdated(
	logger *log.MOLogger,
	txnMeta txn.TxnMeta) {
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("txn updated", zap.String("txn", txnMeta.DebugString()))
	}
}

// LogTxnWaiterAdded log txn waiter added
func LogTxnWaiterAdded(
	logger *log.MOLogger,
	txnMeta txn.TxnMeta,
	waitStatus txn.TxnStatus) {
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("txn waiter added", zap.String("txn", txnMeta.DebugString()))
	}
}

// LogTxnHandleRequest log txn handle request
func LogTxnHandleRequest(
	logger *log.MOLogger,
	request *txn.TxnRequest) {
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("txn handle request",
			TxnIDFieldWithID(request.Txn.ID),
			zap.String("request", request.DebugString()))
	}
}

// LogTxnHandleResult log txn handle request
func LogTxnHandleResult(
	logger *log.MOLogger,
	response *txn.TxnResponse) {
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("txn handle result",
			zap.String("response", response.DebugString()))
	}
}

// LogTxnSendRequests log txn send txn requests
func LogTxnSendRequests(
	logger *log.MOLogger,
	requests []txn.TxnRequest) {
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("txn send requests",
			zap.String("requests", txn.RequestsDebugString(requests, true)))
	}
}

// LogTxnSendRequestsFailed log txn send txn requests failed
func LogTxnSendRequestsFailed(
	logger *log.MOLogger,
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
	logger *log.MOLogger,
	responses []txn.TxnResponse) {
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("txn received responses",
			zap.String("responses", txn.ResponsesDebugString(responses)))
	}
}

// LogTxnCreateOn log Txn create on dn shard.
func LogTxnCreateOn(
	logger *log.MOLogger,
	txnMeta txn.TxnMeta,
	dn metadata.DNShard) {
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("txn created on DNShard",
			TxnField(txnMeta),
			TxnDNShardField(dn))
	}
}

// LogTxnReadBlockedByUncommittedTxns log Txn read blocked by other txns
func LogTxnReadBlockedByUncommittedTxns(
	logger *log.MOLogger,
	txnMeta txn.TxnMeta,
	waitTxns [][]byte) {
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("txn read blocked by other uncommitted txns",
			TxnField(txnMeta),
			TxnIDsField(waitTxns))
	}
}

// LogTxnWaitUncommittedTxnsFailed log Txn wait other uncommitted txns change to committed or abortted
// failed.
func LogTxnWaitUncommittedTxnsFailed(
	logger *log.MOLogger,
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
	logger *log.MOLogger,
	txnMeta txn.TxnMeta,
	dn metadata.DNShard) {
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("txn not found on DNShard",
			TxnField(txnMeta),
			TxnDNShardField(dn))
	}
}

// LogTxnWriteOnInvalidStatus log Txn write on invalid txn status.
func LogTxnWriteOnInvalidStatus(
	logger *log.MOLogger,
	txnMeta txn.TxnMeta) {
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("txn write on invalid status",
			TxnField(txnMeta))
	}
}

// LogTxnCommitOnInvalidStatus log Txn commit on invalid txn status.
func LogTxnCommitOnInvalidStatus(
	logger *log.MOLogger,
	txnMeta txn.TxnMeta) {
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("txn commit on invalid status",
			TxnField(txnMeta))
	}
}

// LogTxnReadFailed log Txn read failed.
func LogTxnReadFailed(
	logger *log.MOLogger,
	txnMeta txn.TxnMeta,
	err error) {
	logger.Error("txn read failed",
		TxnField(txnMeta),
		zap.Error(err))
}

// LogTxnWriteFailed log Txn write failed.
func LogTxnWriteFailed(
	logger *log.MOLogger,
	txnMeta txn.TxnMeta,
	err error) {
	logger.Error("txn write failed",
		TxnField(txnMeta),
		zap.Error(err))
}

// LogTxnParallelPrepareFailed log Txn parallel prepare failed
func LogTxnParallelPrepareFailed(
	logger *log.MOLogger,
	txnMeta txn.TxnMeta,
	err error) {
	logger.Error("txn parallel prepare failed",
		TxnField(txnMeta),
		zap.Error(err))
}

// LogTxnParallelPrepareCompleted log Txn parallel prepare completed
func LogTxnParallelPrepareCompleted(
	logger *log.MOLogger,
	txnMeta txn.TxnMeta) {
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("txn parallel prepare completed",
			TxnField(txnMeta))
	}
}

// LogTxnPrepareFailedOn log Tx prepare failed on DNShard
func LogTxnPrepareFailedOn(
	logger *log.MOLogger,
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
	logger *log.MOLogger,
	txnMeta txn.TxnMeta,
	dn metadata.DNShard,
	preparedTS timestamp.Timestamp) {
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("txn prepare completed on DNShard",
			TxnField(txnMeta),
			TxnDNShardField(dn),
			zap.String("prepared-ts", preparedTS.DebugString()))
	}
}

// LogTxnStartAsyncCommit log start async commit distributed txn task
func LogTxnStartAsyncCommit(
	logger *log.MOLogger,
	txnMeta txn.TxnMeta) {
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("async commit task started",
			TxnField(txnMeta))
	}
}

// LogTxnStartAsyncRollback log start async rollback txn task
func LogTxnStartAsyncRollback(
	logger *log.MOLogger,
	txnMeta txn.TxnMeta) {
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("async rollback task started",
			TxnField(txnMeta))
	}
}

// LogTxnRollbackCompleted log Txn rollback completed
func LogTxnRollbackCompleted(
	logger *log.MOLogger,
	txnMeta txn.TxnMeta) {
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("txn rollback completed",
			TxnField(txnMeta))
	}
}

// LogTxnCommittingFailed log Txn Committing failed on coordinator failed
func LogTxnCommittingFailed(
	logger *log.MOLogger,
	txnMeta txn.TxnMeta,
	err error) {
	logger.Error("txn committing failed, retry later",
		TxnDNShardField(txnMeta.DNShards[0]),
		TxnField(txnMeta),
		zap.Error(err))
}

// LogTxnStart1PCCommit log Txn start 1pc commit
func LogTxnStart1PCCommit(
	logger *log.MOLogger,
	txnMeta txn.TxnMeta) {
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("txn commit with 1 PC",
			TxnField(txnMeta))
	}
}

// LogTxn1PCCommitCompleted log Txn 1pc commit completed
func LogTxn1PCCommitCompleted(
	logger *log.MOLogger,
	txnMeta txn.TxnMeta) {
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("txn commit with 1 PC completed",
			TxnField(txnMeta))
	}
}

// LogTxnStart1PCCommitFailed log Txn 1pc commit failed
func LogTxnStart1PCCommitFailed(
	logger *log.MOLogger,
	txnMeta txn.TxnMeta,
	err error) {
	logger.Error("txn commit with 1 PC failed",
		TxnField(txnMeta),
		zap.Error(err))
}

// LogTxnStart2PCCommit log Txn start 2pc commit
func LogTxnStart2PCCommit(
	logger *log.MOLogger,
	txnMeta txn.TxnMeta) {
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("txn commit with 2 PC",
			TxnField(txnMeta))
	}
}

// LogTxnCommittingCompleted log Txn Committing completed on coordinator failed
func LogTxnCommittingCompleted(
	logger *log.MOLogger,
	txnMeta txn.TxnMeta) {
	if logger.Enabled(zap.DebugLevel) {
		logger.Debug("txn committing completed",
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
