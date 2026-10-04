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
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"go.uber.org/zap"
)

func GetLogger(sid string) *log.MOLogger {
	return runtime.ServiceRuntime(sid).Logger()
}

// LogTxnSnapshotTimestamp log txn snapshot ts use pushed latest commit ts
func LogTxnSnapshotTimestamp(
	logger *log.MOLogger,
	min timestamp.Timestamp,
	latest timestamp.Timestamp,
) {
	if logger.Enabled(zap.DebugLevel) {
		logger.Log("txn use pushed latest commit ts",
			log.DefaultLogOptions().AddCallerSkip(1).WithLevel(zap.DebugLevel),
			zap.String("min-ts", min.DebugString()),
			zap.String("latest-ts", latest.DebugString()))
	}
}

// LogTxnPushedTimestampUpdated log tn pushed timestamp updated
func LogTxnPushedTimestampUpdated(
	logger *log.MOLogger,
	value timestamp.Timestamp,
) {
	if logger.Enabled(zap.DebugLevel) {
		logger.Log("txn use pushed latest commit ts updated",
			log.DefaultLogOptions().AddCallerSkip(1).WithLevel(zap.DebugLevel),
			zap.String("latest-ts", value.DebugString()))
	}
}

func LogTimestampWaiterCanceled(
	logger *log.MOLogger,
) {
	logger.Log(
		"timestamp waiter canceled",
		log.DefaultLogOptions().AddCallerSkip(1).WithLevel(zap.InfoLevel),
	)
}

// LogTxnRead log txn read
func LogTxnRead(
	logger *log.MOLogger,
	txnMeta txn.TxnMeta,
) {
	if logger.Enabled(zap.DebugLevel) {
		logger.Log(
			"txn read",
			log.DefaultLogOptions().AddCallerSkip(1).WithLevel(zap.DebugLevel),
			zap.String("txn", txnMeta.DebugString()),
		)
	}
}

// LogTxnWrite log txn write
func LogTxnWrite(
	logger *log.MOLogger,
	txnMeta txn.TxnMeta,
) {
	if logger.Enabled(zap.DebugLevel) {
		logger.Log(
			"txn write",
			log.DefaultLogOptions().AddCallerSkip(1).WithLevel(zap.DebugLevel),
			zap.String("txn", txnMeta.DebugString()),
		)
	}
}

// LogTxnCommit log txn commit
func LogTxnCommit(
	logger *log.MOLogger,
	txnMeta txn.TxnMeta,
) {
	if logger.Enabled(zap.DebugLevel) {
		logger.Log(
			"txn commit",
			log.DefaultLogOptions().AddCallerSkip(1).WithLevel(zap.DebugLevel),
			zap.String("txn", txnMeta.DebugString()),
		)
	}
}

// LogTxnRollback log txn rollback
func LogTxnRollback(
	logger *log.MOLogger,
	txnMeta txn.TxnMeta,
) {
	if logger.Enabled(zap.DebugLevel) {
		logger.Log(
			"txn rollback",
			log.DefaultLogOptions().AddCallerSkip(1).WithLevel(zap.DebugLevel),
			zap.String("txn", txnMeta.DebugString()),
		)
	}
}

// LogTxnCreated log txn created
func LogTxnCreated(
	logger *log.MOLogger,
	txnMeta txn.TxnMeta,
) {
	if logger.Enabled(zap.DebugLevel) {
		logger.Log(
			"txn created",
			log.DefaultLogOptions().AddCallerSkip(1).WithLevel(zap.DebugLevel),
			zap.String("txn", txnMeta.DebugString()),
		)
	}
}

// LogTxnUpdated log txn updated
func LogTxnUpdated(
	logger *log.MOLogger,
	txnMeta txn.TxnMeta,
) {
	if logger.Enabled(zap.DebugLevel) {
		logger.Log(
			"txn updated",
			log.DefaultLogOptions().AddCallerSkip(1).WithLevel(zap.DebugLevel),
			zap.String("txn", txnMeta.DebugString()),
		)
	}
}

// LogTxnWaiterAdded log txn waiter added
func LogTxnWaiterAdded(
	logger *log.MOLogger,
	txnMeta txn.TxnMeta,
	waitStatus txn.TxnStatus,
) {
	if logger.Enabled(zap.DebugLevel) {
		logger.Log(
			"txn waiter added",
			log.DefaultLogOptions().AddCallerSkip(1).WithLevel(zap.DebugLevel),
			zap.String("txn", txnMeta.DebugString()),
		)
	}
}

// LogTxnHandleRequest log txn handle request
func LogTxnHandleRequest(
	logger *log.MOLogger,
	request *txn.TxnRequest,
) {
	if logger.Enabled(zap.DebugLevel) {
		logger.Log(
			"txn handle request",
			log.DefaultLogOptions().AddCallerSkip(1).WithLevel(zap.DebugLevel),
			TxnIDFieldWithID(request.Txn.ID),
			zap.String("request", request.DebugString()),
		)
	}
}

// LogTxnHandleResult log txn handle request
func LogTxnHandleResult(
	logger *log.MOLogger,
	response *txn.TxnResponse,
) {
	if logger.Enabled(zap.DebugLevel) {
		logger.Log(
			"txn handle result",
			log.DefaultLogOptions().AddCallerSkip(1).WithLevel(zap.DebugLevel),
			zap.String("response", response.DebugString()),
		)
	}
}

// LogTxnSendRequests log txn send txn requests
func LogTxnSendRequests(
	logger *log.MOLogger,
	requests []txn.TxnRequest,
) {
	if logger.Enabled(zap.DebugLevel) {
		logger.Log(
			"txn send requests",
			log.DefaultLogOptions().AddCallerSkip(1).WithLevel(zap.DebugLevel),
			zap.String("requests", txn.RequestsDebugString(requests, true)),
		)
	}
}

// LogTxnSendRequestsFailed log txn send txn requests failed
func LogTxnSendRequestsFailed(
	logger *log.MOLogger,
	requests []txn.TxnRequest,
	err error,
) {

	// The payload cannot be recorded here because reading the payload field would
	// cause a DATA RACE, as it is possible that morpc was still processing the send
	// at the time of the error and would have manipulated the payload field. And logging
	// the error, the payload field does not need to be logged to the log either, you can
	// find the previous log to view the paylaod based on the request-id.
	logger.Log(
		"txn send requests failed",
		log.DefaultLogOptions().AddCallerSkip(1).WithLevel(zap.ErrorLevel),
		zap.String("requests", txn.RequestsDebugString(requests, false)),
		zap.Error(err),
	)
}

// LogTxnReceivedResponses log received txn responses
func LogTxnReceivedResponses(
	logger *log.MOLogger,
	responses []txn.TxnResponse,
) {
	if logger.Enabled(zap.DebugLevel) {
		logger.Log(
			"txn received responses",
			log.DefaultLogOptions().AddCallerSkip(1).WithLevel(zap.DebugLevel),
			zap.String("responses", txn.ResponsesDebugString(responses)),
		)
	}
}

// LogTxnCreateOn log Txn create on tn shard.
func LogTxnCreateOn(
	logger *log.MOLogger,
	txnMeta txn.TxnMeta,
	tn metadata.TNShard,
) {
	if logger.Enabled(zap.DebugLevel) {
		logger.Log(
			"txn created on DNShard",
			log.DefaultLogOptions().AddCallerSkip(1).WithLevel(zap.DebugLevel),
			TxnField(txnMeta),
			TxnTNShardField(tn),
		)
	}
}

// LogTxnReadBlockedByUncommittedTxns log Txn read blocked by other txns
func LogTxnReadBlockedByUncommittedTxns(
	logger *log.MOLogger,
	txnMeta txn.TxnMeta,
	waitTxns [][]byte,
) {
	if logger.Enabled(zap.DebugLevel) {
		logger.Log(
			"txn read blocked by other uncommitted txns",
			log.DefaultLogOptions().AddCallerSkip(1).WithLevel(zap.DebugLevel),
			TxnField(txnMeta),
			TxnIDsField(waitTxns),
		)
	}
}

// LogTxnWaitUncommittedTxnsFailed log Txn wait other uncommitted txns change to committed or aborted
// failed.
func LogTxnWaitUncommittedTxnsFailed(
	logger *log.MOLogger,
	txnMeta txn.TxnMeta,
	waitTxns [][]byte,
	err error,
) {
	logger.Log(
		"txn wait other uncommitted txns failed",
		log.DefaultLogOptions().AddCallerSkip(1).WithLevel(zap.ErrorLevel),
		TxnField(txnMeta),
		TxnIDsField(waitTxns),
		zap.Error(err),
	)
}

// LogTxnReadFailed log Txn read failed.
func LogTxnNotFoundOn(
	logger *log.MOLogger,
	txnMeta txn.TxnMeta,
	tn metadata.TNShard,
) {
	if logger.Enabled(zap.DebugLevel) {
		logger.Log(
			"txn not found on DNShard",
			log.DefaultLogOptions().AddCallerSkip(1).WithLevel(zap.DebugLevel),
			TxnField(txnMeta),
			TxnTNShardField(tn),
		)
	}
}

// LogTxnWriteOnInvalidStatus log Txn write on invalid txn status.
func LogTxnWriteOnInvalidStatus(
	logger *log.MOLogger,
	txnMeta txn.TxnMeta,
) {
	if logger.Enabled(zap.DebugLevel) {
		logger.Log(
			"txn write on invalid status",
			log.DefaultLogOptions().AddCallerSkip(1).WithLevel(zap.DebugLevel),
			TxnField(txnMeta),
		)
	}
}

// LogTxnCommitOnInvalidStatus log Txn commit on invalid txn status.
func LogTxnCommitOnInvalidStatus(
	logger *log.MOLogger,
	txnMeta txn.TxnMeta,
) {
	if logger.Enabled(zap.DebugLevel) {
		logger.Log(
			"txn commit on invalid status",
			log.DefaultLogOptions().AddCallerSkip(1).WithLevel(zap.DebugLevel),
			TxnField(txnMeta),
		)
	}
}

// LogTxnReadFailed log Txn read failed.
func LogTxnReadFailed(
	logger *log.MOLogger,
	txnMeta txn.TxnMeta,
	err error,
) {
	logger.Log(
		"txn read failed",
		log.DefaultLogOptions().AddCallerSkip(1).WithLevel(zap.ErrorLevel),
		TxnField(txnMeta),
		zap.Error(err),
	)
}

// LogTxnWriteFailed log Txn write failed.
func LogTxnWriteFailed(
	logger *log.MOLogger,
	txnMeta txn.TxnMeta,
	err error,
) {
	logger.Log(
		"txn write failed",
		log.DefaultLogOptions().AddCallerSkip(1).WithLevel(zap.ErrorLevel),
		TxnField(txnMeta),
		zap.Error(err),
	)
}

// LogTxnStart1PCCommit log Txn start 1pc commit
func LogTxnStart1PCCommit(
	logger *log.MOLogger,
	txnMeta txn.TxnMeta,
) {
	if logger.Enabled(zap.DebugLevel) {
		logger.Log(
			"txn commit with 1 PC",
			log.DefaultLogOptions().AddCallerSkip(1).WithLevel(zap.DebugLevel),
			TxnField(txnMeta),
		)
	}
}

// LogTxn1PCCommitCompleted log Txn 1pc commit completed
func LogTxn1PCCommitCompleted(
	logger *log.MOLogger,
	txnMeta txn.TxnMeta,
) {
	if logger.Enabled(zap.DebugLevel) {
		logger.Log(
			"txn commit with 1 PC completed",
			log.DefaultLogOptions().AddCallerSkip(1).WithLevel(zap.DebugLevel),
			TxnField(txnMeta),
		)
	}
}

// LogTxnStart1PCCommitFailed log Txn 1pc commit failed
func LogTxnStart1PCCommitFailed(
	logger *log.MOLogger,
	txnMeta txn.TxnMeta,
	err error,
) {
	logger.Log(
		"txn commit with 1 PC failed",
		log.DefaultLogOptions().AddCallerSkip(1).WithLevel(zap.ErrorLevel),
		TxnField(txnMeta),
		zap.Error(err),
	)
}

// TxnIDField returns a txn id field
func TxnIDField(
	txnMeta txn.TxnMeta,
) zap.Field {
	return TxnIDFieldWithID(txnMeta.ID)
}

// TxnIDsField returns a txn ids field
func TxnIDsField(
	txnIDs [][]byte,
) zap.Field {
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

// TxnTNShardField returns a tn shard zap field
func TxnTNShardField(tn metadata.TNShard) zap.Field {
	return zap.String("dn-shard", tn.DebugString())
}

// TxnField returns a txn zap field
func TxnField(txnMeta txn.TxnMeta) zap.Field {
	return zap.String("txn", txnMeta.DebugString())
}
