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

package service

import (
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
)

func newTAEReadError(err error) *txn.TxnError {
	return &txn.TxnError{
		Code:    txn.ErrorCode_TAERead,
		Message: err.Error(),
	}
}

func newTAECommitError(err error) *txn.TxnError {
	return &txn.TxnError{
		Code:    txn.ErrorCode_TAECommit,
		Message: err.Error(),
	}
}

func newTAERollbackError(err error) *txn.TxnError {
	return &txn.TxnError{
		Code:    txn.ErrorCode_TAERollback,
		Message: err.Error(),
	}
}

func newTxnNotFoundError() *txn.TxnError {
	return &txn.TxnError{
		Code:    txn.ErrorCode_TxnNotFound,
		Message: "txn not found, maybe write after committed or aborted",
	}
}

func newTxnNotActiveError() *txn.TxnError {
	return &txn.TxnError{
		Code:    txn.ErrorCode_TxnNotActive,
		Message: "txn not in active status, cannot execute prepare/commit/rollback operations",
	}
}

func newTAEWriteError(err error) *txn.TxnError {
	return &txn.TxnError{
		Code:    txn.ErrorCode_TAEWrite,
		Message: err.Error(),
	}
}

func newRPCError(err error) *txn.TxnError {
	return &txn.TxnError{
		Code:    txn.ErrorCode_RPCError,
		Message: err.Error(),
	}
}

func newTAEPrepareError(err error) *txn.TxnError {
	return &txn.TxnError{
		Code:    txn.ErrorCode_TAEPrepare,
		Message: err.Error(),
	}
}

func newWaitTxnError(err error) *txn.TxnError {
	return &txn.TxnError{
		Code:    txn.ErrorCode_WaitTxn,
		Message: err.Error(),
	}
}
