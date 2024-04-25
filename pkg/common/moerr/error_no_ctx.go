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

package moerr

import (
	"encoding/hex"
	"fmt"
)

func NewInfoNoCtx(msg string) *Error {
	return newError(Context(), ErrInfo, msg)
}

func NewBadS3ConfigNoCtx(msg string) *Error {
	return newError(Context(), ErrBadS3Config, msg)
}

func NewInternalErrorNoCtx(msg string, args ...any) *Error {
	xmsg := fmt.Sprintf(msg, args...)
	return newError(Context(), ErrInternal, xmsg)
}

func NewNYINoCtx(msg string, args ...any) *Error {
	xmsg := fmt.Sprintf(msg, args...)
	return newError(Context(), ErrNYI, xmsg)
}

func NewNotSupportedNoCtx(msg string, args ...any) *Error {
	xmsg := fmt.Sprintf(msg, args...)
	return newError(Context(), ErrNotSupported, xmsg)
}

func NewOOMNoCtx() *Error {
	return newError(Context(), ErrOOM)
}

func NewDivByZeroNoCtx() *Error {
	return newError(Context(), ErrDivByZero)
}

func NewOutOfRangeNoCtx(typ string, msg string, args ...any) *Error {
	xmsg := fmt.Sprintf(msg, args...)
	return newError(Context(), ErrOutOfRange, typ, xmsg)
}

func NewDataTruncatedNoCtx(typ string, msg string, args ...any) *Error {
	xmsg := fmt.Sprintf(msg, args...)
	return newError(Context(), ErrDataTruncated, typ, xmsg)
}

func NewInvalidArgNoCtx(arg string, val any) *Error {
	return newError(Context(), ErrInvalidArg, arg, fmt.Sprintf("%v", val))
}

func NewBadConfigNoCtx(msg string, args ...any) *Error {
	xmsg := fmt.Sprintf(msg, args...)
	return newError(Context(), ErrBadConfig, xmsg)
}

func NewInvalidInputNoCtx(msg string, args ...any) *Error {
	xmsg := fmt.Sprintf(msg, args...)
	return newError(Context(), ErrInvalidInput, xmsg)
}

func NewArrayInvalidOpNoCtx(expected, actual int) *Error {
	xmsg := fmt.Sprintf("vector ops between different dimensions (%v, %v) is not permitted.", expected, actual)
	return newError(Context(), ErrInvalidInput, xmsg)
}

func NewArrayDefMismatchNoCtx(expected, actual int) *Error {
	xmsg := fmt.Sprintf("expected vector dimension %v != actual dimension %v.", expected, actual)
	return newError(Context(), ErrInvalidInput, xmsg)
}

func NewSyntaxErrorNoCtx(msg string, args ...any) *Error {
	xmsg := fmt.Sprintf(msg, args...)
	return newError(Context(), ErrSyntaxError, xmsg)
}

func NewParseErrorNoCtx(msg string, args ...any) *Error {
	xmsg := fmt.Sprintf(msg, args...)
	return newError(Context(), ErrParseError, xmsg)
}

func NewConstraintViolationNoCtx(msg string, args ...any) *Error {
	xmsg := fmt.Sprintf(msg, args...)
	return newError(Context(), ErrConstraintViolation, xmsg)
}

func NewEmptyVectorNoCtx() *Error {
	return newError(Context(), ErrEmptyVector)
}

func NewFileNotFoundNoCtx(f string) *Error {
	return newError(Context(), ErrFileNotFound, f)
}

func NewFileAlreadyExistsNoCtx(f string) *Error {
	return newError(Context(), ErrFileAlreadyExists, f)
}

func NewDBAlreadyExistsNoCtx(db string) *Error {
	return newError(Context(), ErrDBAlreadyExists, db)
}

func NewTableAlreadyExistsNoCtx(t string) *Error {
	return newError(Context(), ErrTableAlreadyExists, t)
}

func NewUnexpectedEOFNoCtx(f string) *Error {
	return newError(Context(), ErrUnexpectedEOF, f)
}

func NewEmptyRangeNoCtx(f string) *Error {
	return newError(Context(), ErrEmptyRange, f)
}

func NewSizeNotMatchNoCtx(f string) *Error {
	return newError(Context(), ErrSizeNotMatch, f)
}

func NewInvalidPathNoCtx(f string) *Error {
	return newError(Context(), ErrInvalidPath, f)
}

func NewInvalidStateNoCtx(msg string, args ...any) *Error {
	xmsg := fmt.Sprintf(msg, args...)
	return newError(Context(), ErrInvalidState, xmsg)
}

func NewInvalidServiceIndexNoCtx(idx int) *Error {
	return newError(Context(), ErrInvalidServiceIndex, idx)
}

func NewBadDBNoCtx(name string) *Error {
	return newError(Context(), ErrBadDB, name)
}

func NewNoDBNoCtx() *Error {
	return newError(Context(), ErrNoDB)
}

func NewNoWorkingStoreNoCtx() *Error {
	return newError(Context(), ErrNoWorkingStore)
}

func NewNoServiceNoCtx(name string) *Error {
	return newError(Context(), ErrNoService, name)
}

func NewDupServiceNameNoCtx(name string) *Error {
	return newError(Context(), ErrDupServiceName, name)
}

func NewWrongServiceNoCtx(exp, got string) *Error {
	return newError(Context(), ErrWrongService, exp, got)
}

func NewNoSuchTableNoCtx(db, tbl string) *Error {
	return newError(Context(), ErrNoSuchTable, db, tbl)
}

func NewClientClosedNoCtx() *Error {
	return newError(Context(), ErrClientClosed)
}

func NewBackendClosedNoCtx() *Error {
	return newError(Context(), ErrBackendClosed)
}

func NewStreamClosedNoCtx() *Error {
	return newError(Context(), ErrStreamClosed)
}

func NewNoAvailableBackendNoCtx() *Error {
	return newError(Context(), ErrNoAvailableBackend)
}

func NewBackendCannotConnectNoCtx(args ...any) *Error {
	if len(args) == 0 {
		return newError(Context(), ErrBackendCannotConnect, "none")
	}
	return newError(Context(), ErrBackendCannotConnect, args...)
}

func NewTxnClosedNoCtx(txnID []byte) *Error {
	id := "unknown"
	if len(txnID) > 0 {
		id = hex.EncodeToString(txnID)
	}
	return newError(Context(), ErrTxnClosed, id)
}

func NewTxnWriteConflictNoCtx(msg string, args ...any) *Error {
	xmsg := fmt.Sprintf(msg, args...)
	return newError(Context(), ErrTxnWriteConflict, xmsg)
}

func NewMissingTxnNoCtx() *Error {
	return newError(Context(), ErrMissingTxn)
}

func NewTAEErrorNoCtx(msg string, args ...any) *Error {
	xmsg := fmt.Sprintf(msg, args...)
	return newError(Context(), ErrTAEError, xmsg)
}

func NewTNShardNotFoundNoCtx(uuid string, id uint64) *Error {
	return newError(Context(), ErrTNShardNotFound, uuid, id)
}

func NewShardNotReportedNoCtx(uuid string, id uint64) *Error {
	return newError(Context(), ErrShardNotReported, uuid, id)
}

func NewRpcErrorNoCtx(msg string) *Error {
	return newError(Context(), ErrRpcError, msg)
}

func NewTxnNotFoundNoCtx() *Error {
	return newError(Context(), ErrTxnNotFound)
}

func NewTxnNotActiveNoCtx(st string) *Error {
	return newError(Context(), ErrTxnNotActive, st)
}

func NewTAECommitNoCtx(msg string, args ...any) *Error {
	xmsg := fmt.Sprintf(msg, args...)
	return newError(Context(), ErrTAECommit, xmsg)
}

func NewTAERollbackNoCtx(msg string, args ...any) *Error {
	xmsg := fmt.Sprintf(msg, args...)
	return newError(Context(), ErrTAERollback, xmsg)
}

func NewTAEPrepareNoCtx(msg string, args ...any) *Error {
	xmsg := fmt.Sprintf(msg, args...)
	return newError(Context(), ErrTAEPrepare, xmsg)
}

func NewTxnRWConflictNoCtx() *Error {
	return newError(Context(), ErrTxnRWConflict)
}

func NewTxnWWConflictNoCtx(
	tableID uint64,
	s string) *Error {
	return NewTxnWWConflict(Context(), tableID, s)
}

func NewTAENeedRetryNoCtx() *Error {
	return newError(Context(), ErrTAENeedRetry)
}

func NewTxnStaleNoCtx() *Error {
	return newError(Context(), ErrTxnStale)
}

func NewWaiterPausedNoCtx() *Error {
	return newError(Context(), ErrWaiterPaused)
}

func NewRetryForCNRollingRestart() *Error {
	return newError(Context(), ErrRetryForCNRollingRestart)
}

func NewNewTxnInCNRollingRestart() *Error {
	return newError(Context(), ErrNewTxnInCNRollingRestart)
}

func NewNotFoundNoCtx() *Error {
	return newError(Context(), ErrNotFound)
}

func NewDuplicateNoCtx() *Error {
	return newError(Context(), ErrDuplicate)
}

func NewDuplicateEntryNoCtx(entry string, key string) *Error {
	return newError(Context(), ErrDuplicateEntry, entry, key)
}

func NewRoleGrantedToSelfNoCtx(from, to string) *Error {
	return newError(Context(), ErrRoleGrantedToSelf, from, to)
}

func NewTxnReadConflictNoCtx(msg string, args ...any) *Error {
	xmsg := fmt.Sprintf(msg, args...)
	return newError(Context(), ErrTxnReadConflict, xmsg)
}

func NewAppendableObjectNotFoundNoCtx() *Error {
	return newError(Context(), ErrAppendableObjectNotFound)
}

func NewAppendableBlockNotFoundNoCtx() *Error {
	return newError(Context(), ErrAppendableBlockNotFound)
}

func NewDeadLockDetectedNoCtx() *Error {
	return newError(Context(), ErrDeadLockDetected)
}

func NewDeadlockCheckBusyNoCtx() *Error {
	return newError(Context(), ErrDeadlockCheckBusy)
}

func NewCannotCommitOrphanNoCtx() *Error {
	return NewCannotCommitOrphan(Context())
}

func NewLockTableBindChangedNoCtx() *Error {
	return newError(Context(), ErrLockTableBindChanged)
}

func NewLockTableNotFoundNoCtx() *Error {
	return newError(Context(), ErrLockTableNotFound)
}

func NewLockConflictNoCtx() *Error {
	return newError(Context(), ErrLockConflict)
}

func NewUDFAlreadyExistsNoCtx(f string) *Error {
	return newError(Context(), ErrFunctionAlreadyExists, f)
}

func NewNoUDFNoCtx(f string) *Error {
	return newError(Context(), ErrDropNonExistsFunction, f)
}

func NewProcedureAlreadyExistsNoCtx(f string) *Error {
	return newError(Context(), ErrProcedureAlreadyExists, f)
}

func NewTxnNeedRetryNoCtx() *Error {
	return newError(Context(), ErrTxnNeedRetry)
}

func NewTxnNeedRetryWithDefChangedNoCtx() *Error {
	return newError(Context(), ErrTxnNeedRetryWithDefChanged)
}

func NewTxnCannotRetryNoCtx() *Error {
	return newError(Context(), ErrTxnCannotRetry)
}

func NewRPCTimeoutNoCtx() *Error {
	return NewRPCTimeout(Context())
}
