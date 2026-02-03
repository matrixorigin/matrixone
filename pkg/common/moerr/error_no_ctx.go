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

	"github.com/matrixorigin/matrixone/pkg/util/errutil"
)

func NewInfoNoCtx(msg string) *Error {
	return newError(Context(), ErrInfo, msg)
}

func NewBadS3ConfigNoCtx(msg string) *Error {
	return newError(Context(), ErrBadS3Config, msg)
}

func NewInternalErrorNoCtxf(format string, args ...any) *Error {
	return newError(Context(), ErrInternal, fmt.Sprintf(format, args...))
}

func NewInternalErrorNoCtx(msg string) *Error {
	return newError(Context(), ErrInternal, msg)
}

func NewNYINoCtxf(format string, args ...any) *Error {
	return newError(Context(), ErrNYI, fmt.Sprintf(format, args...))
}

func NewNYINoCtx(msg string) *Error {
	return newError(Context(), ErrNYI, msg)
}

func NewNotSupportedNoCtxf(format string, args ...any) *Error {
	return newError(Context(), ErrNotSupported, fmt.Sprintf(format, args...))
}

func NewNotSupportedNoCtx(msg string) *Error {
	return newError(Context(), ErrNotSupported, msg)
}

func NewOOMNoCtx() *Error {
	return newError(Context(), ErrOOM)
}

func NewDivByZeroNoCtx() *Error {
	return newError(Context(), ErrDivByZero)
}

func NewOutOfRangeNoCtxf(typ string, format string, args ...any) *Error {
	return newError(Context(), ErrOutOfRange, typ, fmt.Sprintf(format, args...))
}

func NewOutOfRangeNoCtx(typ string, msg string) *Error {
	return newError(Context(), ErrOutOfRange, typ, msg)
}

func NewDataTruncatedNoCtxf(typ string, format string, args ...any) *Error {
	return newError(Context(), ErrDataTruncated, typ, fmt.Sprintf(format, args...))
}

func NewDataTruncatedNoCtx(typ string, msg string) *Error {
	return newError(Context(), ErrDataTruncated, typ, msg)
}

func NewInvalidArgNoCtx(arg string, val any) *Error {
	return newError(Context(), ErrInvalidArg, arg, fmt.Sprintf("%v", val))
}

func NewBadConfigNoCtxf(format string, args ...any) *Error {
	return newError(Context(), ErrBadConfig, fmt.Sprintf(format, args...))
}

func NewBadConfigNoCtx(msg string) *Error {
	return newError(Context(), ErrBadConfig, msg)
}

func NewInvalidInputNoCtxf(format string, args ...any) *Error {
	return newError(Context(), ErrInvalidInput, fmt.Sprintf(format, args...))
}

func NewInvalidInputNoCtx(msg string) *Error {
	return newError(Context(), ErrInvalidInput, msg)
}

func NewArrayInvalidOpNoCtx(expected, actual int) *Error {
	xmsg := fmt.Sprintf("vector ops between different dimensions (%v, %v) is not permitted.", expected, actual)
	return newError(Context(), ErrInvalidInput, xmsg)
}

func NewArrayDefMismatchNoCtx(expected, actual int) *Error {
	xmsg := fmt.Sprintf("expected vector dimension %v != actual dimension %v.", expected, actual)
	return newError(Context(), ErrInvalidInput, xmsg)
}

func NewSyntaxErrorNoCtxf(format string, args ...any) *Error {
	return newError(Context(), ErrSyntaxError, fmt.Sprintf(format, args...))
}

func NewSyntaxErrorNoCtx(msg string) *Error {
	return newError(Context(), ErrSyntaxError, msg)
}

func NewParseErrorNoCtxf(format string, args ...any) *Error {
	return newError(Context(), ErrParseError, fmt.Sprintf(format, args...))
}

func NewParseErrorNoCtx(msg string) *Error {
	return newError(Context(), ErrParseError, msg)
}

func NewConstraintViolationNoCtxf(format string, args ...any) *Error {
	return newError(Context(), ErrConstraintViolation, fmt.Sprintf(format, args...))
}

func NewConstraintViolationNoCtx(msg string) *Error {
	return newError(Context(), ErrConstraintViolation, msg)
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

func NewInvalidStateNoCtxf(format string, args ...any) *Error {
	return newError(Context(), ErrInvalidState, fmt.Sprintf(format, args...))
}

func NewInvalidStateNoCtx(msg string) *Error {
	return newError(Context(), ErrInvalidState, msg)
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

// NewClientClosedNoCtx creates a client closed error without logging.
// RPC errors use NoReportContext() to avoid log storms in retry loops.
func NewClientClosedNoCtx() *Error {
	return newError(NoReportContext(), ErrClientClosed)
}

// NewBackendClosedNoCtx creates a backend closed error without logging.
// RPC errors use NoReportContext() to avoid log storms in retry loops.
func NewBackendClosedNoCtx() *Error {
	return newError(NoReportContext(), ErrBackendClosed)
}

// NewStreamClosedNoCtx creates a stream closed error without logging.
// RPC errors use NoReportContext() to avoid log storms in retry loops.
func NewStreamClosedNoCtx() *Error {
	return newError(NoReportContext(), ErrStreamClosed)
}

// NewNoAvailableBackendNoCtx creates a no available backend error without logging.
// RPC errors use NoReportContext() to avoid log storms in retry loops.
func NewNoAvailableBackendNoCtx() *Error {
	return newError(NoReportContext(), ErrNoAvailableBackend)
}

// NewBackendCannotConnectNoCtx creates a backend connection error without logging.
// RPC errors use NoReportContext() to avoid log storms in retry loops.
func NewBackendCannotConnectNoCtx(args ...any) *Error {
	if len(args) == 0 {
		return newError(NoReportContext(), ErrBackendCannotConnect, "none")
	}
	return newError(NoReportContext(), ErrBackendCannotConnect, args...)
}

// NewServiceUnavailableNoCtx creates a service unavailable error without logging.
// RPC errors use NoReportContext() to avoid log storms in retry loops.
func NewServiceUnavailableNoCtx(reason string) *Error {
	return newError(NoReportContext(), ErrServiceUnavailable, reason)
}

// NewConnectionResetNoCtx creates a connection reset error without logging.
// RPC errors use NoReportContext() to avoid log storms in retry loops.
func NewConnectionResetNoCtx() *Error {
	return newError(NoReportContext(), ErrConnectionReset)
}

func NewTxnClosedNoCtx(txnID []byte) *Error {
	id := "unknown"
	if len(txnID) > 0 {
		id = hex.EncodeToString(txnID)
	}
	return newError(Context(), ErrTxnClosed, id)
}

func NewTxnWriteConflictNoCtxf(format string, args ...any) *Error {
	return newError(Context(), ErrTxnWriteConflict, fmt.Sprintf(format, args...))
}

func NewTxnWriteConflictNoCtx(msg string) *Error {
	return newError(Context(), ErrTxnWriteConflict, msg)
}

func NewMissingTxnNoCtx() *Error {
	return newError(Context(), ErrMissingTxn)
}

func NewTAEErrorNoCtxf(format string, args ...any) *Error {
	return newError(Context(), ErrTAEError, fmt.Sprintf(format, args...))
}

func NewTAEErrorNoCtx(msg string) *Error {
	return newError(Context(), ErrTAEError, msg)
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

func NewTAECommitNoCtxf(format string, args ...any) *Error {
	return newError(Context(), ErrTAECommit, fmt.Sprintf(format, args...))
}

func NewTAECommitNoCtx(msg string) *Error {
	return newError(Context(), ErrTAECommit, msg)
}

func NewTAERollbackNoCtxf(format string, args ...any) *Error {
	return newError(Context(), ErrTAERollback, fmt.Sprintf(format, args...))
}

func NewTAERollbackNoCtx(msg string) *Error {
	return newError(Context(), ErrTAERollback, msg)
}

func NewTAEPrepareNoCtxf(format string, args ...any) *Error {
	return newError(Context(), ErrTAEPrepare, fmt.Sprintf(format, args...))
}

func NewTAEPrepareNoCtx(msg string) *Error {
	return newError(Context(), ErrTAEPrepare, msg)
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

func NewTxnStaleNoCtxf(
	format string, args ...any,
) *Error {
	msg := fmt.Sprintf(format, args...)
	ctx := errutil.ContextWithNoReport(Context(), true)
	return newError(ctx, ErrTxnStale, msg)
}

func NewRetryForCNRollingRestart() *Error {
	return newError(Context(), ErrRetryForCNRollingRestart)
}

func NewNewTxnInCNRollingRestart() *Error {
	return newError(Context(), ErrNewTxnInCNRollingRestart)
}

func NewPrevCheckpointNotFinished() *Error {
	return newError(Context(), ErrPrevCheckpointNotFinished)
}

func NewCantDelGCCheckerNoCtx() *Error {
	return newError(Context(), ErrCantDelGCChecker)
}

func NewTxnControlErrorNoCtxf(format string, args ...any) *Error {
	return newError(Context(), ErrTxnControl, fmt.Sprintf(format, args...))
}

func NewOfflineTxnWriteNoCtx(msg string) *Error {
	return newError(Context(), ErrOfflineTxnWrite, msg)
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

func NewTxnReadConflictNoCtxf(format string, args ...any) *Error {
	return newError(Context(), ErrTxnReadConflict, fmt.Sprintf(format, args...))
}

func NewTxnReadConflictNoCtx(msg string) *Error {
	return newError(Context(), ErrTxnReadConflict, msg)
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
	return newError(Context(), ErrCannotCommitOrphan)
}

func NewCannotCommitOnInvalidCNNoCtx() *Error {
	return newError(Context(), ErrCannotCommitOnInvalidCN)
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

func NewLockNeedUpgradeNoCtx() *Error {
	return newError(Context(), ErrLockNeedUpgrade)
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

// NewRPCTimeoutNoCtx creates an RPC timeout error without logging.
// RPC errors use NoReportContext() to avoid log storms in retry loops.
func NewRPCTimeoutNoCtx() *Error {
	return newError(NoReportContext(), ErrRPCTimeout)
}

func NewKeyAlreadyExistsNoCtx() *Error {
	return newError(Context(), ErrKeyAlreadyExists)
}

func NewErrTooLargeObjectSizeNoCtx(option uint64) *Error {
	return newError(Context(), ErrTooLargeObjectSize, option)
}

func NewErrStaleReadNoCtx(minTS, start string) *Error {
	return newError(Context(), ErrStaleRead, minTS, start)
}

func NewErrNoWatermarkFoundNoCtx(dbName, tblName string) *Error {
	return newError(Context(), ErrNoWatermarkFound, dbName, tblName)
}

func NewArenaFullNoCtx() *Error {
	return newError(Context(), ErrArenaFull)
}

func NewReplicaNotFound(replica string) *Error {
	return newError(Context(), ErrReplicaNotFound, replica)
}

func NewReplicaNotMatch(current, received string) *Error {
	return newError(Context(), ErrReplicaNotMatch, current, received)
}

func NewCantCompileForPrepareNoCtx() *Error {
	return newError(Context(), ErrCantCompileForPrepare)
}

func NewSchedulerClosedNoCtx() *Error {
	return newError(Context(), ErrSchedulerClosed)
}
