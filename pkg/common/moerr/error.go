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
	"context"
	"fmt"
	"runtime/debug"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/util/errors"
)

type MOErrorCode = uint16

const (
	// 0 is OK.
	SUCCESS = 0
	INFO    = 1
	WARN    = 2

	// Group 1: Internal errors
	ERROR_START              = 1000
	INTERNAL_ERROR           = 1001
	NYI                      = 1002
	ERROR_FUNCTION_PARAMETER = 1003

	// Group 2: numeric
	DIVIVISION_BY_ZERO = 2000
	OUT_OF_RANGE       = 2001
	DATA_TRUNCATED     = 2002
	INVALID_ARGUMENT   = 2003

	// Group 3: invalid input
	BAD_CONFIGURATION = 3000
	INVALID_INPUT     = 3001

	// Group 4: unexpected state
	INVALID_STATE         = 4000
	LOG_SERVICE_NOT_READY = 4001

	// group 5: rpc timeout
	// ErrRPCTimeout rpc timeout
	ErrRPCTimeout = 5000
	// ErrClientClosed rpc client closed
	ErrClientClosed = 5001
	// ErrBackendClosed backend closed
	ErrBackendClosed = 5002
	// ErrStreamClosed rpc stream closed
	ErrStreamClosed = 5003
	// ErrNoAvailableBackend no available backend
	ErrNoAvailableBackend = 5004

	// Group 6: sql error, can usually correspond one-to-one to MysqlError, val in [6000, 6999]
	ErrNoDatabaseSelected = 6000 + iota
	ErrBadTable

	// Group 10: txn
	// ErrTxnAborted read and write a transaction that has been rolled back.
	ErrTxnClosed = 10000
	// ErrTxnWriteConflict write conflict error for concurrent transactions
	ErrTxnWriteConflict = 10001
	// ErrMissingTxn missing transaction error
	ErrMissingTxn = 10002
	// ErrUnresolvedConflict read transaction encounters unresolved data
	ErrUnresolvedConflict = 10003
	// ErrTxnError TxnError wrapper
	ErrTxnError = 10004
	// ErrDNShardNotFound DNShard not found, need to get the latest DN list from HAKeeper
	ErrDNShardNotFound = 10005

	// ErrEnd, the max value of MOErrorCode
	ErrEnd = 65535
)

type moErrorMsgItem struct {
	errorCode        MOErrorCode
	mysqlErrorCode   uint16
	errorMsgOrFormat string
}

var errorMsgRefer = map[int32]moErrorMsgItem{
	SUCCESS: {0, 0, "ok"},
	INFO:    {20001, 0, "%s"},
	WARN:    {20002, 0, "%s"},

	// Group 1: Internal errors
	ERROR_START:              {21000, 0, "%s"},
	INTERNAL_ERROR:           {21001, 0, "%s"},
	NYI:                      {21002, 0, "%s"},
	ERROR_FUNCTION_PARAMETER: {21003, 0, "%s"},

	// Group 2: numeric
	DIVIVISION_BY_ZERO: {22000, 0, "division by zero"},
	OUT_OF_RANGE:       {22001, 0, "overflow from %s to %s"},
	DATA_TRUNCATED:     {22002, 0, "%s data truncated"},
	INVALID_ARGUMENT:   {22003, 0, "%s"},

	// Group 3: invalid input
	BAD_CONFIGURATION: {23000, 0, "invalid %s configuration"},
	INVALID_INPUT:     {23001, 0, "%s"},

	// Group 4: unexpected state
	INVALID_STATE:         {24000, 0, "%s"},
	LOG_SERVICE_NOT_READY: {24001, 0, "log service not ready"},

	// group 5: rpc timeout
	ErrRPCTimeout:         {25000, 0, "%s"},
	ErrClientClosed:       {25001, 0, "client closed"},
	ErrBackendClosed:      {25002, 0, "backend closed"},
	ErrStreamClosed:       {25003, 0, "stream closed"},
	ErrNoAvailableBackend: {25004, 0, "no available backend"},

	// Group 1: sql error
	ErrNoDatabaseSelected: {26000, 1046, "No database selected"},
	ErrBadTable:           {26001, 1051, "Unknown table '%-.129s.%-.129s'"},

	// Group 10: txn
	ErrTxnClosed:          {30000, 0, "the transaction has been committed or aborted"},
	ErrTxnWriteConflict:   {30001, 0, "write conflict"},
	ErrMissingTxn:         {30002, 0, "missing txn"},
	ErrUnresolvedConflict: {30003, 0, "unresolved conflict"},
	ErrTxnError:           {30004, 0, "%s"},
	ErrDNShardNotFound:    {30005, 0, "%s"},

	// Group End: max value of MOErrorCode
	ErrEnd: {65535, 0, "%s"},
}

func New(code int32, args ...any) *Error {
	return newWithDepth(Context(), code, args...)
}

func NewWithContext(ctx context.Context, code int32, args ...any) *Error {
	return newWithDepth(ctx, code, args...)
}

func newWithDepth(ctx context.Context, code int32, args ...any) *Error {
	var err *Error
	item, has := errorMsgRefer[code]
	if !has {
		panic(fmt.Errorf("not exist MOErrorCode: %d", code))
	}
	if len(args) == 0 {
		err = &Error{
			MysqlErrCode: item.mysqlErrorCode,
			Code:         code,
			ErrorCode:    item.errorCode,
			Message:      item.errorMsgOrFormat,
		}
	} else {
		err = &Error{
			MysqlErrCode: item.mysqlErrorCode,
			Code:         code,
			ErrorCode:    item.errorCode,
			Message:      fmt.Sprintf(item.errorMsgOrFormat, args...),
		}
	}
	_ = errors.WithContextWithDepth(ctx, err, 2)
	return err
}

type Error struct {
	MysqlErrCode uint16
	ErrorCode    MOErrorCode
	Code         int32
	Message      string
}

func (e *Error) Ok() bool {
	return e.Code < ERROR_START
}

func (e *Error) Error() string {
	return e.Message
}

func (e *Error) MyErrorCode() uint16 {
	if e.MysqlErrCode > 0 {
		return e.MysqlErrCode
	} else {
		return e.ErrorCode
	}
}

func (e *Error) SqlState() string {
	return "HY000"
}

func IsMoErrCode(e error, rc int32) bool {
	me, ok := e.(*Error)
	if !ok {
		// This is not a moerr
		return false
	}
	return me.Code == rc
}

//
// Most of the times should not call this.  Just use nil
// func NewSUCCESS() *Error {
// 	return &Error{SUCCESS, ""}
// }
//

func NewInfo(msg string) error {
	return NewError(INFO, msg)
}

func NewWarn(msg string) error {
	return NewError(WARN, msg)
}

func NewInternalError(msg string, args ...any) *Error {
	return newWithDepth(Context(), INTERNAL_ERROR, fmt.Sprintf("Internal error: "+msg, args...))
}

// NewPanicError converts a runtime panic to internal error.
func NewPanicError(v interface{}) *Error {
	if e, ok := v.(*Error); ok {
		return e
	}
	return newWithDepth(Context(), INTERNAL_ERROR, fmt.Sprintf("panic %v: %s", v, debug.Stack()))
}

func NewError(code int32, msg string) *Error {
	err := &Error{Code: code, Message: msg}
	if item, has := errorMsgRefer[code]; !has {
		panic(fmt.Errorf("not exist MOErrorCode: %d", code))
	} else {
		err.MysqlErrCode = item.mysqlErrorCode
		err.ErrorCode = item.errorCode
	}
	_ = errors.WithContextWithDepth(Context(), err, 1)
	return err
}

var contextFunc atomic.Value

func SetContextFunc(f func() context.Context) {
	contextFunc.Store(f)
}

// Context should be trace.DefaultContext
func Context() context.Context {
	return contextFunc.Load().(func() context.Context)()
}

func init() {
	SetContextFunc(func() context.Context { return context.Background() })
}
