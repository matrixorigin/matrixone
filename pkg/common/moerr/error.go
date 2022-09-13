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

const MySQLDefaultSqlState = "HY000"

const (
	// 0 is OK.
	SUCCESS uint16 = 0
	INFO    uint16 = 1
	WARN    uint16 = 2

	// Group 1: Internal errors
	ERROR_START              uint16 = 20100
	INTERNAL_ERROR           uint16 = 20101
	NYI                      uint16 = 20102
	ERROR_FUNCTION_PARAMETER uint16 = 20103

	// Group 2: numeric
	DIVIVISION_BY_ZERO uint16 = 20200
	OUT_OF_RANGE       uint16 = 20201
	DATA_TRUNCATED     uint16 = 20202
	INVALID_ARGUMENT   uint16 = 20203

	// Group 3: invalid input
	BAD_CONFIGURATION uint16 = 20300
	INVALID_INPUT     uint16 = 20301

	// Group 4: unexpected state
	INVALID_STATE         uint16 = 20400
	LOG_SERVICE_NOT_READY uint16 = 20401

	// Group 5: rpc timeout
	// ErrRPCTimeout rpc timeout
	ErrRPCTimeout uint16 = 20500
	// ErrClientClosed rpc client closed
	ErrClientClosed uint16 = 20501
	// ErrBackendClosed backend closed
	ErrBackendClosed uint16 = 20502
	// ErrStreamClosed rpc stream closed
	ErrStreamClosed uint16 = 20503
	// ErrNoAvailableBackend no available backend
	ErrNoAvailableBackend uint16 = 20504

	// Group 6: txn
	// ErrTxnAborted read and write a transaction that has been rolled back.
	ErrTxnClosed uint16 = 20600
	// ErrTxnWriteConflict write conflict error for concurrent transactions
	ErrTxnWriteConflict uint16 = 20601
	// ErrMissingTxn missing transaction error
	ErrMissingTxn uint16 = 20602
	// ErrUnresolvedConflict read transaction encounters unresolved data
	ErrUnresolvedConflict uint16 = 20603
	// ErrTxnError TxnError wrapper
	ErrTxnError uint16 = 20604
	// ErrDNShardNotFound DNShard not found, need to get the latest DN list from HAKeeper
	ErrDNShardNotFound uint16 = 20605

	// ErrEnd, the max value of MOErrorCode
	ErrEnd uint16 = 65535
)

type moErrorMsgItem struct {
	errorCode        uint16
	sqlStates        []string
	errorMsgOrFormat string
}

var errorMsgRefer = map[uint16]moErrorMsgItem{
	SUCCESS: {0, []string{MySQLDefaultSqlState}, "ok"},
	INFO:    {1, []string{MySQLDefaultSqlState}, "%s"},
	WARN:    {2, []string{MySQLDefaultSqlState}, "%s"},

	// Group 1: Internal errors
	ERROR_START:              {20100, []string{MySQLDefaultSqlState}, "%s"},
	INTERNAL_ERROR:           {20101, []string{MySQLDefaultSqlState}, "%s"},
	NYI:                      {20102, []string{MySQLDefaultSqlState}, "%s"},
	ERROR_FUNCTION_PARAMETER: {20103, []string{MySQLDefaultSqlState}, "%s"},

	// Group 2: numeric
	DIVIVISION_BY_ZERO: {20200, []string{MySQLDefaultSqlState}, "division by zero"},
	OUT_OF_RANGE:       {20201, []string{MySQLDefaultSqlState}, "overflow from %s to %s"},
	DATA_TRUNCATED:     {20202, []string{MySQLDefaultSqlState}, "%s data truncated"},
	INVALID_ARGUMENT:   {20203, []string{MySQLDefaultSqlState}, "%s"},

	// Group 3: invalid input
	BAD_CONFIGURATION: {20300, []string{MySQLDefaultSqlState}, "invalid %s configuration"},
	INVALID_INPUT:     {20301, []string{MySQLDefaultSqlState}, "%s"},

	// Group 4: unexpected state
	INVALID_STATE:         {20400, []string{MySQLDefaultSqlState}, "%s"},
	LOG_SERVICE_NOT_READY: {20401, []string{MySQLDefaultSqlState}, "log service not ready"},

	// Group 5: rpc timeout
	ErrRPCTimeout:         {20500, []string{MySQLDefaultSqlState}, "%s"},
	ErrClientClosed:       {20501, []string{MySQLDefaultSqlState}, "client closed"},
	ErrBackendClosed:      {20502, []string{MySQLDefaultSqlState}, "backend closed"},
	ErrStreamClosed:       {20503, []string{MySQLDefaultSqlState}, "stream closed"},
	ErrNoAvailableBackend: {20504, []string{MySQLDefaultSqlState}, "no available backend"},

	// Group 6: txn
	ErrTxnClosed:          {20600, []string{MySQLDefaultSqlState}, "the transaction has been committed or aborted"},
	ErrTxnWriteConflict:   {20601, []string{MySQLDefaultSqlState}, "write conflict"},
	ErrMissingTxn:         {20602, []string{MySQLDefaultSqlState}, "missing txn"},
	ErrUnresolvedConflict: {20603, []string{MySQLDefaultSqlState}, "unresolved conflict"},
	ErrTxnError:           {20604, []string{MySQLDefaultSqlState}, "%s"},
	ErrDNShardNotFound:    {20605, []string{MySQLDefaultSqlState}, "%s"},

	// Group End: max value of MOErrorCode
	ErrEnd: {65535, []string{MySQLDefaultSqlState}, "%s"},
}

func New(code uint16, args ...any) *Error {
	return newWithDepth(Context(), code, args...)
}

func NewWithContext(ctx context.Context, code uint16, args ...any) *Error {
	return newWithDepth(ctx, code, args...)
}

func newWithDepth(ctx context.Context, code uint16, args ...any) *Error {
	var err *Error
	// We should try to find the corresponding error information and error code in mysql_error_define, in order to be more compatible with MySQL.
	// you can customize moerr if you can't find it.
	if t, ok := MysqlErrorMsgRefer[uint16(code)]; ok {
		if len(args) == 0 {
			err = &Error{
				Code:     code,
				Message:  t.ErrorMsgOrFormat,
				SqlState: MysqlErrorMsgRefer[code].SqlStates[0],
			}
		} else {
			err = &Error{
				Code:     code,
				Message:  fmt.Sprintf(t.ErrorMsgOrFormat, args...),
				SqlState: MysqlErrorMsgRefer[code].SqlStates[0],
			}
		}
	} else {
		item, has := errorMsgRefer[code]
		if !has {
			panic(fmt.Errorf("not exist MOErrorCode: %d", code))
		}
		if len(args) == 0 {
			err = &Error{
				Code:     code,
				Message:  item.errorMsgOrFormat,
				SqlState: item.sqlStates[0],
			}
		} else {
			err = &Error{
				Code:     code,
				Message:  fmt.Sprintf(item.errorMsgOrFormat, args...),
				SqlState: item.sqlStates[0],
			}
		}
	}

	_ = errors.WithContextWithDepth(ctx, err, 2)
	return err
}

type Error struct {
	Code     uint16
	Message  string
	SqlState string
}

func (e *Error) Ok() bool {
	return e.Code < ERROR_START
}

func (e *Error) Error() string {
	return e.Message
}

func IsMoErrCode(e error, rc uint16) bool {
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

func NewError(code uint16, msg string) *Error {
	err := &Error{Code: code, Message: msg}
	if t, ok := MysqlErrorMsgRefer[code]; ok {
		err.SqlState = t.SqlStates[0]
	} else if item, has := errorMsgRefer[code]; has {
		err.SqlState = item.sqlStates[0]
	} else {
		panic(fmt.Errorf("not exist MOErrorCode: %d", code))
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
