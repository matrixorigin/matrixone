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
	"fmt"
	"runtime/debug"
)

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
	INVALID_STATE = 4000

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

	// Group 10: txn
	// ErrTxnAborted read and write a transaction that has been rolled back.
	ErrTxnClosed = 10000
	// ErrTxnWriteConflict write conflict error for concurrent transactions
	ErrTxnWriteConflict = 10001
	// ErrMissingTxn missing transaction error
	ErrMissingTxn = 10002
	// ErrUnreslovedConflict read transaction encounters unresloved data
	ErrUnreslovedConflict = 10003
	// ErrTxnError TxnError wrapper
	ErrTxnError = 10004
	// ErrDNShardNotFound DNShard not found, need to get the latest DN list from HAKeeper
	ErrDNShardNotFound = 10005
)

type Error struct {
	Code    int32
	Message string
}

func (e *Error) Ok() bool {
	return e.Code < ERROR_START
}

func (e *Error) Error() string {
	return e.Message
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

func NewInfo(msg string) *Error {
	return &Error{INFO, msg}
}

func NewWarn(msg string) *Error {
	return &Error{WARN, msg}
}

func NewInternalError(msg string, args ...interface{}) *Error {
	var err = Error{Code: INTERNAL_ERROR}
	err.Message = fmt.Sprintf("Internal error: "+msg, args...)
	return &err
}

// NewPanicError converts a runtime panic to internal error.
func NewPanicError(v interface{}) *Error {
	if e, ok := v.(*Error); ok {
		return e
	}
	return NewInternalError("panic %v: %s", v, debug.Stack())
}

func NewError(code int32, msg string) *Error {
	return &Error{code, msg}
}
