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
	SUCCESS = iota
	INFO
	WARN

	// Group 1: Internal errors
	ERROR_START = 1000 + iota
	INTERNAL_ERROR
	NYI
	ERROR_FUNCTION_PARAMETER

	// Group 2: numeric
	DIVIVISION_BY_ZERO = 2000 + iota
	OUT_OF_RANGE
	INVALID_ARGUMENT

	// Group 3: invalid input
	BAD_CONFIGURATION = 3000 + iota
	INVALID_INPUT

	// Group 4: unexpected state
	INVALID_STATE = 4000 + iota

	// group 5: rpc timeout
	// ErrRPCTimeout rpc timeout
	ErrRPCTimeout = 5000 + iota
	// ErrClientClosed rpc client closed
	ErrClientClosed
	// ErrBackendClosed backend closed
	ErrBackendClosed
	// ErrStreamClosed rpc stream closed
	ErrStreamClosed
	// ErrNoAvailableBackend no available backend
	ErrNoAvailableBackend

	// Group 10: txn
	// ErrTxnAborted read and write a transaction that has been rolled back.
	ErrTxnClosed = 10000 + iota
	// ErrTxnWriteConflict write conflict error for concurrent transactions
	ErrTxnWriteConflict
	// ErrMissingTxn missing transaction error
	ErrMissingTxn
	// ErrUnreslovedConflict read transaction encounters unresloved data
	ErrUnreslovedConflict
	// ErrTxnError TxnError wrapper
	ErrTxnError
	// ErrDNShardNotFound DNShard not found, need to get the latest DN list from HAKeeper
	ErrDNShardNotFound
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
