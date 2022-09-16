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
	"strings"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/util/errors"
)

const MySQLDefaultSqlState = "HY000"

const (
	// 0 is OK.
	SUCCESS Code = 0
	INFO    Code = 1
	WARN    Code = 2

	// Group 1: Internal errors
	ERROR_START              Code = 20100
	INTERNAL_ERROR           Code = 20101
	NYI                      Code = 20102
	ERROR_FUNCTION_PARAMETER Code = 20103

	// Group 2: numeric
	DIVIVISION_BY_ZERO Code = 20200
	OUT_OF_RANGE       Code = 20201
	DATA_TRUNCATED     Code = 20202
	INVALID_ARGUMENT   Code = 20203

	// Group 3: invalid input
	BAD_CONFIGURATION Code = 20300
	INVALID_INPUT     Code = 20301

	// Group 4: unexpected state
	INVALID_STATE         Code = 20400
	LOG_SERVICE_NOT_READY Code = 20401

	// Group 5: rpc timeout
	// ErrRPCTimeout rpc timeout
	ErrRPCTimeout Code = 20500
	// ErrClientClosed rpc client closed
	ErrClientClosed Code = 20501
	// ErrBackendClosed backend closed
	ErrBackendClosed Code = 20502
	// ErrStreamClosed rpc stream closed
	ErrStreamClosed Code = 20503
	// ErrNoAvailableBackend no available backend
	ErrNoAvailableBackend Code = 20504

	// Group 6: txn
	// ErrTxnAborted read and write a transaction that has been rolled back.
	ErrTxnClosed Code = 20600
	// ErrTxnWriteConflict write conflict error for concurrent transactions
	ErrTxnWriteConflict Code = 20601
	// ErrMissingTxn missing transaction error
	ErrMissingTxn Code = 20602
	// ErrUnresolvedConflict read transaction encounters unresolved data
	ErrUnresolvedConflict Code = 20603
	// ErrTxnError TxnError wrapper
	ErrTxnError Code = 20604
	// ErrDNShardNotFound DNShard not found, need to get the latest DN list from HAKeeper
	ErrDNShardNotFound Code = 20605

	// ErrEnd, the max value of MOErrorCode
	ErrEnd Code = 65535
)

type moErrorMsgItem struct {
	errorCode        Code
	sqlStates        []SqlState
	errorMsgOrFormat string
}

var errorMsgRefer = map[Code]moErrorMsgItem{
	SUCCESS: {0, []SqlState{MySQLDefaultSqlState}, "ok"},
	INFO:    {1, []SqlState{MySQLDefaultSqlState}, "%s"},
	WARN:    {2, []SqlState{MySQLDefaultSqlState}, "%s"},

	// Group 1: Internal errors
	ERROR_START:              {20100, []SqlState{MySQLDefaultSqlState}, "%s"},
	INTERNAL_ERROR:           {20101, []SqlState{MySQLDefaultSqlState}, "%s"},
	NYI:                      {20102, []SqlState{MySQLDefaultSqlState}, "%s"},
	ERROR_FUNCTION_PARAMETER: {20103, []SqlState{MySQLDefaultSqlState}, "%s"},

	// Group 2: numeric
	DIVIVISION_BY_ZERO: {20200, []SqlState{MySQLDefaultSqlState}, "division by zero"},
	OUT_OF_RANGE:       {20201, []SqlState{MySQLDefaultSqlState}, "overflow from %s to %s"},
	DATA_TRUNCATED:     {20202, []SqlState{MySQLDefaultSqlState}, "%s data truncated"},
	INVALID_ARGUMENT:   {20203, []SqlState{MySQLDefaultSqlState}, "%s"},

	// Group 3: invalid input
	BAD_CONFIGURATION: {20300, []SqlState{MySQLDefaultSqlState}, "invalid %s configuration"},
	INVALID_INPUT:     {20301, []SqlState{MySQLDefaultSqlState}, "%s"},

	// Group 4: unexpected state
	INVALID_STATE:         {20400, []SqlState{MySQLDefaultSqlState}, "%s"},
	LOG_SERVICE_NOT_READY: {20401, []SqlState{MySQLDefaultSqlState}, "log service not ready"},

	// Group 5: rpc timeout
	ErrRPCTimeout:         {20500, []SqlState{MySQLDefaultSqlState}, "%s"},
	ErrClientClosed:       {20501, []SqlState{MySQLDefaultSqlState}, "client closed"},
	ErrBackendClosed:      {20502, []SqlState{MySQLDefaultSqlState}, "backend closed"},
	ErrStreamClosed:       {20503, []SqlState{MySQLDefaultSqlState}, "stream closed"},
	ErrNoAvailableBackend: {20504, []SqlState{MySQLDefaultSqlState}, "no available backend"},

	// Group 6: txn
	ErrTxnClosed:          {20600, []SqlState{MySQLDefaultSqlState}, "the transaction has been committed or aborted"},
	ErrTxnWriteConflict:   {20601, []SqlState{MySQLDefaultSqlState}, "write conflict"},
	ErrMissingTxn:         {20602, []SqlState{MySQLDefaultSqlState}, "missing txn"},
	ErrUnresolvedConflict: {20603, []SqlState{MySQLDefaultSqlState}, "unresolved conflict"},
	ErrTxnError:           {20604, []SqlState{MySQLDefaultSqlState}, "%s"},
	ErrDNShardNotFound:    {20605, []SqlState{MySQLDefaultSqlState}, "%s"},

	// Group End: max value of MOErrorCode
	ErrEnd: {65535, []SqlState{MySQLDefaultSqlState}, "%s"},
}

func New(code Code, args ...any) *Error {
	return newWithDepth(Context(), code, args...)
}

func NewWithContext(ctx context.Context, code Code, args ...any) *Error {
	return newWithDepth(ctx, code, args...)
}

func newWithDepth(ctx context.Context, code Code, args ...any) *Error {
	// We should try to find the corresponding error information and error code in mysql_error_define, in order to be more compatible with MySQL.
	// you can customize moerr if you can't find it.
	err := Error{code}
	if t, ok := MysqlErrorMsgRefer[code]; ok {
		if len(args) == 0 {
			err = append(err, Message(t.ErrorMsgOrFormat))
			err = append(err, MysqlErrorMsgRefer[code].SqlStates[0])
		} else {
			err = append(err, Message(fmt.Sprintf(t.ErrorMsgOrFormat, args...)))
			err = append(err, MysqlErrorMsgRefer[code].SqlStates[0])
		}
	} else {
		item, has := errorMsgRefer[code]
		if !has {
			panic(fmt.Errorf("not exist MOErrorCode: %d", code))
		}
		if len(args) == 0 {
			err = append(err, Message(item.errorMsgOrFormat))
			err = append(err, item.sqlStates[0])
		} else {
			err = append(err, Message(fmt.Sprintf(item.errorMsgOrFormat, args...)))
			err = append(err, item.sqlStates[0])
		}
	}

	_ = errors.WithContextWithDepth(ctx, &err, 2)
	return &err
}

type Error []error

func (e *Error) Error() string {
	var msg Message
	if errors.As(e, &msg) {
		return string(msg)
	}
	buf := new(strings.Builder)
	for i, err := range *e {
		if i > 0 {
			buf.WriteString("\n")
		}
		buf.WriteString(err.Error())
	}
	return buf.String()
}

func (e Error) Is(target error) bool {
	for _, err := range e {
		if errors.Is(err, target) {
			return true
		}
	}
	return false
}

func (e Error) As(target any) bool {
	for _, err := range e {
		if errors.As(err, target) {
			return true
		}
	}
	return false
}

func (e *Error) Wrap(errs ...error) *Error {
	err := append(*e, errs...)
	return &err
}

type Code uint16

func (c Code) Error() string {
	return fmt.Sprintf("code: %d", c)
}

func (e *Error) GetCode() (code Code) {
	if !errors.As(e, &code) {
		panic("no code in error")
	}
	return
}

type Message string

func (m Message) Error() string {
	return string(m)
}

type SqlState string

func (s SqlState) Error() string {
	return "sql state: " + string(s)
}

func (e *Error) GetSqlState() (state SqlState) {
	if !errors.As(e, &state) {
		panic("no sql state in error")
	}
	return
}

func (e *Error) Ok() bool {
	return e.GetCode() < ERROR_START
}

func IsMoErrCode(e error, rc Code) bool {
	var code Code
	return errors.As(e, &code) && code == rc
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

func NewError(code Code, msg string) *Error {
	err := Error{code, Message(msg)}
	if t, ok := MysqlErrorMsgRefer[code]; ok {
		err = append(err, t.SqlStates[0])
	} else if item, has := errorMsgRefer[code]; has {
		err = append(err, item.sqlStates[0])
	} else {
		panic(fmt.Errorf("not exist MOErrorCode: %d", code))
	}
	_ = errors.WithContextWithDepth(Context(), &err, 1)
	return &err
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
