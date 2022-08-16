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
	goErrors "errors"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func pf1() {
	panic("foo")
}

func pf2(a, b int) int {
	return a / b
}

func pf3() {
	panic(NewInternalError("%s %s %s %d", "foo", "bar", "zoo", 2))
}

func PanicF(i int) (err *Error) {
	defer func() {
		if e := recover(); e != nil {
			err = NewPanicError(e)
		}
	}()
	switch i {
	case 1:
		pf1()
	case 2:
		foo := pf2(1, 0)
		panic(foo)
	case 3:
		pf3()
	default:
		return nil
	}
	return
}

func TestPanicError(t *testing.T) {
	for i := 0; i <= 3; i++ {
		err := PanicF(i)
		if i == 0 {
			if err != nil {
				t.Errorf("No panic should be OK")
			}
		} else {
			if err == nil {
				t.Errorf("Uncaught panic")
			}
			if err.Ok() {
				t.Errorf("Caught OK panic")
			}
		}
	}
}

func TestNew(t *testing.T) {
	type args struct {
		code int32
		args []any
	}
	tests := []struct {
		name string
		args args
		want *Error
	}{
		{
			name: "DIVISION_BY_ZERO",
			args: args{code: DIVIVISION_BY_ZERO, args: []any{}},
			want: &Error{Code: DIVIVISION_BY_ZERO, ErrorCode: 20000 + DIVIVISION_BY_ZERO, Message: "division by zero"},
		},
		{
			name: "OUT_OF_RANGE",
			args: args{code: OUT_OF_RANGE, args: []any{"double", "bigint"}},
			want: &Error{Code: OUT_OF_RANGE, ErrorCode: 20000 + OUT_OF_RANGE, Message: "overflow from double to bigint", MysqlErrCode: 0},
		},
		{
			name: "DATA_TRUNCATED",
			args: args{code: DATA_TRUNCATED, args: []any{"decimal128"}},
			want: &Error{Code: DATA_TRUNCATED, ErrorCode: 20000 + DATA_TRUNCATED, Message: "decimal128 data truncated"},
		},
		{
			name: "BAD_CONFIGURATION",
			args: args{code: BAD_CONFIGURATION, args: []any{"log"}},
			want: &Error{Code: BAD_CONFIGURATION, ErrorCode: 20000 + BAD_CONFIGURATION, Message: "invalid log configuration"},
		},
		{
			name: "LOG_SERVICE_NOT_READY",
			args: args{code: LOG_SERVICE_NOT_READY, args: []any{}},
			want: &Error{Code: LOG_SERVICE_NOT_READY, ErrorCode: 20000 + LOG_SERVICE_NOT_READY, Message: "log service not ready"},
		},
		{
			name: "ErrClientClosed",
			args: args{code: ErrClientClosed, args: []any{}},
			want: &Error{Code: ErrClientClosed, ErrorCode: 20000 + ErrClientClosed, Message: "client closed"},
		},
		{
			name: "ErrBackendClosed",
			args: args{code: ErrBackendClosed, args: []any{}},
			want: &Error{Code: ErrBackendClosed, ErrorCode: 20000 + ErrBackendClosed, Message: "backend closed"},
		},
		{
			name: "ErrStreamClosed",
			args: args{code: ErrStreamClosed, args: []any{}},
			want: &Error{Code: ErrStreamClosed, ErrorCode: 20000 + ErrStreamClosed, Message: "stream closed"},
		},
		{
			name: "ErrNoAvailableBackend",
			args: args{code: ErrNoAvailableBackend, args: []any{}},
			want: &Error{Code: ErrNoAvailableBackend, ErrorCode: 20000 + ErrNoAvailableBackend, Message: "no available backend"},
		},
		{
			name: "ErrTxnClosed",
			args: args{code: ErrTxnClosed, args: []any{}},
			want: &Error{Code: ErrTxnClosed, ErrorCode: 20000 + ErrTxnClosed, Message: "the transaction has been committed or aborted"},
		},
		{
			name: "ErrTxnWriteConflict",
			args: args{code: ErrTxnWriteConflict, args: []any{}},
			want: &Error{Code: ErrTxnWriteConflict, ErrorCode: 20000 + ErrTxnWriteConflict, Message: "write conflict"},
		},
		{
			name: "ErrMissingTxn",
			args: args{code: ErrMissingTxn, args: []any{}},
			want: &Error{Code: ErrMissingTxn, ErrorCode: 20000 + ErrMissingTxn, Message: "missing txn"},
		},
		{
			name: "ErrUnresolvedConflict",
			args: args{code: ErrUnresolvedConflict, args: []any{}},
			want: &Error{Code: ErrUnresolvedConflict, ErrorCode: 20000 + ErrUnresolvedConflict, Message: "unresolved conflict"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := New(tt.args.code, tt.args.args...)
			require.Equal(t, tt.want, got)
			require.Equal(t, tt.want.Message, got.Error())
		})
	}
}

func TestNewError(t *testing.T) {
	type args struct {
		code int32
		msg  string
	}
	tests := []struct {
		name string
		args args
		want *Error
	}{
		{
			name: "DIVISION_BY_ZERO",
			want: New(DIVIVISION_BY_ZERO),
			args: args{code: DIVIVISION_BY_ZERO, msg: "division by zero"},
		},
		{
			name: "OUT_OF_RANGE",
			want: New(OUT_OF_RANGE, "double", "bigint"),
			args: args{code: OUT_OF_RANGE, msg: "overflow from double to bigint"},
		},
		{
			name: "DATA_TRUNCATED",
			want: New(DATA_TRUNCATED, "decimal128"),
			args: args{code: DATA_TRUNCATED, msg: "decimal128 data truncated"},
		},
		{
			name: "BAD_CONFIGURATION",
			want: New(BAD_CONFIGURATION, "log"),
			args: args{code: BAD_CONFIGURATION, msg: "invalid log configuration"},
		},
		{
			name: "LOG_SERVICE_NOT_READY",
			want: New(LOG_SERVICE_NOT_READY),
			args: args{code: LOG_SERVICE_NOT_READY, msg: "log service not ready"},
		},
		{
			name: "ErrClientClosed",
			want: New(ErrClientClosed),
			args: args{code: ErrClientClosed, msg: "client closed"},
		},
		{
			name: "ErrBackendClosed",
			want: New(ErrBackendClosed),
			args: args{code: ErrBackendClosed, msg: "backend closed"},
		},
		{
			name: "ErrStreamClosed",
			want: New(ErrStreamClosed),
			args: args{code: ErrStreamClosed, msg: "stream closed"},
		},
		{
			name: "ErrNoAvailableBackend",
			want: New(ErrNoAvailableBackend),
			args: args{code: ErrNoAvailableBackend, msg: "no available backend"},
		},
		{
			name: "ErrTxnClosed",
			want: New(ErrTxnClosed),
			args: args{code: ErrTxnClosed, msg: "the transaction has been committed or aborted"},
		},
		{
			name: "ErrTxnWriteConflict",
			want: New(ErrTxnWriteConflict),
			args: args{code: ErrTxnWriteConflict, msg: "write conflict"},
		},
		{
			name: "ErrMissingTxn",
			want: New(ErrMissingTxn),
			args: args{code: ErrMissingTxn, msg: "missing txn"},
		},
		{
			name: "ErrUnresolvedConflict",
			want: New(ErrUnresolvedConflict),
			args: args{code: ErrUnresolvedConflict, msg: "unresolved conflict"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NewError(tt.args.code, tt.args.msg)
			require.Equal(t, tt.want, got)
			require.Equal(t, tt.want.Message, got.Error())
			require.Equal(t, IsMoErrCode(got, tt.want.Code), true)
		})
	}
}

func TestNew_panic(t *testing.T) {
	type args struct {
		code int32
		msg  string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "panic",
			args: args{code: 65534},
			want: "not exist MOErrorCode: 65534",
		},
	}
	defer func() {
		var err any
		if err = recover(); err != nil {
			require.Equal(t, tests[0].want, err.(error).Error())
			t.Logf("err: %+v", err)
		}
	}()
	for _, tt := range tests {
		got := New(tt.args.code, tt.args.msg)
		require.Equal(t, nil, got)
	}
}

func TestNew_MyErrorCode(t *testing.T) {
	type args struct {
		code int32
		args []any
	}
	tests := []struct {
		name string
		args args
		want uint16
	}{
		{
			name: "hasMysqlErrorCode",
			args: args{code: ErrNoDatabaseSelected, args: []any{}},
			want: 1046,
		},
	}
	for _, tt := range tests {
		got := New(tt.args.code, tt.args.args...)
		require.Equal(t, got.MyErrorCode(), tt.want)
	}
}

func TestNewError_panic(t *testing.T) {
	type args struct {
		code int32
		msg  string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "panic",
			args: args{code: 65534, msg: "not exist error code"},
			want: "not exist MOErrorCode: 65534",
		},
	}
	defer func() {
		var err any
		if err = recover(); err != nil {
			require.Equal(t, tests[0].want, err.(error).Error())
			t.Logf("err: %+v", err)
		}
	}()
	for _, tt := range tests {
		got := NewError(tt.args.code, tt.args.msg)
		require.Equal(t, nil, got)
	}
}

func TestNewInfo(t *testing.T) {
	type args struct {
		msg string
	}
	tests := []struct {
		name string
		args args
		want *Error
	}{
		{
			name: "normal",
			args: args{msg: "info msg"},
			want: New(INFO, "info msg"),
		},
	}
	for _, tt := range tests {
		got := NewInfo(tt.args.msg)
		require.Equal(t, tt.want, got)
		require.Equal(t, tt.want.Message, got.Error())
		require.Equal(t, IsMoErrCode(got, tt.want.Code), true)
	}
}

func TestNewWarn(t *testing.T) {
	type args struct {
		msg string
	}
	tests := []struct {
		name string
		args args
		want *Error
	}{
		{
			name: "normal",
			args: args{msg: "error msg"},
			want: New(WARN, "error msg"),
		},
	}
	for _, tt := range tests {
		got := NewWarn(tt.args.msg)
		require.Equal(t, tt.want, got)
		require.Equal(t, tt.want.Message, got.Error())
		require.Equal(t, tt.want.ErrorCode, got.(*Error).MyErrorCode())
		require.Equal(t, "HY000", got.(*Error).SqlState())
		require.Equal(t, IsMoErrCode(got, tt.want.Code), true)
	}
}

func TestIsMoErrCode(t *testing.T) {
	type args struct {
		e  error
		rc int32
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "not Error",
			args: args{e: goErrors.New("raw error"), rc: INFO},
			want: false,
		},
		{
			name: "End Error",
			args: args{e: New(ErrEnd, "max value of MOError"), rc: ErrEnd},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsMoErrCode(tt.args.e, tt.args.rc); got != tt.want {
				t.Errorf("IsMoErrCode() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewWithContext(t *testing.T) {
	type args struct {
		ctx  context.Context
		code int32
		args []any
	}
	tests := []struct {
		name string
		args args
		want *Error
	}{
		{
			name: "normal",
			args: args{ctx: context.Background(), code: DIVIVISION_BY_ZERO, args: []any{}},
			want: &Error{Code: DIVIVISION_BY_ZERO, ErrorCode: 20000 + DIVIVISION_BY_ZERO, Message: "division by zero"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewWithContext(tt.args.ctx, tt.args.code, tt.args.args...); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewWithContext() = %v, want %v", got, tt.want)
			}
		})
	}
}
