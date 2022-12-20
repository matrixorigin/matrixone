// Copyright 2022 Matrix Origin
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

package errutil

import (
	"context"
	"github.com/stretchr/testify/require"
	"reflect"
	"sync/atomic"
	"testing"
)

func mockReportError(_ context.Context, err error, depth int) {}

func TestGetReportErrorFunc(t *testing.T) {
	tests := []struct {
		name string
		f    reportErrorFunc
		want uintptr
	}{
		{
			name: "noop",
			want: reflect.ValueOf(noopReportError).Pointer(),
		},
		{
			name: "mark",
			f:    mockReportError,
			want: reflect.ValueOf(mockReportError).Pointer(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.f != nil {
				SetErrorReporter(tt.f)
			}
			if got := GetReportErrorFunc(); reflect.ValueOf(got).Pointer() != tt.want {
				t.Errorf("GetReportErrorFunc() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestReportError(t *testing.T) {
	type args struct {
		ctx context.Context
		err error
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "normal",
			args: args{context.Background(), testErr},
		},
		{
			name: "noReport",
			args: args{ContextWithNoReport(context.Background(), true), testErr},
		},
	}

	var counter atomic.Int32
	var doReportFunc = func(context.Context, error, int) {
		counter.Add(1)
	}
	SetErrorReporter(doReportFunc)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ReportError(tt.args.ctx, tt.args.err)
		})
	}
	require.Equal(t, counter.Load(), int32(len(tests)-1))
}

func TestWalkDeep(t *testing.T) {
	type args struct {
		err     error
		visitor func(err error) bool
	}
	var depth int32 = 0
	var visitor = func(error) bool {
		atomic.AddInt32(&depth, 1)
		return false
	}
	tests := []struct {
		name      string
		args      args
		wantDepth int32
	}{
		{
			name: "depth_2",
			args: args{
				err:     stackErr,
				visitor: visitor,
			},
			wantDepth: 2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			atomic.StoreInt32(&depth, 0)
			_ = WalkDeep(tt.args.err, tt.args.visitor)
			if depth != tt.wantDepth {
				t.Errorf("WalkDeep() depth = %v, want %v", depth, tt.wantDepth)
			}
		})
	}
}

func TestWrap(t *testing.T) {
	type args struct {
		err     error
		message string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name:    "normal",
			args:    args{testErr, "normal message"},
			wantErr: true,
		},
		{
			name:    "nil",
			args:    args{},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := Wrap(tt.args.err, tt.args.message); (err != nil) != tt.wantErr {
				t.Errorf("Wrap() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestWrapf(t *testing.T) {
	type args struct {
		err    error
		format string
		args   []interface{}
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name:    "normal",
			args:    args{testErr, "normal message", []any{}},
			wantErr: true,
		},
		{
			name:    "nil",
			args:    args{},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := Wrapf(tt.args.err, tt.args.format, tt.args.args...); (err != nil) != tt.wantErr {
				t.Errorf("Wrapf() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_noopReportError(t *testing.T) {
	type args struct {
		in0 context.Context
		in1 error
		in2 int
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "normal",
			args: args{context.Background(), testErr, 1},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			noopReportError(tt.args.in0, tt.args.in1, tt.args.in2)
		})
	}
}
