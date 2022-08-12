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

package errors

import (
	"context"
	"reflect"
	"sync/atomic"
	"testing"
)

func TestAs(t *testing.T) {
	type args struct {
		err    error
		target error
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "withContext",
			args: args{
				err:    WithContext(context.Background(), msgErr),
				target: &withContext{},
			},
			want: true,
		},
		{
			name: "withStack",
			args: args{
				err:    WithContext(context.Background(), msgErr),
				target: &withStack{},
			},
			want: true,
		},
		{
			name: "withMessage",
			args: args{
				err:    WithContext(context.Background(), msgErr),
				target: &withMessage{},
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := As(tt.args.err, &tt.args.target); got != tt.want {
				t.Errorf("As() = %v, want %v", got, tt.want)
			}
			t.Logf("target:: %v, type: %v", tt.args.target, reflect.ValueOf(tt.args.target).Type())
		})
	}
}

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

func TestIs(t *testing.T) {
	type args struct {
		err    error
		target error
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "withContext",
			args: args{
				err:    WithContext(context.Background(), msgErr),
				target: &withContext{msgErr, context.Background()},
			},
			want: true,
		},
		{
			name: "withStack",
			args: args{
				err:    WithContext(context.Background(), msgErr),
				target: &withStack{testErr, nil},
			},
			want: false,
		},
		{
			name: "withMessage",
			args: args{
				err:    WithContext(context.Background(), msgErr),
				target: msgErr,
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Is(tt.args.err, tt.args.target); got != tt.want {
				t.Errorf("Is() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNew(t *testing.T) {
	type args struct {
		text string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name:    "normal",
			args:    args{text: "message"},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := New(tt.args.text); (err != nil) != tt.wantErr {
				t.Errorf("New() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestNewWithContext(t *testing.T) {
	type args struct {
		ctx  context.Context
		text string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "normal",
			args: args{
				ctx:  context.Background(),
				text: "normal",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := NewWithContext(tt.args.ctx, tt.args.text); (err != nil) != tt.wantErr {
				t.Errorf("NewWithContext() error = %v, wantErr %v", err, tt.wantErr)
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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ReportError(tt.args.ctx, tt.args.err)
		})
	}
}

func TestUnwrap(t *testing.T) {
	type args struct {
		err error
	}
	tests := []struct {
		name string
		args args
		want error
	}{
		{
			name: "withContext",
			args: args{err: WithContext(context.Background(), stackErr)},
			want: stackErr,
		},
		{
			name: "withStack",
			args: args{err: stackErr},
			want: testErr,
		},
		{
			name: "withMessage",
			args: args{err: msgErr},
			want: stackErr,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := Unwrap(tt.args.err); !reflect.DeepEqual(err, tt.want) {
				t.Errorf("Unwrap() error = %v, wantErr %v", err, tt.want)
			}
		})
	}
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
		{
			name: "depth_4",
			args: args{
				err:     msg2Err,
				visitor: visitor,
			},
			wantDepth: 4,
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
