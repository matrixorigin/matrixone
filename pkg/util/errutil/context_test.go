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
	goErrors "errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/util/stack"
)

var ctx = context.Background()
var testErr = goErrors.New("test error")
var stackErr error = &withStack{cause: testErr, Stack: stack.Callers(1)}

func TestGetContextTracer(t *testing.T) {
	type args struct {
		err error
	}
	tests := []struct {
		name string
		args args
		want context.Context
	}{
		{
			name: "nil",
			args: args{err: goErrors.New("test error")},
			want: nil,
		},
		{
			name: "context",
			args: args{err: WithContext(context.Background(), goErrors.New("test error"))},
			want: context.Background(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetContextTracer(tt.args.err); !reflect.DeepEqual(got, tt.want) && !reflect.DeepEqual(got.Context(), tt.want) {
				t.Errorf("GetContextTracer() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHasContext(t *testing.T) {
	type args struct {
		err error
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "nil",
			args: args{err: goErrors.New("test error")},
			want: false,
		},
		{
			name: "context",
			args: args{err: WithContext(context.Background(), goErrors.New("test error"))},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := HasContext(tt.args.err); got != tt.want {
				t.Errorf("HasContext() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_withContext_Cause(t *testing.T) {
	type fields struct {
		cause error
		ctx   context.Context
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr error
	}{
		{
			name:    "normal",
			fields:  fields{cause: testErr, ctx: ctx},
			wantErr: testErr,
		},
		{
			name:    "stack",
			fields:  fields{stackErr, ctx},
			wantErr: stackErr,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &withContext{
				cause: tt.fields.cause,
				ctx:   tt.fields.ctx,
			}
			if got := w.Cause(); !dummyEqualError(got, tt.wantErr) {
				t.Errorf("Cause() error = %v, wantErr %v", got, tt.wantErr)
			}
		})
	}
}

func Test_withContext_Context(t *testing.T) {
	type fields struct {
		cause error
		ctx   context.Context
	}
	tests := []struct {
		name   string
		fields fields
		want   context.Context
	}{
		{
			name:   "normal",
			fields: fields{cause: testErr, ctx: ctx},
			want:   ctx,
		},
		{
			name:   "stack",
			fields: fields{stackErr, ctx},
			want:   ctx,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &withContext{
				cause: tt.fields.cause,
				ctx:   tt.fields.ctx,
			}
			if got := w.Context(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Context() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_withContext_Error(t *testing.T) {
	type fields struct {
		cause error
		ctx   context.Context
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name:   "normal",
			fields: fields{cause: testErr, ctx: ctx},
			want:   "test error",
		},
		{
			name:   "stack",
			fields: fields{stackErr, ctx},
			want:   "test error",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &withContext{
				cause: tt.fields.cause,
				ctx:   tt.fields.ctx,
			}
			if got := w.Error(); got != tt.want {
				t.Errorf("Error() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_withContext_Unwrap(t *testing.T) {
	type fields struct {
		cause error
		ctx   context.Context
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr error
	}{
		{
			name:    "normal",
			fields:  fields{cause: testErr, ctx: ctx},
			wantErr: testErr,
		},
		{
			name:    "stack",
			fields:  fields{stackErr, ctx},
			wantErr: stackErr,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &withContext{
				cause: tt.fields.cause,
				ctx:   tt.fields.ctx,
			}
			if got := w.Unwrap(); !dummyEqualError(got, tt.wantErr) {
				t.Errorf("Unwrap() = %v, want %v", got, tt.wantErr)
			}
		})
	}
}

func TestWithContext(t *testing.T) {
	type args struct {
		ctx context.Context
		err error
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name:    "normal",
			args:    args{context.Background(), testErr},
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
			if err := WithContext(tt.args.ctx, tt.args.err); (err != nil) != tt.wantErr {
				t.Errorf("TestWithContext() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func dummyEqualError(err1, err2 error) bool {
	return fmt.Sprintf("%+v", err1) == fmt.Sprintf("%+v", err2)
}
