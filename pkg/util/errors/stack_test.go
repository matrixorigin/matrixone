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
	"github.com/matrixorigin/matrixone/pkg/util"
	"github.com/stretchr/testify/require"
	"reflect"
	"testing"
)

func TestGetStackTracer(t *testing.T) {
	type args struct {
		cause error
	}
	tests := []struct {
		name           string
		args           args
		hasStackTracer bool
		want           util.StackTrace
	}{
		{
			name: "normal",
			args: args{
				cause: stackErr,
			},
			hasStackTracer: true,
			want:           stackErr.(*withStack).Stack.StackTrace(),
		},
		{
			name: "withContext",
			args: args{
				cause: WithContext(context.Background(), stackErr),
			},
			hasStackTracer: true,
			want:           stackErr.(*withStack).Stack.StackTrace(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := GetStackTracer(tt.args.cause)
			if !tt.hasStackTracer {
				require.Empty(t, got, "GetStackTracer not empty")
				return
			}
			if !reflect.DeepEqual(got.StackTrace(), tt.want) {
				t.Errorf("GetStackTracer() = %v, want %v", got.StackTrace(), tt.want)
			}
		})
	}
}

func TestHasStack(t *testing.T) {
	type args struct {
		err error
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "normal",
			args: args{
				err: stackErr,
			},
			want: true,
		},
		{
			name: "withContext",
			args: args{
				err: WithContext(context.Background(), stackErr),
			},
			want: true,
		},
		{
			name: "raw error",
			args: args{
				err: testErr,
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := HasStack(tt.args.err); got != tt.want {
				t.Errorf("HasStack() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_withStack_Cause(t *testing.T) {
	type fields struct {
		cause error
		Stack *util.Stack
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name: "normal",
			fields: fields{
				cause: stackErr,
				Stack: stackErr.(*withStack).Stack,
			},
			wantErr: true,
		},
		{
			name: "withContext",
			fields: fields{
				cause: msgErr,
				Stack: stackErr.(*withStack).Stack,
			},
			wantErr: true,
		},
		{
			name: "empty",
			fields: fields{
				cause: nil,
				Stack: nil,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &withStack{
				cause: tt.fields.cause,
				Stack: tt.fields.Stack,
			}
			if err := w.Cause(); (err != nil) != tt.wantErr {
				t.Errorf("Cause() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_WithStack_HasStack(t *testing.T) {
	type fields struct {
		cause error
		Stack *util.Stack
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		{
			name: "normal",
			fields: fields{
				cause: stackErr,
				Stack: stackErr.(*withStack).Stack,
			},
			want: true,
		},
		{
			name: "withContext",
			fields: fields{
				cause: WithContext(context.Background(), stackErr),
				Stack: stackErr.(*withStack).Stack,
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &withStack{
				cause: tt.fields.cause,
				Stack: tt.fields.Stack,
			}
			if got := w.HasStack(); got != tt.want {
				t.Errorf("HasStack() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_WithStack(t *testing.T) {
	type fields struct {
		cause error
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		{
			name:   "nil",
			fields: fields{},
			want:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := WithStack(tt.fields.cause)
			if (got != nil) != tt.want {
				t.Errorf("HasStack() = %v, want %v", got, tt.want)
			}
		})
	}
}
