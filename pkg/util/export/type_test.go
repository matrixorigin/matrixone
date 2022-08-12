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

package export

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/util/batchpipe"
	"reflect"
	"testing"
)

type dummyContextKey int

var dummyCKKey = dummyContextKey(0)

func TestDefaultContext(t *testing.T) {
	tests := []struct {
		name string
		f    getContextFunc
		want context.Context
	}{
		{
			name: "normal",
			want: context.Background(),
		},
		{
			name: "set",
			f: func() context.Context {
				return context.WithValue(context.Background(), dummyCKKey, "val")
			},
			want: context.WithValue(context.Background(), dummyCKKey, "val"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.f != nil {
				SetDefaultContextFunc(tt.f)
			}
			if got := DefaultContext(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DefaultContext() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetGlobalBatchProcessor(t *testing.T) {
	tests := []struct {
		name string
		want BatchProcessor
	}{
		{
			name: "normal",
			want: &noopBatchProcessor{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetGlobalBatchProcessor(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetGlobalBatchProcessor() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRegister(t *testing.T) {
	type args struct {
		name batchpipe.HasName
		impl batchpipe.PipeImpl[batchpipe.HasName, any]
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "normal",
			args: args{
				name: newNum(1),
				impl: &dummyNumPipeImpl{},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			Register(tt.args.name, tt.args.impl)
		})
	}
}
