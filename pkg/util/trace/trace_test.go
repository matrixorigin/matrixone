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

package trace

import (
	"context"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/util/export"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"reflect"
	"testing"
)

func TestDefaultContext(t *testing.T) {
	tests := []struct {
		name string
		want context.Context
	}{
		{
			name: "normal",
			want: ContextWithSpanContext(context.Background(), SpanContextWithIDs(0, 0)),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, DefaultContext(), "DefaultContext()")
		})
	}
}

func TestDefaultSpanContext(t *testing.T) {
	sc := SpanContextWithIDs(0, 0)
	tests := []struct {
		name string
		want *SpanContext
	}{
		{
			name: "normal",
			want: &sc,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, DefaultSpanContext(), "DefaultSpanContext()")
		})
	}
}

func TestGetNodeResource(t *testing.T) {
	tests := []struct {
		name string
		want *MONodeResource
	}{
		{
			name: "normal",
			want: &MONodeResource{0, NodeTypeNode},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, GetNodeResource(), "GetNodeResource()")
		})
	}
}

func Test_initExport(t *testing.T) {
	type args struct {
		config *tracerProviderConfig
	}
	ch := make(chan string, 10)
	tests := []struct {
		name  string
		args  args
		empty bool
	}{
		{
			name: "disable",
			args: args{
				config: &tracerProviderConfig{enableTracer: false},
			},
			empty: true,
		},
		{
			name: "enable_InternalExecutor",
			args: args{
				config: &tracerProviderConfig{
					enableTracer: true, batchProcessMode: InternalExecutor, sqlExecutor: newExecutorFactory(ch),
				}},
			empty: false,
		},
	}
	sysVar := &config.GlobalSystemVariables
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sysVar.SetEnableTrace(tt.args.config.enableTracer)
			initExport(tt.args.config)
			if tt.empty {
				require.Equal(t, fmt.Sprintf("%v", reflect.ValueOf(export.GetGlobalBatchProcessor()).Type()), "*export.noopBatchProcessor")
			} else {
				require.Equal(t, fmt.Sprintf("%v", reflect.ValueOf(export.GetGlobalBatchProcessor()).Type()), "*export.MOCollector")
			}
			require.Equal(t, Shutdown(context.Background(), sysVar), nil)
		})
	}
}
