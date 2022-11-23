// Copyright The OpenTelemetry Authors
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

// Portions of this file are additionally subject to the following
// copyright.
//
// Copyright (C) 2022 Matrix Origin.
//
// Modified the behavior and the interface of the step.

package trace

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/util/batchpipe"
	"github.com/stretchr/testify/assert"
)

var _ batchpipe.HasName = &namedNoopSpan{}

type namedNoopSpan struct{ noopSpan }

func (n namedNoopSpan) GetName() string { return "NamedNopSpan" }

func TestNewBatchSpanProcessor(t *testing.T) {
	type args struct {
		exporter BatchProcessor
	}
	tests := []struct {
		name string
		args args
		want SpanProcessor
	}{
		{
			name: "normal",
			args: args{exporter: &noopBatchProcessor{}},
			want: &batchSpanProcessor{
				e:      &noopBatchProcessor{},
				stopCh: make(chan struct{}),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewBatchSpanProcessor(tt.args.exporter); reflect.DeepEqual(got, tt.want) {
				assert.Equalf(t, tt.want, got, "NewBatchSpanProcessor(%v)", tt.args.exporter)
			}
		})
	}
}

func Test_batchSpanProcessor_OnEnd(t *testing.T) {
	type fields struct {
		e BatchProcessor
	}
	type args struct {
		c context.Context
		s Span
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name:   "normal",
			fields: fields{noopBatchProcessor{}},
			args:   args{context.Background(), namedNoopSpan{}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bsp := NewBatchSpanProcessor(tt.fields.e)
			bsp.OnStart(tt.args.c, tt.args.s)
			bsp.OnEnd(tt.args.s)
		})
	}
}

func Test_batchSpanProcessor_Shutdown(t *testing.T) {
	type fields struct {
		e      BatchProcessor
		stopCh chan struct{}
	}
	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "normal",
			fields: fields{
				e:      noopBatchProcessor{},
				stopCh: make(chan struct{}),
			},
			args: args{context.Background()},
			wantErr: func(t assert.TestingT, err error, msgs ...interface{}) bool {
				if err != nil {
					t.Errorf(msgs[0].(string), msgs[1:])
					return false
				}
				return true
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bsp := &batchSpanProcessor{
				e:      tt.fields.e,
				stopCh: tt.fields.stopCh,
			}
			tt.wantErr(t, bsp.Shutdown(tt.args.ctx), fmt.Sprintf("Shutdown(%v)", tt.args.ctx))
		})
	}
}
