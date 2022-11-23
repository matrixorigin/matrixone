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
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

var _1TxnID = [16]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x1}
var _1SesID = [16]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x1}
var _1TraceID TraceID = [16]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x1}
var _2TraceID TraceID = [16]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x2}
var _10F0TraceID TraceID = [16]byte{0x09, 0x87, 0x65, 0x43, 0x21, 0xFF, 0xFF, 0xFF, 0, 0, 0, 0, 0, 0, 0, 0}
var _1SpanID SpanID = [8]byte{0, 0, 0, 0, 0, 0, 0, 1}
var _2SpanID SpanID = [8]byte{0, 0, 0, 0, 0, 0, 0, 2}
var _16SpanID SpanID = [8]byte{0, 0, 0, 0, 0, 0x12, 0x34, 0x56}

func TestMOTracer_Start(t *testing.T) {
	if runtime.GOOS == `linux` {
		t.Skip()
		return
	}
	type fields struct {
		Enable bool
	}
	type args struct {
		ctx  context.Context
		name string
		opts []SpanOption
	}
	rootCtx := ContextWithSpanContext(context.Background(), SpanContextWithIDs(_1TraceID, _1SpanID))
	stmCtx := ContextWithSpanContext(context.Background(), SpanContextWithID(_1TraceID, SpanKindStatement))
	dAtA := make([]byte, 24)
	span := SpanFromContext(stmCtx)
	c := span.SpanContext()
	cnt, err := c.MarshalTo(dAtA)
	require.Nil(t, err)
	require.Equal(t, 24, cnt)
	sc := &SpanContext{}
	err = sc.Unmarshal(dAtA)
	require.Nil(t, err)
	remoteCtx := ContextWithSpanContext(context.Background(), *sc)
	tests := []struct {
		name             string
		fields           fields
		args             args
		wantNewRoot      bool
		wantTraceId      TraceID
		wantParentSpanId SpanID
		wantKind         SpanKind
	}{
		{
			name:             "normal",
			fields:           fields{Enable: true},
			args:             args{ctx: rootCtx, name: "normal", opts: []SpanOption{}},
			wantNewRoot:      false,
			wantTraceId:      _1TraceID,
			wantParentSpanId: _1SpanID,
			wantKind:         SpanKindInternal,
		},
		{
			name:             "newRoot",
			fields:           fields{Enable: true},
			args:             args{ctx: rootCtx, name: "newRoot", opts: []SpanOption{WithNewRoot(true)}},
			wantNewRoot:      true,
			wantTraceId:      _1TraceID,
			wantParentSpanId: _1SpanID,
			wantKind:         SpanKindInternal,
		},
		{
			name:             "statement",
			fields:           fields{Enable: true},
			args:             args{ctx: stmCtx, name: "newStmt", opts: []SpanOption{}},
			wantNewRoot:      false,
			wantTraceId:      _1TraceID,
			wantParentSpanId: nilSpanID,
			wantKind:         SpanKindStatement,
		},
		{
			name:             "empty",
			fields:           fields{Enable: true},
			args:             args{ctx: context.Background(), name: "backgroundCtx", opts: []SpanOption{}},
			wantNewRoot:      true,
			wantTraceId:      nilTraceID,
			wantParentSpanId: _1SpanID,
			wantKind:         SpanKindInternal,
		},
		{
			name:             "remote",
			fields:           fields{Enable: true},
			args:             args{ctx: remoteCtx, name: "remoteCtx", opts: []SpanOption{}},
			wantNewRoot:      false,
			wantTraceId:      _1TraceID,
			wantParentSpanId: nilSpanID,
			wantKind:         SpanKindRemote,
		},
	}
	tracer := &MOTracer{
		TracerConfig: TracerConfig{Name: "motrace_test"},
		provider:     defaultMOTracerProvider(),
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t1 *testing.T) {
			tracer.provider.enable = tt.fields.Enable
			newCtx, span := tracer.Start(tt.args.ctx, tt.args.name, tt.args.opts...)
			if !tt.wantNewRoot {
				require.Equal(t1, tt.wantTraceId, span.SpanContext().TraceID)
				require.Equal(t1, tt.wantParentSpanId, span.ParentSpanContext().SpanID)
				require.Equal(t1, tt.wantParentSpanId, SpanFromContext(newCtx).ParentSpanContext().SpanID)
			} else {
				require.NotEqualf(t1, tt.wantTraceId, span.SpanContext().TraceID, "want %s, but got %s", tt.wantTraceId.String(), span.SpanContext().TraceID.String())
				require.NotEqual(t1, tt.wantParentSpanId, span.ParentSpanContext().SpanID)
				require.NotEqual(t1, tt.wantParentSpanId, SpanFromContext(newCtx).ParentSpanContext().SpanID)
			}
			require.Equal(t1, tt.wantKind, SpanFromContext(newCtx).ParentSpanContext().Kind)
			require.Equal(t1, span, SpanFromContext(newCtx))
		})
	}
}

func TestSpanContext_MarshalTo(t *testing.T) {
	type fields struct {
		TraceID TraceID
		SpanID  SpanID
	}
	type args struct {
		dAtA []byte
	}
	tests := []struct {
		name      string
		fields    fields
		args      args
		want      int
		wantBytes []byte
	}{
		{
			name: "normal",
			fields: fields{
				TraceID: nilTraceID,
				SpanID:  _16SpanID,
			},
			args: args{dAtA: make([]byte, 24)},
			want: 24,
			//                1  2  3  4  5  6  7  8, 1  2  3  4  5  6  7  8--1  2  3  4  5  6     7     8
			wantBytes: []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x12, 0x34, 0x56},
		},
		{
			name: "not-zero",
			fields: fields{
				TraceID: _10F0TraceID,
				SpanID:  _16SpanID,
			},
			args: args{dAtA: make([]byte, 24)},
			want: 24,
			//                1     2     3     4     5     6     7     8,    1  2  3  4  5  6  7  8--1  2  3  4  5  6     7     8
			wantBytes: []byte{0x09, 0x87, 0x65, 0x43, 0x21, 0xff, 0xff, 0xff, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x12, 0x34, 0x56},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &SpanContext{
				TraceID: tt.fields.TraceID,
				SpanID:  tt.fields.SpanID,
				Kind:    SpanKindRemote,
			}
			got, err := c.MarshalTo(tt.args.dAtA)
			require.Equal(t, nil, err)
			require.Equal(t, got, tt.want)
			require.Equal(t, tt.wantBytes, tt.args.dAtA)
			newC := &SpanContext{}
			err = newC.Unmarshal(tt.args.dAtA)
			require.Equal(t, nil, err)
			require.Equal(t, c, newC)
			require.Equal(t, tt.fields.TraceID, newC.TraceID)
			require.Equal(t, tt.fields.SpanID, newC.SpanID)
		})
	}
}
