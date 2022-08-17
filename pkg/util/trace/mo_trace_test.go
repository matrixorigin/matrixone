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
	"github.com/stretchr/testify/require"
	"testing"
)

func TestMOTracer_Start(t1 *testing.T) {
	type fields struct {
		TracerConfig TracerConfig
	}
	type args struct {
		ctx  context.Context
		name string
		opts []SpanOption
	}
	rootCtx := ContextWithSpanContext(context.Background(), SpanContextWithIDs(1, 1))
	tests := []struct {
		name             string
		fields           fields
		args             args
		wantNewRoot      bool
		wantTraceId      TraceID
		wantParentSpanId SpanID
	}{
		{
			name: "normal",
			fields: fields{
				TracerConfig: TracerConfig{Name: "normal"},
			},
			args: args{
				ctx:  rootCtx,
				name: "normal",
				opts: []SpanOption{},
			},
			wantNewRoot:      false,
			wantTraceId:      1,
			wantParentSpanId: 1,
		},
		{
			name: "newRoot",
			fields: fields{
				TracerConfig: TracerConfig{Name: "newRoot"},
			},
			args: args{
				ctx:  rootCtx,
				name: "newRoot",
				opts: []SpanOption{WithNewRoot(true)},
			},
			wantNewRoot:      true,
			wantTraceId:      1,
			wantParentSpanId: 1,
		},
	}
	for _, tt := range tests {
		t1.Run(tt.name, func(t1 *testing.T) {
			t := &MOTracer{
				TracerConfig: tt.fields.TracerConfig,
				provider:     gTracerProvider,
			}
			newCtx, span := t.Start(tt.args.ctx, tt.args.name, tt.args.opts...)
			if !tt.wantNewRoot {
				require.Equal(t1, tt.wantTraceId, span.SpanContext().TraceID)
				require.Equal(t1, tt.wantParentSpanId, span.ParentSpanContext().SpanID)
				require.Equal(t1, tt.wantParentSpanId, SpanFromContext(newCtx).ParentSpanContext().SpanID)
			} else {
				require.NotEqual(t1, tt.wantTraceId, span.SpanContext().TraceID)
				require.NotEqual(t1, tt.wantParentSpanId, span.ParentSpanContext().SpanID)
				require.NotEqual(t1, tt.wantParentSpanId, SpanFromContext(newCtx).ParentSpanContext().SpanID)
			}
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
				TraceID: 0,
				SpanID:  0x123456,
			},
			args: args{dAtA: make([]byte, 16)},
			want: 16,
			//                1  2  3  4  5  6  7  8--1  2  3  4  5  6     7     8
			wantBytes: []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x12, 0x34, 0x56},
		},
		{
			name: "not-zero",
			fields: fields{
				TraceID: 0x0987654321FFFFFF,
				SpanID:  0x123456,
			},
			args: args{dAtA: make([]byte, 16)},
			want: 16,
			//                1  2  3  4  5  6  7  8--1  2  3  4  5  6     7     8
			wantBytes: []byte{0x09, 0x87, 0x65, 0x43, 0x21, 0xff, 0xff, 0xff, 0, 0, 0, 0, 0, 0x12, 0x34, 0x56},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &SpanContext{
				TraceID: tt.fields.TraceID,
				SpanID:  tt.fields.SpanID,
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
