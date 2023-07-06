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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

var _1TraceID TraceID = [16]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x1}
var _1SpanID SpanID = [8]byte{0, 0, 0, 0, 0, 0, 0, 1}

func TestTraceID_IsZero(t *testing.T) {
	tests := []struct {
		name string
		t    TraceID
		want bool
	}{
		{
			name: "normal",
			t:    _1TraceID,
			want: false,
		},
		{
			name: "nil",
			t:    NilTraceID,
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, tt.t.IsZero(), "IsZero()")
		})
	}
}

func TestSpanID_SetByUUID_IsZero(t *testing.T) {
	type args struct {
		id string
	}
	tests := []struct {
		name     string
		s        SpanID
		args     args
		wantZero bool
	}{
		{
			name:     "normal",
			args:     args{id: "node_uuid"},
			wantZero: false,
		},
		{
			name:     "short",
			args:     args{id: "1234"},
			wantZero: false,
		},
		{
			name:     "nil",
			args:     args{id: "00000000-0000-0000-0000-000000000000"},
			wantZero: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.s.SetByUUID(tt.args.id)
			t.Logf("SpanID: %s", tt.s.String())
			require.Equal(t, tt.wantZero, tt.s.IsZero())
		})
	}
}

func TestSpanContext_IsEmpty(t *testing.T) {
	type fields struct {
		TraceID TraceID
		SpanID  SpanID
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		{
			name: "normal",
			fields: fields{
				TraceID: _1TraceID,
				SpanID:  _1SpanID,
			},
			want: false,
		},
		{
			name: "nilTraceID",
			fields: fields{
				TraceID: NilTraceID,
				SpanID:  _1SpanID,
			},
			want: false,
		},
		{
			name: "nilSpanID",
			fields: fields{
				TraceID: _1TraceID,
				SpanID:  NilSpanID,
			},
			want: false,
		},
		{
			name: "nil",
			fields: fields{
				TraceID: NilTraceID,
				SpanID:  NilSpanID,
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &SpanContext{
				TraceID: tt.fields.TraceID,
				SpanID:  tt.fields.SpanID,
			}
			assert.Equalf(t, tt.want, c.IsEmpty(), "IsEmpty()")
		})
	}
}

func TestSpanConfig_ProfileRuntime(t *testing.T) {
	type fields struct {
		goroutine    bool
		heap         bool
		alloc        bool
		threadCreate bool
		block        bool
		mutex        bool
	}
	tests := []struct {
		name    string
		fields  fields
		prepare func(*SpanConfig)
		need    bool
	}{
		{
			name:   "goroutinue",
			fields: fields{goroutine: true, heap: false, alloc: false, threadCreate: false, block: false, mutex: false},
			need:   true,
		},
		{
			name:   "heap",
			fields: fields{goroutine: false, heap: true, alloc: false, threadCreate: false, block: false, mutex: false},
			need:   true,
		},
		{
			name:   "alloc",
			fields: fields{goroutine: false, heap: false, alloc: true, threadCreate: false, block: false, mutex: false},
			need:   true,
		},
		{
			name:   "threadcreate",
			fields: fields{goroutine: false, heap: false, alloc: false, threadCreate: true, block: false, mutex: false},
			need:   true,
		},
		{
			name:   "block",
			fields: fields{goroutine: false, heap: false, alloc: false, threadCreate: false, block: true, mutex: false},
			need:   true,
		},
		{
			name:   "mutex",
			fields: fields{goroutine: false, heap: false, alloc: false, threadCreate: false, block: false, mutex: true},
			need:   true,
		},
		{
			name:   "cpu",
			fields: fields{goroutine: false, heap: false, alloc: false, threadCreate: false, block: false, mutex: false},
			prepare: func(c *SpanConfig) {
				WithProfileCpuSecs(time.Millisecond).ApplySpanStart(c)
			},
			need: true,
		},
		{
			name:   "trace",
			fields: fields{goroutine: false, heap: false, alloc: false, threadCreate: false, block: false, mutex: false},
			prepare: func(c *SpanConfig) {
				WithProfileTraceSecs(time.Millisecond).ApplySpanStart(c)
			},
			need: true,
		},
		{
			name:   "trace_cpu",
			fields: fields{goroutine: false, heap: false, alloc: false, threadCreate: false, block: false, mutex: false},
			prepare: func(c *SpanConfig) {
				WithProfileTraceSecs(time.Millisecond).ApplySpanStart(c)
				WithProfileCpuSecs(time.Millisecond).ApplySpanStart(c)
			},
			need: true,
		},
		{
			name:    "no_need",
			fields:  fields{goroutine: false, heap: false, alloc: false, threadCreate: false, block: false, mutex: false},
			prepare: nil,
			need:    false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &SpanConfig{}
			if tt.fields.goroutine {
				WithProfileGoroutine().ApplySpanStart(c)
			}
			if tt.fields.heap {
				WithProfileHeap().ApplySpanStart(c)
			}
			if tt.fields.alloc {
				WithProfileAllocs().ApplySpanStart(c)
			}
			if tt.fields.threadCreate {
				WithProfileThreadCreate().ApplySpanStart(c)
			}
			if tt.fields.block {
				WithProfileBlock().ApplySpanStart(c)
			}
			if tt.fields.mutex {
				WithProfileMutex().ApplySpanStart(c)
			}
			if tt.prepare != nil {
				tt.prepare(c)
			}
			assert.Equal(t, tt.fields.goroutine, c.ProfileGoroutine())
			assert.Equal(t, tt.fields.heap, c.ProfileHeap())
			assert.Equal(t, tt.fields.alloc, c.ProfileAllocs())
			assert.Equal(t, tt.fields.threadCreate, c.ProfileThreadCreate())
			assert.Equal(t, tt.fields.block, c.ProfileBlock())
			assert.Equal(t, tt.fields.mutex, c.ProfileMutex())
			assert.Equal(t, tt.need, c.NeedProfile())
		})
	}
}
