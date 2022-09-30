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
)

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
			t:    nilTraceID,
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
				TraceID: nilTraceID,
				SpanID:  _1SpanID,
			},
			want: false,
		},
		{
			name: "nilSpanID",
			fields: fields{
				TraceID: _1TraceID,
				SpanID:  nilSpanID,
			},
			want: false,
		},
		{
			name: "nil",
			fields: fields{
				TraceID: nilTraceID,
				SpanID:  nilSpanID,
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
