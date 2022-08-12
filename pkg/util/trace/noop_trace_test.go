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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_nonRecordingSpan_ParentSpanContext(t *testing.T) {
	type fields struct {
		noopSpan noopSpan
		sc       SpanContext
	}
	tests := []struct {
		name   string
		fields fields
		want   SpanContext
	}{
		{
			name:   "normal",
			fields: fields{noopSpan: noopSpan{}, sc: SpanContext{}},
			want:   SpanContext{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &nonRecordingSpan{
				noopSpan: tt.fields.noopSpan,
				sc:       tt.fields.sc,
			}
			require.Equal(t, tt.want, s.ParentSpanContext())
		})
	}
}

func Test_nonRecordingSpan_SpanContext(t *testing.T) {
	type fields struct {
		noopSpan noopSpan
		sc       SpanContext
	}
	tests := []struct {
		name   string
		fields fields
		want   SpanContext
	}{
		{
			name:   "normal",
			fields: fields{noopSpan: noopSpan{}, sc: SpanContext{}},
			want:   SpanContext{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &nonRecordingSpan{
				noopSpan: tt.fields.noopSpan,
				sc:       tt.fields.sc,
			}
			assert.Equalf(t, tt.want, s.SpanContext(), "SpanContext()")
		})
	}
}

func Test_noopSpan_End(t *testing.T) {
	type args struct {
		in0 []SpanEndOption
	}
	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			no := noopSpan{}
			no.End(tt.args.in0...)
		})
	}
}

func Test_noopSpan_ParentSpanContext(t *testing.T) {
	tests := []struct {
		name string
		want SpanContext
	}{
		{
			name: "normal",
			want: SpanContext{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			no := noopSpan{}
			assert.Equalf(t, tt.want, no.ParentSpanContext(), "ParentSpanContext()")
		})
	}
}

func Test_noopSpan_SetName(t *testing.T) {
	type args struct {
		in0 string
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "normal",
			args: args{in0: "name"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			no := noopSpan{}
			no.SetName(tt.args.in0)
		})
	}
}

func Test_noopSpan_SpanContext(t *testing.T) {
	tests := []struct {
		name string
		want SpanContext
	}{
		{name: "normal", want: SpanContext{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			no := noopSpan{}
			assert.Equalf(t, tt.want, no.SpanContext(), "SpanContext()")
		})
	}
}

func Test_noopSpan_TracerProvider(t *testing.T) {
	tests := []struct {
		name string
		want TracerProvider
	}{
		{
			name: "normal",
			want: gTracerProvider,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			no := noopSpan{}
			assert.Equalf(t, tt.want, no.TracerProvider(), "TracerProvider()")
		})
	}
}

func Test_noopTracer_Start(t1 *testing.T) {
	type args struct {
		ctx   context.Context
		name  string
		in2   []SpanOption
		endIn []SpanEndOption
	}
	tests := []struct {
		name  string
		args  args
		want  context.Context
		want1 Span
	}{
		{
			name: "normal",
			args: args{
				ctx:   context.Background(),
				name:  "noopTracer_Start",
				in2:   []SpanOption{WithNewRoot(true)},
				endIn: []SpanEndOption{},
			},
			want:  ContextWithSpan(context.Background(), noopSpan{}),
			want1: noopSpan{},
		},
		{
			name: "nonRecording",
			args: args{
				ctx:   ContextWithSpan(context.Background(), &nonRecordingSpan{}),
				name:  "noopTracer_Start",
				in2:   []SpanOption{WithNewRoot(true)},
				endIn: []SpanEndOption{},
			},
			want:  ContextWithSpan(ContextWithSpan(context.Background(), &nonRecordingSpan{}), noopSpan{}),
			want1: noopSpan{},
		},
	}
	for _, tt := range tests {
		t1.Run(tt.name, func(t1 *testing.T) {
			t := noopTracer{}
			got, got1 := t.Start(tt.args.ctx, tt.args.name, tt.args.in2...)
			assert.Equalf(t1, tt.want, got, "Start(%v, %v, %v)", tt.args.ctx, tt.args.name, tt.args.in2)
			assert.Equalf(t1, tt.want1, got1, "Start(%v, %v, %v)", tt.args.ctx, tt.args.name, tt.args.in2)
			got1.End(tt.args.endIn...)
		})
	}
}
