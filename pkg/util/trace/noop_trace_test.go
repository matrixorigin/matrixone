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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type sequentialIDGenerator struct {
	next byte
}

func (g *sequentialIDGenerator) NewIDs() (TraceID, SpanID) {
	g.next++
	var traceID TraceID
	traceID[len(traceID)-1] = g.next
	var spanID SpanID
	spanID[len(spanID)-1] = g.next
	return traceID, spanID
}

func (g *sequentialIDGenerator) NewSpanID() SpanID {
	g.next++
	var spanID SpanID
	spanID[len(spanID)-1] = g.next
	return spanID
}

func Test_nonRecordingSpan_ParentSpanContext(t *testing.T) {
	type fields struct {
		NoopSpan NoopSpan
		sc       SpanContext
	}
	tests := []struct {
		name   string
		fields fields
		want   SpanContext
	}{
		{
			name:   "normal",
			fields: fields{NoopSpan: NoopSpan{}, sc: SpanContext{}},
			want:   SpanContext{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &NonRecordingSpan{
				NoopSpan: tt.fields.NoopSpan,
				sc:       tt.fields.sc,
			}
			require.Equal(t, tt.want, s.ParentSpanContext())
		})
	}
}

func Test_nonRecordingSpan_SpanContext(t *testing.T) {
	type fields struct {
		NoopSpan NoopSpan
		sc       SpanContext
	}
	tests := []struct {
		name   string
		fields fields
		want   SpanContext
	}{
		{
			name:   "normal",
			fields: fields{NoopSpan: NoopSpan{}, sc: SpanContext{}},
			want:   SpanContext{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &NonRecordingSpan{
				NoopSpan: tt.fields.NoopSpan,
				sc:       tt.fields.sc,
			}
			assert.Equalf(t, tt.want, s.SpanContext(), "SpanContext()")
		})
	}
}

func Test_NoopSpan_End(t *testing.T) {
	type args struct {
		in0 []SpanEndOption
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "normal",
			args: args{in0: []SpanEndOption{}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			no := NoopSpan{}
			no.End(tt.args.in0...)
		})
	}
}

func Test_NoopSpan_ParentSpanContext(t *testing.T) {
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
			no := NoopSpan{}
			assert.Equalf(t, tt.want, no.ParentSpanContext(), "ParentSpanContext()")
		})
	}
}

func Test_NoopSpan_SetName(t *testing.T) {
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
			no := NoopSpan{}
			no.SetName(tt.args.in0)
		})
	}
}

func Test_NoopSpan_SpanContext(t *testing.T) {
	tests := []struct {
		name string
		want SpanContext
	}{
		{name: "normal", want: SpanContext{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			no := NoopSpan{}
			assert.Equalf(t, tt.want, no.SpanContext(), "SpanContext()")
		})
	}
}

func Test_NoopSpan_TracerProvider(t *testing.T) {
	tests := []struct {
		name string
		want TracerProvider
	}{
		{
			name: "normal",
			want: noopTracerProvider{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			no := NoopSpan{}
			assert.Equalf(t, tt.want, no.TracerProvider(), "TracerProvider()")
		})
	}
}

func Test_NoopTracer_Start(t1 *testing.T) {
	type args struct {
		ctx   context.Context
		name  string
		in2   []SpanStartOption
		endIn []SpanEndOption
	}
	tests := []struct {
		name  string
		args  args
		want  context.Context
		want1 Span
	}{
		{
			name: "with_options",
			args: args{
				ctx:   context.Background(),
				name:  "NoopTracer_Start",
				in2:   []SpanStartOption{WithNewRoot(true)},
				endIn: []SpanEndOption{},
			},
			// NoopTracer returns ctx and NoopSpan directly without any allocation
			// All options are ignored since NoopTracer performs no operations
			want:  context.Background(),
			want1: NoopSpan{},
		},
		{
			name: "empty_options",
			args: args{
				ctx:   context.Background(),
				name:  "NoopTracer_Start",
				in2:   []SpanStartOption{},
				endIn: []SpanEndOption{},
			},
			// NoopTracer returns ctx and NoopSpan directly without any allocation
			want:  context.Background(),
			want1: NoopSpan{},
		},
		{
			name: "with_existing_span",
			args: args{
				ctx:   ContextWithSpan(context.Background(), &NonRecordingSpan{}),
				name:  "NoopTracer_Start",
				in2:   []SpanStartOption{WithNewRoot(true)},
				endIn: []SpanEndOption{},
			},
			// NoopTracer returns original ctx and NoopSpan directly
			// NoopTracer is only used when trace is disabled, so empty SpanContext is expected
			want:  ContextWithSpan(context.Background(), &NonRecordingSpan{}),
			want1: NoopSpan{},
		},
	}
	for _, tt := range tests {
		t1.Run(tt.name, func(t1 *testing.T) {
			t := NoopTracer{}
			got, got1 := t.Start(tt.args.ctx, tt.args.name, tt.args.in2...)
			assert.Equalf(t1, tt.want, got, "Start(%v, %v, %v)", tt.args.ctx, tt.args.name, tt.args.in2)
			assert.Equalf(t1, tt.want1, got1, "Start(%v, %v, %v)", tt.args.ctx, tt.args.name, tt.args.in2)
			got1.End(tt.args.endIn...)
		})
	}
}

func TestNonRecordingTracerPreservesTraceContext(t *testing.T) {
	tracer := NewNonRecordingTracer(&sequentialIDGenerator{})

	rootCtx, root := tracer.Start(nil, "root", WithKind(SpanKindSession))
	rootSC := root.SpanContext()
	require.False(t, rootSC.IsEmpty())
	require.Equal(t, SpanKindSession, rootSC.Kind)
	require.Equal(t, SpanContext{}, root.ParentSpanContext())
	require.Equal(t, rootSC, SpanFromContext(rootCtx).SpanContext())

	childCtx, child := tracer.Start(rootCtx, "child")
	childSC := child.SpanContext()
	require.Equal(t, rootSC.TraceID, childSC.TraceID)
	require.NotEqual(t, rootSC.SpanID, childSC.SpanID)
	require.Equal(t, rootSC, child.ParentSpanContext())
	require.Equal(t, childSC, SpanFromContext(childCtx).SpanContext())

	newRootCtx, newRoot := tracer.Start(childCtx, "new-root", WithNewRoot(true))
	newRootSC := newRoot.SpanContext()
	require.NotEqual(t, childSC.TraceID, newRootSC.TraceID)
	require.Equal(t, SpanContext{}, newRoot.ParentSpanContext())
	require.Equal(t, newRootSC, SpanFromContext(newRootCtx).SpanContext())

	root.End()
	child.End()
	newRoot.End()
	require.False(t, tracer.IsEnable())
}

func TestNonRecordingTracerKeepsRetiredControlledKindsDisabled(t *testing.T) {
	tracer := NewNonRecordingTracer(&sequentialIDGenerator{})
	rootCtx, root := tracer.Start(context.Background(), "root", WithNewRoot(true))
	rootSC := root.SpanContext()
	require.False(t, rootSC.IsEmpty())
	require.Equal(t, SpanKindInternal, rootSC.Kind)

	for _, kind := range []SpanKind{
		SpanKindStatement,
		SpanKindRemoteFSVis,
		SpanKindLocalFSVis,
		SpanKindTNRPCHandle,
	} {
		t.Run(kind.String(), func(t *testing.T) {
			gotCtx, gotSpan := tracer.Start(rootCtx, "retired-controlled", WithKind(kind))
			require.Equal(t, rootCtx, gotCtx)
			require.IsType(t, NoopSpan{}, gotSpan)
			require.Equal(t, rootSC, SpanFromContext(gotCtx).SpanContext())
		})
	}

	gotCtx, gotSpan := tracer.Start(nil, "retired-controlled", WithKind(SpanKindStatement))
	require.Equal(t, context.Background(), gotCtx)
	require.IsType(t, NoopSpan{}, gotSpan)
}

func TestNonRecordingTracerRetiredKindOptionsUseGeneralPath(t *testing.T) {
	tracer := NewNonRecordingTracer(&sequentialIDGenerator{})
	ctx := context.Background()

	t.Run("single custom option", func(t *testing.T) {
		applied := false
		option := spanOptionFunc(func(cfg *SpanConfig) {
			applied = true
			cfg.Kind = SpanKindStatement
		})

		gotCtx, gotSpan := tracer.Start(ctx, "retired-controlled", option)
		require.True(t, applied)
		require.Equal(t, ctx, gotCtx)
		require.IsType(t, NoopSpan{}, gotSpan)
	})

	t.Run("additional option", func(t *testing.T) {
		applied := false
		option := spanOptionFunc(func(*SpanConfig) {
			applied = true
		})

		gotCtx, gotSpan := tracer.Start(ctx, "retired-controlled",
			WithKind(SpanKindStatement), option)
		require.True(t, applied)
		require.Equal(t, ctx, gotCtx)
		require.IsType(t, NoopSpan{}, gotSpan)
	})

	t.Run("later non-retired kind wins", func(t *testing.T) {
		gotCtx, gotSpan := tracer.Start(ctx, "non-retired",
			WithKind(SpanKindStatement), WithKind(SpanKindSession))
		require.IsType(t, &NonRecordingSpan{}, gotSpan)
		require.Equal(t, SpanKindSession, gotSpan.SpanContext().Kind)
		require.Equal(t, gotSpan.SpanContext(), SpanFromContext(gotCtx).SpanContext())
	})

	t.Run("later retired kind wins", func(t *testing.T) {
		gotCtx, gotSpan := tracer.Start(ctx, "retired-controlled",
			WithKind(SpanKindSession), WithKind(SpanKindStatement))
		require.Equal(t, ctx, gotCtx)
		require.IsType(t, NoopSpan{}, gotSpan)
	})
}

func TestGenerateUsesNonRecordingTraceContext(t *testing.T) {
	previous := DefaultTracer()
	SetDefaultTracer(NewNonRecordingTracer(&sequentialIDGenerator{}))
	defer SetDefaultTracer(previous)

	first := SpanFromContext(Generate(context.Background())).SpanContext()
	second := SpanFromContext(Generate(context.Background())).SpanContext()
	require.False(t, first.IsEmpty())
	require.False(t, second.IsEmpty())
	require.NotEqual(t, first.TraceID, second.TraceID)
	require.NotEqual(t, first.SpanID, second.SpanID)
}

func TestNonRecordingTracerDebugRemainsNoop(t *testing.T) {
	tracer := NewNonRecordingTracer(&sequentialIDGenerator{})
	ctx := context.Background()

	gotCtx, span := tracer.Debug(ctx, "debug")
	require.Equal(t, ctx, gotCtx)
	require.IsType(t, NoopSpan{}, span)
}

func TestNewNonRecordingTracerWithNilGeneratorIsNoop(t *testing.T) {
	require.IsType(t, NoopTracer{}, NewNonRecordingTracer(nil))
}

// BenchmarkNoopTracer_Start verifies that NoopTracer.Start has zero allocation overhead.
func BenchmarkNoopTracer_Start(b *testing.B) {
	tracer := NoopTracer{}
	ctx := context.Background()
	opts := []SpanStartOption{WithNewRoot(true)}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = tracer.Start(ctx, "benchmark", opts...)
	}
}

func BenchmarkNonRecordingTracer_StartRetiredKind(b *testing.B) {
	tracer := NewNonRecordingTracer(&sequentialIDGenerator{})
	ctx := context.Background()
	option := WithKind(SpanKindStatement)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = tracer.Start(ctx, "benchmark", option)
	}
}

type benchmarkKindOption SpanKind

func (k benchmarkKindOption) ApplySpanStart(cfg *SpanConfig) {
	cfg.Kind = SpanKind(k)
}

func BenchmarkNonRecordingTracer_StartRetiredKindGeneralPath(b *testing.B) {
	tracer := NewNonRecordingTracer(&sequentialIDGenerator{})
	ctx := context.Background()
	option := benchmarkKindOption(SpanKindStatement)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = tracer.Start(ctx, "benchmark", option)
	}
}

// BenchmarkNoopTracer_Debug verifies that NoopTracer.Debug has zero allocation overhead.
func BenchmarkNoopTracer_Debug(b *testing.B) {
	tracer := NoopTracer{}
	ctx := context.Background()
	opts := []SpanStartOption{WithNewRoot(true)}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = tracer.Debug(ctx, "benchmark", opts...)
	}
}
