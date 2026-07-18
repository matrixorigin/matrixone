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
	"go.uber.org/zap"
)

var _ TracerProvider = &noopTracerProvider{}
var _ Tracer = &NoopTracer{}
var _ Span = &NoopSpan{}

type noopTracerProvider struct{}

func (n noopTracerProvider) Tracer(string, ...TracerOption) Tracer {
	return NoopTracer{}
}

// NoopTracer is an implementation of Tracer that performs no operations.
// It should have ZERO allocation overhead.
// MatrixOne uses it before telemetry initialization and when tracing is fully
// disabled. Enabled telemetry uses NonRecordingTracer to retain correlation IDs.
type NoopTracer struct{}

// Start returns ctx and NoopSpan directly without any allocation.
// All parameters are ignored since NoopTracer performs no operations.
// SpanContext and RPC propagation remain available separately for logs,
// errors, and compatibility with existing wire formats.
func (t NoopTracer) Start(ctx context.Context, _ string, _ ...SpanStartOption) (context.Context, Span) {
	return ctx, NoopSpan{}
}

func (t NoopTracer) Debug(ctx context.Context, _ string, _ ...SpanStartOption) (context.Context, Span) {
	return ctx, NoopSpan{}
}

func (t NoopTracer) IsEnable(opts ...SpanStartOption) bool { return false }

// NoopSpan is an implementation of Span that preforms no operations.
type NoopSpan struct{}

var _ Span = NoopSpan{}

// SpanContext returns an empty span context.
func (NoopSpan) SpanContext() SpanContext { return SpanContext{} }

func (NoopSpan) ParentSpanContext() SpanContext { return SpanContext{} }

// End does nothing.
func (NoopSpan) End(...SpanEndOption) {}

func (NoopSpan) AddExtraFields(...zap.Field) {}

// SetName does nothing.
func (NoopSpan) SetName(string) {}

// TracerProvider returns a no-op TracerProvider.
func (NoopSpan) TracerProvider() TracerProvider { return noopTracerProvider{} }

// NonRecordingSpan keep SpanContext{TraceID, SpanID}
type NonRecordingSpan struct {
	NoopSpan
	sc     SpanContext
	parent SpanContext
}

func (s *NonRecordingSpan) SpanContext() SpanContext       { return s.sc }
func (s *NonRecordingSpan) ParentSpanContext() SpanContext { return s.parent }

// NonRecordingTracer preserves trace-context generation and propagation without
// creating, recording, profiling, or exporting Spans.
type NonRecordingTracer struct {
	idGenerator IDGenerator
}

var _ Tracer = &NonRecordingTracer{}

func NewNonRecordingTracer(idGenerator IDGenerator) Tracer {
	if idGenerator == nil {
		return NoopTracer{}
	}
	return &NonRecordingTracer{idGenerator: idGenerator}
}

func (t *NonRecordingTracer) Start(
	ctx context.Context,
	_ string,
	opts ...SpanStartOption,
) (context.Context, Span) {
	if ctx == nil {
		ctx = context.Background()
	}

	var cfg SpanConfig
	for _, opt := range opts {
		opt.ApplySpanStart(&cfg)
	}

	parent := SpanFromContext(ctx).SpanContext()
	spanContext := SpanContext{Kind: cfg.Kind}
	if cfg.NewRoot || parent.IsEmpty() {
		spanContext.TraceID, spanContext.SpanID = t.idGenerator.NewIDs()
		parent = SpanContext{}
	} else {
		spanContext.TraceID = parent.TraceID
		spanContext.SpanID = t.idGenerator.NewSpanID()
	}

	span := &NonRecordingSpan{sc: spanContext, parent: parent}
	return ContextWithSpan(ctx, span), span
}

// Debug remains a no-op because debug Span recording is retired and disabled by
// default. Existing trace context in ctx is preserved unchanged.
func (t *NonRecordingTracer) Debug(ctx context.Context, _ string, _ ...SpanStartOption) (context.Context, Span) {
	return ctx, NoopSpan{}
}

func (t *NonRecordingTracer) IsEnable(...SpanStartOption) bool { return false }
