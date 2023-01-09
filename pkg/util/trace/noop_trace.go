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
)

var _ TracerProvider = &noopTracerProvider{}
var _ Tracer = &NoopTracer{}
var _ Span = &NoopSpan{}

type noopTracerProvider struct{}

func (n noopTracerProvider) Tracer(string, ...TracerOption) Tracer {
	return NoopTracer{}
}

// NoopTracer is an implementation of Tracer that preforms no operations.
type NoopTracer struct{}

// Start carries forward a non-recording Span, if one is present in the context, otherwise it
// creates a no-op Span.
func (t NoopTracer) Start(ctx context.Context, name string, _ ...SpanOption) (context.Context, Span) {
	span := SpanFromContext(ctx)
	if _, ok := span.(NoopSpan); !ok {
		// span is likely already a NoopSpan, but let's be sure
		span = NoopSpan{}
	}
	return ContextWithSpan(ctx, span), span
}

func (t NoopTracer) Debug(ctx context.Context, name string, opts ...SpanOption) (context.Context, Span) {
	return t.Start(ctx, name, opts...)
}

func (t NoopTracer) IsEnable() bool { return false }

// NoopSpan is an implementation of Span that preforms no operations.
type NoopSpan struct{}

var _ Span = NoopSpan{}

// SpanContext returns an empty span context.
func (NoopSpan) SpanContext() SpanContext { return SpanContext{} }

func (NoopSpan) ParentSpanContext() SpanContext { return SpanContext{} }

// End does nothing.
func (NoopSpan) End(...SpanEndOption) {}

// SetName does nothing.
func (NoopSpan) SetName(string) {}

// TracerProvider returns a no-op TracerProvider.
func (NoopSpan) TracerProvider() TracerProvider { return noopTracerProvider{} }

// NonRecordingSpan keep SpanContext{TraceID, SpanID}
type NonRecordingSpan struct {
	NoopSpan
	sc SpanContext
}

func (s *NonRecordingSpan) SpanContext() SpanContext       { return s.sc }
func (s *NonRecordingSpan) ParentSpanContext() SpanContext { return SpanContext{} }
