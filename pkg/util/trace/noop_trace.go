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
)

var _ Tracer = &noopTracer{}
var _ Span = &noopSpan{}

// noopTracer is an implementation of Tracer that preforms no operations.
type noopTracer struct{}

// Start carries forward a non-recording Span, if one is present in the context, otherwise it
// creates a no-op Span.
func (t noopTracer) Start(ctx context.Context, name string, _ ...SpanOption) (context.Context, Span) {
	span := SpanFromContext(ctx)
	if _, ok := span.(noopSpan); !ok {
		// span is likely already a noopSpan, but let's be sure
		span = noopSpan{}
	}
	return ContextWithSpan(ctx, span), span
}

// noopSpan is an implementation of Span that preforms no operations.
type noopSpan struct{}

var _ Span = noopSpan{}

// SpanContext returns an empty span context.
func (noopSpan) SpanContext() SpanContext { return SpanContext{} }

func (noopSpan) ParentSpanContext() SpanContext { return SpanContext{} }

// End does nothing.
func (noopSpan) End(...SpanEndOption) {}

// SetName does nothing.
func (noopSpan) SetName(string) {}

// TracerProvider returns a no-op TracerProvider.
func (noopSpan) TracerProvider() TracerProvider { return gTracerProvider }

// nonRecordingSpan keep SpanContext{TraceID, SpanID}
type nonRecordingSpan struct {
	noopSpan
	sc SpanContext
}

func (s *nonRecordingSpan) SpanContext() SpanContext       { return s.sc }
func (s *nonRecordingSpan) ParentSpanContext() SpanContext { return SpanContext{} }
