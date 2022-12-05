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
	"github.com/matrixorigin/matrixone/pkg/util/batchpipe"
	"time"
)

type TracerProvider interface {
	Tracer(instrumentationName string, opts ...TracerOption) Tracer
}

type Tracer interface {
	// Start creates a span and a context.Context containing the newly-created span.
	Start(ctx context.Context, spanName string, opts ...SpanOption) (context.Context, Span)
	// Debug creates a span only with DebugMode
	Debug(ctx context.Context, spanName string, opts ...SpanOption) (context.Context, Span)
}

type Span interface {
	// End completes the Span. The Span is considered complete and ready to be
	// delivered through the rest of the telemetry pipeline after this method
	// is called. Therefore, updates to the Span are not allowed after this
	// method has been called.
	End(options ...SpanEndOption)

	// SpanContext returns the SpanContext of the Span. The returned SpanContext
	// is usable even after the End method has been called for the Span.
	SpanContext() SpanContext

	ParentSpanContext() SpanContext
}

type SpanProcessor interface {
	OnStart(ctx context.Context, s Span)
	OnEnd(s Span)
	Shutdown(ctx context.Context) error
}

type IDGenerator interface {
	NewIDs() (TraceID, SpanID)
	NewSpanID() SpanID
}

type PipeImpl batchpipe.PipeImpl[batchpipe.HasName, any]

type BatchProcessor interface {
	Collect(context.Context, batchpipe.HasName) error
	Start() bool
	Stop(graceful bool) error
	Register(name batchpipe.HasName, impl PipeImpl)
}

var _ BatchProcessor = &noopBatchProcessor{}

type noopBatchProcessor struct {
}

func (n noopBatchProcessor) Collect(context.Context, batchpipe.HasName) error { return nil }
func (n noopBatchProcessor) Start() bool                                      { return true }
func (n noopBatchProcessor) Stop(bool) error                                  { return nil }
func (n noopBatchProcessor) Register(batchpipe.HasName, PipeImpl)             {}

func GetGlobalBatchProcessor() BatchProcessor {
	return GetTracerProvider().batchProcessor
}

const timestampFormatter = "2006-01-02 15:04:05.000000"

func Time2DatetimeString(t time.Time) string {
	return t.Format(timestampFormatter)
}
