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
	"github.com/matrixorigin/matrixone/pkg/util/batchpipe"
	"github.com/matrixorigin/matrixone/pkg/util/export"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/util"
)

var _ Tracer = &MOTracer{}

// MOTracer is the creator of Spans.
type MOTracer struct {
	TracerConfig
	ptrace   TracerProvider
	spanPool *sync.Pool
	node     *MONodeResource
}

func (t *MOTracer) Start(ctx context.Context, name string, opts ...SpanOption) (context.Context, Span) {
	span := t.spanPool.Get().(*MOSpan)
	opts = append(opts, withNodeResource(t.node))
	span.init(name, opts...)

	parent := SpanFromContext(ctx)

	if span.NewRoot {
		span.traceID, span.spanID = TraceID(util.Fastrand64()), SpanID(util.Fastrand64())
	} else {
		span.spanID = SpanID(util.Fastrand64())
		span.parent = parent
	}

	return ContextWithSpan(ctx, span), span
}

// SpanContext contains identifying trace information about a Span.
type SpanContext struct {
	traceID    TraceID
	spanID     SpanID
	traceFlags TraceFlags
	remote     bool
}

func SpanContextWithID(id TraceID) SpanContext {
	return SpanContext{
		traceID: id,
	}
}

type MONodeResource struct {
	NodeID   int64
	NodeType SpanKind
}

// SpanConfig is a group of options for a Span.
type SpanConfig struct {
	SpanContext

	// NewRoot identifies a Span as the root Span for a new trace. This is
	// commonly used when an existing trace crosses trust boundaries and the
	// remote parent span context should be ignored for security.
	NewRoot bool
	Node    *MONodeResource
	parent  Span
}

// SpanStartOption applies an option to a SpanConfig. These options are applicable
// only when the span is created.
type SpanStartOption interface {
	applySpanStart(*SpanConfig)
}

type SpanEndOption interface {
	applySpanEnd(*SpanConfig)
}

// SpanOption applies an option to a SpanConfig.
type SpanOption interface {
	SpanStartOption
	SpanEndOption
}

type spanOptionFunc func(*SpanConfig)

func (f spanOptionFunc) applySpanEnd(cfg *SpanConfig) {
	f(cfg)
}

func (f spanOptionFunc) applySpanStart(cfg *SpanConfig) {
	f(cfg)
}

func WithNewRoot(newRoot bool) spanOptionFunc {
	return spanOptionFunc(func(cfg *SpanConfig) {
		cfg.NewRoot = newRoot
	})
}

func withNodeResource(r *MONodeResource) spanOptionFunc {
	return spanOptionFunc(func(cfg *SpanConfig) {
		cfg.Node = r
	})
}

func WithRemote(remote bool) spanOptionFunc {
	return spanOptionFunc(func(cfg *SpanConfig) {
		cfg.remote = remote
	})
}

var _ Span = &MOSpan{}
var _ batchpipe.HasName = &MOSpan{}

type MOSpan struct {
	SpanConfig
	name        string
	startTimeNS util.TimeNano
	duration    util.TimeNano

	tracer *MOTracer
}

func (s *MOSpan) GetName() string {
	return "MOSpan"
}

func (s *MOSpan) init(name string, opts ...SpanOption) {
	s.name = name
	s.startTimeNS = util.NowNS()
	//s.duration = 0
	for _, opt := range opts {
		opt.applySpanStart(&s.SpanConfig)
	}
}

func (s *MOSpan) End(options ...SpanEndOption) {
	s.duration = util.NowNS() - s.startTimeNS

	//TODO implement me: cooperate with Exporter

	panic("implement me")
}

func (s *MOSpan) SpanContext() SpanContext {
	return s.SpanConfig.SpanContext
}

func (s *MOSpan) SetName(name string) {
	s.name = name
}

func (s MOSpan) GetItemType() string {
	return "MOSpan"
}

type TraceItemBuffer[T any, B any] struct {
	reminder export.Reminder
	mux      sync.Mutex
}
