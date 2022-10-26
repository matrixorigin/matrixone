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
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/util/export"
	"sync"
	"time"
	"unsafe"
)

// TracerConfig is a group of options for a Tracer.
type TracerConfig struct {
	Name string
}

// TracerOption applies an option to a TracerConfig.
type TracerOption interface {
	apply(*TracerConfig)
}

var _ TracerOption = tracerOptionFunc(nil)

type tracerOptionFunc func(*TracerConfig)

func (f tracerOptionFunc) apply(cfg *TracerConfig) {
	f(cfg)
}

const (
	// FlagsSampled is a bitmask with the sampled bit set. A SpanContext
	// with the sampling bit set means the span is sampled.
	FlagsSampled = TraceFlags(0x01)
)

// TraceFlags contains flags that can be set on a SpanContext.
type TraceFlags byte //nolint:revive // revive complains about stutter of `trace.TraceFlags`.

// IsSampled returns if the sampling bit is set in the TraceFlags.
func (tf TraceFlags) IsSampled() bool {
	return tf&FlagsSampled == FlagsSampled
}

// WithSampled sets the sampling bit in a new copy of the TraceFlags.
func (tf TraceFlags) WithSampled(sampled bool) TraceFlags { // nolint:revive  // sampled is not a control flag.
	if sampled {
		return tf | FlagsSampled
	}

	return tf &^ FlagsSampled
}

// String returns the hex string representation form of TraceFlags.
func (tf TraceFlags) String() string {
	return hex.EncodeToString([]byte{byte(tf)}[:])
}

var _ Tracer = &MOTracer{}

// MOTracer is the creator of Spans.
type MOTracer struct {
	TracerConfig
	provider *MOTracerProvider
}

func (t *MOTracer) Start(ctx context.Context, name string, opts ...SpanOption) (context.Context, Span) {
	span := newMOSpan()
	span.init(name, opts...)
	span.tracer = t

	parent := SpanFromContext(ctx)

	if span.NewRoot {
		span.TraceID, span.SpanID = t.provider.idGenerator.NewIDs()
		span.parent = noopSpan{}
	} else if span.SpanID.IsZero() {
		span.TraceID, span.SpanID = parent.SpanContext().TraceID, t.provider.idGenerator.NewSpanID()
		span.parent = parent
	} else {
		span.parent = parent
	}

	return ContextWithSpan(ctx, span), span
}

var _ Span = (*MOSpan)(nil)
var _ IBuffer2SqlItem = (*MOSpan)(nil)
var _ CsvFields = (*MOSpan)(nil)

type MOSpan struct {
	SpanConfig
	Name      bytes.Buffer `json:"name"`
	StartTime time.Time    `json:"start_time"`
	EndTime   time.Time    `jons:"end_time"`
	Duration  uint64       `json:"duration"`

	tracer *MOTracer `json:"-"`
}

var spanPool = &sync.Pool{New: func() any {
	return &MOSpan{}
}}

func newMOSpan() *MOSpan {
	return spanPool.Get().(*MOSpan)
}

func (s *MOSpan) init(name string, opts ...SpanOption) {
	s.Name.WriteString(name)
	s.StartTime = time.Now()
	for _, opt := range opts {
		opt.applySpanStart(&s.SpanConfig)
	}
}

func (s *MOSpan) Size() int64 {
	return int64(unsafe.Sizeof(*s)) + int64(s.Name.Cap())
}

var zeroTime = time.Time{}

func (s *MOSpan) Free() {
	s.Name.Reset()
	s.tracer = nil
	s.StartTime = zeroTime
	s.EndTime = zeroTime
	spanPool.Put(s)
}

func (s *MOSpan) GetName() string {
	return spanView.OriginTable.GetName()
}

func (s *MOSpan) GetRow() *export.Row { return spanView.OriginTable.GetRow() }

func (s *MOSpan) CsvFields(row *export.Row) []string {
	row.Reset()
	row.SetColumnVal(rawItemCol, spanView.Table)
	row.SetColumnVal(spanIDCol, s.SpanID.String())
	row.SetColumnVal(stmtIDCol, s.TraceID.String())
	row.SetColumnVal(parentSpanIDCol, s.parent.SpanContext().SpanID.String())
	row.SetColumnVal(nodeUUIDCol, GetNodeResource().NodeUuid)
	row.SetColumnVal(nodeTypeCol, GetNodeResource().NodeType)
	row.SetColumnVal(spanNameCol, s.Name.String())
	row.SetColumnVal(startTimeCol, time2DatetimeString(s.StartTime))
	row.SetColumnVal(endTimeCol, time2DatetimeString(s.EndTime))
	row.SetColumnVal(durationCol, fmt.Sprintf("%d", s.EndTime.Sub(s.StartTime))) // Duration
	row.SetColumnVal(resourceCol, s.tracer.provider.resource.String())
	return row.ToStrings()
}

func (s *MOSpan) End(options ...SpanEndOption) {
	for _, opt := range options {
		opt.applySpanEnd(&s.SpanConfig)
	}
	if s.EndTime.IsZero() {
		s.EndTime = time.Now()
	}
	for _, sp := range s.tracer.provider.spanProcessors {
		sp.OnEnd(s)
	}
}

func (s *MOSpan) SpanContext() SpanContext {
	return s.SpanConfig.SpanContext
}

func (s *MOSpan) ParentSpanContext() SpanContext {
	return s.SpanConfig.parent.SpanContext()
}
