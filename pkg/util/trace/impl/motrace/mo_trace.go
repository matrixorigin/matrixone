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

package motrace

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"sync"
	"time"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/util/export/table"
)

//nolint:revive // revive complains about stutter of `trace.TraceFlags`.

var _ trace.Tracer = &MOTracer{}

// MOTracer is the creator of Spans.
type MOTracer struct {
	trace.TracerConfig
	provider *MOTracerProvider
}

func (t *MOTracer) Start(ctx context.Context, name string, opts ...trace.SpanOption) (context.Context, trace.Span) {
	if !t.IsEnable() {
		return ctx, trace.NoopSpan{}
	}
	span := newMOSpan()
	span.init(name, opts...)
	span.tracer = t

	parent := trace.SpanFromContext(ctx)
	psc := parent.SpanContext()

	if span.NewRoot || psc.IsEmpty() {
		span.TraceID, span.SpanID = t.provider.idGenerator.NewIDs()
		span.Parent = trace.NoopSpan{}
	} else {
		span.TraceID, span.SpanID, span.Kind = psc.TraceID, t.provider.idGenerator.NewSpanID(), psc.Kind
		span.Parent = parent
	}

	return trace.ContextWithSpan(ctx, span), span
}

func (t *MOTracer) Debug(ctx context.Context, name string, opts ...trace.SpanOption) (context.Context, trace.Span) {
	if !t.provider.debugMode {
		return ctx, trace.NoopSpan{}
	}
	return t.Start(ctx, name, opts...)
}

func (t *MOTracer) IsEnable() bool {
	return t.provider.IsEnable()
}

var _ trace.Span = (*MOSpan)(nil)

// MOSpan implement export.IBuffer2SqlItem and export.CsvFields
type MOSpan struct {
	trace.SpanConfig
	Name      string    `json:"name"`
	StartTime time.Time `json:"start_time"`
	EndTime   time.Time `jons:"end_time"`
	Duration  uint64    `json:"duration"`

	tracer *MOTracer `json:"-"`
}

var spanPool = &sync.Pool{New: func() any {
	return &MOSpan{}
}}

func newMOSpan() *MOSpan {
	return spanPool.Get().(*MOSpan)
}

func (s *MOSpan) init(name string, opts ...trace.SpanOption) {
	s.Name = name
	s.StartTime = time.Now()
	for _, opt := range opts {
		opt.ApplySpanStart(&s.SpanConfig)
	}
}

func (s *MOSpan) Size() int64 {
	return int64(unsafe.Sizeof(*s)) + int64(len(s.Name))
}

func (s *MOSpan) Free() {
	s.SpanConfig.Reset()
	s.Parent = nil
	s.Name = ""
	s.tracer = nil
	s.StartTime = table.ZeroTime
	s.EndTime = table.ZeroTime
	spanPool.Put(s)
}

func (s *MOSpan) GetName() string {
	return spanView.OriginTable.GetName()
}

func (s *MOSpan) GetTable() *table.Table { return spanView.OriginTable }

func (s *MOSpan) FillRow(ctx context.Context, row *table.Row) {
	row.Reset()
	row.SetColumnVal(rawItemCol, table.StringField(spanView.Table))
	row.SetColumnVal(spanIDCol, table.BytesField(s.SpanID[:]))
	row.SetColumnVal(traceIDCol, table.UuidField(s.TraceID[:]))
	row.SetColumnVal(spanKindCol, table.StringField(s.Kind.String()))
	psc := s.Parent.SpanContext()
	if psc.SpanID != trace.NilSpanID {
		row.SetColumnVal(parentSpanIDCol, table.BytesField(psc.SpanID[:]))
	}
	row.SetColumnVal(nodeUUIDCol, table.StringField(GetNodeResource().NodeUuid))
	row.SetColumnVal(nodeTypeCol, table.StringField(GetNodeResource().NodeType))
	row.SetColumnVal(spanNameCol, table.StringField(s.Name))
	row.SetColumnVal(startTimeCol, table.TimeField(s.StartTime))
	row.SetColumnVal(endTimeCol, table.TimeField(s.EndTime))
	row.SetColumnVal(durationCol, table.Uint64Field(uint64(s.EndTime.Sub(s.StartTime)))) // Duration
	row.SetColumnVal(resourceCol, table.StringField(s.tracer.provider.resource.String()))
}

func (s *MOSpan) End(options ...trace.SpanEndOption) {
	for _, opt := range options {
		opt.ApplySpanEnd(&s.SpanConfig)
	}
	if s.EndTime.IsZero() {
		s.EndTime = time.Now()
	}
	for _, sp := range s.tracer.provider.spanProcessors {
		sp.OnEnd(s)
	}
}

func (s *MOSpan) SpanContext() trace.SpanContext {
	return s.SpanConfig.SpanContext
}

func (s *MOSpan) ParentSpanContext() trace.SpanContext {
	return s.SpanConfig.Parent.SpanContext()
}

const timestampFormatter = "2006-01-02 15:04:05.000000"

func Time2DatetimeString(t time.Time) string {
	return t.Format(timestampFormatter)
}
