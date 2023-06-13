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
	"encoding/hex"
	"sync"
	"time"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
	"github.com/matrixorigin/matrixone/pkg/util/trace"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

//nolint:revive // revive complains about stutter of `trace.TraceFlags`.

var _ trace.Tracer = &MOTracer{}

// MOTracer is the creator of Spans.
type MOTracer struct {
	trace.TracerConfig
	provider *MOTracerProvider
}

func (t *MOTracer) Start(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	if !t.IsEnable() {
		return ctx, trace.NoopSpan{}
	}
	span := newMOSpan()
	span.tracer = t
	span.ctx = ctx
	span.init(name, opts...)

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

func (t *MOTracer) Debug(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
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
	// Duration
	Duration time.Duration `json:"duration"`
	// ExtraFields
	ExtraFields []zap.Field `json:"extra"`

	tracer     *MOTracer       `json:"-"`
	ctx        context.Context `json:"-"`
	needRecord bool            `json:"-"`
}

var spanPool = &sync.Pool{New: func() any {
	return &MOSpan{}
}}

func newMOSpan() *MOSpan {
	return spanPool.Get().(*MOSpan)
}

func (s *MOSpan) init(name string, opts ...trace.SpanStartOption) {
	s.Name = name
	s.StartTime = time.Now()
	s.LongTimeThreshold = s.tracer.provider.longSpanTime
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
	s.ctx = nil
	s.needRecord = false
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
	row.SetColumnVal(spanIDCol, table.StringField(hex.EncodeToString(s.SpanID[:])))
	row.SetColumnVal(traceIDCol, table.UuidField(s.TraceID[:]))
	row.SetColumnVal(spanKindCol, table.StringField(s.Kind.String()))
	psc := s.Parent.SpanContext()
	if psc.SpanID != trace.NilSpanID {
		row.SetColumnVal(parentSpanIDCol, table.StringField(hex.EncodeToString(psc.SpanID[:])))
	}
	row.SetColumnVal(nodeUUIDCol, table.StringField(GetNodeResource().NodeUuid))
	row.SetColumnVal(nodeTypeCol, table.StringField(GetNodeResource().NodeType))
	row.SetColumnVal(spanNameCol, table.StringField(s.Name))
	row.SetColumnVal(startTimeCol, table.TimeField(s.StartTime))
	row.SetColumnVal(endTimeCol, table.TimeField(s.EndTime))
	row.SetColumnVal(timestampCol, table.TimeField(s.EndTime))
	row.SetColumnVal(durationCol, table.Uint64Field(uint64(s.Duration)))
	row.SetColumnVal(resourceCol, table.StringField(s.tracer.provider.resource.String()))

	// fill extra fields
	if len(s.ExtraFields) > 0 {
		encoder := getEncoder()
		buf, err := encoder.EncodeEntry(zapcore.Entry{}, s.ExtraFields)
		if buf != nil {
			defer buf.Free()
		}
		if err != nil {
			moerr.ConvertGoError(ctx, err)
		} else {
			row.SetColumnVal(extraCol, table.StringField(buf.String()))
		}
	}
}

// End record span which meets any of the following condition
// 1. span's duration >= LongTimeThreshold
// 2. span's ctx, which specified at the MOTracer.Start, encounters the deadline
func (s *MOSpan) End(options ...trace.SpanEndOption) {
	s.EndTime = time.Now()
	deadline, hasDeadline := s.ctx.Deadline()
	s.Duration = s.EndTime.Sub(s.StartTime)
	// check need record
	if s.Duration >= s.GetLongTimeThreshold() {
		s.needRecord = true
	} else if hasDeadline && s.EndTime.After(deadline) {
		s.needRecord = true
		s.ExtraFields = append(s.ExtraFields, zap.Error(s.ctx.Err()))
	}
	if !s.needRecord {
		go s.Free()
		return
	}
	// do record
	for _, opt := range options {
		opt.ApplySpanEnd(&s.SpanConfig)
	}
	for _, sp := range s.tracer.provider.spanProcessors {
		sp.OnEnd(s)
	}
}

func (s *MOSpan) AddExtraFields(fields ...zap.Field) {
	s.ExtraFields = append(s.ExtraFields, fields...)
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

var jsonEncoder zapcore.Encoder
var jsonEncoderInit sync.Once

func getEncoder() zapcore.Encoder {
	jsonEncoderInit.Do(func() {
		jsonEncoder = zapcore.NewJSONEncoder(zapcore.EncoderConfig{
			TimeKey:        "",
			LevelKey:       "",
			NameKey:        "",
			CallerKey:      "",
			FunctionKey:    zapcore.OmitKey,
			MessageKey:     "",
			StacktraceKey:  "",
			SkipLineEnding: true,
		})
	})
	return jsonEncoder.Clone()
}
