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
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
	"github.com/matrixorigin/matrixone/pkg/util/profile"
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

	// handle HungThreshold
	if threshold := span.SpanConfig.HungThreshold(); threshold > 0 {
		s := newMOHungSpan(span)
		go s.loop()
		return trace.ContextWithSpan(ctx, s), s
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

var _ trace.Span = (*MOHungSpan)(nil)

type MOHungSpan struct {
	*MOSpan
	quitCtx        context.Context
	quitCancel     context.CancelFunc
	deadlineCtx    context.Context
	deadlineCancel context.CancelFunc
	// mux control doProfile exec order
	mux sync.Mutex
}

func newMOHungSpan(span *MOSpan) *MOHungSpan {
	originCtx, originCancel := context.WithCancel(span.ctx)
	ctx, cancel := context.WithTimeout(span.ctx, span.SpanConfig.HungThreshold())
	span.ctx = ctx
	return &MOHungSpan{
		MOSpan:         span,
		quitCtx:        originCtx,
		quitCancel:     originCancel,
		deadlineCtx:    ctx,
		deadlineCancel: cancel,
	}
}

func (s *MOHungSpan) loop() {
	select {
	case <-s.quitCtx.Done():
	case <-s.deadlineCtx.Done():
		s.mux.Lock()
		defer s.mux.Unlock()
		if e := s.quitCtx.Err(); e == context.Canceled {
			break
		}
		s.doProfile()
		logutil.Warn("span trigger hung threshold",
			trace.SpanField(s.SpanContext()),
			zap.String("span_name", s.Name),
			zap.Duration("threshold", s.HungThreshold()))
	}
	s.deadlineCancel()
}

func (s *MOHungSpan) End(options ...trace.SpanEndOption) {
	s.mux.Lock()
	defer s.mux.Unlock()
	s.quitCancel()
	s.MOSpan.End(options...)
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

	tracer     *MOTracer
	ctx        context.Context
	needRecord bool

	doneProfile bool
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
	s.ExtraFields = nil
	s.StartTime = table.ZeroTime
	s.EndTime = table.ZeroTime
	s.doneProfile = false
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

// End record span which meets the following condition
// If set Deadline in ctx, which specified at the MOTracer.Start, just check if encounters the deadline.
// If not set, check condition: duration > span.GetLongTimeThreshold()
func (s *MOSpan) End(options ...trace.SpanEndOption) {
	var err error
	s.EndTime = time.Now()
	deadline, hasDeadline := s.ctx.Deadline()
	s.Duration = s.EndTime.Sub(s.StartTime)
	// check need record
	if hasDeadline {
		if s.EndTime.After(deadline) {
			s.needRecord = true
			err = s.ctx.Err()
		}
	} else {
		if s.Duration >= s.GetLongTimeThreshold() {
			s.needRecord = true
		}
	}
	if !s.needRecord {
		go freeMOSpan(s)
		return
	}
	// apply End option
	for _, opt := range options {
		opt.ApplySpanEnd(&s.SpanConfig)
	}
	// do profile
	if !s.NeedProfile() {
		s.doProfile()
	}
	// record error info
	if err != nil {
		s.ExtraFields = append(s.ExtraFields, zap.Error(err))
	}
	// do Collect
	for _, sp := range s.tracer.provider.spanProcessors {
		sp.OnEnd(s)
	}
}

func (s *MOSpan) doProfileRuntime(ctx context.Context, name string, debug int) {
	now := time.Now()
	factory := s.tracer.provider.writerFactory
	filepath := profile.GetProfileName(name, s.SpanID.String(), now)
	w := factory.GetWriter(ctx, filepath)
	err := profile.ProfileRuntime(name, w, debug)
	if err == nil {
		err = w.Close()
	}
	if err != nil {
		s.AddExtraFields(zap.String(name, err.Error()))
	} else {
		s.AddExtraFields(zap.String(name, filepath))
	}
}

// doProfile is sync op.
func (s *MOSpan) doProfile() {
	if s.doneProfile {
		return
	}
	factory := s.tracer.provider.writerFactory
	ctx := DefaultContext()
	// do profile goroutine txt
	if s.ProfileGoroutine() {
		s.doProfileRuntime(ctx, profile.GOROUTINE, 2)
	}
	// do profile heap pprof
	if s.ProfileHeap() {
		s.doProfileRuntime(ctx, profile.HEAP, 0)
	}
	// do profile allocs
	if s.ProfileAllocs() {
		s.doProfileRuntime(ctx, profile.ALLOCS, 0)
	}
	// do profile threadcreate
	if s.ProfileThreadCreate() {
		s.doProfileRuntime(ctx, profile.THREADCREATE, 0)
	}
	// do profile block
	if s.ProfileBlock() {
		s.doProfileRuntime(ctx, profile.BLOCK, 0)
	}
	// do profile mutex
	if s.ProfileMutex() {
		s.doProfileRuntime(ctx, profile.MUTEX, 0)
	}
	// profile cpu should be the last one op, caused by it will sustain few seconds
	if s.ProfileCpuSecs() > 0 {
		filepath := profile.GetProfileName(profile.CPU, s.SpanID.String(), s.EndTime)
		w := factory.GetWriter(ctx, filepath)
		err := profile.ProfileCPU(w, s.ProfileCpuSecs())
		if err == nil {
			err = w.Close()
		}
		if err != nil {
			s.AddExtraFields(zap.String(profile.CPU, err.Error()))
		} else {
			s.AddExtraFields(zap.String(profile.CPU, filepath))
		}
	}
	// profile trace is a sync-op, it will sustain few seconds
	if s.ProfileTraceSecs() > 0 {
		filepath := profile.GetProfileName(profile.TRACE, s.SpanID.String(), s.EndTime)
		w := factory.GetWriter(ctx, filepath)
		err := profile.ProfileTrace(w, s.ProfileTraceSecs())
		if err == nil {
			err = w.Close()
		}
		if err != nil {
			s.AddExtraFields(zap.String(profile.TRACE, err.Error()))
		} else {
			s.AddExtraFields(zap.String(profile.TRACE, filepath))
		}
	}
	s.doneProfile = true
}

var freeMOSpan = func(s *MOSpan) {
	s.Free()
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
