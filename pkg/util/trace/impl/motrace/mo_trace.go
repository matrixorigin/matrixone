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
	"errors"
	"sync"
	"time"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
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

// Start starts a Span and returns it along with a context containing it.
//
// The Span is created with the provided name and as a child of any existing
// span context found in passed context. The created Span will be
// configured appropriately by any SpanOption passed.
//
// Only timeout Span can be recorded, hold in SpanConfig.LongTimeThreshold.
// There are 4 diff ways to setting threshold value:
// 1. using default val, which was hold by MOTracerProvider.longSpanTime.
// 2. using `WithLongTimeThreshold()` SpanOption, that will override the default val.
// 3. passing the Deadline context, then it will check the Deadline at Span ended instead of checking Threshold.
// 4. using `WithHungThreshold()` SpanOption, that will override the passed context with context.WithTimeout(ctx, hungThreshold)
// and create a new work goroutine to check Deadline event.
//
// When Span pass timeout threshold or got the deadline event, not only the Span will be recorded,
// but also trigger profile dump specify by `WithProfileGoroutine()`, `WithProfileHeap()`, `WithProfileThreadCreate()`,
// `WithProfileAllocs()`, `WithProfileBlock()`, `WithProfileMutex()`, `WithProfileCpuSecs()`, `WithProfileTraceSecs()` SpanOption.
func (t *MOTracer) Start(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	if !t.IsEnable(opts...) {
		return ctx, trace.NoopSpan{}
	}

	span := newMOSpan()

	// per statement profiler
	ctx, end := fileservice.StatementProfileNewSpan(ctx)
	if end != nil {
		// Tips: check the BenchmarkMOSpan_if_vs_for result
		span.onEnd = append(span.onEnd, end)
	}

	span.tracer = t
	span.ctx = ctx
	span.init(name, opts...)

	parent := trace.SpanFromContext(ctx)
	psc := parent.SpanContext()

	if span.NewRoot || psc.IsEmpty() {
		span.TraceID, span.SpanID = t.provider.idGenerator.NewIDs()
		span.Parent = trace.NoopSpan{}
	} else {
		span.TraceID, span.SpanID = psc.TraceID, t.provider.idGenerator.NewSpanID()
		span.Parent = parent
	}

	// handle HungThreshold
	if threshold := span.SpanConfig.HungThreshold(); threshold > 0 {
		s := newMOHungSpan(span)
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

func (t *MOTracer) IsEnable(opts ...trace.SpanStartOption) bool {
	var cfg trace.SpanConfig
	for idx := range opts {
		opts[idx].ApplySpanStart(&cfg)
	}

	enable := t.provider.IsEnable()

	// check if is this span kind controlled by mo_ctl.
	if has, state, _ := trace.IsMOCtledSpan(cfg.Kind); has {
		return enable && state
	}

	return enable
}

var _ trace.Span = (*MOHungSpan)(nil)

type MOHungSpan struct {
	*MOSpan
	// quitCtx is used to stop the work goroutine loop().
	// Because of quitCtx and deadlineCtx are based on init span.ctx, both can be canceled outside span.
	// So, quitCtx can help to detect this situation, like loop().
	quitCtx context.Context
	// quitCancel cancel quitCtx in End()
	quitCancel context.CancelFunc
	trigger    *time.Timer
	stop       func()
	stopped    bool
	// mux control doProfile exec order, called in loop and End
	mux sync.Mutex
}

func newMOHungSpan(span *MOSpan) *MOHungSpan {
	ctx, cancel := context.WithTimeout(span.ctx, span.SpanConfig.HungThreshold())
	span.ctx = ctx
	s := &MOHungSpan{
		MOSpan:     span,
		quitCtx:    ctx,
		quitCancel: cancel,
	}
	s.trigger = time.AfterFunc(s.HungThreshold(), func() {
		s.mux.Lock()
		defer s.mux.Unlock()
		if e := s.quitCtx.Err(); errors.Is(e, context.Canceled) || s.stopped {
			return
		}
		s.doProfile()
		logutil.Warn("span trigger hung threshold",
			trace.SpanField(s.SpanContext()),
			zap.String("span_name", s.Name),
			zap.Duration("threshold", s.HungThreshold()))
	})
	s.stop = func() {
		s.trigger.Stop()
		s.quitCancel()
		s.stopped = true
	}
	return s
}

func (s *MOHungSpan) End(options ...trace.SpanEndOption) {
	s.mux.Lock()
	defer s.mux.Unlock()
	s.stop()
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
	onEnd       []func()
}

var spanPool = &sync.Pool{New: func() any {
	return &MOSpan{
		onEnd: make([]func(), 0, 2), // speedup first append op
	}
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
	s.onEnd = s.onEnd[:0]
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

// End completes the Span. Span will be recorded if meets the following condition:
// 1. If set Deadline in ctx, which specified at the MOTracer.Start, just check if encounters the Deadline.
// 2. If NOT set Deadline, then check condition: MOSpan.Duration >= MOSpan.GetLongTimeThreshold().
func (s *MOSpan) End(options ...trace.SpanEndOption) {
	var err error
	s.EndTime = time.Now()
	for _, fn := range s.onEnd {
		fn()
	}

	s.needRecord, err = s.NeedRecord()

	if !s.needRecord {
		freeMOSpan(s)
		return
	}
	// apply End option
	for _, opt := range options {
		opt.ApplySpanEnd(&s.SpanConfig)
	}

	s.AddExtraFields(s.SpanConfig.Extra...)

	// do profile
	if s.NeedProfile() {
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

func (s *MOSpan) NeedRecord() (bool, error) {
	// if the span kind falls in mo_ctl controlled spans, we
	// hope it ignores the long time threshold set by the tracer and deadline restrictions.
	// but the threshold set by mo ctl need to be considered
	if has, state, threshold := trace.IsMOCtledSpan(s.Kind); has {
		return state && (s.Duration >= threshold), nil
	}
	// the default logic that before mo_ctl controlled spans have been introduced
	deadline, hasDeadline := s.ctx.Deadline()
	s.Duration = s.EndTime.Sub(s.StartTime)
	if hasDeadline {
		if s.EndTime.After(deadline) {
			return true, s.ctx.Err()
		}
	} else {
		return s.Duration >= s.LongTimeThreshold, nil
	}
	return false, nil
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
