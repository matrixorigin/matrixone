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
	"bytes"
	"context"
	"encoding/hex"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"sync"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/util"
	"github.com/matrixorigin/matrixone/pkg/util/batchpipe"
)

// TracerConfig is a group of options for a Tracer.
type TracerConfig struct {
	Name     string
	reminder batchpipe.Reminder // WithReminder
}

// TracerOption applies an option to a TracerConfig.
type TracerOption interface {
	apply(*TracerConfig)
}

type tracerOptionFunc func(*TracerConfig)

func (f tracerOptionFunc) apply(cfg *TracerConfig) {
	f(cfg)
}

func WithReminder(r batchpipe.Reminder) tracerOptionFunc {
	return tracerOptionFunc(func(cfg *TracerConfig) {
		cfg.reminder = r
	})
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
	} else {
		span.TraceID, span.SpanID = parent.SpanContext().TraceID, t.provider.idGenerator.NewSpanID()
		span.parent = parent
	}

	return ContextWithSpan(ctx, span), span
}

var _ zapcore.ObjectMarshaler = (*SpanContext)(nil)

const SpanFieldKey = "span"

func SpanField(sc SpanContext) zap.Field {
	return zap.Object(SpanFieldKey, &sc)
}

func IsSpanField(field zapcore.Field) bool {
	return field.Key == SpanFieldKey
}

// SpanContext contains identifying trace information about a Span.
type SpanContext struct {
	TraceID TraceID `json:"trace_id"`
	SpanID  SpanID  `json:"span_id"`
}

func (c SpanContext) GetIDs() (TraceID, SpanID) {
	return c.TraceID, c.SpanID
}

func (c *SpanContext) Reset() {
	c.TraceID = 0
	c.SpanID = 0
}

func (c *SpanContext) IsEmpty() bool {
	return c.TraceID == 0
}

func (c *SpanContext) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddUint64("TraceId", uint64(c.TraceID))
	enc.AddUint64("SpanId", uint64(c.SpanID))
	return nil
}

func SpanContextWithID(id TraceID) SpanContext {
	return SpanContext{TraceID: id}
}

func SpanContextWithIDs(tid TraceID, sid SpanID) SpanContext {
	return SpanContext{TraceID: tid, SpanID: sid}
}

// SpanConfig is a group of options for a Span.
type SpanConfig struct {
	SpanContext

	// NewRoot identifies a Span as the root Span for a new trace. This is
	// commonly used when an existing trace crosses trust boundaries and the
	// remote parent span context should be ignored for security.
	NewRoot bool `json:"NewRoot"` // see WithNewRoot
	parent  Span `json:"-"`
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

var _ Span = &MOSpan{}
var _ IBuffer2SqlItem = &MOSpan{}

type MOSpan struct {
	SpanConfig
	Name        bytes.Buffer  `json:"name"`
	StartTimeNS util.TimeNano `json:"start_time"`
	EndTimeNS   util.TimeNano `jons:"end_time"`
	Duration    util.TimeNano `json:"duration"`

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
	s.StartTimeNS = util.NowNS()
	for _, opt := range opts {
		opt.applySpanStart(&s.SpanConfig)
	}
}

func (s *MOSpan) Size() int64 {
	return int64(unsafe.Sizeof(*s)) + int64(s.Name.Cap())
}

func (s *MOSpan) Free() {
	s.Name.Reset()
	s.Duration = 0
	s.tracer = nil
	s.StartTimeNS = 0
	s.EndTimeNS = 0
	spanPool.Put(s)
}

func (s *MOSpan) GetName() string {
	return MOSpanType
}

func (s *MOSpan) End(options ...SpanEndOption) {
	s.EndTimeNS = util.NowNS()
	s.Duration = s.EndTimeNS - s.StartTimeNS

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

func (s MOSpan) GetItemType() string {
	return "MOSpan"
}
