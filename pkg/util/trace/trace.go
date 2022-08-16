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
	goErrors "errors"
	"go.uber.org/zap/zapcore"
	"io"
	"sync/atomic"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/logutil/logutil2"
	"github.com/matrixorigin/matrixone/pkg/util/errors"
	"github.com/matrixorigin/matrixone/pkg/util/export"

	"go.uber.org/zap"
)

const (
	InternalExecutor = "InternalExecutor"
	FileService      = "FileService"
)

var gTracerProvider *MOTracerProvider
var gTracer Tracer
var gTraceContext context.Context = context.Background()
var gSpanContext atomic.Value

func Init(ctx context.Context, opts ...TracerProviderOption) (context.Context, error) {

	// init tool dependence
	logutil.SetLogReporter(&logutil.TraceReporter{ReportLog: ReportLog, ReportZap: ReportZap, LevelSignal: SetLogLevel, ContextField: ContextField})
	logutil.SpanFieldKey.Store(SpanFieldKey)
	errors.SetErrorReporter(HandleError)
	export.SetDefaultContextFunc(DefaultContext)

	// init TraceProvider
	gTracerProvider = newMOTracerProvider(opts...)
	config := &gTracerProvider.tracerProviderConfig

	// init Tracer
	gTracer = gTracerProvider.Tracer("MatrixOrigin")

	// init Node DefaultContext
	var spanId SpanID
	spanId.SetByUUID(config.getNodeResource().NodeUuid)
	sc := SpanContextWithIDs(nilTraceID, spanId)
	gSpanContext.Store(&sc)
	gTraceContext = ContextWithSpanContext(ctx, sc)

	initExport(ctx, config)

	errors.WithContext(DefaultContext(), goErrors.New("finish trace init"))

	return gTraceContext, nil
}

func initExport(ctx context.Context, config *tracerProviderConfig) {
	if !config.IsEnable() {
		logutil2.Infof(context.TODO(), "initExport pass.")
		return
	}
	var p export.BatchProcessor
	// init BatchProcess for trace/log/error
	switch {
	case config.batchProcessMode == InternalExecutor:
		// init schema
		InitSchemaByInnerExecutor(ctx, config.sqlExecutor)
		// register buffer pipe implements
		export.Register(&MOSpan{}, NewBufferPipe2SqlWorker(
			bufferWithSizeThreshold(MB),
		))
		export.Register(&MOLog{}, NewBufferPipe2SqlWorker())
		export.Register(&MOZap{}, NewBufferPipe2SqlWorker())
		export.Register(&StatementInfo{}, NewBufferPipe2SqlWorker())
		export.Register(&MOErrorHolder{}, NewBufferPipe2SqlWorker())
		logutil2.Infof(context.TODO(), "init GlobalBatchProcessor")
		// init BatchProcessor for standalone mode.
		p = export.NewMOCollector()
		export.SetGlobalBatchProcessor(p)
		p.Start()
	case config.batchProcessMode == FileService:
		// TODO: will write csv file.
	}
	if p != nil {
		config.spanProcessors = append(config.spanProcessors, NewBatchSpanProcessor(p))
		logutil2.Infof(context.TODO(), "trace span processor")
		logutil2.Info(context.TODO(), "[Debug]", zap.String("operation", "value1"), zap.String("operation_1", "value2"))
	}
}

func Shutdown(ctx context.Context) error {
	if !gTracerProvider.IsEnable() {
		return nil
	}

	gTracerProvider.EnableTracer(false)
	tracer := noopTracer{}
	_ = atomic.SwapPointer((*unsafe.Pointer)(unsafe.Pointer(gTracer.(*MOTracer))), unsafe.Pointer(&tracer))

	// fixme: need stop timeout
	return export.GetGlobalBatchProcessor().Stop(true)
}

func Start(ctx context.Context, spanName string, opts ...SpanOption) (context.Context, Span) {
	return gTracer.Start(ctx, spanName, opts...)
}

func DefaultContext() context.Context {
	return gTraceContext
}

func DefaultSpanContext() *SpanContext {
	return gSpanContext.Load().(*SpanContext)
}

func GetNodeResource() *MONodeResource {
	return gTracerProvider.getNodeResource()
}

type TraceID [16]byte

var nilTraceID TraceID

// IsZero checks whether the trace TraceID is 0 value.
func (t TraceID) IsZero() bool {
	return !bytes.Equal(t[:], nilTraceID[:])
}

func (t TraceID) String() string {
	var dst [36]byte
	bytes2Uuid(dst, t)
	return string(dst[:])
}

type SpanID [8]byte

// SetByUUID use prefix of uuid as value
func (s *SpanID) SetByUUID(uuid string) {
	var dst [16]byte
	uuid2Bytes(dst, uuid)
	copy(s[:], dst[0:8])
}

func (s SpanID) String() string {
	return hex.EncodeToString(s[:])
}

func uuid2Bytes(dst [16]byte, uuid string) {
	l := len(uuid)
	if l != 36 || uuid[8] != '-' || uuid[13] != '-' || uuid[18] != '-' || uuid[23] != '-' {
		return
	}
	hex.Decode(dst[0:4], []byte(uuid[0:8]))
	hex.Decode(dst[4:6], []byte(uuid[9:13]))
	hex.Decode(dst[6:8], []byte(uuid[14:18]))
	hex.Decode(dst[8:10], []byte(uuid[19:23]))
	hex.Decode(dst[10:], []byte(uuid[24:]))
	return
}

func bytes2Uuid(dst [36]byte, src [16]byte) {
	hex.Encode(dst[0:8], src[0:4])
	hex.Encode(dst[9:13], src[4:6])
	hex.Encode(dst[14:18], src[6:8])
	hex.Encode(dst[19:23], src[8:10])
	hex.Encode(dst[24:], src[10:])
	dst[9] = '-'
	dst[13] = '-'
	dst[18] = '-'
	dst[23] = '-'
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

func (c *SpanContext) Size() (n int) {
	return 24
}

func (c *SpanContext) MarshalTo(dAtA []byte) (int, error) {
	l := cap(dAtA)
	if l < c.Size() {
		return -1, io.ErrUnexpectedEOF
	}
	copy(dAtA, c.TraceID[:])
	copy(dAtA[16:], c.SpanID[:])
	return c.Size(), nil
}

func (c *SpanContext) Unmarshal(dAtA []byte) error {
	l := cap(dAtA)
	if l < c.Size() {
		return io.ErrUnexpectedEOF
	}
	copy(c.TraceID[:], dAtA[0:16])
	copy(c.SpanID[:], dAtA[16:24])
	return nil
}

func (c SpanContext) GetIDs() (TraceID, SpanID) {
	return c.TraceID, c.SpanID
}

func (c *SpanContext) Reset() {
	c.TraceID = TraceID{}
	c.SpanID = SpanID{}
}

func (c *SpanContext) IsEmpty() bool {
	return c.TraceID.IsZero()
}

func (c *SpanContext) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("TraceId", c.TraceID.String())
	enc.AddString("SpanId", c.SpanID.String())
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
