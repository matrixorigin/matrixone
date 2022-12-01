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
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/util/errutil"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
)

func Start(ctx context.Context, spanName string, opts ...SpanOption) (context.Context, Span) {
	return gTracer.Start(ctx, spanName, opts...)
}

func Debug(ctx context.Context, spanName string, opts ...SpanOption) (context.Context, Span) {
	return gTracer.Debug(ctx, spanName, opts...)
}

func Generate(ctx context.Context) context.Context {
	ctx, _ = gTracer.Start(ctx, "generate", WithNewRoot(true))
	return ctx
}

var gTracerProvider atomic.Value
var gTracer Tracer
var gTraceContext atomic.Value
var gSpanContext atomic.Value

func init() {
	SetDefaultSpanContext(&SpanContext{})
	SetDefaultContext(context.Background())
	tp := newMOTracerProvider(EnableTracer(false))
	gTracer = tp.Tracer("default")
	SetTracerProvider(tp)
}

var inited uint32

func Init(ctx context.Context, opts ...TracerProviderOption) (context.Context, error) {
	// fix multi-init in standalone
	if !atomic.CompareAndSwapUint32(&inited, 0, 1) {
		return ContextWithSpanContext(ctx, *DefaultSpanContext()), nil
	}

	// init TraceProvider
	SetTracerProvider(newMOTracerProvider(opts...))
	config := &GetTracerProvider().tracerProviderConfig

	// init Tracer
	gTracer = GetTracerProvider().Tracer("MatrixOne")
	_, span := gTracer.Start(ctx, "TraceInit")
	defer span.End()

	// init DefaultContext / DefaultSpanContext
	var spanId SpanID
	spanId.SetByUUID(config.getNodeResource().NodeUuid)
	sc := SpanContextWithIDs(nilTraceID, spanId)
	SetDefaultSpanContext(&sc)
	SetDefaultContext(ContextWithSpanContext(ctx, sc))

	// init Exporter
	if err := initExporter(ctx, config); err != nil {
		return nil, err
	}

	// init tool dependence
	logutil.SetLogReporter(&logutil.TraceReporter{ReportZap: ReportZap, ContextField: ContextField})
	logutil.SpanFieldKey.Store(SpanFieldKey)
	errutil.SetErrorReporter(ReportError)

	logutil.Infof("trace with LongQueryTime: %v", time.Duration(GetTracerProvider().longQueryTime))

	return DefaultContext(), nil
}

func initExporter(ctx context.Context, config *tracerProviderConfig) error {
	if !config.IsEnable() {
		return nil
	}
	if config.needInit {
		if err := InitSchema(ctx, config.sqlExecutor); err != nil {
			return err
		}
	}
	defaultReminder := batchpipe.NewConstantClock(config.exportInterval)
	defaultOptions := []BufferOption{BufferWithReminder(defaultReminder)}
	var p = config.batchProcessor
	// init BatchProcess for trace/log/error
	switch {
	case config.batchProcessMode == InternalExecutor:
		// register buffer pipe implements
		panic(moerr.NewNotSupported(ctx, "not support process mode: %s", config.batchProcessMode))
	case config.batchProcessMode == FileService:
		p.Register(&MOSpan{}, NewBufferPipe2CSVWorker(defaultOptions...))
		p.Register(&MOZapLog{}, NewBufferPipe2CSVWorker(defaultOptions...))
		p.Register(&StatementInfo{}, NewBufferPipe2CSVWorker(defaultOptions...))
		p.Register(&MOErrorHolder{}, NewBufferPipe2CSVWorker(defaultOptions...))
	default:
		return moerr.NewInternalError(ctx, "unknown batchProcessMode: %s", config.batchProcessMode)
	}
	logutil.Info("init GlobalBatchProcessor")
	if !p.Start() {
		return moerr.NewInternalError(ctx, "trace exporter already started")
	}
	config.spanProcessors = append(config.spanProcessors, NewBatchSpanProcessor(p))
	logutil.Info("init trace span processor")
	return nil
}

// InitSchema
// PS: only in standalone or CN node can init schema
func InitSchema(ctx context.Context, sqlExecutor func() ie.InternalExecutor) error {
	config := &GetTracerProvider().tracerProviderConfig
	switch config.batchProcessMode {
	case InternalExecutor, FileService:
		if err := InitSchemaByInnerExecutor(ctx, sqlExecutor); err != nil {
			return err
		}
	default:
		return moerr.NewInternalError(ctx, "unknown batchProcessMode: %s", config.batchProcessMode)
	}
	return nil
}

func Shutdown(ctx context.Context) error {
	if !GetTracerProvider().IsEnable() {
		return nil
	}

	GetTracerProvider().SetEnable(false)
	tracer := noopTracer{}
	_ = atomic.SwapPointer((*unsafe.Pointer)(unsafe.Pointer(gTracer.(*MOTracer))), unsafe.Pointer(&tracer))

	// fixme: need stop timeout
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	for _, p := range GetTracerProvider().spanProcessors {
		if err := p.Shutdown(shutdownCtx); err != nil {
			return err
		}
	}
	return nil
}

type contextHolder struct {
	ctx context.Context
}

func SetDefaultContext(ctx context.Context) {
	gTraceContext.Store(&contextHolder{ctx})
}

func DefaultContext() context.Context {
	return gTraceContext.Load().(*contextHolder).ctx
}

func SetDefaultSpanContext(sc *SpanContext) {
	gSpanContext.Store(sc)
}

func DefaultSpanContext() *SpanContext {
	return gSpanContext.Load().(*SpanContext)
}

func GetNodeResource() *MONodeResource {
	return GetTracerProvider().getNodeResource()
}

func SetTracerProvider(p *MOTracerProvider) {
	gTracerProvider.Store(p)
}
func GetTracerProvider() *MOTracerProvider {
	return gTracerProvider.Load().(*MOTracerProvider)
}
