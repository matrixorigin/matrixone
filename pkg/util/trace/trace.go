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
	"sync/atomic"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/util/errutil"
	"github.com/matrixorigin/matrixone/pkg/util/export"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
)

var gTracerProvider atomic.Value
var gTracer Tracer
var gTraceContext atomic.Value
var gSpanContext atomic.Value

func init() {
	SetDefaultSpanContext(&SpanContext{})
	SetDefaultContext(context.Background())
	SetTracerProvider(newMOTracerProvider(EnableTracer(false)))
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
	gTracer = GetTracerProvider().Tracer("MatrixOrigin")

	// init Node DefaultContext
	var spanId SpanID
	spanId.SetByUUID(config.getNodeResource().NodeUuid)
	sc := SpanContextWithIDs(nilTraceID, spanId)
	SetDefaultSpanContext(&sc)
	SetDefaultContext(ContextWithSpanContext(ctx, sc))

	if err := initExport(ctx, config); err != nil {
		return nil, err
	}

	// init tool dependence
	logutil.SetLogReporter(&logutil.TraceReporter{ReportLog: ReportLog, ReportZap: ReportZap, LevelSignal: SetLogLevel, ContextField: ContextField})
	logutil.SpanFieldKey.Store(SpanFieldKey)
	errutil.SetErrorReporter(HandleError)
	export.SetDefaultContextFunc(DefaultContext)

	return DefaultContext(), nil
}

func initExport(ctx context.Context, config *tracerProviderConfig) error {
	if !config.IsEnable() {
		logutil.Info("initExport pass.")
		return nil
	}
	var p export.BatchProcessor
	// init BatchProcess for trace/log/error
	switch {
	case config.batchProcessMode == InternalExecutor:
		// init schema
		if config.needInit {
			if err := InitSchemaByInnerExecutor(ctx, config.sqlExecutor); err != nil {
				return err
			}
		}
		// register buffer pipe implements
		export.Register(&MOSpan{}, NewBufferPipe2SqlWorker(
			bufferWithSizeThreshold(MB),
		))
		export.Register(&MOLog{}, NewBufferPipe2SqlWorker())
		export.Register(&MOZapLog{}, NewBufferPipe2SqlWorker())
		export.Register(&StatementInfo{}, NewBufferPipe2SqlWorker())
		export.Register(&MOErrorHolder{}, NewBufferPipe2SqlWorker())
	case config.batchProcessMode == FileService:
		if config.needInit {
			// PS: only in standalone mode or CN node can init schema
			if err := InitExternalTblSchema(ctx, config.sqlExecutor); err != nil {
				return err
			}
		}
		export.Register(&MOSpan{}, NewBufferPipe2CSVWorker())
		export.Register(&MOLog{}, NewBufferPipe2CSVWorker())
		export.Register(&MOZapLog{}, NewBufferPipe2CSVWorker())
		export.Register(&StatementInfo{}, NewBufferPipe2CSVWorker())
		export.Register(&MOErrorHolder{}, NewBufferPipe2CSVWorker())
	default:
		return moerr.NewInternalError("unknown batchProcessMode: %s", config.batchProcessMode)
	}
	logutil.Info("init GlobalBatchProcessor")
	// init BatchProcessor for standalone mode.
	p = export.NewMOCollector()
	export.SetGlobalBatchProcessor(p)
	if !p.Start() {
		return moerr.NewInternalError("trace exporter already started")
	}
	config.spanProcessors = append(config.spanProcessors, NewBatchSpanProcessor(p))
	logutil.Info("init trace span processor")
	return nil
}

func InitSchema(ctx context.Context, sqlExecutor func() ie.InternalExecutor) error {
	config := &GetTracerProvider().tracerProviderConfig
	switch {
	case config.batchProcessMode == InternalExecutor:
		if err := InitSchemaByInnerExecutor(ctx, sqlExecutor); err != nil {
			return err
		}
	case config.batchProcessMode == FileService:
		// PS: only in standalone mode or CN node can init schema
		if err := InitExternalTblSchema(ctx, sqlExecutor); err != nil {
			return err
		}
	default:
		return moerr.NewInternalError("unknown batchProcessMode: %s", config.batchProcessMode)
	}
	return nil
}

func Shutdown(ctx context.Context) error {
	if !GetTracerProvider().IsEnable() {
		return nil
	}

	GetTracerProvider().EnableTracer(false)
	tracer := noopTracer{}
	_ = atomic.SwapPointer((*unsafe.Pointer)(unsafe.Pointer(gTracer.(*MOTracer))), unsafe.Pointer(&tracer))

	// fixme: need stop timeout
	return export.GetGlobalBatchProcessor().Stop(true)
}

func Start(ctx context.Context, spanName string, opts ...SpanOption) (context.Context, Span) {
	return gTracer.Start(ctx, spanName, opts...)
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
