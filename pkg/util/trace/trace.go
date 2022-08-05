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
	goErrors "errors"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/logutil/logutil2"
	"github.com/matrixorigin/matrixone/pkg/util/batchpipe"
	"github.com/matrixorigin/matrixone/pkg/util/errors"
	"github.com/matrixorigin/matrixone/pkg/util/export"

	"go.uber.org/zap"
)

type TraceID uint64
type SpanID uint64

var gTracerProvider *MOTracerProvider
var gTracer Tracer
var gTraceContext context.Context
var gSpanContext atomic.Value

var ini sync.Once

func Init(ctx context.Context, sysVar *config.SystemVariables, options ...TracerProviderOption) (context.Context, error) {

	// init tool dependence
	logutil.SetLogReporter(&logutil.TraceReporter{ReportLog, ReportZap, SetLogLevel, ContextField})
	errors.SetErrorReporter(HandleError)

	// init TraceProvider
	var opts = []TracerProviderOption{
		EnableTracer(sysVar.GetEnableTrace()),
		WithNode(sysVar.GetNodeID(), NodeTypeNode),
		WithBatchProcessMode(sysVar.GetTraceBatchProcessor()),
		DebugMode(sysVar.GetEnableTraceDebug()),
	}
	opts = append(opts, options...)
	gTracerProvider = newMOTracerProvider(opts...)
	config := &gTracerProvider.tracerProviderConfig

	// init Tracer
	gTracer = gTracerProvider.Tracer("MatrixOrigin",
		WithReminder(batchpipe.NewConstantClock(5*time.Second)),
	)

	// init Node DefaultContext
	sc := SpanContextWithIDs(TraceID(1), SpanID(config.getNodeResource().NodeID))
	gSpanContext.Store(&sc)
	gTraceContext = ContextWithSpanContext(ctx, sc)

	// init schema
	InitSchemaByInnerExecutor(config.sqlExecutor)

	initExport(config)

	errors.WithContext(DefaultContext(), goErrors.New("finish trace init"))

	return gTraceContext, nil
}

func initExport(config *tracerProviderConfig) {
	if !config.enableTracer {
		logutil2.Infof(nil, "initExport pass.")
		return
	}
	var p export.BatchProcessor
	// init BatchProcess for trace/log/error
	switch {
	case config.batchProcessMode == "standalone":
		export.Register(&MOSpan{}, NewBufferPipe2SqlWorker(
			bufferWithSizeThreshold(MB),
		))
		export.Register(&MOLog{}, NewBufferPipe2SqlWorker())
		export.Register(&MOZap{}, NewBufferPipe2SqlWorker())
		export.Register(&StatementInfo{}, NewBufferPipe2SqlWorker())
		export.Register(&MOErrorHolder{}, NewBufferPipe2SqlWorker())
		logutil2.Infof(nil, "init GlobalBatchProcessor")
		p = export.NewMOCollector()
		export.SetGlobalBatchProcessor(p)
		p.Start()
	case config.batchProcessMode == "distributed":
		//export.Register(&MOTracer{}, NewBufferPipe2SqlWorker())
	}
	if p != nil {
		config.spanProcessors = append(config.spanProcessors, NewBatchSpanProcessor(p))
		logutil2.Infof(nil, "trace span processor")
		logutil2.Info(nil, "[Debug]", zap.String("operation", "value1"), zap.String("operation_1", "value2"))
	}
}

func Shutdown(ctx context.Context, sysVar *config.SystemVariables) error {
	if !sysVar.GetEnableTrace() {
		return nil
	}

	gTracerProvider.enableTracer = false
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
