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
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/logutil/logutil2"
	"github.com/matrixorigin/matrixone/pkg/util/batchpipe"
	"github.com/matrixorigin/matrixone/pkg/util/errors"
	"github.com/matrixorigin/matrixone/pkg/util/export"

	"go.uber.org/zap"
)

const (
	InternalExecutor = "InternalExecutor"
	FileService      = "FileService"
)

type TraceID uint64
type SpanID uint64

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
	gTracer = gTracerProvider.Tracer("MatrixOrigin",
		WithReminder(batchpipe.NewConstantClock(5*time.Second)),
	)

	// init Node DefaultContext
	sc := SpanContextWithIDs(TraceID(0), SpanID(config.getNodeResource().NodeID))
	gSpanContext.Store(&sc)
	gTraceContext = ContextWithSpanContext(ctx, sc)

	initExport(config)

	errors.WithContext(DefaultContext(), goErrors.New("finish trace init"))

	return gTraceContext, nil
}

func initExport(config *tracerProviderConfig) {
	if !config.IsEnable() {
		logutil2.Infof(context.TODO(), "initExport pass.")
		return
	}
	var p export.BatchProcessor
	// init BatchProcess for trace/log/error
	switch {
	case config.batchProcessMode == InternalExecutor:
		// init schema
		InitSchemaByInnerExecutor(config.sqlExecutor)
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
