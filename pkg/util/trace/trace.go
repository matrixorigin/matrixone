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
	"github.com/matrixorigin/matrixone/pkg/logutil/logutil2"
	"github.com/matrixorigin/matrixone/pkg/util/errors"
	"go.uber.org/zap"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/util/batchpipe"
	"github.com/matrixorigin/matrixone/pkg/util/export"
)

type TraceID uint64
type SpanID uint64

var gTracerProvider *MOTracerProvider
var gTracer Tracer
var gTraceContext context.Context

var ini sync.Once

func Init(ctx context.Context, sysVar *config.SystemVariables, options ...TracerProviderOption) (context.Context, error) {

	// init tool dependence
	logutil.SetLogReporter(&logutil.TraceReporter{ReportLog, ReportZap, SetLogLevel, logutil.ContextFieldsFunc(ContextFields)})
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
	gTraceContext = ContextWithSpanContext(ctx, SpanContextWithIDs(TraceID(0), SpanID(config.getNodeResource().NodeID)))

	// init schema
	InitSchemaByInnerExecutor(config.sqlExecutor)

	initExport(config)

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

func Start(ctx context.Context, spanName string, opts ...SpanOption) (context.Context, Span) {
	return gTracer.Start(ctx, spanName, opts...)
}

func DefaultContext() context.Context {
	return gTraceContext
}

func GetNodeResource() *MONodeResource {
	return gTracerProvider.getNodeResource()
}
