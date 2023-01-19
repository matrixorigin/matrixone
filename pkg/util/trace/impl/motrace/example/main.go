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

package main

import (
	"context"
	goErrors "errors"
	"fmt"
	"io"
	"time"

	"github.com/lni/dragonboat/v4/logger"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/util/batchpipe"
	"github.com/matrixorigin/matrixone/pkg/util/errutil"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace"
	"go.uber.org/zap"
)

var _ ie.InternalExecutor = &logOutputExecutor{}

type logOutputExecutor struct{}

func (l logOutputExecutor) Exec(ctx context.Context, s string, s2 ie.SessionOverrideOptions) error {
	logutil.Info(s)
	return nil
}
func (l logOutputExecutor) Query(ctx context.Context, s string, _ ie.SessionOverrideOptions) ie.InternalExecResult {
	logutil.Info(s)
	return nil
}
func (l logOutputExecutor) ApplySessionOverride(ie.SessionOverrideOptions) {}

type dummyStringWriter struct{}

func (w *dummyStringWriter) WriteString(s string) (n int, err error) {
	return fmt.Printf("dummyStringWriter: %s\n", s)
}

var dummyFSWriterFactory = func(context.Context, string, batchpipe.HasName, motrace.WriteFactoryConfig) io.StringWriter {
	return &dummyStringWriter{}
}

func bootstrap(ctx context.Context) (context.Context, error) {
	logutil.SetupMOLogger(&logutil.LogConfig{Format: "console", DisableStore: false})
	// init trace/log/error framework & BatchProcessor
	return motrace.Init(ctx,
		motrace.WithMOVersion("v0.6.0"),
		// nodeType like CN/DN/LogService; id maybe in config.
		motrace.WithNode("node_uuid", trace.NodeTypeStandalone),
		// config[enableTrace], default: true
		motrace.EnableTracer(true),
		// config[traceBatchProcessor], distributed node should use "FileService" in system_vars_config.toml
		// "FileService" is not implement yet
		motrace.WithBatchProcessMode("FileService"),
		// WithFSWriterFactory for config[traceBatchProcessor] = "FileService"
		motrace.WithFSWriterFactory(dummyFSWriterFactory),
		// WithSQLExecutor for config[traceBatchProcessor] = "InternalExecutor"
		motrace.WithSQLExecutor(func() ie.InternalExecutor {
			return &logOutputExecutor{}
		}),
	)

}

func traceUsage(ctx context.Context) {
	// Case 1: start new span, which calculate duration of function traceUsage()
	newCtx, span1 := trace.Start(ctx, "traceUsage")
	// calling End() will calculate running duration(us)
	defer span1.End()
	logutil.Info("1st span with TraceId & SpanID", trace.SpanField(span1.SpanContext()))

	// case 2: calling another function, please pass newCtx
	traceUsageDepth_1(newCtx)
	// case 4: calling again, will have new span
	traceUsageDepth_1(newCtx)

	// case 5: new span with same parent of span_1, you should use in-args ctx
	// span2 will be brother with span1
	newCtx2, span2 := trace.Start(ctx, "traceUsage_2")
	traceUsageDepth_1_1(newCtx2)
	span2.End()
}

func traceUsageDepth_1(ctx context.Context) {
	// case 3: start new spanChild using ctx in-args,
	// spanChild like a children of span1
	depth1Ctx, spanChild := trace.Start(ctx, "traceUsage")
	defer spanChild.End()
	logutil.Info("2rd spanChild has same TraceId & new SpanID", trace.SpanField(spanChild.SpanContext()))
	logutil.Info("ctx contain the spanChild info", trace.ContextField(depth1Ctx))
	logutil.Infof("2rd spanChild has parent spanChild info, like parent span_id: %d", spanChild.ParentSpanContext().SpanID)
}

func traceUsageDepth_1_1(ctx context.Context) {
	logutil.Info("traceUsageDepth_1_1 working")
}

func logUsage(ctx context.Context) {
	// case 1: use logutil.Info/Infof/..., without context.Context
	// it will store log into db, related to Node
	logutil.Info("use without ctx")

	// case 2: use logutil.Info/Infof/..., with context.Context
	// it will store log into db, related to span, which save in ctx
	// Suggestion: trace.ContextField should be 1st Field arg, which will help log to find span info faster.
	logutil.Info("use with ctx", trace.ContextField(ctx), zap.Int("int", 1))

	// case 3: use logutil2.Info/Infof/..., with contex.Context
	// it will store log into db, related to span, which save in ctx
	// (removed)

	// case4: 3rd lib like dragonboat, could use logutil.DragonboatFactory, like
	//logger.SetLoggerFactory(logutil.DragonboatFactory)
	plog := logger.GetLogger("dragonboat.logger")
	plog.Infof("log with DragonboatFactory, now: %s", time.Now())
}

func outputError(msg string, err error) {
	logutil.Infof("%s %%s: %s", msg, err)
	logutil.Infof("%s %%+s: %+s", msg, err)
	logutil.Infof("%s %%v: %v", msg, err)
	logutil.Infof("%s %%+v: %+v", msg, err)
	logutil.Infof("%s Error(): %v", msg, err.Error()) // just like raw error
	logutil.Info("---")
}

func errorUsage(ctx context.Context) {
	newCtx, span := trace.Start(ctx, "errorUsage")
	defer span.End()

	base := goErrors.New("base error")
	logutil.Infof("base err: %v", base)

	// case 1: WithMessage
	// removed

	// case 2: WithStack
	// removed

	// case 3: WithContext, store db & log
	logutil.Info("WithContext with default action: 1) store in db; 2) gen log")
	outputError("WithContext", errutil.WithContext(newCtx, base))
	outputError("Wrapf", errutil.Wrapf(base, "extra message"))

	// case 4: NewWithContext, store db & log
	// removed

	// case 5: ContextWithNoReport
	noReportCtx := errutil.ContextWithNoReport(newCtx, true)
	errutil.ReportError(noReportCtx, base)

}

type FunctionRequest struct {
	trace.SpanContext
}

type rpcRequest struct {
	message []byte
}

func rpcUsage(ctx context.Context) {
	ctx, span := trace.Start(ctx, "rpcUsage", trace.WithNewRoot(true))
	defer span.End()
	req := &FunctionRequest{
		SpanContext: trace.SpanFromContext(ctx).SpanContext(),
	}
	logutil.Info("client call Function", trace.ContextField(ctx))

	// serialize
	rpcReq := &rpcRequest{message: make([]byte, 24)}
	if _, err := req.SpanContext.MarshalTo(rpcReq.message); err != nil {
		logutil.Errorf("callFunction: %v", err)
		panic(err)
	}
	logutil.Infof("message: %x", rpcReq.message)

	// deserialize
	var sc trace.SpanContext
	if err := sc.Unmarshal(rpcReq.message); err != nil {
		panic(err)
	}
	svrRootCtx := trace.ContextWithSpanContext(ctx, sc)
	logutil.Info("server accept request", trace.ContextField(svrRootCtx))
	newCtx2, span2 := trace.Start(svrRootCtx, "Function")
	defer span2.End()

	logutil.Info("server do Function, have same TraceId from client.", trace.ContextField(newCtx2))
}

func mixUsage(ctx context.Context) {
	newCtx, span := trace.Start(ctx, "mixUsage")
	defer span.End()

	logutil.Info("message", trace.ContextField(newCtx))

	err := childFunc(newCtx)
	motrace.ReportError(newCtx, errutil.Wrapf(err, "extra %s", "message"), 0)
	logutil.Warnf("ReportError with NoReport: '%v'", err)
	noReportCtx := errutil.ContextWithNoReport(newCtx, true)
	motrace.ReportError(noReportCtx, err, 0)
	logutil.Warnf("ReportError with NoReport, Done.")

}

func childFunc(ctx context.Context) error {
	err := goErrors.New("example: not found Database")
	return errutil.WithContext(ctx, err)
}

func shutdown(ctx context.Context) {
	logutil.Warn("shutdown", trace.ContextField(ctx))
	motrace.Shutdown(ctx)
}

func main() {
	ctx := context.Background()

	// rootCtx should be root Context of Server running, you can get it also by trace.DefaultContext()
	rootCtx, err := bootstrap(ctx)
	if err != nil {
		panic(err)
	}
	// show rootCtx in zap.logger format
	logutil.Info("root ctx", trace.ContextField(rootCtx))
	logutil.Info("default ctx", trace.ContextField(motrace.DefaultContext()))

	traceUsage(rootCtx)

	logUsage(rootCtx)

	errorUsage(rootCtx)

	rpcUsage(rootCtx)

	mixUsage(rootCtx)

	logutil.Warn("wait 5s to see insert sql", trace.ContextField(rootCtx))

	shutdown(rootCtx)
}
