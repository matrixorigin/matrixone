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
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/util/batchpipe"
	"github.com/matrixorigin/matrixone/pkg/util/errutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	db_holder "github.com/matrixorigin/matrixone/pkg/util/export/etl/db"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
)

var gTracerProvider atomic.Value
var gTracer trace.Tracer
var gTraceContext atomic.Value
var gSpanContext atomic.Value

func init() {
	SetDefaultSpanContext(&trace.SpanContext{})
	SetDefaultContext(context.Background())
	tp := newMOTracerProvider(EnableTracer(false))
	gTracer = tp.Tracer("default")
	SetTracerProvider(tp)
}

var inited uint32

func InitWithConfig(ctx context.Context, SV *config.ObservabilityParameters, opts ...TracerProviderOption) (error, bool) {
	opts = append(opts,
		withMOVersion(SV.MoVersion),
		EnableTracer(!SV.DisableTrace),
		WithExportInterval(SV.TraceExportInterval),
		WithLongQueryTime(SV.LongQueryTime),
		WithLongSpanTime(SV.LongSpanTime.Duration),
		WithSpanDisable(SV.DisableSpan),
		WithErrorDisable(SV.DisableError),
		WithSkipRunningStmt(SV.SkipRunningStmt),
		WithSQLWriterDisable(SV.DisableSqlWriter),
		WithAggregatorDisable(SV.DisableStmtAggregation),
		WithAggregatorWindow(SV.AggregationWindow.Duration),
		WithSelectThreshold(SV.SelectAggrThreshold.Duration),
		WithStmtMergeEnable(SV.EnableStmtMerge),
		WithCUConfig(SV.CU, SV.CUv1),
		WithTCPPacket(SV.TCPPacket),
		WithLabels(SV.LabelSelector),

		DebugMode(SV.EnableTraceDebug),
		WithBufferSizeThreshold(SV.BufferSize),
	)
	return Init(ctx, opts...)
}

// Init initializes the tracer with the given options.
// If EnableTracer is set to false, this function does nothing.
// If EnableTracer is set to true, the tracer is initialized.
// Init only allow called once.
func Init(ctx context.Context, opts ...TracerProviderOption) (err error, act bool) {
	// fix multi-init in standalone
	if !atomic.CompareAndSwapUint32(&inited, 0, 1) {
		return nil, false
	}

	// init TraceProvider
	SetTracerProvider(newMOTracerProvider(opts...))
	config := &GetTracerProvider().tracerProviderConfig

	if !config.disableSpan {
		// init Tracer
		gTracer = GetTracerProvider().Tracer("MatrixOne")
		_, span := gTracer.Start(ctx, "TraceInit")
		defer span.End()
		defer trace.SetDefaultTracer(gTracer)
	}
	if config.disableError {
		DisableLogErrorReport(true)
	}

	// init DefaultContext / DefaultSpanContext
	var spanId trace.SpanID
	spanId.SetByUUID(config.getNodeResource().NodeUuid)
	sc := trace.SpanContextWithIDs(trace.NilTraceID, spanId)
	SetDefaultSpanContext(&sc)
	serviceCtx := context.Background()
	SetDefaultContext(trace.ContextWithSpanContext(serviceCtx, sc))
	SetCuConfig(&config.cuConfig, &config.cuConfigV1)

	// init Exporter
	if err := initExporter(ctx, config); err != nil {
		return err, true
	}

	// init all mo_ctl controlled spans
	trace.InitMOCtledSpan()

	// init tool dependence
	logutil.SetLogReporter(&logutil.TraceReporter{ReportZap: ReportZap, ContextField: trace.ContextField})
	logutil.SpanFieldKey.Store(trace.SpanFieldKey)
	errutil.SetErrorReporter(ReportError)

	// init db_hodler
	db_holder.SetLabelSelector(config.labels)

	logutil.Debugf("trace with LongQueryTime: %v", time.Duration(GetTracerProvider().longQueryTime))
	logutil.Debugf("trace with LongSpanTime: %v", GetTracerProvider().longSpanTime)
	logutil.Debugf("trace with DisableSpan: %v", GetTracerProvider().disableSpan)

	return nil, true
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
	defaultOptions := []BufferOption{BufferWithReminder(defaultReminder), BufferWithSizeThreshold(config.bufferSizeThreshold)}
	var p = config.batchProcessor
	// init BatchProcess for trace/log/error
	p.Register(&MOSpan{}, NewBufferPipe2CSVWorker(defaultOptions...))
	p.Register(&MOZapLog{}, NewBufferPipe2CSVWorker(defaultOptions...))
	p.Register(&StatementInfo{}, NewBufferPipe2CSVWorker(defaultOptions...))
	p.Register(&MOErrorHolder{}, NewBufferPipe2CSVWorker(defaultOptions...))
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
	ctx = defines.AttachAccount(ctx, catalog.System_Account, catalog.System_User, catalog.System_Role)
	c := &GetTracerProvider().tracerProviderConfig
	WithSQLExecutor(sqlExecutor).apply(c)
	if err := InitSchemaByInnerExecutor(ctx, sqlExecutor); err != nil {
		return err
	}
	return nil
}

// InitSchema2
// PS: only in system bootstrap init schema with `executor.TxnExecutor`
func InitSchemaWithTxn(ctx context.Context, txn executor.TxnExecutor) error {
	_, err := txn.Exec(sqlCreateDBConst, executor.StatementOption{})
	if err != nil {
		return err
	}

	var createCost time.Duration
	defer func() {
		logutil.Debugf("[Trace] init tables: create cost %d ms", createCost.Milliseconds())
	}()

	instant := time.Now()
	for _, tbl := range tables {
		_, err = txn.Exec(tbl.ToCreateSql(ctx, true), executor.StatementOption{})
		if err != nil {
			return err
		}
	}

	for _, v := range views {
		_, err = txn.Exec(v.ToCreateSql(ctx, true), executor.StatementOption{})
		if err != nil {
			return err
		}
	}
	createCost = time.Since(instant)
	return nil
}

func Shutdown(ctx context.Context) error {
	if !GetTracerProvider().IsEnable() {
		return nil
	}
	GetTracerProvider().SetEnable(false)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	for _, p := range GetTracerProvider().spanProcessors {
		if err := p.Shutdown(shutdownCtx); err != nil {
			return err
		}
	}
	logutil.Info("Shutdown trace complete.")
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

func SetDefaultSpanContext(sc *trace.SpanContext) {
	gSpanContext.Store(sc)
}

func DefaultSpanContext() *trace.SpanContext {
	return gSpanContext.Load().(*trace.SpanContext)
}

func GetNodeResource() *trace.MONodeResource {
	return GetTracerProvider().getNodeResource()
}

func SetTracerProvider(p *MOTracerProvider) {
	gTracerProvider.Store(p)
}

// GetTracerProvider returns the global TracerProvider.
// It will be initialized at startup.
var GetTracerProvider = func() *MOTracerProvider {
	return gTracerProvider.Load().(*MOTracerProvider)
}

func GetSQLExecutorFactory() func() ie.InternalExecutor {
	p := GetTracerProvider()
	if p != nil {
		p.mux.Lock()
		defer p.mux.Unlock()
		return p.tracerProviderConfig.sqlExecutor
	}
	return nil
}

type PipeImpl batchpipe.PipeImpl[batchpipe.HasName, any]

type BatchProcessor interface {
	Collect(context.Context, batchpipe.HasName) error
	Start() bool
	Stop(graceful bool) error
	Register(name batchpipe.HasName, impl PipeImpl)
}

type DiscardableCollector interface {
	DiscardableCollect(context.Context, batchpipe.HasName) error
}

var _ BatchProcessor = &NoopBatchProcessor{}

type NoopBatchProcessor struct {
}

func (n NoopBatchProcessor) Collect(context.Context, batchpipe.HasName) error { return nil }
func (n NoopBatchProcessor) Start() bool                                      { return true }
func (n NoopBatchProcessor) Stop(bool) error                                  { return nil }
func (n NoopBatchProcessor) Register(batchpipe.HasName, PipeImpl)             {}

func GetGlobalBatchProcessor() BatchProcessor {
	return GetTracerProvider().batchProcessor
}
