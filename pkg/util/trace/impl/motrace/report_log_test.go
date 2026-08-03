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

package motrace

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/util/batchpipe"
	"github.com/matrixorigin/matrixone/pkg/util/errutil"
	"github.com/matrixorigin/matrixone/pkg/util/stack"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
)

func TestReportZap(t *testing.T) {
	type args struct {
		jsonEncoder zapcore.Encoder
		entry       zapcore.Entry
		fields      []zapcore.Field
	}
	spanField := trace.ContextField(trace.ContextWithSpanContext(context.Background(), trace.SpanContext{}))
	entry := zapcore.Entry{
		Level:      zapcore.InfoLevel,
		Time:       time.Unix(0, 0),
		LoggerName: "test",
		Message:    "info message",
		Caller:     zapcore.NewEntryCaller(uintptr(stack.Caller(3)), "file", 123, true),
	}
	encoder := zapcore.NewJSONEncoder(
		zapcore.EncoderConfig{
			StacktraceKey:  "stacktrace",
			SkipLineEnding: true,
			LineEnding:     zapcore.DefaultLineEnding,
			EncodeLevel:    zapcore.LowercaseLevelEncoder,
			EncodeTime:     zapcore.EpochTimeEncoder,
			EncodeDuration: zapcore.SecondsDurationEncoder,
			EncodeCaller:   zapcore.ShortCallerEncoder,
		})
	intField := zap.Int("key", 1)
	strField := zap.String("str", "1")
	boolField := zap.Bool("bool", true)
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "normal",
			args: args{
				jsonEncoder: encoder,
				entry:       entry,
				fields:      []zapcore.Field{intField},
			},
			want: `{"key":1}`,
		},
		{
			name: "remove first span",
			args: args{
				jsonEncoder: encoder,
				entry:       entry,
				fields:      []zapcore.Field{spanField, intField, strField},
			},
			want: `{"str":"1","key":1}`,
		},
		{
			name: "remove middle span",
			args: args{
				jsonEncoder: encoder,
				entry:       entry,
				fields:      []zapcore.Field{intField, spanField, strField},
			},
			want: `{"key":1,"str":"1"}`,
		},
		{
			name: "remove double middle span",
			args: args{
				jsonEncoder: encoder,
				entry:       entry,
				fields:      []zapcore.Field{intField, spanField, spanField, strField, boolField},
			},
			want: `{"key":1,"bool":true,"str":"1"}`,
		},
		{
			name: "remove end span",
			args: args{
				jsonEncoder: encoder,
				entry:       entry,
				fields:      []zapcore.Field{intField, strField, spanField, spanField},
			},
			want: `{"key":1,"str":"1"}`,
		},
		{
			name: "remove multi span",
			args: args{
				jsonEncoder: encoder,
				entry:       entry,
				fields:      []zapcore.Field{intField, strField, spanField, boolField, spanField, spanField},
			},
			want: `{"key":1,"str":"1","bool":true}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ReportZap(tt.args.jsonEncoder, tt.args.entry, tt.args.fields)
			require.Equal(t, nil, err)
			require.Equal(t, tt.want, got.String())
		})
	}
}

func TestGeneratedTraceContextSurvivesLogAndErrorExtraction(t *testing.T) {
	exportMux.Lock()
	defer exportMux.Unlock()

	collector := newRecordingBatchProcessor()
	provider := newMOTracerProvider(EnableTracer(true), WithBatchProcessor(collector))
	stubs := gostub.Stub(&GetTracerProvider, func() *MOTracerProvider { return provider })
	defer stubs.Reset()

	ctx, span := provider.Tracer("test").Start(context.Background(), "root", trace.WithNewRoot(true))
	spanContext := span.SpanContext()
	require.False(t, spanContext.IsEmpty())
	require.Equal(t, trace.SpanKindInternal, spanContext.Kind)

	// LocalFS spans were mo_ctl opt-in and disabled by default. Retiring their
	// recording/runtime control must preserve the parent correlation context,
	// rather than introducing localFSOperation into system.rawlog.
	ctx, retiredSpan := provider.Tracer("test").Start(ctx, "local-fs", trace.WithKind(trace.SpanKindLocalFSVis))
	require.IsType(t, trace.NoopSpan{}, retiredSpan)
	require.Equal(t, spanContext, trace.SpanFromContext(ctx).SpanContext())

	encoder := zapcore.NewJSONEncoder(zapcore.EncoderConfig{SkipLineEnding: true})
	_, err := ReportZap(encoder, zapcore.Entry{Level: zapcore.InfoLevel}, []zapcore.Field{trace.ContextField(ctx)})
	require.NoError(t, err)
	require.Len(t, collector.collected, 1)
	logItem, ok := collector.collected[0].(*MOZapLog)
	require.True(t, ok)
	defer logItem.Free()
	require.Equal(t, spanContext, *logItem.SpanContext)
	logRow := logView.OriginTable.GetRow(ctx)
	defer logRow.Free()
	logItem.FillRow(ctx, logRow)
	logValues := logRow.ToStrings()
	foundSpanKind := false
	for idx, column := range logRow.Table.Columns {
		if column.Name == spanKindCol.Name {
			foundSpanKind = true
			require.Equal(t, trace.SpanKindInternal.String(), logValues[idx])
		}
	}
	require.True(t, foundSpanKind)

	previousReporter := errutil.GetReportErrorFunc()
	errutil.SetErrorReporter(func(context.Context, error, int) {})
	defer errutil.SetErrorReporter(previousReporter)
	wrapped := errutil.WithContext(ctx, errors.New("generated trace context"))
	errorItem := &MOErrorHolder{Error: wrapped}
	row := errorView.OriginTable.GetRow(ctx)
	defer row.Free()
	errorItem.FillRow(ctx, row)
	values := row.ToStrings()
	var foundTraceID, foundSpanID bool
	for idx, column := range row.Table.Columns {
		switch column.Name {
		case traceIDCol.Name:
			foundTraceID = true
			require.Equal(t, spanContext.TraceID.String(), values[idx])
		case spanIDCol.Name:
			foundSpanID = true
			require.Equal(t, spanContext.SpanID.String(), values[idx])
		}
	}
	require.True(t, foundTraceID)
	require.True(t, foundSpanID)
}

var _ BatchProcessor = (*dummyCollectorCounter)(nil)
var _ DiscardableCollector = (*dummyCollectorCounter)(nil)

type dummyCollectorCounter struct {
	collectCnt atomic.Int64
	discardCnt atomic.Int64
}

func newDummyCollectorCounter() *dummyCollectorCounter {
	return &dummyCollectorCounter{}
}

func (d *dummyCollectorCounter) DiscardableCollect(ctx context.Context, name batchpipe.HasName) error {
	d.discardCnt.Add(1)
	return nil
}

func (d *dummyCollectorCounter) Collect(ctx context.Context, name batchpipe.HasName) error {
	d.collectCnt.Add(1)
	return nil
}

func (d *dummyCollectorCounter) Start() bool                                    { return true }
func (d *dummyCollectorCounter) Stop(graceful bool) error                       { return nil }
func (d *dummyCollectorCounter) Register(name batchpipe.HasName, impl PipeImpl) {}

func TestReportZap_Discardable(t *testing.T) {

	exportMux.Lock()
	defer exportMux.Unlock()

	// Setup a Runtime
	runtime.SetupServiceBasedRuntime("", runtime.NewRuntime(metadata.ServiceType_CN, "test", logutil.GetGlobalLogger()))

	collector := newDummyCollectorCounter()
	p := newMOTracerProvider(EnableTracer(true), WithBatchProcessor(collector))
	stubs := gostub.Stub(&GetTracerProvider, func() *MOTracerProvider {
		return p
	})
	defer stubs.Reset()

	logutil.Info("normal log 1")
	require.Equal(t, int64(1), collector.collectCnt.Load())

	logutil.Info("discard log 1", logutil.Discardable())
	require.Equal(t, int64(1), collector.discardCnt.Load())

	logger := runtime.ServiceRuntime("").Logger().With(logutil.Discardable())
	logger.Info("discard log 2")
	require.Equal(t, int64(2), collector.discardCnt.Load())
	logger.Info("discard log 3")
	require.Equal(t, int64(3), collector.discardCnt.Load())

	logutil.Info("normal log 2")
	require.Equal(t, int64(2), collector.collectCnt.Load())
}
