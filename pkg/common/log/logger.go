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

package log

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// GetServiceLogger returns service logger, it will using the service as the logger name, and
// append FieldNameServiceUUID field to the logger
func GetServiceLogger(logger *zap.Logger, service metadata.ServiceType, uuid string) *MOLogger {
	return wrap(logger.
		Named(fmt.Sprintf("%s-service", strings.ToLower(service.String()))).
		With(zap.String(FieldNameServiceUUID, uuid)))
}

// GetModuleLogger returns the module logger, it will add ".module" to logger name.
// e.g. if the logger's name is cn-service, module is txn, the new logger's name is
// "cn-service.txn".
func GetModuleLogger(logger *MOLogger, module Module) *MOLogger {
	return logger.derive(logger.logger.Named(string(module)), logger.ctx)
}

// With creates a child logger and adds structured context to it. Fields added
// to the child don't affect the parent, and vice versa.
func (l *MOLogger) With(fields ...zap.Field) *MOLogger {
	return l.derive(l.logger.With(fields...), l.ctx)
}

// WithOptions creates a child logger with zap options.
func (l *MOLogger) WithOptions(opts ...zap.Option) *MOLogger {
	return l.derive(l.logger.WithOptions(opts...), l.ctx)
}

// Named adds a new path segment to the logger's name. Segments are joined by
// periods. By default, Loggers are unnamed.
func (l *MOLogger) Named(name string) *MOLogger {
	return l.derive(l.logger.Named(name), l.ctx)
}

func (l *MOLogger) WithContext(ctx context.Context) *MOLogger {
	if ctx == nil {
		panic("nil context is not supported")
	}
	// When tracing is enabled, context.TODO() and context.Background() are not allowed
	// because they don't have trace information. When tracing is disabled, they are allowed.
	if trace.IsEnable() {
		if ctx == context.TODO() || ctx == context.Background() {
			panic("context.TODO() and context.Background() are not supported when tracing is enabled")
		}
		if sc := trace.SpanFromContext(ctx).SpanContext(); sc.IsEmpty() {
			panic("context with empty SpanContext are not supported when tracing is enabled")
		}
	}
	return l.derive(l.logger, ctx)
}

func newMOLogger(logger *zap.Logger, ctx context.Context) *MOLogger {
	return &MOLogger{
		logger: logger,
		ctx:    ctx,
		m: map[int]*zap.Logger{
			1: logger.WithOptions(zap.AddCallerSkip(1)),
			2: logger.WithOptions(zap.AddCallerSkip(2)),
			3: logger.WithOptions(zap.AddCallerSkip(3)),
		},
	}
}

func (l *MOLogger) derive(logger *zap.Logger, ctx context.Context) *MOLogger {
	return newMOLogger(logger, ctx)
}

// WithProcess if the current log belongs to a certain process, the process name and process ID
// can be recorded. When analyzing the log, all related logs can be retrieved according to the
// process ID.
func (l *MOLogger) WithProcess(process Process) *MOLogger {
	return l.With(zap.String(FieldNameProcess, string(process)))
}

// Enabled returns true if the level is enabled
func (l *MOLogger) Enabled(level zapcore.Level) bool {
	return l.logger.Core().Enabled(level)
}

// RawLogger returns the raw zap logger
func (l *MOLogger) RawLogger() *zap.Logger {
	return l.logger
}

// Info shortcuts to print info log
func (l *MOLogger) Info(msg string, fields ...zap.Field) bool {
	return l.Log(msg, DefaultLogOptions().WithLevel(zap.InfoLevel).AddCallerSkip(1), fields...)
}

// InfoAction shortcuts to print info action log
func (l *MOLogger) InfoAction(msg string, fields ...zap.Field) func() {
	return l.LogAction(msg, DefaultLogOptions().WithLevel(zap.InfoLevel).AddCallerSkip(1), fields...)
}

// Debug shortcuts to  print debug log
func (l *MOLogger) Debug(msg string, fields ...zap.Field) bool {
	return l.Log(msg, DefaultLogOptions().WithLevel(zap.DebugLevel).AddCallerSkip(1), fields...)
}

// InfoDebugAction shortcuts to print debug action log
func (l *MOLogger) DebugAction(msg string, fields ...zap.Field) func() {
	return l.LogAction(msg, DefaultLogOptions().WithLevel(zap.DebugLevel).AddCallerSkip(1), fields...)
}

// Error shortcuts to  print error log
func (l *MOLogger) Error(msg string, fields ...zap.Field) bool {
	return l.Log(msg, DefaultLogOptions().WithLevel(zap.ErrorLevel).AddCallerSkip(1), fields...)
}

// Warn shortcuts to  print warn log
func (l *MOLogger) Warn(msg string, fields ...zap.Field) bool {
	return l.Log(msg, DefaultLogOptions().WithLevel(zap.WarnLevel).AddCallerSkip(1), fields...)
}

// Panic shortcuts to  print panic log
func (l *MOLogger) Panic(msg string, fields ...zap.Field) bool {
	return l.Log(msg, DefaultLogOptions().WithLevel(zap.PanicLevel).AddCallerSkip(1), fields...)
}

// Fatal shortcuts to print fatal log
func (l *MOLogger) Fatal(msg string, fields ...zap.Field) bool {
	return l.Log(msg, DefaultLogOptions().WithLevel(zap.FatalLevel).AddCallerSkip(1), fields...)
}

// DebugEvent, InfoEvent, WarnEvent, and ErrorEvent use the same stable Event
// contract as the global logger while preserving this service logger's context.
func (l *MOLogger) DebugEvent(event logutil.Event, fields ...zap.Field) bool {
	return l.logEvent(event, DefaultLogOptions().WithLevel(zap.DebugLevel).AddCallerSkip(2), logutil.DefaultRateLimitConfig, fields...)
}
func (l *MOLogger) InfoEvent(event logutil.Event, fields ...zap.Field) bool {
	return l.logEvent(event, DefaultLogOptions().WithLevel(zap.InfoLevel).AddCallerSkip(2), logutil.DefaultRateLimitConfig, fields...)
}
func (l *MOLogger) WarnEvent(event logutil.Event, fields ...zap.Field) bool {
	return l.logEvent(event, DefaultLogOptions().WithLevel(zap.WarnLevel).AddCallerSkip(2), logutil.DefaultRateLimitConfig, fields...)
}
func (l *MOLogger) ErrorEvent(event logutil.Event, fields ...zap.Field) bool {
	return l.logEvent(event, DefaultLogOptions().WithLevel(zap.ErrorLevel).AddCallerSkip(2), logutil.DefaultRateLimitConfig, fields...)
}
func (l *MOLogger) DebugEventLazy(event logutil.Event, build logutil.EventFieldBuilder) bool {
	return l.logEventLazy(event, DefaultLogOptions().WithLevel(zap.DebugLevel).AddCallerSkip(2), build)
}
func (l *MOLogger) InfoEventLazy(event logutil.Event, build logutil.EventFieldBuilder) bool {
	return l.logEventLazy(event, DefaultLogOptions().WithLevel(zap.InfoLevel).AddCallerSkip(2), build)
}
func (l *MOLogger) WarnEventLazy(event logutil.Event, build logutil.EventFieldBuilder) bool {
	return l.logEventLazy(event, DefaultLogOptions().WithLevel(zap.WarnLevel).AddCallerSkip(2), build)
}
func (l *MOLogger) ErrorEventLazy(event logutil.Event, build logutil.EventFieldBuilder) bool {
	return l.logEventLazy(event, DefaultLogOptions().WithLevel(zap.ErrorLevel).AddCallerSkip(2), build)
}

func (l *MOLogger) logEventLazy(event logutil.Event, opts LogOptions, build logutil.EventFieldBuilder) bool {
	if !l.Enabled(opts.level) {
		return false
	}
	decision, ok := logutil.AllowEvent(event.Name, logutil.DefaultRateLimitConfig)
	if !ok {
		return false
	}
	var fields []zap.Field
	if build != nil {
		fields = build()
	}
	return l.logEventDecision(event.Message, opts, decision, fields)
}

func (l *MOLogger) logEvent(event logutil.Event, opts LogOptions, config logutil.RateLimitConfig, fields ...zap.Field) bool {
	if !l.Enabled(opts.level) {
		return false
	}
	decision, ok := logutil.AllowEvent(event.Name, config)
	if !ok {
		return false
	}
	return l.logEventDecision(event.Message, opts, decision, fields)
}

func (l *MOLogger) logEventDecision(msg string, opts LogOptions, decision logutil.RateLimitDecision, fields []zap.Field) bool {
	return l.logRaw(msg, opts, logutil.EventFieldsWithDecision(fields, decision)...)
}

// Log is the entry point for mo log printing. Return true to indicate that the log
// is being recorded by the current LogContext.
func (l *MOLogger) Log(msg string, opts LogOptions, fields ...zap.Field) bool {
	return l.logRaw(msg, opts, fields...)
}

func (l *MOLogger) logRaw(msg string, opts LogOptions, fields ...zap.Field) bool {
	if l.logger == nil {
		panic("missing logger")
	}

	for _, filter := range filters {
		if !filter(opts) {
			return false
		}
	}

	if opts.ctx == nil {
		opts.ctx = l.ctx
	}

	logger, has := l.m[opts.callerSkip+1]
	if !has {
		logger = l.logger
	}
	if ce := logger.Check(opts.level, msg); ce != nil {
		out := fields
		if len(opts.fields) > 0 || opts.ctx != nil {
			// Copy only when adding contextual fields. This keeps the legacy
			// hot path allocation-free while ensuring appends never mutate a
			// caller-owned backing array.
			out = append([]zap.Field(nil), fields...)
			out = append(out, opts.fields...)
			if opts.ctx != nil {
				out = append(out, trace.ContextField(opts.ctx))
			}
		}

		ce.Write(out...)
		return true
	}
	return false
}

// LogAction used to log an action, or generate 2 logs, the first log occurring
// at the place where the call is made and the second log occurring at the end
// of the function where the LogAction is called, with the additional time consuming.
// e.g.:
//
//	func LogActionExample() {
//	    defer log.Info(zapLogger).LogAction("example action")()
//	}
//
// This method should often be used to log the elapsed time of a function and, as the
// logs appear in pairs, can also be used to check whether a function has been executed.
func (l *MOLogger) LogAction(action string, opts LogOptions, fields ...zap.Field) func() {
	if !l.Log(action, opts.AddCallerSkip(1), fields...) {
		return nothing
	}
	startAt := time.Now()
	return func() {
		fields = append(fields, zap.Duration(FieldNameCost, time.Since(startAt)))
		l.Log(action, opts, fields...)
	}
}

func wrap(logger *zap.Logger) *MOLogger {
	return wrapWithContext(logger, nil)
}

func wrapWithContext(logger *zap.Logger, ctx context.Context) *MOLogger {
	if logger == nil {
		panic("zap logger is nil")
	}
	// When tracing is enabled, context.TODO() and context.Background() are not allowed
	// because they don't have trace information. When tracing is disabled, they are allowed.
	if ctx != nil && trace.IsEnable() {
		if ctx == context.TODO() || ctx == context.Background() {
			panic("TODO and Background are not supported when tracing is enabled")
		}
		if sc := trace.SpanFromContext(ctx).SpanContext(); sc.IsEmpty() {
			panic("context with empty SpanContext are not supported when tracing is enabled")
		}
	}

	return newMOLogger(
		logger,
		ctx,
	)
}

func nothing() {}

// DefaultLogOptions default log options
func DefaultLogOptions() LogOptions {
	return LogOptions{}
}

// WithContext set log trace context.
func (opts LogOptions) WithContext(ctx context.Context) LogOptions {
	if ctx == nil {
		panic("context is nil")
	}
	// When tracing is enabled, context.TODO() and context.Background() are not allowed
	// because they don't have trace information. When tracing is disabled, they are allowed.
	if trace.IsEnable() {
		if ctx == context.TODO() || ctx == context.Background() {
			panic("TODO and Background contexts are not supported when tracing is enabled")
		}
		if sc := trace.SpanFromContext(ctx).SpanContext(); sc.IsEmpty() {
			panic("context with empty SpanContext are not supported when tracing is enabled")
		}
	}

	opts.ctx = ctx
	return opts
}

// WithLevel set log print level
func (opts LogOptions) WithLevel(level zapcore.Level) LogOptions {
	opts.level = level
	return opts
}

// WithSample sample print the log, using log counts as sampling frequency. First time must output.
func (opts LogOptions) WithSample(sampleType SampleType) LogOptions {
	opts.sampleType = sampleType
	return opts
}

// AddCallerSkip help to show the logger real caller, by skip n call stack
func (opts LogOptions) AddCallerSkip(n int) LogOptions {
	opts.callerSkip += n
	return opts
}

// WithProcess if the current log belongs to a certain process, the process name and process ID
// can be recorded. When analyzing the log, all related logs can be retrieved according to the
// process ID.
func (opts LogOptions) WithProcess(process Process, processID string) LogOptions {
	opts.fields = append(opts.fields,
		zap.String(FieldNameProcess, string(process)),
		zap.String(FieldNameProcessID, processID))
	return opts
}
