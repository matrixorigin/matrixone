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
	"time"

	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// GetServiceLogger returns service logger, it will using the service as the logger name, and
// append FieldNameServiceUUID field to the logger
func GetServiceLogger(logger *zap.Logger, service ServiceType, uuid string) *zap.Logger {
	return logger.Named(string(service)).With(zap.String(FieldNameServiceUUID, uuid))
}

// GetModuleLogger returns the module logger, it will add ".module" to logger name.
// e.g. if the logger's name is cn-service, module is txn, the new logger's name is
// "cn-service.txn".
func GetModuleLogger(logger *zap.Logger, module Module) *zap.Logger {
	return logger.Named(string(module))
}

// Log is the entry point for mo log printing. Return true to indicate that the log
// is being recorded by the current LogContext.
func (c LogContext) Log(msg string, fields ...zap.Field) bool {
	if c.logger == nil {
		panic("missing logger")
	}

	for _, fiter := range filters {
		if !fiter(c) {
			return false
		}
	}

	if ce := c.logger.Check(c.level, msg); ce != nil {
		if len(c.fields) == 0 {
			c.fields = fields
		} else {
			c.fields = append(c.fields, fields...)
		}
		if c.ctx != nil {
			c.fields = append(c.fields, trace.ContextField(c.ctx))
		}

		ce.Write(c.fields...)
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
func (c LogContext) LogAction(action string, fields ...zap.Field) func() {
	startAt := time.Now()
	if !c.Log(action, fields...) {
		return nothing
	}
	return func() {
		fields = append(fields, zap.Duration(FieldNameCost, time.Since(startAt)))
		c.Log(action, fields...)
	}
}

// LogContext used to describe how log are recorded
type LogContext struct {
	logger          *zap.Logger
	ctx             context.Context
	level           zapcore.Level
	fields          []zap.Field
	sampleType      SampleType
	samplefrequency uint64
}

// WithContext aappend ctx to the log context. Used to log trace information, e.g. trace id.
func (c LogContext) WithContext(ctx context.Context) LogContext {
	c.ctx = ctx
	return c
}

// WithProcess if the current log belongs to a certain process, the process name and process ID
// can be recorded. When analyzing the log, all related logs can be retrieved according to the
// process ID.
func (c LogContext) WithProcess(process Process, processID string) LogContext {
	c.fields = append(c.fields,
		zap.String(FieldNameProcess, string(process)),
		zap.String(FieldNameProcessID, processID))
	return c
}

// WithSampleLog sample print the log, using log counts as sampling frequency
func (c LogContext) WithSampleLog(sampleType SampleType, frequency uint64) LogContext {
	c.sampleType = sampleType
	c.samplefrequency = frequency
	return c
}

// Info info log context
func Info(logger *zap.Logger) LogContext {
	return defaultLogContext(logger, zap.InfoLevel)
}

// Error errror log context
func Error(logger *zap.Logger) LogContext {
	return defaultLogContext(logger, zap.ErrorLevel)
}

// Debug debug log context
func Debug(logger *zap.Logger) LogContext {
	return defaultLogContext(logger, zap.DebugLevel)
}

// Warn warn log context
func Warn(logger *zap.Logger) LogContext {
	return defaultLogContext(logger, zap.WarnLevel)
}

// Panic panic log context
func Panic(logger *zap.Logger) LogContext {
	return defaultLogContext(logger, zap.PanicLevel)
}

// Fatal fatal log context
func Fatal(logger *zap.Logger) LogContext {
	return defaultLogContext(logger, zap.FatalLevel)
}

func defaultLogContext(logger *zap.Logger, level zapcore.Level) LogContext {
	return LogContext{
		logger: logger,
		level:  level,
	}
}

func nothing() {}
