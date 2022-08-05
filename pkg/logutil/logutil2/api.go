// Copyright 2021 Matrix Origin
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

package logutil2

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/util/trace"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Debugf only use in develop mode
func Debugf(ctx context.Context, msg string, fields ...interface{}) {
	trace.ReportLog(ctx, zapcore.DebugLevel, 1, msg, fields...)
	logutil.GetGlobalLogger().WithOptions(zap.AddCallerSkip(1)).Sugar().Debugf(msg, fields...)
}

// Infof only use in develop mode
func Infof(ctx context.Context, msg string, fields ...interface{}) {
	trace.ReportLog(ctx, zapcore.InfoLevel, 1, msg, fields...)
	logutil.GetGlobalLogger().WithOptions(zap.AddCallerSkip(1)).Sugar().Infof(msg, fields...)
}

// Warnf only use in develop mode
func Warnf(ctx context.Context, msg string, fields ...interface{}) {
	trace.ReportLog(ctx, zapcore.WarnLevel, 1, msg, fields...)
	logutil.GetGlobalLogger().WithOptions(zap.AddCallerSkip(1)).Sugar().Warnf(msg, fields...)
}

// Errorf only use in develop mode
func Errorf(ctx context.Context, msg string, fields ...interface{}) {
	trace.ReportLog(ctx, zapcore.ErrorLevel, 1, msg, fields...)
	logutil.GetGlobalLogger().WithOptions(zap.AddStacktrace(zap.ErrorLevel)).Sugar().Errorf(msg, fields...)
}

// Panicf only use in develop mode
func Panicf(ctx context.Context, msg string, fields ...interface{}) {
	trace.ReportLog(ctx, zapcore.PanicLevel, 1, msg, fields...)
	logutil.GetGlobalLogger().WithOptions(zap.AddCallerSkip(1)).Sugar().Panicf(msg, fields...)
}

// Fatalf only use in develop mode
func Fatalf(ctx context.Context, msg string, fields ...interface{}) {
	trace.ReportLog(ctx, zapcore.FatalLevel, 1, msg, fields...)
	logutil.GetGlobalLogger().WithOptions(zap.AddCallerSkip(1)).Sugar().Fatalf(msg, fields...)
}
