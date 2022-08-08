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
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func Debug(ctx context.Context, msg string, fields ...zap.Field) {
	fields = append(fields, ContextField(ctx))
	logutil.GetGlobalLogger().WithOptions(zap.AddCallerSkip(1)).Debug(msg, fields...)
}

func Info(ctx context.Context, msg string, fields ...zap.Field) {
	fields = append(fields, ContextField(ctx))
	logutil.GetGlobalLogger().WithOptions(zap.AddCallerSkip(1)).Info(msg, fields...)
}

func Warn(ctx context.Context, msg string, fields ...zap.Field) {
	fields = append(fields, ContextField(ctx))
	logutil.GetGlobalLogger().WithOptions(zap.AddCallerSkip(1)).Warn(msg, fields...)
}

func Error(ctx context.Context, msg string, fields ...zap.Field) {
	fields = append(fields, ContextField(ctx))
	logutil.GetGlobalLogger().WithOptions(zap.AddCallerSkip(1)).Error(msg, fields...)
}

func Panic(ctx context.Context, msg string, fields ...zap.Field) {
	fields = append(fields, ContextField(ctx))
	logutil.GetGlobalLogger().WithOptions(zap.AddCallerSkip(1)).Panic(msg, fields...)
}

func Fatal(ctx context.Context, msg string, fields ...zap.Field) {
	fields = append(fields, ContextField(ctx))
	logutil.GetGlobalLogger().WithOptions(zap.AddCallerSkip(1)).Fatal(msg, fields...)
}

// Debugf only use in develop mode
func Debugf(ctx context.Context, msg string, fields ...interface{}) {
	logutil.GetGlobalLogger().WithOptions(zap.AddCallerSkip(1), ContextFieldsOption(ctx)).Sugar().With().Debugf(msg, fields...)
}

// Infof only use in develop mode
func Infof(ctx context.Context, msg string, fields ...interface{}) {
	logutil.GetGlobalLogger().WithOptions(zap.AddCallerSkip(1), ContextFieldsOption(ctx)).Sugar().Infof(msg, fields...)
}

// Warnf only use in develop mode
func Warnf(ctx context.Context, msg string, fields ...interface{}) {
	logutil.GetGlobalLogger().WithOptions(zap.AddCallerSkip(1), ContextFieldsOption(ctx)).Sugar().Warnf(msg, fields...)
}

// Errorf only use in develop mode
func Errorf(ctx context.Context, msg string, fields ...interface{}) {
	logutil.GetGlobalLogger().WithOptions(zap.AddCallerSkip(1), ContextFieldsOption(ctx), zap.AddStacktrace(zap.ErrorLevel)).Sugar().Errorf(msg, fields...)
}

// Panicf only use in develop mode
func Panicf(ctx context.Context, msg string, fields ...interface{}) {
	logutil.GetGlobalLogger().WithOptions(zap.AddCallerSkip(1), ContextFieldsOption(ctx)).Sugar().Panicf(msg, fields...)
}

// Fatalf only use in develop mode
func Fatalf(ctx context.Context, msg string, fields ...interface{}) {
	logutil.GetGlobalLogger().WithOptions(zap.AddCallerSkip(1), ContextFieldsOption(ctx)).Sugar().Fatalf(msg, fields...)
}

// hook can catch zapcore.Entry, which can add by WithOptions(zap.Hooks(hook))
// But what we need is zapcore.CheckedEntry
// @deprecated
func hook(e zapcore.Entry) error { return nil }

var _ = hook(zapcore.Entry{})

func ContextFieldsOption(ctx context.Context) zap.Option {
	return zap.Fields(logutil.GetContextFieldFunc()(ctx))
}
func ContextField(ctx context.Context) zap.Field {
	return logutil.GetContextFieldFunc()(ctx)
}
