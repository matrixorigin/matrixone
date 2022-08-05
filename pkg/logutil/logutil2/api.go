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
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func Debug(ctx context.Context, msg string, fields ...zap.Field) {
	logutil.GetGlobalLogger().WithOptions(zap.AddCallerSkip(1), logutil.ContextFields()(ctx)).Debug(msg, fields...)
}

func Info(ctx context.Context, msg string, fields ...zap.Field) {
	logutil.GetGlobalLogger().WithOptions(zap.AddCallerSkip(1), logutil.ContextFields()(ctx)).Info(msg, fields...)
}

func Warn(ctx context.Context, msg string, fields ...zap.Field) {
	logutil.GetGlobalLogger().WithOptions(zap.AddCallerSkip(1), logutil.ContextFields()(ctx)).Warn(msg, fields...)
}

func Error(ctx context.Context, msg string, fields ...zap.Field) {
	logutil.GetGlobalLogger().WithOptions(zap.AddCallerSkip(1), logutil.ContextFields()(ctx)).Error(msg, fields...)
}

func Panic(ctx context.Context, msg string, fields ...zap.Field) {
	logutil.GetGlobalLogger().WithOptions(zap.AddCallerSkip(1), logutil.ContextFields()(ctx)).Panic(msg, fields...)
}

func Fatal(ctx context.Context, msg string, fields ...zap.Field) {
	logutil.GetGlobalLogger().WithOptions(zap.AddCallerSkip(1), logutil.ContextFields()(ctx)).Fatal(msg, fields...)
}

// Debugf only use in develop mode
func Debugf(ctx context.Context, msg string, fields ...interface{}) {
	logutil.GetGlobalLogger().WithOptions(zap.AddCallerSkip(1), logutil.ContextFields()(ctx)).Sugar().Debugf(msg, fields...)
}

// Infof only use in develop mode
func Infof(ctx context.Context, msg string, fields ...interface{}) {
	logutil.GetGlobalLogger().WithOptions(zap.AddCallerSkip(1), logutil.ContextFields()(ctx)).Sugar().Infof(msg, fields...)
}

// Warnf only use in develop mode
func Warnf(ctx context.Context, msg string, fields ...interface{}) {
	logutil.GetGlobalLogger().WithOptions(zap.AddCallerSkip(1), logutil.ContextFields()(ctx)).Sugar().Warnf(msg, fields...)
}

// Errorf only use in develop mode
func Errorf(ctx context.Context, msg string, fields ...interface{}) {
	logutil.GetGlobalLogger().WithOptions(zap.AddCallerSkip(1), logutil.ContextFields()(ctx), zap.AddStacktrace(zap.ErrorLevel)).Sugar().Errorf(msg, fields...)
}

// Panicf only use in develop mode
func Panicf(ctx context.Context, msg string, fields ...interface{}) {
	logutil.GetGlobalLogger().WithOptions(zap.AddCallerSkip(1), logutil.ContextFields()(ctx)).Sugar().Panicf(msg, fields...)
}

// Fatalf only use in develop mode
func Fatalf(ctx context.Context, msg string, fields ...interface{}) {
	logutil.GetGlobalLogger().WithOptions(zap.AddCallerSkip(1), logutil.ContextFields()(ctx)).Sugar().Fatalf(msg, fields...)
}

// hook can catch zapcore.Entry, which can add by WithOptions(zap.Hooks(hook))
// But what we need is zapcore.CheckedEntry
// @deprecated
func hook(e zapcore.Entry) error {
	fmt.Printf("entry: %v\n", e)
	return nil
}
