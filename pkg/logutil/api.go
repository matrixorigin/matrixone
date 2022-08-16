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

package logutil

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func Debug(msg string, fields ...zap.Field) {
	GetGlobalLogger().WithOptions(zap.AddCallerSkip(1)).Debug(msg, fields...)
}

func Info(msg string, fields ...zap.Field) {
	GetGlobalLogger().WithOptions(zap.AddCallerSkip(1)).Info(msg, fields...)
}

func Warn(msg string, fields ...zap.Field) {
	GetGlobalLogger().WithOptions(zap.AddCallerSkip(1)).Warn(msg, fields...)
}

func Error(msg string, fields ...zap.Field) {
	GetGlobalLogger().WithOptions(zap.AddCallerSkip(1)).Error(msg, fields...)
}

func Panic(msg string, fields ...zap.Field) {
	GetGlobalLogger().WithOptions(zap.AddCallerSkip(1)).Panic(msg, fields...)
}

func Fatal(msg string, fields ...zap.Field) {
	GetGlobalLogger().WithOptions(zap.AddCallerSkip(1)).Fatal(msg, fields...)
}

// Debugf only use in develop mode
func Debugf(msg string, fields ...interface{}) {
	GetGlobalLogger().WithOptions(zap.AddCallerSkip(1)).Sugar().Debugf(msg, fields...)
}

// Infof only use in develop mode
func Infof(msg string, fields ...interface{}) {
	GetGlobalLogger().WithOptions(zap.AddCallerSkip(1)).Sugar().Infof(msg, fields...)
}

// Warnf only use in develop mode
func Warnf(msg string, fields ...interface{}) {
	GetGlobalLogger().WithOptions(zap.AddCallerSkip(1)).Sugar().Warnf(msg, fields...)
}

// Errorf only use in develop mode
func Errorf(msg string, fields ...interface{}) {
	GetGlobalLogger().WithOptions(zap.AddCallerSkip(1), zap.AddStacktrace(zap.ErrorLevel)).Sugar().Errorf(msg, fields...)
}

// Panicf only use in develop mode
func Panicf(msg string, fields ...interface{}) {
	GetGlobalLogger().WithOptions(zap.AddCallerSkip(1)).Sugar().Panicf(msg, fields...)
}

// Fatalf only use in develop mode
func Fatalf(msg string, fields ...interface{}) {
	GetGlobalLogger().WithOptions(zap.AddCallerSkip(1)).Sugar().Fatalf(msg, fields...)
}

// TODO: uncomment the function when changing log level at runtime is required
//func handleLevelChange(port string, pattern string, level zap.AtomicLevel) {
//	http.HandleFunc(pattern, level.ServeHTTP)
//	go func() {
//		if err := http.ListenAndServe(port, nil); err != nil {
//			panic(err)
//		}
//	}()
//}

type GoettyLogger struct{}

func (l *GoettyLogger) Infof(msg string, fields ...interface{}) {
	Infof(msg, fields...)
}

func (l *GoettyLogger) Debugf(msg string, fields ...interface{}) {
	Debugf(msg, fields...)
}

func (l *GoettyLogger) Errorf(msg string, fields ...interface{}) {
	Errorf(msg, fields...)
}

func (l *GoettyLogger) Fatalf(msg string, fields ...interface{}) {
	Fatalf(msg, fields...)
}

// Adjust returns default logger if logger is nil
func Adjust(logger *zap.Logger, options ...zap.Option) *zap.Logger {
	if logger != nil {
		return logger
	}
	return GetLogger(options...)
}

// GetLoggerWithOptions get default zap logger
func GetLoggerWithOptions(level zapcore.LevelEnabler, encoder zapcore.Encoder, syncer zapcore.WriteSyncer, options ...zap.Option) *zap.Logger {
	var cores []zapcore.Core
	options = append(options, zap.AddStacktrace(zapcore.FatalLevel), zap.AddCaller())
	if syncer == nil {
		syncer = getConsoleSyncer()
	}
	if encoder == nil {
		encoder = getLoggerEncoder("")
	}
	cores = append(cores, zapcore.NewCore(encoder, syncer, level))

	if EnableStoreDB() {
		encoder, syncer := getTraceLogSinks()
		cores = append(cores, zapcore.NewCore(encoder, syncer, level))
	}

	GetLevelChangeFunc()(level)
	return zap.New(zapcore.NewTee(cores...), options...)
}

// GetLogger get default zap logger
func GetLogger(options ...zap.Option) *zap.Logger {
	return GetLoggerWithOptions(zapcore.InfoLevel, nil, nil, options...)
}

// GetPanicLogger returns a zap logger which will panic on Fatal(). The
// returned zap logger should only be used in tests.
func GetPanicLogger(options ...zap.Option) *zap.Logger {
	return GetPanicLoggerWithLevel(zapcore.InfoLevel, options...)
}

// GetPanicLoggerWithLevel returns a zap logger which will panic on Fatal(). The
// returned zap logger should only be used in tests.
func GetPanicLoggerWithLevel(level zapcore.Level, options ...zap.Option) *zap.Logger {
	return GetLoggerWithOptions(level, nil, nil,
		zap.OnFatal(zapcore.WriteThenPanic))
}
