// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package logutil

import (
	"go.uber.org/zap"
	"net/http"
)

func Debug(msg string, fields ...zap.Field) {
	L().WithOptions(zap.AddCallerSkip(1)).Debug(msg, fields...)
}

// Debugf only use in develop mode
func Debugf(msg string, fields ...interface{}) {
	L().WithOptions(zap.AddCallerSkip(1)).Sugar().Debugf(msg, fields)
}

func Info(msg string, fields ...zap.Field) {
	L().WithOptions(zap.AddCallerSkip(1)).Info(msg, fields...)
}

// Infof only use in develop mode
func Infof(msg string, fields ...interface{}) {
	L().WithOptions(zap.AddCallerSkip(1)).Sugar().Infof(msg, fields)
}

func Warn(msg string, fields ...zap.Field) {
	L().WithOptions(zap.AddCallerSkip(1)).Warn(msg, fields...)
}

func Warnf(msg string, fields ...interface{}) {
	L().WithOptions(zap.AddCallerSkip(1)).Sugar().Warnf(msg, fields)
}

func Error(msg string, fields ...zap.Field) {
	L().WithOptions(zap.AddCallerSkip(1)).Error(msg, fields...)
}

// Errorf only use in develop mode
func Errorf(msg string, fields ...interface{}) {
	L().WithOptions(zap.AddCallerSkip(1)).Sugar().Errorf(msg, fields...)
}

func Panic(msg string, fields ...zap.Field) {
	L().WithOptions(zap.AddCallerSkip(1)).Panic(msg, fields...)
}

func Fatal(msg string, fields ...zap.Field) {
	L().WithOptions(zap.AddCallerSkip(1)).Fatal(msg, fields...)
}

// Fatalf only use in develop mode
func Fatalf(msg string, fields ...interface{}) {
	L().WithOptions(zap.AddCallerSkip(1)).Sugar().Fatalf(msg, fields...)
}

// With creates a child logger and adds structured context to it.
// Fields added to the child don't affect the parent, and vice versa.
func With(fields ...zap.Field) *zap.Logger {
	return L().WithOptions(zap.AddCallerSkip(1)).With(fields...)
}

func handleLevelChange(port string, pattern string, level zap.AtomicLevel) {
	http.HandleFunc(pattern, level.ServeHTTP)
	go func() {
		if err := http.ListenAndServe(port, nil); err != nil {
			panic(err)
		}
	}()
}

type GoettyLogger struct {}

func (l *GoettyLogger) Infof(msg string, fields ...interface{}) {
	Infof(msg, fields)
}

func (l *GoettyLogger) Debugf(msg string, fields ...interface{}) {
	Debugf(msg, fields)
}

func (l *GoettyLogger) Errorf(msg string, fields ...interface{}) {
	Errorf(msg, fields)
}

func (l *GoettyLogger) Fatalf(msg string, fields ...interface{}) {
	Fatalf(msg, fields)
}