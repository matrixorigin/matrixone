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
	"context"
	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"
	"sync/atomic"
)

// logReporter should be trace.ReportLog
var logReporter atomic.Value

var zapReporter atomic.Value

// logReporter should be trace.SetLogLevel
var levelChangeFunc atomic.Value

type TraceReporter struct {
	ReportLog   reportLogFunc
	ReportZap   reportZapFunc
	LevelSignal levelChangeSignal
}

type reportLogFunc func(context.Context, zapcore.Level, int, string, ...any)
type reportZapFunc func(zapcore.Encoder, zapcore.Entry, []zapcore.Field)
type levelChangeSignal func(zapcore.LevelEnabler)

func noopReportLog(context.Context, zapcore.Level, int, string, ...any) {}
func noopReportZap(zapcore.Encoder, zapcore.Entry, []zapcore.Field)     {}
func noopLevelSignal(zapcore.LevelEnabler)                              {}

func SetLogReporter(r *TraceReporter) {
	if r.ReportLog != nil {
		logReporter.Store(r.ReportLog)
	}
	if r.ReportZap != nil {
		zapReporter.Store(r.ReportZap)
	}
	if r.LevelSignal != nil {
		levelChangeFunc.Store(r.LevelSignal)
	}
}

func GetReportLogFunc() reportLogFunc {
	return logReporter.Load().(reportLogFunc)
}

func GetReportZapFunc() reportZapFunc {
	return zapReporter.Load().(reportZapFunc)
}

func GetLevelChangeFunc() levelChangeSignal {
	return levelChangeFunc.Load().(levelChangeSignal)
}

var _ zapcore.Encoder = (*TraceLogEncoder)(nil)

type TraceLogEncoder struct {
	zapcore.Encoder
}

func (e *TraceLogEncoder) EncodeEntry(entry zapcore.Entry, fields []zapcore.Field) (*buffer.Buffer, error) {
	GetReportZapFunc()(e.Encoder, entry, fields)
	return buffer.NewPool().Get(), nil
}

func newTraceLogEncoder() *TraceLogEncoder {
	// default like zap.NewProductionEncoderConfig()
	e := &TraceLogEncoder{
		Encoder: zapcore.NewJSONEncoder(
			zapcore.EncoderConfig{
				StacktraceKey:  "stacktrace",
				LineEnding:     zapcore.DefaultLineEnding,
				EncodeLevel:    zapcore.LowercaseLevelEncoder,
				EncodeTime:     zapcore.EpochTimeEncoder,
				EncodeDuration: zapcore.SecondsDurationEncoder,
				EncodeCaller:   zapcore.ShortCallerEncoder,
			}),
	}
	return e
}
