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

package trace

import (
	"context"
	"fmt"
	"go.uber.org/zap/buffer"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/util"
	"github.com/matrixorigin/matrixone/pkg/util/batchpipe"
	"github.com/matrixorigin/matrixone/pkg/util/export"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func init() {
	SetLogLevel(zapcore.DebugLevel)
}

var _ batchpipe.HasName = &MOLog{}
var _ IBuffer2SqlItem = &MOLog{}

type MOLog struct {
	StatementId TraceID       `json:"statement_id"`
	SpanId      SpanID        `json:"span_id"`
	Timestamp   util.TimeNano `json:"timestamp"`
	Level       zapcore.Level `json:"level"`
	Caller      util.Frame    `json:"caller"` // like "util/trace/trace.go:666"
	Name        string        `json:"name"`
	Message     string        `json:"Message"`
	Extra       string        `json:"extra"` // like json text
}

func newMOLog() *MOLog {
	return &MOLog{}
}

func (MOLog) GetName() string {
	return MOLogType
}

func (l MOLog) Size() int64 {
	return int64(unsafe.Sizeof(l)) + int64(len(l.Message))
}

func (l MOLog) Free() {}

var logLevelEnabler atomic.Value

func SetLogLevel(l zapcore.LevelEnabler) {
	logLevelEnabler.Store(l)
}

func ReportLog(ctx context.Context, level zapcore.Level, depth int, formatter string, args ...any) {
	if !logLevelEnabler.Load().(zapcore.LevelEnabler).Enabled(level) {
		return
	}
	if !gTracerProvider.IsEnable() {
		return
	}
	_, newSpan := Start(DefaultContext(), "ReportLog")
	defer newSpan.End()

	span := SpanFromContext(ctx)
	sc := span.SpanContext()
	if sc.IsEmpty() {
		span = SpanFromContext(DefaultContext())
		sc = span.SpanContext()
	}
	log := newMOLog()
	log.StatementId = sc.TraceID
	log.SpanId = sc.SpanID
	log.Timestamp = util.NowNS()
	log.Level = level
	log.Caller = util.Caller(depth + 1)
	log.Message = fmt.Sprintf(formatter, args...)
	log.Extra = "{}"
	export.GetGlobalBatchProcessor().Collect(DefaultContext(), log)
}

func ContextField(ctx context.Context) zap.Field {
	return SpanField(SpanFromContext(ctx).SpanContext())
}

var _ batchpipe.HasName = &MOZap{}
var _ IBuffer2SqlItem = &MOZap{}

type MOZap struct {
	Level       zapcore.Level `json:"Level"`
	SpanContext *SpanContext  `json:"span"`
	Timestamp   time.Time     `json:"timestamp"`
	LoggerName  string
	Caller      string `json:"caller"` // like "util/trace/trace.go:666"
	Message     string `json:"message"`
	Extra       string `json:"extra"` // like json text
}

func newMOZap() *MOZap {
	return &MOZap{}
}

func (m MOZap) GetName() string {
	return MOZapType
}

// Size 计算近似值
func (m MOZap) Size() int64 {
	return int64(unsafe.Sizeof(m) + unsafe.Sizeof(len(m.LoggerName)+len(m.Caller)+len(m.Message)+len(m.Extra)))
}

func (m MOZap) Free() {}

const moInternalFiledKeyNoopReport = "moInternalFiledKeyNoopReport"

func NoReportFiled() zap.Field {
	return zap.Bool(moInternalFiledKeyNoopReport, false)
}

func ReportZap(jsonEncoder zapcore.Encoder, entry zapcore.Entry, fields []zapcore.Field) (*buffer.Buffer, error) {
	var needReport = true
	if !gTracerProvider.IsEnable() {
		return jsonEncoder.EncodeEntry(entry, []zap.Field{})
	}
	log := newMOZap()
	log.LoggerName = entry.LoggerName
	log.Level = entry.Level
	log.Message = entry.Message
	log.Caller = entry.Caller.TrimmedPath()
	log.Timestamp = entry.Time
	log.SpanContext = DefaultSpanContext()
	for _, v := range fields {
		if IsSpanField(v) {
			log.SpanContext = v.Interface.(*SpanContext)
			break
		}
		if v.Type == zapcore.BoolType && v.Key == moInternalFiledKeyNoopReport {
			needReport = false
			break
		}
	}
	if !needReport {
		log.Free()
		return jsonEncoder.EncodeEntry(entry, []zap.Field{})
	}
	buffer, err := jsonEncoder.EncodeEntry(entry, fields)
	log.Extra = buffer.String()
	export.GetGlobalBatchProcessor().Collect(DefaultContext(), log)
	return buffer, err
}
