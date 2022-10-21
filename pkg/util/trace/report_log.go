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
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/util/stack"
	"go.uber.org/zap/buffer"

	"github.com/matrixorigin/matrixone/pkg/util"
	"github.com/matrixorigin/matrixone/pkg/util/batchpipe"
	"github.com/matrixorigin/matrixone/pkg/util/export"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func init() {
	SetLogLevel(zapcore.DebugLevel)
}

var _ batchpipe.HasName = (*MOLog)(nil)
var _ IBuffer2SqlItem = (*MOLog)(nil)
var _ CsvFields = (*MOLog)(nil)

type MOLog struct {
	TraceID   TraceID       `json:"trace_id"`
	SpanID    SpanID        `json:"span_id"`
	Timestamp util.TimeNano `json:"timestamp"`
	Level     zapcore.Level `json:"level"`
	Caller    stack.Frame   `json:"caller"` // like "util/trace/trace.go:666"
	Name      string        `json:"name"`
	Message   string        `json:"Message"`
	Extra     string        `json:"extra"` // like json text
}

func newMOLog() *MOLog {
	return &MOLog{}
}

func (MOLog) GetName() string {
	return MORawLogType
}

func (l MOLog) Size() int64 {
	return int64(unsafe.Sizeof(l)) + int64(len(l.Message))
}

func (l MOLog) Free() {}

func (l MOLog) CsvFields() []string {
	var result []string
	result = append(result, l.TraceID.String())
	result = append(result, l.SpanID.String())
	result = append(result, GetNodeResource().NodeUuid)
	result = append(result, GetNodeResource().NodeType)
	result = append(result, nanoSec2DatetimeString(l.Timestamp))
	result = append(result, l.Name)
	result = append(result, l.Level.String())
	result = append(result, fmt.Sprintf(logStackFormatter.Load().(string), l.Caller))
	result = append(result, l.Message)
	result = append(result, l.Extra)
	return result
}

var logLevelEnabler atomic.Value

func SetLogLevel(l zapcore.LevelEnabler) {
	logLevelEnabler.Store(l)
}

func ReportLog(ctx context.Context, level zapcore.Level, depth int, formatter string, args ...any) {
	if !logLevelEnabler.Load().(zapcore.LevelEnabler).Enabled(level) {
		return
	}
	if !GetTracerProvider().IsEnable() {
		return
	}
	_, newSpan := Start(DefaultContext(), "ReportLog")
	defer newSpan.End()

	sc := SpanFromContext(ctx).SpanContext()
	if sc.IsEmpty() {
		sc = *DefaultSpanContext()
	}
	log := newMOLog()
	log.TraceID = sc.TraceID
	log.SpanID = sc.SpanID
	log.Timestamp = util.NowNS()
	log.Level = level
	log.Caller = stack.Caller(depth + 1)
	log.Message = fmt.Sprintf(formatter, args...)
	log.Extra = "{}"
	export.GetGlobalBatchProcessor().Collect(DefaultContext(), log)
}

func ContextField(ctx context.Context) zap.Field {
	return SpanField(SpanFromContext(ctx).SpanContext())
}

var _ batchpipe.HasName = (*MOZapLog)(nil)
var _ IBuffer2SqlItem = (*MOZapLog)(nil)
var _ CsvFields = (*MOZapLog)(nil)

type MOZapLog struct {
	Level       zapcore.Level `json:"Level"`
	SpanContext *SpanContext  `json:"span"`
	Timestamp   time.Time     `json:"timestamp"`
	LoggerName  string
	Caller      string `json:"caller"` // like "util/trace/trace.go:666"
	Message     string `json:"message"`
	Extra       string `json:"extra"` // like json text
}

func newMOZap() *MOZapLog {
	return &MOZapLog{}
}

func (m *MOZapLog) GetName() string {
	return MOLogType
}

// Size 计算近似值
func (m *MOZapLog) Size() int64 {
	return int64(unsafe.Sizeof(m) + unsafe.Sizeof(len(m.LoggerName)+len(m.Caller)+len(m.Message)+len(m.Extra)))
}

func (m *MOZapLog) Free() {
	m.SpanContext = nil
	m.LoggerName = ""
	m.Caller = ""
	m.Message = ""
	m.Extra = ""
}

func (m *MOZapLog) CsvFields() []string {
	var result []string
	result = append(result, m.SpanContext.TraceID.String())
	result = append(result, m.SpanContext.SpanID.String())
	result = append(result, GetNodeResource().NodeUuid)
	result = append(result, GetNodeResource().NodeType)
	result = append(result, time2DatetimeString(m.Timestamp))
	result = append(result, m.LoggerName)
	result = append(result, m.Level.String())
	result = append(result, m.Caller)
	result = append(result, m.Message)
	result = append(result, m.Extra)
	return result
}

func ReportZap(jsonEncoder zapcore.Encoder, entry zapcore.Entry, fields []zapcore.Field) (*buffer.Buffer, error) {
	var needReport = true
	if !GetTracerProvider().IsEnable() {
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
