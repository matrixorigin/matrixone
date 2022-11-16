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
	"time"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/util/batchpipe"
	"github.com/matrixorigin/matrixone/pkg/util/export"

	"go.uber.org/zap"
	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"
)

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
	Stack       string `json:"stack"`
}

func newMOZap() *MOZapLog {
	return &MOZapLog{}
}

func (m *MOZapLog) GetName() string {
	return logView.OriginTable.GetName()
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

func (m *MOZapLog) GetRow() *export.Row { return logView.OriginTable.GetRow() }

func (m *MOZapLog) CsvFields(row *export.Row) []string {
	row.Reset()
	row.SetColumnVal(rawItemCol, logView.Table)
	row.SetColumnVal(traceIDCol, m.SpanContext.TraceID.String())
	row.SetColumnVal(spanIDCol, m.SpanContext.SpanID.String())
	row.SetColumnVal(spanKindCol, m.SpanContext.Kind.String())
	row.SetColumnVal(nodeUUIDCol, GetNodeResource().NodeUuid)
	row.SetColumnVal(nodeTypeCol, GetNodeResource().NodeType)
	row.SetColumnVal(timestampCol, time2DatetimeString(m.Timestamp))
	row.SetColumnVal(loggerNameCol, m.LoggerName)
	row.SetColumnVal(levelCol, m.Level.String())
	row.SetColumnVal(callerCol, m.Caller)
	row.SetColumnVal(messageCol, m.Message)
	row.SetColumnVal(extraCol, m.Extra)
	row.SetColumnVal(stackCol, m.Stack)
	return row.ToStrings()
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
	log.Stack = entry.Stack
	// find SpanContext
	endIdx := len(fields) - 1
	for idx, v := range fields {
		if IsSpanField(v) {
			log.SpanContext = v.Interface.(*SpanContext)
			// find endIdx
			for ; idx < endIdx && IsSpanField(fields[endIdx]); endIdx-- {
			}
			if idx <= endIdx {
				fields[idx], fields[endIdx] = fields[endIdx], fields[idx]
				endIdx--
			}
			continue
		}
		if idx == endIdx {
			break
		}
	}
	if !needReport {
		log.Free()
		return jsonEncoder.EncodeEntry(entry, []zap.Field{})
	}
	buffer, err := jsonEncoder.EncodeEntry(entry, fields[:endIdx+1])
	log.Extra = buffer.String()
	export.GetGlobalBatchProcessor().Collect(DefaultContext(), log)
	return buffer, err
}
