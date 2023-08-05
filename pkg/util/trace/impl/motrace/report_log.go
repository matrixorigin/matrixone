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

package motrace

import (
	"context"
	"encoding/hex"
	"sync"
	"time"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/util/batchpipe"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
	"github.com/matrixorigin/matrixone/pkg/util/metric"
	"github.com/matrixorigin/matrixone/pkg/util/trace"

	"go.uber.org/zap"
	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"
)

var _ batchpipe.HasName = (*MOZapLog)(nil)

// MOZapLog implement export.IBuffer2SqlItem and export.CsvFields
type MOZapLog struct {
	Level       zapcore.Level      `json:"Level"`
	SpanContext *trace.SpanContext `json:"span"`
	Timestamp   time.Time          `json:"timestamp"`
	LoggerName  string
	Caller      string `json:"caller"` // like "util/trace/trace.go:666"
	Message     string `json:"message"`
	Extra       string `json:"extra"` // like json text
	Stack       string `json:"stack"`
}

var logPool = sync.Pool{
	New: func() any {
		return &MOZapLog{}
	},
}

func newMOZap() *MOZapLog {
	return logPool.Get().(*MOZapLog)
}

func (m *MOZapLog) GetName() string {
	return logView.OriginTable.GetName()
}

// deltaContentLength approximate value that may gen as table record
// timestamp: 26
// level: 5
// itemName: 8
// nodeInfo: 40 /*36+4*/
// spanInfo: 36+16
const deltaContentLength = int64(26 + 5 + 8 + 40 + 36 + 16)

// Size 计算近似值
func (m *MOZapLog) Size() int64 {
	return int64(unsafe.Sizeof(m)) + int64(len(m.LoggerName)+len(m.Caller)+len(m.Message)+len(m.Extra)+len(m.Stack)) +
		deltaContentLength
}

func (m *MOZapLog) Free() {
	m.SpanContext = nil
	m.LoggerName = ""
	m.Caller = ""
	m.Message = ""
	m.Extra = ""
	logPool.Put(m)
}

func (m *MOZapLog) GetTable() *table.Table { return logView.OriginTable }

func (m *MOZapLog) FillRow(ctx context.Context, row *table.Row) {
	row.Reset()
	row.SetColumnVal(rawItemCol, table.StringField(logView.Table))
	if m.SpanContext.TraceID != trace.NilTraceID {
		row.SetColumnVal(traceIDCol, table.UuidField(m.SpanContext.TraceID[:]))
	}
	if m.SpanContext.SpanID != trace.NilSpanID {
		row.SetColumnVal(spanIDCol, table.StringField(hex.EncodeToString(m.SpanContext.SpanID[:])))
	}
	row.SetColumnVal(spanKindCol, table.StringField(m.SpanContext.Kind.String()))
	row.SetColumnVal(nodeUUIDCol, table.StringField(GetNodeResource().NodeUuid))
	row.SetColumnVal(nodeTypeCol, table.StringField(GetNodeResource().NodeType))
	row.SetColumnVal(timestampCol, table.TimeField(m.Timestamp))
	row.SetColumnVal(loggerNameCol, table.StringField(m.LoggerName))
	row.SetColumnVal(levelCol, table.StringField(m.Level.String()))
	row.SetColumnVal(callerCol, table.StringField(m.Caller))
	row.SetColumnVal(messageCol, table.StringField(m.Message))
	row.SetColumnVal(extraCol, table.StringField(m.Extra))
	row.SetColumnVal(stackCol, table.StringField(m.Stack))
}

func ReportZap(jsonEncoder zapcore.Encoder, entry zapcore.Entry, fields []zapcore.Field) (*buffer.Buffer, error) {
	var discardable = false
	// count log message each minute
	metric.MOLogMessageCounter(entry.Level.String()).Add(1)
	// check trace is enable
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
		if v.Type == zapcore.BoolType && v.Key == logutil.MOInternalFiledKeyDiscardable {
			discardable = true
		}
		if trace.IsSpanField(v) {
			log.SpanContext = v.Interface.(*trace.SpanContext)
			// find endIdx
			for ; idx < endIdx && trace.IsSpanField(fields[endIdx]); endIdx-- {
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
	buffer, err := jsonEncoder.EncodeEntry(entry, fields[:endIdx+1])
	log.Extra = buffer.String()
	collector := GetGlobalBatchProcessor()
	var collectFunc = collector.Collect
	if discardable {
		if c, support := collector.(DiscardableCollector); support {
			collectFunc = c.DiscardableCollect
		}
	}
	switch entry.Level {
	case zap.PanicLevel, zap.DPanicLevel, zap.FatalLevel:
		syncer := NewItemSyncer(log)
		collectFunc(DefaultContext(), syncer)
		syncer.Wait()
	default:
		collectFunc(DefaultContext(), log)
	}
	return buffer, err
}
