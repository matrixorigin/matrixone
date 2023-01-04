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

package logs

import (
	"context"
	"encoding/json"
	"sync"
	"time"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/util/export/observability"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
)

type LogRecord struct {
	TraceId     string
	SpanId      string
	Timestamp   time.Time
	CollectTime time.Time
	LoggerName  string
	Level       string
	Caller      string
	Message     string
	Stack       string
	// Labels as extra
	// transfer kubernetes as labels
	// transfer kubernetes.labels into labels
	Labels map[string]any // json
}

var logPool = &sync.Pool{New: func() any {
	return &LogRecord{}
}}

func NewLogRecord() *LogRecord {
	return logPool.Get().(*LogRecord)
}

func (*LogRecord) GetName() string {
	return observability.LogsTable.GetIdentify()
}

func (*LogRecord) GetRow() *table.Row {
	return observability.LogsTable.GetRow(context.Background())
}

func (l *LogRecord) CsvFields(ctx context.Context, row *table.Row) []string {
	row.Reset()
	row.SetColumnVal(observability.LogsTraceIDCol, l.TraceId)
	row.SetColumnVal(observability.LogsSpanIDCol, l.SpanId)
	row.SetColumnVal(observability.LogsTimestampCol, observability.Time2DatetimeString(l.Timestamp))
	row.SetColumnVal(observability.LogsCollectTimeCol, observability.Time2DatetimeString(l.CollectTime))
	row.SetColumnVal(observability.LogsLoggerNameCol, l.LoggerName)
	row.SetColumnVal(observability.LogsLevelCol, l.Level)
	row.SetColumnVal(observability.LogsCallerCol, l.Caller)
	row.SetColumnVal(observability.LogsMessageCol, l.Message)
	row.SetColumnVal(observability.LogsStackCol, l.Stack)

	labels, err := json.Marshal(&l.Labels)
	if err != nil {
		panic(err)
	}
	row.SetColumnVal(observability.LogsLabelsCol, string(labels))

	return row.ToStrings()
}

func (l *LogRecord) Size() int64 {
	return int64(unsafe.Sizeof(l)) + int64(
		len(l.TraceId)+len(l.SpanId)+len(l.LoggerName)+len(l.Level)+
			len(l.Caller)+len(l.Message)+len(l.Stack),
	)
}

func (l *LogRecord) Free() {
	l.TraceId = ""
	l.SpanId = ""
	l.Timestamp = time.Time{}
	l.CollectTime = time.Time{}
	l.LoggerName = ""
	l.Level = ""
	l.Message = ""
	l.Stack = ""
	l.Labels = nil
	logPool.Put(l)
}
