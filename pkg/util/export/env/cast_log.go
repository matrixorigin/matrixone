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

package env

import (
	"context"
	"encoding/json"
	"sync"
	"time"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/util/export/table"
)

type Log struct {
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
	return &Log{}
}}

func NewLog() *Log {
	return logPool.Get().(*Log)
}

func (*Log) GetName() string {
	return LogsTable.GetIdentify()
}

func (*Log) GetRow() *table.Row {
	return LogsTable.GetRow(context.Background())
}

func (l *Log) CsvFields(ctx context.Context, row *table.Row) []string {
	row.Reset()
	row.SetColumnVal(LogsTraceIDCol, l.TraceId)
	row.SetColumnVal(LogsSpanIDCol, l.SpanId)
	row.SetColumnVal(LogsTimestampCol, Time2DatetimeString(l.Timestamp))
	row.SetColumnVal(LogsCollectTimeCol, Time2DatetimeString(l.CollectTime))
	row.SetColumnVal(LogsLoggerNameCol, l.LoggerName)
	row.SetColumnVal(LogsLevelCol, l.Level)
	row.SetColumnVal(LogsCallerCol, l.Caller)
	row.SetColumnVal(LogsMessageCol, l.Message)
	row.SetColumnVal(LogsStackCol, l.Stack)

	labels, err := json.Marshal(&l.Labels)
	if err != nil {
		panic(err)
	}
	row.SetColumnVal(LogsLabelsCol, string(labels))

	return row.ToStrings()
}

func (l *Log) Size() int64 {
	return int64(unsafe.Sizeof(l)) + int64(
		len(l.TraceId)+len(l.SpanId)+len(l.LoggerName)+len(l.Level)+
			len(l.Caller)+len(l.Message)+len(l.Stack),
	)
}

func (l *Log) Free() {
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
