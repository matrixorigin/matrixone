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

package observability

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
)

const AccountUnset = "unset"
const DatabaseUnset = "unset"

const MetricsTableName = "metrics"
const LogsTableName = "logs"
const SpansTableName = "spans"

const DATETIME = "DATETIME(6)"
const STRING = "VARCHAR(1024)"
const DOUBLE = "DOUBLE"
const UINT64 = "BIGINT UNSIGNED"
const TEXT = "TEXT"
const JSON = "JSON"

var (
	MetricNameColumn      = table.StringColumn("name", "metric name")
	MetricTimestampColumn = table.DatetimeColumn(`timestamp`, `metric data collect time`)
	MetricValueColumn     = table.ValueColumn(`value`, `metric value`)
	MetricLabelsColumn    = table.JsonColumn(`labels`, `key-value json mark labels`)
	MetricSeriesIDColumn  = table.StringColumn("series_id", `abstract of json labels`)
)

var MetricTable = &table.Table{
	Account:  AccountUnset,
	Database: DatabaseUnset,
	Table:    MetricsTableName,
	Columns: []table.Column{
		MetricNameColumn, MetricTimestampColumn, MetricValueColumn, MetricLabelsColumn, MetricSeriesIDColumn,
	},
	PrimaryKeyColumn: []table.Column{},
	Engine:           table.ExternalTableEngine,
	Comment:          `metric data`,
	PathBuilder:      table.NewAccountDatePathBuilder(table.WithDatabase(true)),
	AccountColumn:    nil,
	// SupportUserAccess
	SupportUserAccess:  false,
	SupportConstAccess: true,
}

var (
	LogsTraceIDCol     = table.UuidStringColumn("trace_id", "related request's TraceId")
	LogsSpanIDCol      = table.SpanIDStringColumn("span_id", "related request's SpanId")
	LogsTimestampCol   = table.DatetimeColumn(`timestamp`, "log recorded timestamp")
	LogsCollectTimeCol = table.DatetimeColumn(`collect_time`, "log recorded timestamp")
	LogsLoggerNameCol  = table.StringColumn("logger_name", "logger name")
	LogsLevelCol       = table.StringColumn("level", "log level, enum: debug, info, warn, error, panic, fatal")
	LogsCallerCol      = table.StringColumn("caller", "log caller, like: package/file.go:123")
	LogsMessageCol     = table.TextColumn("message", "log message content")
	LogsStackCol       = table.StringColumn("stack", "log caller stack info")
	LogsLabelsCol      = table.JsonColumn(`labels`, `key-value json mark labels`)
)

var LogsTable = &table.Table{
	Account:  AccountUnset,
	Database: DatabaseUnset,
	Table:    LogsTableName,
	Columns: []table.Column{
		LogsTraceIDCol, LogsSpanIDCol,
		LogsTimestampCol, LogsCollectTimeCol,
		LogsLoggerNameCol, LogsLevelCol, LogsCallerCol, LogsMessageCol, LogsStackCol,
		LogsLabelsCol,
	},
	PrimaryKeyColumn: []table.Column{},
	Engine:           table.ExternalTableEngine,
	Comment:          `logs data`,
	PathBuilder:      table.NewAccountDatePathBuilder(table.WithDatabase(true)),
	AccountColumn:    nil,
	// SupportUserAccess
	SupportUserAccess:  false,
	SupportConstAccess: true,
}

var (
	SpansTraceIDCol       = table.UuidStringColumn("trace_id", "TraceId")
	SpansSpanIDCol        = table.SpanIDStringColumn("span_id", "SpanId")
	SpansParentTraceIDCol = table.SpanIDStringColumn("parent_span_id", "Parent span Id")
	SpansSpanKindCol      = table.StringDefaultColumn("span_kind", "internal", "span kind")
	SpansSpanNameCol      = table.StringColumn("span_name", "span name")
	SpansStartTimeCol     = table.DatetimeColumn("start_time", "start time")
	SpansEndTimeCol       = table.DatetimeColumn("end_time", "end time")
	SpansDurationCol      = table.UInt64Column("duration", "exec time, unit: ns")
	SpansResourceCol      = table.JsonColumn(`resource`, `key-value json`)
	SpansAttributesCol    = table.JsonColumn(`attributes`, `key-value json`)
	SpansStatusCol        = table.JsonColumn(`status`, `key-value json`)
	SpansEventsCol        = table.JsonColumn(`event`, `key-value json`)
	SpansLinksCol         = table.JsonColumn(`links`, `array json`)
)

var SpansTable = &table.Table{
	Account:  AccountUnset,
	Database: DatabaseUnset,
	Table:    SpansTableName,
	Columns: []table.Column{
		SpansTraceIDCol, SpansSpanIDCol, SpansParentTraceIDCol, SpansSpanKindCol,
		SpansSpanNameCol, SpansStartTimeCol, SpansEndTimeCol, SpansDurationCol,
		SpansResourceCol, SpansAttributesCol, SpansStatusCol, SpansEventsCol, SpansLinksCol,
	},
	PrimaryKeyColumn: []table.Column{},
	Engine:           table.ExternalTableEngine,
	Comment:          `spans data`,
	PathBuilder:      table.NewAccountDatePathBuilder(table.WithDatabase(true)),
	AccountColumn:    nil,
	// SupportUserAccess
	SupportUserAccess:  false,
	SupportConstAccess: true,
}

func init() {
	var tables = []*table.Table{MetricTable, LogsTable, SpansTable}
	for _, tbl := range tables {
		tbl.GetRow(context.Background()).Free()
		if old := table.RegisterTableDefine(tbl); old != nil {
			panic(moerr.NewInternalError(context.Background(), "table already registered: %s", old.GetIdentify()))
		}
	}
}
