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
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
	"time"
)

const AccountUnset = "unset"
const DatabaseUnset = "unset"

const MetricsTableName = "metrics"
const LogsTableName = "logs"
const SpansTableName = "logs"

const DATETIME = "DATETIME(6)"
const STRING = "VARCHAR(1024)"
const DOUBLE = "DOUBLE"
const UINT64 = "BIGINT UNSIGNED"
const TEXT = "TEXT"
const JSON = "JSON"

func StringColumn(name, comment string) table.Column {
	return table.Column{
		Name:    name,
		Type:    STRING,
		Default: "",
		Comment: comment,
	}
}
func StringDefaultColumn(name, defaultVal, comment string) table.Column {
	return table.Column{
		Name:    name,
		Type:    STRING,
		Default: defaultVal,
		Comment: comment,
	}
}

func TextColumn(name, comment string) table.Column {
	return table.Column{
		Name:    name,
		Type:    TEXT,
		Default: "",
		Comment: comment,
	}
}

func DatetimeColumn(name, comment string) table.Column {
	return table.Column{
		Name:    name,
		Type:    DATETIME,
		Default: "",
		Comment: comment,
	}
}

func JsonColumn(name, comment string) table.Column {
	return table.Column{
		Name:    name,
		Type:    JSON,
		Default: "{}",
		Comment: comment,
	}
}

func ValueColumn(name, comment string) table.Column {
	return table.Column{
		Name:    name,
		Type:    DOUBLE,
		Default: "0.0",
		Comment: comment,
	}
}

func Uint64Column(name, comment string) table.Column {
	return table.Column{
		Name:    name,
		Type:    UINT64,
		Default: "0",
		Comment: comment,
	}
}

var (
	MetricNameColumn      = StringColumn("name", "metric name")
	MetricTimestampColumn = DatetimeColumn(`timestamp`, `metric data collect time`)
	MetricValueColumn     = ValueColumn(`value`, `metric value`)
	MetricLabelsColumn    = JsonColumn(`labels`, `key-value json mark labels`)
	MetricSeriesIDColumn  = StringColumn("series_id", `abstract of json labels`)
	//metricNodeColumn        = table.Column{Name: `node`, Type: `VARCHAR(36)`, Default: ALL_IN_ONE_MODE, Comment: `mo node uuid`}
	//metricRoleColumn        = table.Column{Name: `role`, Type: `VARCHAR(32)`, Default: ALL_IN_ONE_MODE, Comment: `mo node role, like: CN, DN, LOG`}
	//metricAccountColumn     = table.Column{Name: `account`, Type: `VARCHAR(128)`, Default: `sys`, Comment: `account name`}
	//metricTypeColumn        = table.Column{Name: `type`, Type: `VARCHAR(32)`, Comment: `sql type, like: insert, select, ...`}
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
	LogsTraceIDCol     = table.Column{Name: "trace_id", Type: "varchar(36)", Default: "0", Comment: "related request's TraceId"}
	LogsSpanIDCol      = table.Column{Name: "span_id", Type: "varchar(16)", Default: "0", Comment: "related request's SpanId"}
	LogsTimestampCol   = DatetimeColumn(`timestamp`, "log recorded timestamp")
	LogsCollectTimeCol = DatetimeColumn(`collect_time`, "log recorded timestamp")
	LogsLoggerNameCol  = StringColumn("logger_name", "logger name")
	LogsLevelCol       = StringColumn("level", "log level, enum: debug, info, warn, error, panic, fatal")
	LogsCallerCol      = StringColumn("caller", "log caller, like: package/file.go:123")
	LogsMessageCol     = TextColumn("message", "log message content")
	LogsStackCol       = StringColumn("stack", "log caller stack info")
	LogsLabelsCol      = JsonColumn(`labels`, `key-value json mark labels`)
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
	SpansTraceIDCol       = table.Column{Name: "trace_id", Type: "varchar(36)", Default: "0", Comment: "TraceId"}
	SpansSpanIDCol        = table.Column{Name: "span_id", Type: "varchar(16)", Default: "0", Comment: "SpanId"}
	SpansParentTraceIDCol = table.Column{Name: "parent_trace_id", Type: "varchar(36)", Default: "0", Comment: "Parent TraceId"}
	SpansSpanKindCol      = StringDefaultColumn("span_kind", "internal", "Parent TraceId")
	SpansSpanNameCol      = StringColumn("span_name", "span name")
	SpansStartTimeCol     = DatetimeColumn("start_time", "start time")
	SpansEndTimeCol       = DatetimeColumn("end_time", "end time")
	SpansDurationCol      = Uint64Column("duration", "exec time, unit: ns")
	SpansResourceCol      = JsonColumn(`resource`, `key-value json`)
	SpansAttributesCol    = JsonColumn(`attributes`, `key-value json`)
	SpansStatusCol        = JsonColumn(`status`, `key-value json`)
	SpansEventsCol        = JsonColumn(`event`, `key-value json`)
	SpansLinksCol         = JsonColumn(`links`, `array json`)
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

const timestampFormatter = "2006-01-02 15:04:05.000000"

func Time2DatetimeString(t time.Time) string {
	return t.Format(timestampFormatter)
}
