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
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
)

const AccountUnset = "unset"
const DatabaseUnset = "unset"

const MetricTableName = "metric"
const LogsTableName = "logs"

const DATETIME = "DATETIME(6)"
const STRING = "VARCHAR(1024)"
const DOUBLE = "DOUBLE"
const JSON = "JSON"

func StringColumn(name, comment string) table.Column {
	return table.Column{
		Name:    name,
		Type:    STRING,
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

var (
	MetricNameCol         = StringColumn("name", "metric name")
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
	Table:    MetricTableName,
	Columns: []table.Column{
		MetricNameCol, MetricTimestampColumn, MetricValueColumn, MetricLabelsColumn, MetricSeriesIDColumn,
	},
	PrimaryKeyColumn: []table.Column{},
	Engine:           table.ExternalTableEngine,
	Comment:          `metric data`,
	PathBuilder:      table.NewAccountDatePathBuilder(),
	AccountColumn:    nil,
	// SupportUserAccess
	SupportUserAccess: true,
}

var (
	LogsTraceIDCol    = table.Column{Name: "trace_id", Type: "varchar(36)", Default: "0", Comment: "related request's TraceId"}
	LogsSpanIDCol     = table.Column{Name: "span_id", Type: "varchar(16)", Default: "0", Comment: "related request's SpanId"}
	LogsTimestampCol  = DatetimeColumn(`timestamp`, "log recorded timestamp")
	LogsLoggerNameCol = StringColumn("logger_name", "logger name")
)

/*
*
MySQL [system]> select * from log_info limit 1\G
*************************** 1. row ***************************

	 trace_id: 0
	  span_id: 7c4dccb44d3c41f8
	span_kind: internal
	node_uuid: 7c4dccb4-4d3c-41f8-b482-5251dc7a41bf
	node_type: ALL
	timestamp: 2022-12-27 17:27:59.871151

logger_name:

	  level: info
	 caller: trace/trace.go:97
	message: trace with LongQueryTime: 0s
	  extra: {}
	  stack:

---
Resource
Attributes

字段名 描述  必选
Timestamp   日志时间戳   是
TraceId 关联请求的TraceId    否
SpanId  关联请求的SpanId 否
TraceFlags  W3C trace flag. 否
SeverityText    日志等级的可读描述.  否
SeverityNumber  日志等级.   否
ShortName   用于标识日志类型的短语.    否
Body    具体的日志内容.    否
Resource    Log关联的Resource. 否
Attributes  额外关联属性. 否

MySQL [system]> select * from span_info limit 1\G
*************************** 1. row ***************************

	trace_id: cb550f09-f65d-239e-a73d-563e50995dc2
	 span_id: 8ba86c187a4086c9

parent_span_id: 0

	 span_kind: internal
	 node_uuid: 7c4dccb4-4d3c-41f8-b482-5251dc7a41bf
	 node_type: ALL
	 span_name: TraceInit
	start_time: 2022-12-27 17:27:59.870632
	  end_time: 2022-12-27 17:27:59.871165
	  duration: 532783
	  resource: {"Node": {"node_type": "ALL", "node_uuid": "7c4dccb4-4d3c-41f8-b482-5251dc7a41bf"}, "version": "v0.6.0"}

----
status
attributes
event ?
*/
var LogsTable = &table.Table{
	Account:  AccountUnset,
	Database: DatabaseUnset,
	Table:    LogsTableName,
	Columns: []table.Column{
		traceIDCol,
		spanIDCol,
		spanKindCol,
		nodeUUIDCol,
		nodeTypeCol,
		timestampCol,
		loggerNameCol,
		levelCol,
		callerCol,
		messageCol,
		extraCol,
		stackCol,
	},
	Condition: &table.ViewSingleCondition{Column: rawItemCol, Table: logInfoTbl},
}
