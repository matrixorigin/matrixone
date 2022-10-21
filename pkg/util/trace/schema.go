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
	"github.com/matrixorigin/matrixone/pkg/util/export"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
)

const (
	SystemDBConst = "system"
	StatsDatabase = SystemDBConst
	// statementInfoTbl is an EXTERNAL table
	statementInfoTbl = "statement_info"
	rawLogTbl        = "rawlog"

	// spanInfoTbl is a view
	spanInfoTbl  = "span_info"
	logInfoTbl   = "log_info"
	errorInfoTbl = "error_info"

	uuidColType        = "varchar(36)"
	spanIDType         = "varchar(16)"
	datetime6Type      = "datetime(6)"
	bigintUnsignedType = "bigint unsigned"
	stringType         = "varchar(1024)"

	jsonColumnDEFAULT = "{}"
)

var (
	stmtIDCol    = export.Column{Name: "statement_id", Type: uuidColType, Default: "0", Comment: "statement uniq id"}
	txnIDCol     = export.Column{Name: "transaction_id", Type: uuidColType, Default: "0", Comment: "txn uniq id"}
	sesIDCol     = export.Column{Name: "sessionID", Type: uuidColType, Default: "0", Comment: "session uniq id"}
	accountCol   = export.Column{Name: "account", Type: stringType, Default: "", Comment: "account name"}
	userCol      = export.Column{Name: "user", Type: stringType, Default: "", Comment: "user name"}
	hostCol      = export.Column{Name: "host", Type: stringType, Default: "", Comment: "user client ip"}
	dbCol        = export.Column{Name: "database", Type: stringType, Default: "", Comment: "what database current session stay in."}
	stmtCol      = export.Column{Name: "statement", Type: "TEXT", Default: "", Comment: "sql statement"}
	stmtTagCol   = export.Column{Name: "statement_tag", Type: "TEXT", Default: "", Comment: "note tag in statement(Reserved)"}
	stmtFgCol    = export.Column{Name: "statement_fingerprint", Type: "TEXT", Default: "", Comment: "note tag in statement(Reserved)"}
	nodeUUIDCol  = export.Column{Name: "node_uuid", Type: uuidColType, Default: "0", Comment: "node uuid, which node gen this data."}
	nodeTypeCol  = export.Column{Name: "node_type", Type: "varchar(64)", Default: "node", Comment: "node type in MO, val in [DN, CN, LOG]"}
	reqAtCol     = export.Column{Name: "request_at", Type: datetime6Type, Default: "", Comment: "request accept datetime"}
	respAtCol    = export.Column{Name: "response_at", Type: datetime6Type, Default: "", Comment: "response send datetime"}
	durationCol  = export.Column{Name: "duration", Type: bigintUnsignedType, Default: "0", Comment: "exec time, unit: ns"}
	statusCol    = export.Column{Name: "status", Type: "varchar(32)", Default: "Running", Comment: "sql statement running status, enum: Running, Success, Failed"}
	errorCol     = export.Column{Name: "error", Type: "TEXT", Default: "", Comment: "error message"}
	execPlanCol  = export.Column{Name: "exec_plan", Type: "JSON", Default: jsonColumnDEFAULT, Comment: "statement execution plan"}
	rowsReadCol  = export.Column{Name: "rows_read", Type: bigintUnsignedType, Default: "0", Comment: "rows read total"}
	bytesScanCol = export.Column{Name: "bytes_scan", Type: bigintUnsignedType, Default: "0", Comment: "bytes scan total"}

	SingleStatementTable = &export.Table{
		Database: StatsDatabase,
		Table:    statementInfoTbl,
		Columns: []export.Column{
			stmtIDCol,
			txnIDCol,
			sesIDCol,
			accountCol,
			userCol,
			hostCol,
			dbCol,
			stmtCol,
			stmtTagCol,
			stmtFgCol,
			nodeUUIDCol,
			nodeTypeCol,
			reqAtCol,
			respAtCol,
			durationCol,
			statusCol,
			errCodeCol,
			errorCol,
			execPlanCol,
			rowsReadCol,
			bytesScanCol,
		},
		PrimaryKeyColumn: []export.Column{stmtIDCol},
		Engine:           export.ExternalTableEngine,
		Comment:          "record each statement and stats info",
		PathBuilder:      export.NewAccountDatePathBuilder(),
		AccountColumn:    &accountCol,
	}

	rawItemCol      = export.Column{Name: "raw_item", Type: stringType, Comment: "raw log item"}
	timestampCol    = export.Column{Name: "timestamp", Type: datetime6Type, Comment: "timestamp of action"}
	loggerNameCol   = export.Column{Name: "logger_name", Type: stringType, Comment: "logger name"}
	levelCol        = export.Column{Name: "level", Type: stringType, Comment: "log level, enum: debug, info, warn, error, panic, fatal"}
	callerCol       = export.Column{Name: "caller", Type: stringType, Comment: "where it log, like: package/file.go:123"}
	messageCol      = export.Column{Name: "message", Type: "TEXT", Comment: "log message"}
	extraCol        = export.Column{Name: "extra", Type: "JSON", Default: jsonColumnDEFAULT, Comment: "log dynamic fields"}
	errCodeCol      = export.Column{Name: "err_code", Type: stringType, Default: "0"}
	stackCol        = export.Column{Name: "stack", Type: "varchar(4096)"}
	spanIDCol       = export.Column{Name: "span_id", Type: spanIDType, Default: "0", Comment: "span uniq id"}
	parentSpanIDCol = export.Column{Name: "parent_span_id", Type: spanIDType, Default: "0", Comment: "parent span uniq id"}
	spanNameCol     = export.Column{Name: "span_name", Type: stringType, Default: "", Comment: "span name, for example: step name of execution plan, function name in code, ..."}
	startTimeCol    = export.Column{Name: "start_time", Type: datetime6Type, Default: ""}
	endTimeCol      = export.Column{Name: "end_time", Type: datetime6Type, Default: ""}
	resourceCol     = export.Column{Name: "resource", Type: "JSON", Default: jsonColumnDEFAULT, Comment: "static resource information"}

	SingleRowLogTable = &export.Table{
		Database: StatsDatabase,
		Table:    rawLogTbl,
		Columns: []export.Column{
			rawItemCol,
			nodeUUIDCol,
			nodeTypeCol,
			spanIDCol,
			stmtIDCol,
			loggerNameCol,
			timestampCol,
			levelCol,
			callerCol,
			messageCol,
			extraCol,
			errCodeCol,
			errorCol,
			stackCol,
			spanNameCol,
			parentSpanIDCol,
			startTimeCol,
			endTimeCol,
			durationCol,
			resourceCol,
		},
		PrimaryKeyColumn: nil,
		Engine:           export.ExternalTableEngine,
		Comment:          "read merge data from log, error, span",
		PathBuilder:      export.NewAccountDatePathBuilder(),
		AccountColumn:    nil,
	}

	logView = &export.View{
		Database:    StatsDatabase,
		Table:       logInfoTbl,
		OriginTable: SingleRowLogTable,
		Columns: []export.Column{
			stmtIDCol,
			spanIDCol,
			nodeUUIDCol,
			nodeTypeCol,
			timestampCol,
			loggerNameCol,
			levelCol,
			callerCol,
			messageCol,
			extraCol,
		},
		Condition: &export.ViewSingleCondition{Column: rawItemCol, Table: logInfoTbl},
	}

	errorView = &export.View{
		Database:    StatsDatabase,
		Table:       errorInfoTbl,
		OriginTable: SingleRowLogTable,
		Columns: []export.Column{
			timestampCol,
			errCodeCol,
			errorCol,
			nodeUUIDCol,
			nodeTypeCol,
			stackCol,
		},
		Condition: &export.ViewSingleCondition{Column: rawItemCol, Table: errorInfoTbl},
	}

	spanView = &export.View{
		Database:    StatsDatabase,
		Table:       spanInfoTbl,
		OriginTable: SingleRowLogTable,
		Columns: []export.Column{
			spanIDCol,
			stmtIDCol,
			parentSpanIDCol,
			nodeUUIDCol,
			nodeTypeCol,
			spanNameCol,
			startTimeCol,
			endTimeCol,
			durationCol,
			resourceCol,
		},
		Condition: &export.ViewSingleCondition{Column: rawItemCol, Table: spanInfoTbl},
	}
)

const (
	sqlCreateDBConst = `create database if not exists ` + StatsDatabase

	sqlCreateSpanInfoTable = `CREATE TABLE IF NOT EXISTS span_info(
 span_id varchar(16) NOT NULL,
 statement_id varchar(36) NOT NULL,
 parent_span_id varchar(16) NOT NULL,
 node_uuid varchar(36) NOT NULL COMMENT "node uuid in MO, which node accept this request",
 node_type varchar(64) NOT NULL COMMENT "node type in MO, enum: DN, CN, LogService",
 name varchar(1024) NOT NULL COMMENT "span name, for example: step name of execution plan, function name in code, ...",
 start_time datetime(6) NOT NULL,
 end_time datetime(6) NOT NULL,
 duration BIGINT default 0 COMMENT "execution time, unit: ns",
 resource JSON NOT NULL COMMENT "json, static resource information"
)`
	sqlCreateLogInfoTable = `CREATE TABLE IF NOT EXISTS log_info(
 statement_id varchar(36) NOT NULL,
 span_id varchar(16) NOT NULL,
 node_uuid varchar(36) NOT NULL COMMENT "node uuid in MO, which node accept this request",
 node_type varchar(64) NOT NULL COMMENT "node type in MO, enum: DN, CN, LogService;",
 timestamp datetime(6) NOT NULL COMMENT "log timestamp",
 name varchar(1024) NOT NULL COMMENT "logger name",
 level varchar(32) NOT NULL COMMENT "log level, enum: debug, info, warn, error, panic, fatal",
 caller varchar(4096) NOT NULL,
 message TEXT NOT NULL COMMENT "log message",
 extra JSON NOT NULL COMMENT "log extra fields"
)`
	sqlCreateStatementInfoTable = `CREATE TABLE IF NOT EXISTS statement_info(
 statement_id varchar(36) NOT NULL,
 transaction_id varchar(36) NOT NULL,
 session_id varchar(36) NOT NULL,
 account varchar(1024) NOT NULL COMMENT 'account name',
 user varchar(1024) NOT NULL COMMENT 'user name',
 host varchar(1024) NOT NULL COMMENT 'user client ip',
 ` + "`database`" + ` varchar(1024) NOT NULL COMMENT 'database name',
 statement TEXT NOT NULL COMMENT 'sql statement',
 statement_tag TEXT NOT NULL DEFAULT '',
 statement_fingerprint TEXT NOT NULL DEFAULT '' COMMENT 'sql statement fingerprint',
 node_uuid varchar(36) NOT NULL COMMENT "node uuid in MO, which node accept this request",
 node_type varchar(64) NOT NULL COMMENT "node type in MO, enum: DN, CN, LogService;",
 request_at datetime(6) NOT NULL,
 response_at datetime(6) NOT NULL,
 duration bigint unsigned default 0 COMMENT 'exec time, unit: ns',
 ` + "`status`" + ` varchar(32) default 'Running' COMMENT 'sql statement running status, enum: Running, Success, Failed',
 error TEXT NOT NULL COMMENT 'error message',
 exec_plan JSON NOT NULL COMMENT "sql execution plan",
 rows_read bigint default 0 COMMENT 'rows read total',
 bytes_scan bigint default 0 COMMENT 'bytes scan total',
PRIMARY KEY (statement_id)
)`
	sqlCreateErrorInfoTable = `CREATE TABLE IF NOT EXISTS error_info(
 statement_id varchar(36) NOT NULL,
 span_id varchar(16) NOT NULL,
 node_uuid varchar(36) NOT NULL COMMENT "node uuid in MO, which node accept this request",
 node_type varchar(64) NOT NULL COMMENT "node type in MO, enum: DN, CN, LogService;",
 err_code varchar(1024) NOT NULL,
 stack varchar(4096) NOT NULL,
 timestamp datetime(6) NOT NULL
)`
)

var tables = []*export.Table{SingleStatementTable, SingleRowLogTable}
var views = []*export.View{logView, errorView, spanView}

// InitSchemaByInnerExecutor init schema, which can access db by io.InternalExecutor on any Node.
func InitSchemaByInnerExecutor(ctx context.Context, ieFactory func() ie.InternalExecutor) error {
	exec := ieFactory()
	if exec == nil {
		return nil
	}
	exec.ApplySessionOverride(ie.NewOptsBuilder().Database(StatsDatabase).Internal(true).Finish())
	mustExec := func(sql string) error {
		if err := exec.Exec(ctx, sql, ie.NewOptsBuilder().Finish()); err != nil {
			return moerr.NewInternalError("[Trace] init table error: %v, sql: %s", err, sql)
		}
		return nil
	}

	if err := mustExec(sqlCreateDBConst); err != nil {
		return err
	}
	var createCost time.Duration
	defer func() {
		logutil.Debugf("[Trace] init tables: create cost %d ms",
			createCost.Milliseconds())
	}()
	instant := time.Now()

	for _, tbl := range tables {
		if err := mustExec(tbl.ToCreateSql(true)); err != nil {
			return err
		}
	}
	for _, v := range views {
		if err := mustExec(v.ToCreateSql(true)); err != nil {
			return err
		}
	}

	createCost = time.Since(instant)
	return nil
}

func init() {
	for _, tbl := range tables {
		if old := export.RegisterTableDefine(tbl); old != nil {
			panic(moerr.NewInternalError("table already registered: %s", old.GetIdentify()))
		}
	}
}
