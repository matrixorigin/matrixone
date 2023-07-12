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
	"time"

	"github.com/matrixorigin/matrixone/pkg/util/export/table"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
)

const (
	SystemDBConst = "system"
	StatsDatabase = SystemDBConst
)
const (
	// statementInfoTbl is an EXTERNAL table
	statementInfoTbl = "statement_info"
	RawLogTbl        = "rawlog"

	// spanInfoTbl is a view
	spanInfoTbl  = "span_info"
	logInfoTbl   = "log_info"
	errorInfoTbl = "error_info"
)

var (
	stmtIDCol    = table.UuidStringColumn("statement_id", "statement uniq id")
	txnIDCol     = table.UuidStringColumn("transaction_id", "txn uniq id")
	sesIDCol     = table.UuidStringColumn("session_id", "session uniq id")
	accountCol   = table.StringColumn("account", "account name")
	roleIdCol    = table.Int64Column("role_id", "role id")
	userCol      = table.StringColumn("user", "user name")
	hostCol      = table.StringColumn("host", "user client ip")
	dbCol        = table.StringColumn("database", "what database current session stay in.")
	stmtCol      = table.TextColumn("statement", "sql statement")
	stmtTagCol   = table.TextColumn("statement_tag", "note tag in statement(Reserved)")
	stmtFgCol    = table.TextColumn("statement_fingerprint", "note tag in statement(Reserved)")
	nodeUUIDCol  = table.UuidStringColumn("node_uuid", "node uuid, which node gen this data.")
	nodeTypeCol  = table.StringColumn("node_type", "node type in MO, val in [DN, CN, LOG]")
	reqAtCol     = table.DatetimeColumn("request_at", "request accept datetime")
	respAtCol    = table.DatetimeColumn("response_at", "response send datetime")
	durationCol  = table.UInt64Column("duration", "exec time, unit: ns")
	statusCol    = table.StringColumn("status", "sql statement running status, enum: Running, Success, Failed")
	errorCol     = table.TextColumn("error", "error message")
	execPlanCol  = table.TextDefaultColumn("exec_plan", `{}`, "statement execution plan")
	rowsReadCol  = table.Int64Column("rows_read", "rows read total")
	bytesScanCol = table.Int64Column("bytes_scan", "bytes scan total")
	statsCol     = table.TextDefaultColumn("stats", `[]`, "global stats info in exec_plan")
	stmtTypeCol  = table.StringColumn("statement_type", "statement type, val in [Insert, Delete, Update, Drop Table, Drop User, ...]")
	queryTypeCol = table.StringColumn("query_type", "query type, val in [DQL, DDL, DML, DCL, TCL]")
	sqlTypeCol   = table.TextColumn("sql_source_type", "sql statement source type")
	aggrCntCol   = table.Int64Column("aggr_count", "the number of statements aggregated")
	resultCntCol = table.Int64Column("result_count", "the number of rows of sql execution results")

	SingleStatementTable = &table.Table{
		Account:  table.AccountSys,
		Database: StatsDatabase,
		Table:    statementInfoTbl,
		Columns: []table.Column{
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
			statsCol,
			stmtTypeCol,
			queryTypeCol,
			roleIdCol,
			sqlTypeCol,
			aggrCntCol,
			resultCntCol,
		},
		PrimaryKeyColumn: nil,
		ClusterBy:        []table.Column{reqAtCol, accountCol},
		// Engine
		Engine:        table.NormalTableEngine,
		Comment:       "record each statement and stats info",
		PathBuilder:   table.NewAccountDatePathBuilder(),
		AccountColumn: &accountCol,
		// TimestampColumn
		TimestampColumn: &respAtCol,
		// SupportUserAccess
		SupportUserAccess: true,
		// SupportConstAccess
		SupportConstAccess: true,
	}

	rawItemCol      = table.StringColumn("raw_item", "raw log item")
	timestampCol    = table.DatetimeColumn("timestamp", "timestamp of action")
	loggerNameCol   = table.StringColumn("logger_name", "logger name")
	levelCol        = table.StringColumn("level", "log level, enum: debug, info, warn, error, panic, fatal")
	callerCol       = table.StringColumn("caller", "where it log, like: package/file.go:123")
	messageCol      = table.TextColumn("message", "log message")
	extraCol        = table.TextDefaultColumn("extra", `{}`, "log dynamic fields")
	errCodeCol      = table.StringDefaultColumn("err_code", `0`, "error code info")
	stackCol        = table.StringWithScale("stack", 2048, "stack info")
	traceIDCol      = table.UuidStringColumn("trace_id", "trace uniq id")
	spanIDCol       = table.SpanIDStringColumn("span_id", "span uniq id")
	sessionIDCol    = table.SpanIDStringColumn("session_id", "session id")
	statementIDCol  = table.SpanIDStringColumn("statement_id", "statement id")
	spanKindCol     = table.StringColumn("span_kind", "span kind, enum: internal, statement, remote")
	parentSpanIDCol = table.SpanIDStringColumn("parent_span_id", "parent span uniq id")
	spanNameCol     = table.StringColumn("span_name", "span name, for example: step name of execution plan, function name in code, ...")
	startTimeCol    = table.DatetimeColumn("start_time", "start time")
	endTimeCol      = table.DatetimeColumn("end_time", "end time")
	resourceCol     = table.TextDefaultColumn("resource", `{}`, "static resource information")

	UpgradeColumns = map[string]map[string][]table.Column{
		"1.0": {
			"ADD": {
				statementIDCol,
				sessionIDCol,
			},
		},
	}

	SingleRowLogTable = &table.Table{
		Account:  table.AccountSys,
		Database: StatsDatabase,
		Table:    RawLogTbl,
		Columns: []table.Column{
			rawItemCol,
			nodeUUIDCol,
			nodeTypeCol,
			spanIDCol,
			traceIDCol,
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
			spanKindCol,
			statementIDCol,
			sessionIDCol,
		},
		UpgradeColumns:   UpgradeColumns,
		PrimaryKeyColumn: nil,
		ClusterBy:        []table.Column{timestampCol, rawItemCol},
		Engine:           table.NormalTableEngine,
		Comment:          "read merge data from log, error, span",
		PathBuilder:      table.NewAccountDatePathBuilder(),
		AccountColumn:    nil,
		// TimestampColumn
		TimestampColumn: &timestampCol,
		// SupportUserAccess
		SupportUserAccess: false,
		// SupportConstAccess
		SupportConstAccess: true,
	}

	logView = &table.View{
		Database:    StatsDatabase,
		Table:       logInfoTbl,
		OriginTable: SingleRowLogTable,
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

	errorView = &table.View{
		Database:    StatsDatabase,
		Table:       errorInfoTbl,
		OriginTable: SingleRowLogTable,
		Columns: []table.Column{
			timestampCol,
			errCodeCol,
			errorCol,
			traceIDCol,
			spanIDCol,
			spanKindCol,
			nodeUUIDCol,
			nodeTypeCol,
			stackCol,
		},
		Condition: &table.ViewSingleCondition{Column: rawItemCol, Table: errorInfoTbl},
	}

	spanView = &table.View{
		Database:    StatsDatabase,
		Table:       spanInfoTbl,
		OriginTable: SingleRowLogTable,
		Columns: []table.Column{
			traceIDCol,
			spanIDCol,
			parentSpanIDCol,
			spanKindCol,
			nodeUUIDCol,
			nodeTypeCol,
			spanNameCol,
			startTimeCol,
			endTimeCol,
			durationCol,
			resourceCol,
			extraCol,
		},
		Condition: &table.ViewSingleCondition{Column: rawItemCol, Table: spanInfoTbl},
	}
)

const (
	sqlCreateDBConst = `create database if not exists ` + StatsDatabase
)

var tables = []*table.Table{SingleStatementTable, SingleRowLogTable}
var views = []*table.View{logView, errorView, spanView}

// InitSchemaByInnerExecutor init schema, which can access db by io.InternalExecutor on any Node.
func InitSchemaByInnerExecutor(ctx context.Context, ieFactory func() ie.InternalExecutor) error {
	exec := ieFactory()
	if exec == nil {
		return nil
	}
	exec.ApplySessionOverride(ie.NewOptsBuilder().Database(StatsDatabase).Internal(true).Finish())
	mustExec := func(sql string) error {
		if err := exec.Exec(ctx, sql, ie.NewOptsBuilder().Finish()); err != nil {
			return moerr.NewInternalError(ctx, "[Trace] init table error: %v, sql: %s", err, sql)
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
		if err := mustExec(tbl.ToCreateSql(ctx, true)); err != nil {
			return err
		}
		mustExec(tbl.ToUpgradeSql(ctx))
	}
	for _, v := range views {
		if err := mustExec(v.ToCreateSql(ctx, true)); err != nil {
			return err
		}
	}

	createCost = time.Since(instant)
	return nil
}

func GetAllTables() []*table.Table {
	return tables
}

// GetSchemaForAccount return account's table, and view's schema
func GetSchemaForAccount(ctx context.Context, account string) []string {
	var sqls = make([]string, 0, 1)
	for _, tbl := range tables {
		if tbl.SupportUserAccess {
			t := tbl.Clone()
			t.Account = account
			sqls = append(sqls, t.ToCreateSql(ctx, true))
		}
	}
	for _, v := range views {
		if v.OriginTable.SupportUserAccess {
			sqls = append(sqls, v.ToCreateSql(ctx, true))
		}

	}
	return sqls
}

func init() {
	for _, tbl := range tables {
		tbl.GetRow(context.Background()).Free()
		if old := table.RegisterTableDefine(tbl); old != nil {
			panic(moerr.NewInternalError(context.Background(), "table already registered: %s", old.GetIdentify()))
		}
	}
}
