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
	"time"

	"github.com/matrixorigin/matrixone/pkg/util/export/table"

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
	spanKindType       = "varchar(32)"
	datetime6Type      = "datetime(6)"
	bigintUnsignedType = "bigint unsigned"
	stringType         = "varchar(1024)"

	jsonColumnDEFAULT = "{}"
)

var (
	stmtIDCol    = table.Column{Name: "statement_id", Type: uuidColType, Default: "0", Comment: "statement uniq id"}
	txnIDCol     = table.Column{Name: "transaction_id", Type: uuidColType, Default: "0", Comment: "txn uniq id"}
	sesIDCol     = table.Column{Name: "session_id", Type: uuidColType, Default: "0", Comment: "session uniq id"}
	accountCol   = table.Column{Name: "account", Type: stringType, Default: "", Comment: "account name"}
	roleIdCol    = table.Column{Name: "role_id", Type: bigintUnsignedType, Default: "0", Comment: "role id"}
	userCol      = table.Column{Name: "user", Type: stringType, Default: "", Comment: "user name"}
	hostCol      = table.Column{Name: "host", Type: stringType, Default: "", Comment: "user client ip"}
	dbCol        = table.Column{Name: "database", Type: stringType, Default: "", Comment: "what database current session stay in."}
	stmtCol      = table.Column{Name: "statement", Type: "TEXT", Default: "", Comment: "sql statement"}
	stmtTagCol   = table.Column{Name: "statement_tag", Type: "TEXT", Default: "", Comment: "note tag in statement(Reserved)"}
	stmtFgCol    = table.Column{Name: "statement_fingerprint", Type: "TEXT", Default: "", Comment: "note tag in statement(Reserved)"}
	nodeUUIDCol  = table.Column{Name: "node_uuid", Type: uuidColType, Default: "0", Comment: "node uuid, which node gen this data."}
	nodeTypeCol  = table.Column{Name: "node_type", Type: "varchar(64)", Default: "node", Comment: "node type in MO, val in [DN, CN, LOG]"}
	reqAtCol     = table.Column{Name: "request_at", Type: datetime6Type, Default: "", Comment: "request accept datetime"}
	respAtCol    = table.Column{Name: "response_at", Type: datetime6Type, Default: "", Comment: "response send datetime"}
	durationCol  = table.Column{Name: "duration", Type: bigintUnsignedType, Default: "0", Comment: "exec time, unit: ns"}
	statusCol    = table.Column{Name: "status", Type: "varchar(32)", Default: "Running", Comment: "sql statement running status, enum: Running, Success, Failed"}
	errorCol     = table.Column{Name: "error", Type: "TEXT", Default: "", Comment: "error message"}
	execPlanCol  = table.Column{Name: "exec_plan", Type: "JSON", Default: jsonColumnDEFAULT, Comment: "statement execution plan"}
	rowsReadCol  = table.Column{Name: "rows_read", Type: bigintUnsignedType, Default: "0", Comment: "rows read total"}
	bytesScanCol = table.Column{Name: "bytes_scan", Type: bigintUnsignedType, Default: "0", Comment: "bytes scan total"}
	statsCol     = table.Column{Name: "stats", Type: "JSON", Default: jsonColumnDEFAULT, Comment: "global stats info in exec_plan"}
	stmtTypeCol  = table.Column{Name: "statement_type", Type: "varchar(128)", Default: "", Comment: "statement type, val in [Insert, Delete, Update, Drop Table, Drop User, ...]"}
	queryTypeCol = table.Column{Name: "query_type", Type: "varchar(128)", Default: "", Comment: "query type, val in [DQL, DDL, DML, DCL, TCL]"}
	sqlTypeCol   = table.Column{Name: "sql_source_type", Type: "TEXT", Default: "", Comment: "sql statement source type"}

	SingleStatementTable = &table.Table{
		Account:  table.AccountAll,
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
		},
		PrimaryKeyColumn: []table.Column{stmtIDCol},
		Engine:           table.ExternalTableEngine,
		Comment:          "record each statement and stats info",
		PathBuilder:      table.NewAccountDatePathBuilder(),
		AccountColumn:    &accountCol,
		// SupportUserAccess
		SupportUserAccess: true,
	}

	rawItemCol      = table.Column{Name: "raw_item", Type: stringType, Comment: "raw log item"}
	timestampCol    = table.Column{Name: "timestamp", Type: datetime6Type, Comment: "timestamp of action"}
	loggerNameCol   = table.Column{Name: "logger_name", Type: stringType, Comment: "logger name"}
	levelCol        = table.Column{Name: "level", Type: stringType, Comment: "log level, enum: debug, info, warn, error, panic, fatal"}
	callerCol       = table.Column{Name: "caller", Type: stringType, Comment: "where it log, like: package/file.go:123"}
	messageCol      = table.Column{Name: "message", Type: "TEXT", Comment: "log message"}
	extraCol        = table.Column{Name: "extra", Type: "JSON", Default: jsonColumnDEFAULT, Comment: "log dynamic fields"}
	errCodeCol      = table.Column{Name: "err_code", Type: stringType, Default: "0"}
	stackCol        = table.Column{Name: "stack", Type: "varchar(4096)"}
	traceIDCol      = table.Column{Name: "trace_id", Type: uuidColType, Default: "0", Comment: "trace uniq id"}
	spanIDCol       = table.Column{Name: "span_id", Type: spanIDType, Default: "0", Comment: "span uniq id"}
	spanKindCol     = table.Column{Name: "span_kind", Type: spanKindType, Default: "", Comment: "span kind, enum: internal, statement, remote"}
	parentSpanIDCol = table.Column{Name: "parent_span_id", Type: spanIDType, Default: "0", Comment: "parent span uniq id"}
	spanNameCol     = table.Column{Name: "span_name", Type: stringType, Default: "", Comment: "span name, for example: step name of execution plan, function name in code, ..."}
	startTimeCol    = table.Column{Name: "start_time", Type: datetime6Type, Default: ""}
	endTimeCol      = table.Column{Name: "end_time", Type: datetime6Type, Default: ""}
	resourceCol     = table.Column{Name: "resource", Type: "JSON", Default: jsonColumnDEFAULT, Comment: "static resource information"}

	SingleRowLogTable = &table.Table{
		Account:  table.AccountAll,
		Database: StatsDatabase,
		Table:    rawLogTbl,
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
		},
		PrimaryKeyColumn: nil,
		Engine:           table.ExternalTableEngine,
		Comment:          "read merge data from log, error, span",
		PathBuilder:      table.NewAccountDatePathBuilder(),
		AccountColumn:    nil,
		// SupportUserAccess
		SupportUserAccess: false,
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
	}
	for _, v := range views {
		if err := mustExec(v.ToCreateSql(ctx, true)); err != nil {
			return err
		}
	}

	createCost = time.Since(instant)
	return nil
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
		if old := table.RegisterTableDefine(tbl); old != nil {
			panic(moerr.NewInternalError(context.Background(), "table already registered: %s", old.GetIdentify()))
		}
	}
}
