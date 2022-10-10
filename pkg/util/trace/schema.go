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
	"fmt"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
)

const (
	SystemDBConst    = "system"
	StatsDatabase    = SystemDBConst
	spanInfoTbl      = "span_info"
	logInfoTbl       = "log_info"
	statementInfoTbl = "statement_info"
	errorInfoTbl     = "error_info"
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

var initDDLs = []struct{ sqlPrefix, filePrefix string }{
	{sqlCreateStatementInfoTable, MOStatementType},
	{sqlCreateSpanInfoTable, MOSpanType},
	{sqlCreateLogInfoTable, MOLogType},
	{sqlCreateErrorInfoTable, MOErrorType},
}

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

	optFactory := GetOptionFactory(ExternalTableEngine)

	if err := mustExec(sqlCreateDBConst); err != nil {
		return err
	}
	var createCost time.Duration
	defer func() {
		logutil.Debugf("[Trace] init tables: create cost %d ms",
			createCost.Milliseconds())
	}()
	instant := time.Now()

	for _, ddl := range initDDLs {
		opts := optFactory(StatsDatabase, ddl.filePrefix)
		sql := opts.FormatDdl(ddl.sqlPrefix) + opts.GetTableOptions()
		if err := mustExec(sql); err != nil {
			return err
		}
	}

	createCost = time.Since(instant)
	return nil
}

var _ TableOptions = (*CsvTableOptions)(nil)

type CsvTableOptions struct {
	Formatter string
	DbName    string
	TblName   string
}

func getExternalTableDDLPrefix(sql string) string {
	return strings.Replace(sql, "CREATE TABLE", "CREATE EXTERNAL TABLE", 1)
}

func (o *CsvTableOptions) FormatDdl(ddl string) string {
	return getExternalTableDDLPrefix(ddl)
}

func (o *CsvTableOptions) GetCreateOptions() string {
	return "EXTERNAL "
}

func (o *CsvTableOptions) GetTableOptions() string {
	if len(o.Formatter) > 0 {
		return fmt.Sprintf(o.Formatter, o.DbName, o.TblName)
	}
	return ""
}

func GetOptionFactory(engine string) func(db, tbl string) TableOptions {
	switch engine {
	case NormalTableEngine:
		return func(_, _ string) TableOptions { return NoopTableOptions{} }
	case ExternalTableEngine:
		var infileFormatter = ` infile{"filepath"="etl:%s/%s_*.csv","compression"="none"}` +
			` FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 0 lines`
		return func(db, tbl string) TableOptions {
			return &CsvTableOptions{Formatter: infileFormatter, DbName: db, TblName: tbl}
		}
	default:
		panic(moerr.NewInternalError("unknown engine: %s", engine))
	}
}

type CsvOptions struct {
	FieldTerminator rune // like: ','
	EncloseRune     rune // like: '"'
	Terminator      rune // like: '\n'
}

var CommonCsvOptions = &CsvOptions{
	FieldTerminator: ',',
	EncloseRune:     '"',
	Terminator:      '\n',
}
