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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil/logutil2"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	"strings"
	"time"
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
 span_id varchar(16),
 statement_id varchar(36),
 parent_span_id varchar(16),
 node_uuid varchar(36) COMMENT "node uuid in MO, which node accept this request",
 node_type varchar(64) COMMENT "node type in MO, enum: DN, CN, LogService;",
 name varchar(1024) COMMENT "span name, for example: step name of execution plan, function name in code, ...",
 start_time datetime,
 end_time datetime,
 duration BIGINT COMMENT "execution time, unit: ns",
 resource varchar(4096) COMMENT "json, static resource information /*should by json type*/"
)`
	sqlCreateLogInfoTable = `CREATE TABLE IF NOT EXISTS log_info(
 statement_id varchar(36),
 span_id varchar(16),
 node_uuid varchar(36) COMMENT "node uuid in MO, which node accept this request",
 node_type varchar(64) COMMENT "node type in MO, enum: DN, CN, LogService;",
 timestamp datetime COMMENT "log timestamp",
 name varchar(1024) COMMENT "logger name",
 level varchar(32) COMMENT "log level, enum: debug, info, warn, error, panic, fatal",
 caller varchar(4096) ,
 message varchar(4096) COMMENT "log message/*TODO: text with max length*/",
 extra varchar(4096) COMMENT "log extra fields, json"
)`
	sqlCreateStatementInfoTable = `CREATE TABLE IF NOT EXISTS statement_info(
 statement_id varchar(36),
 transaction_id varchar(36),
 session_id varchar(36),
 tenant_id INT UNSIGNED,
 user_id INT UNSIGNED,
 ` + "`account`" + ` varchar(1024) COMMENT 'account name',
 user varchar(1024) COMMENT 'user name',
 host varchar(1024) COMMENT 'user client ip',
 ` + "`database`" + ` varchar(1024) COMMENT 'database name',
 statement varchar(10240) COMMENT 'sql statement/*TODO: should by TEXT, or BLOB */',
 statement_tag varchar(1024),
 statement_fingerprint varchar(40960) COMMENT 'sql statement fingerprint/*TYPE should by TEXT, longer*/',
 node_uuid varchar(36) COMMENT "node uuid in MO, which node accept this request",
 node_type varchar(64) COMMENT "node type in MO, enum: DN, CN, LogService;",
 request_at datetime,
 exec_plan varchar(4096) COMMENT "sql execution plan; /*TODO: 应为JSON 类型*/"
)`
	sqlCreateErrorInfoTable = `CREATE TABLE IF NOT EXISTS error_info(
 statement_id varchar(36),
 span_id varchar(16),
 node_uuid varchar(36) COMMENT "node uuid in MO, which node accept this request",
 node_type varchar(64) COMMENT "node type in MO, enum: DN, CN, LogService;",
 err_code varchar(1024),
 stack varchar(4096),
 timestamp datetime
)`
)

// InitSchemaByInnerExecutor just for standalone version, which can access db itself by io.InternalExecutor on any Node.
func InitSchemaByInnerExecutor(ctx context.Context, ieFactory func() ie.InternalExecutor) error {
	exec := ieFactory()
	if exec == nil {
		return nil
	}
	exec.ApplySessionOverride(ie.NewOptsBuilder().Database(StatsDatabase).Internal(true).Finish())
	mustExec := func(sql string) error {
		if err := exec.Exec(ctx, sql, ie.NewOptsBuilder().Finish()); err != nil {
			return moerr.NewPanicError(fmt.Errorf("[Trace] init table error: %v, sql: %s", err, sql))
		}
		return nil
	}

	mustExec(sqlCreateDBConst)
	var createCost time.Duration
	defer func() {
		logutil2.Debugf(
			DefaultContext(),
			"[Trace] init trace tables: create cost %d ms",
			createCost.Milliseconds())
	}()
	instant := time.Now()

	var initCollectors = []string{
		sqlCreateStatementInfoTable,
		sqlCreateSpanInfoTable,
		sqlCreateLogInfoTable,
		sqlCreateErrorInfoTable,
	}
	for _, sql := range initCollectors {
		if err := mustExec(sql); err != nil {
			return err
		}
	}

	createCost = time.Since(instant)
	return nil
}

// InitExternalTblSchema for FileService
func InitExternalTblSchema(ctx context.Context, ieFactory func() ie.InternalExecutor) error {
	exec := ieFactory()
	if exec == nil {
		return nil
	}
	exec.ApplySessionOverride(ie.NewOptsBuilder().Database(StatsDatabase).Internal(true).Finish())
	mustExec := func(sql string) error {
		if err := exec.Exec(ctx, sql, ie.NewOptsBuilder().Finish()); err != nil {
			return moerr.NewPanicError(fmt.Errorf("[Trace] init table error: %v, sql: %s", err, sql))
		}
		return nil
	}

	optFactory := GetOptionFactory(FileService)

	if err := mustExec(sqlCreateDBConst); err != nil {
		return err
	}
	var createCost time.Duration
	defer func() {
		logutil2.Debugf(
			DefaultContext(),
			"[Trace] init external tables: create cost %d ms",
			createCost.Milliseconds())
	}()
	instant := time.Now()

	var initDDLs = []struct{ sqlPrefix, filePrefix string }{
		{getExternalTableDDLPrefix(sqlCreateStatementInfoTable), MOStatementType},
		{getExternalTableDDLPrefix(sqlCreateSpanInfoTable), MOSpanType},
		{getExternalTableDDLPrefix(sqlCreateLogInfoTable), MOLogType},
		{getExternalTableDDLPrefix(sqlCreateErrorInfoTable), MOErrorType},
	}
	for _, ddl := range initDDLs {
		opts := optFactory(StatsDatabase, ddl.filePrefix)
		sql := ddl.sqlPrefix + opts.GetTableOptions()
		if err := mustExec(sql); err != nil {
			return err
		}
	}

	createCost = time.Since(instant)
	return nil
}

func getExternalTableDDLPrefix(sql string) string {
	return strings.Replace(sql, "CREATE TABLE", "CREATE EXTERNAL TABLE", 1)
}

type TableOptions interface {
	// GetCreateOptions return option for `create {option}table`, which should end with ' '
	GetCreateOptions() string
	GetTableOptions() string
}

var _ TableOptions = (*CsvTableOptions)(nil)

type CsvTableOptions struct {
	formatter string
	dbName    string
	tblName   string
}

func (o *CsvTableOptions) GetCreateOptions() string {
	return "EXTERNAL "
}

func (o *CsvTableOptions) GetTableOptions() string {
	return fmt.Sprintf(o.formatter, o.dbName, o.tblName)
}

var _ TableOptions = (*noopTableOptions)(nil)

type noopTableOptions struct{}

func (o noopTableOptions) GetCreateOptions() string { return "" }
func (o noopTableOptions) GetTableOptions() string  { return "" }

func GetOptionFactory(mode string) func(db, tbl string) TableOptions {
	switch mode {
	case InternalExecutor:
		return func(_, _ string) TableOptions { return noopTableOptions{} }
	case FileService:
		var infileFormatter = ` infile{"filepath"="etl:%s/%s_*.csv","compression"="none"}` +
			` FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 0 lines`
		return func(db, tbl string) TableOptions {
			return &CsvTableOptions{formatter: infileFormatter, dbName: db, tblName: tbl}
		}
	default:
		panic(moerr.NewPanicError(fmt.Errorf("unknown batch process mode: %s", mode)))
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
