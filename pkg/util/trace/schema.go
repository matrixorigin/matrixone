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
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/logutil/logutil2"
	"time"

	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
)

const (
	systemDBConst    = "system"
	statsDatabase    = systemDBConst
	spanInfoTbl      = "span_info"
	logInfoTbl       = "log_info"
	statementInfoTbl = "statement_info"
	errorInfoTbl     = "error_info"
)

const (
	sqlCreateDBConst = `create database if not exists ` + statsDatabase

	sqlCreateSpanInfoTable = `CREATE TABLE IF NOT EXISTS span_info(
 span_id BIGINT UNSIGNED,
 statement_id BIGINT UNSIGNED,
 parent_span_id BIGINT UNSIGNED,
 node_id BIGINT COMMENT "node uuid in MO",
 node_type varchar(64) COMMENT "node type in MO, enum: DN, CN, LogService;",
 name varchar(1024) COMMENT "span name, for example: step name of execution plan, function name in code, ...",
 start_time datetime,
 end_time datetime,
 duration BIGINT COMMENT "execution time, unit: ns",
 resource varchar(4096) COMMENT "json, static resource informations /*should by json type*/"
)`
	sqlCreateLogInfoTable = `CREATE TABLE IF NOT EXISTS log_info(
 statement_id BIGINT UNSIGNED,
 span_id BIGINT UNSIGNED,
 node_id BIGINT COMMENT "node uuid in MO",
 node_type varchar(64) COMMENT "node type in MO, enum: DN, CN, LogService;",
 timestamp datetime COMMENT "log timestamp",
 name varchar(1024) COMMENT "logger name",
 level varchar(32) COMMENT "log level, enum: debug, info, warn, error, panic, fatal",
 caller varchar(4096) ,
 message varchar(4096) COMMENT "log message/*TODO: text with max length*/",
 extra varchar(4096) COMMENT "log extra fields, json"
)`
	sqlCreateStatementInfoTable = `CREATE TABLE IF NOT EXISTS statement_info(
 statement_id BIGINT UNSIGNED,
 transaction_id BIGINT UNSIGNED,
 session_id BIGINT UNSIGNED,
 ` + "`account`" + ` varchar(1024) COMMENT 'account name',
 user varchar(1024) COMMENT 'user name',
 host varchar(1024) COMMENT 'user client ip',
 ` + "`database`" + ` varchar(1024) COMMENT 'database name',
 statement varchar(10240) COMMENT 'sql statement/*TODO: should by TEXT, or BLOB */',
 statement_tag varchar(1024),
 statement_fingerprint varchar(40960) COMMENT 'sql statement fingerprint/*TYPE should by TEXT, longer*/',
 node_id BIGINT COMMENT "node uuid in MO, which node accept this request",
 node_type varchar(64) COMMENT "node type in MO, enum: DN, CN, LogService;",
 request_at datetime,
 status varchar(1024) COMMENT 'sql statement running status, enum: Running, Success, Failed',
 exec_plan varchar(4096) COMMENT "sql execution plan; /*TODO: 应为JSON 类型*/"
)`
	sqlCreateErrorInfoTable = `CREATE TABLE IF NOT EXISTS error_info(
 statement_id BIGINT UNSIGNED,
 span_id BIGINT UNSIGNED,
 node_id BIGINT COMMENT "node uuid in MO, which node accept this request",
 node_type varchar(64) COMMENT "node type in MO, enum: DN, CN, LogService;",
 err_code varchar(1024),
 stack varchar(4096),
 timestamp datetime
)`
)

// InitSchemaByInnerExecutor just for standalone version, which can access db itself by io.InternalExecutor on any Node.
func InitSchemaByInnerExecutor(ieFactory func() ie.InternalExecutor) {
	// fixme: need errors.Recover()
	exec := ieFactory()
	if exec == nil {
		return
	}
	exec.ApplySessionOverride(ie.NewOptsBuilder().Database(statsDatabase).Internal(true).Finish())
	mustExec := func(sql string) {
		if err := exec.Exec(sql, ie.NewOptsBuilder().Finish()); err != nil {
			panic(fmt.Sprintf("[Metric] init metric tables error: %v, sql: %s", err, sql))
		}
	}

	mustExec(sqlCreateDBConst)
	var createCost time.Duration
	defer func() {
		logutil2.Debugf(
			DefaultContext(),
			"[Metric] init metrics tables: create cost %d ms",
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
		mustExec(sql)
	}

	createCost = time.Since(instant)
}
