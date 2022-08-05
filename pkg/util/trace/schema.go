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
 node_id BIGINT COMMENT "MO中的节点ID",
 node_type varchar(64) COMMENT "MO中的节点类型, 例如: DN, CN, LogService; /*TODO: 应为enum类型*/",
 resource varchar(4096) COMMENT "json结构, 记录静态资源信息 /*TODO: 应为JSON类型*/",
 Name varchar(1024) COMMENT "span的名字, 例如: 执行计划的步骤名, 代码中的函数名",
 start_time datetime,
 end_time datetime,
 Duration BIGINT COMMENT "执行耗时, 单位: ns"
)`
	sqlCreateLogInfoTable = `CREATE TABLE IF NOT EXISTS log_info(
 id BIGINT UNSIGNED COMMENT "主键, 应为auto increment类型",
 span_id BIGINT UNSIGNED,
 statement_id BIGINT UNSIGNED,
 node_id BIGINT COMMENT "MO中的节点ID",
 node_type varchar(64) COMMENT "MO中的节点类型, 例如: DN, CN, LogService; /*TODO: 应为enum类型*/",
 timestamp datetime COMMENT "日志时间戳",
 Level varchar(32) COMMENT "日志级别, 例如: DEBUG, INFO, WARN, ERROR",
 code_line varchar(4096) COMMENT "写日志所在代码行",
 message varchar(4096) COMMENT "日志内容/*TODO: 应为text*/"
)`
	sqlCreateStatementInfoTable = `CREATE TABLE IF NOT EXISTS statement_info(
 statement_id BIGINT UNSIGNED,
 transaction_id BIGINT UNSIGNED,
 session_id BIGINT UNSIGNED,
 ` + "`account`" + ` varchar(1024) COMMENT '用户账号',
 user varchar(1024) COMMENT '用户访问DB 鉴权用户名',
 host varchar(1024) COMMENT '用户访问DB 鉴权ip',
 ` + "`database`" + ` varchar(1024) COMMENT '数据库名',
 statement varchar(10240) COMMENT '执行sql/*TODO: 应为类型 TEXT或 BLOB */',
 statement_tag varchar(1024),
 statement_fingerprint varchar(10240) COMMENT '执行SQL脱敏后的语句/*应为TEXT或BLOB类型*/',
 node_id BIGINT COMMENT "MO中的节点ID",
 node_type varchar(64) COMMENT "MO中的节点类型, 例如: DN, CN, LogService; /*TODO: 应为enum类型*/",
 request_at datetime,
 status varchar(1024) COMMENT '运行状态, 包括: Running, Success, Failed',
 exec_plan varchar(4096) COMMENT "Sql执行计划的耗时结果; /*TODO: 应为JSON 类型*/"
)`
	sqlCreateErrorInfoTable = `CREATE TABLE IF NOT EXISTS error_info(
 id BIGINT UNSIGNED COMMENT "主键, 应为auto increment类型",
 statement_id BIGINT UNSIGNED,
 span_id BIGINT UNSIGNED,
 node_id BIGINT COMMENT "MO中的节点ID",
 node_type varchar(64) COMMENT "MO中的节点类型, 例如: DN, CN, LogService; /*TODO: 应为enum类型*/",
 err_code varchar(1024),
 stack varchar(4096),
 timestamp datetime COMMENT "日志时间戳"
)`
)

// InitSchemaByInnerExecutor just for standalone version, which can access db itself by io.InternalExecutor on any Node.
func InitSchemaByInnerExecutor(ieFactory func() ie.InternalExecutor) {
	// fixme: need errors.Recover()
	exec := ieFactory()
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
