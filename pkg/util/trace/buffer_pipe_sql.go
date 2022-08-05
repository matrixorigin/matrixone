package trace

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/util"
	bp "github.com/matrixorigin/matrixone/pkg/util/batchpipe"
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
	createSpanInfoTable = `CREATE TABLE IF NOT EXISTS span_info(
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
	createLogInfoTable = `CREATE TABLE IF NOT EXISTS log_info(
 id BIGINT UNSIGNED COMMENT "主键, 应为auto increment类型",
 span_id BIGINT UNSIGNED,
 statement_id BIGINT UNSIGNED,
 node_id BIGINT COMMENT "MO中的节点ID",
 node_type varchar(64) COMMENT "MO中的节点类型, 例如: DN, CN, LogService; /*TODO: 应为enum类型*/",
 Timestamp datetime COMMENT "日志时间戳",
 Level varchar(32) COMMENT "日志级别, 例如: DEBUG, INFO, WARN, ERROR",
 code_line varchar(4096) COMMENT "写日志所在代码行",
 Message varchar(4096) COMMENT "日志内容/*TODO: 应为text*/"
)`
	createStatementInfoTable = `CREATE TABLE IF NOT EXISTS statement_info(
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
	createErrorInfoTable = `CREATE TABLE IF NOT EXISTS error_info(
 id BIGINT UNSIGNED COMMENT "主键, 应为auto increment类型",
 err_code varchar(1024),
 stack varchar(4096),
 timestamp datetime COMMENT "日志时间戳",
 node_id BIGINT COMMENT "MO中的节点ID",
 node_type varchar(64) COMMENT "MO中的节点类型, 例如: DN, CN, LogService; /*TODO: 应为enum类型*/"
)`
)

const (
	B int64 = 1 << (iota * 10)
	KB
	MB
	GB
)

const (
	MOStatementType = "MOStatementType"
	MOSpanType      = "MOSpan"
	MOLogType       = "MOLog"
	MOErrorType     = "MOError"
)

type IBuffer2SqlItem interface {
	bp.HasName
	Size() int64
	Free()
}

var _ bp.PipeImpl[bp.HasName, any] = &batchSqlHandler{}

type batchSqlHandler struct {
	opts []buffer2SqlOption
}

func NewBufferPipe2SqlWorker(opt ...buffer2SqlOption) bp.PipeImpl[bp.HasName, any] {
	return &batchSqlHandler{opt}
}

// NewItemBuffer implement batchpipe.PipeImpl
func (t batchSqlHandler) NewItemBuffer(name string) bp.ItemBuffer[bp.HasName, any] {
	var f genBatchFunc
	switch name {
	case MOSpanType:
		f = genSpanBatchSql
	case MOLogType:
		f = genLogBatchSql
	case MOStatementType:
		f = genStatementBatchSql
	case MOErrorType:
		f = genErrorBatchSql
	default:
		// fixme: catch Panic Error
		panic(fmt.Sprintf("unknown type %s", name))
	}
	opt := t.opts[:]
	opt = append(opt, bufferWithGenBatchFunc(f), bufferWithType(name))
	return newBuffer2Sql(opt...)
}

// NewItemBatchHandler implement batchpipe.PipeImpl
func (t batchSqlHandler) NewItemBatchHandler() func(batch any) {
	var f = func(b any) {}
	if gTracerProvider.sqlExecutor == nil {
		// fixme: handle error situation, should panic
		logutil.Errorf("[Trace] no SQL Executor.")
		return f
	}
	exec := gTracerProvider.sqlExecutor()
	if exec == nil {
		// fixme: handle error situation, should panic
		logutil.Errorf("[Trace] no SQL Executor.")
		return f
	}
	exec.ApplySessionOverride(ie.NewOptsBuilder().Database(statsDatabase).Internal(true).Finish())
	f = func(b any) {
		_, span := Start(DefaultContext(), "BatchHandle")
		defer span.End()
		batch := b.(string)
		if err := exec.Exec(batch, ie.NewOptsBuilder().Finish()); err != nil {
			// fixme: catch panic error
			// fixme: handle error situation re-try
			logutil.Errorf("[Metric] insert error. sql: %s; err: %v", batch, err)
		}
	}
	return f
}

func quote(value string) string {
	replaceRules := []struct{ src, dst string }{
		{`\\`, `\\\\`},
		{`'`, `\'`},
		{`\0`, `\\0`},
		{"\n", "\\n"},
		{"\r", "\\r"},
		{"\t", "\\t"},
		{`"`, `\"`},
		{"\x1a", "\\\\Z"},
	}
	for _, rule := range replaceRules {
		value = strings.Replace(value, rule.src, rule.dst, -1)
	}
	return value
}

func genSpanBatchSql(in []IBuffer2SqlItem, buf *bytes.Buffer) any {
	buf.Reset()
	if len(in) == 0 {
		return ""
	}

	buf.WriteString(fmt.Sprintf("insert into %s.%s ", statsDatabase, spanInfoTbl))
	buf.WriteString("(")
	buf.WriteString("`span_id`")
	buf.WriteString(", `statement_id`")
	buf.WriteString(", `parent_span_id`")
	buf.WriteString(", `node_id`")
	buf.WriteString(", `node_type`")
	buf.WriteString(", `resource`")
	buf.WriteString(", `name`")
	buf.WriteString(", `start_time`")
	buf.WriteString(", `end_time`")
	buf.WriteString(", `duration`")
	buf.WriteString(") values ")

	moNode := GetNodeResource()

	for _, item := range in {
		s, ok := item.(*MOSpan)
		if !ok {
			panic("Not MOSpan")
		}
		buf.WriteString("(")
		buf.WriteString(fmt.Sprintf("%d", s.SpanID))
		buf.WriteString(fmt.Sprintf(", %d", s.TraceID))
		buf.WriteString(fmt.Sprintf(", %d", s.parent.SpanContext().SpanID))
		buf.WriteString(fmt.Sprintf(", %d", moNode.NodeID))                                  //node_d
		buf.WriteString(fmt.Sprintf(", \"%s\"", moNode.NodeType.String()))                   // node_type
		buf.WriteString(fmt.Sprintf(", \"%s\"", quote(s.tracer.provider.resource.String()))) // resource
		buf.WriteString(fmt.Sprintf(", \"%s\"", quote(s.Name.String())))                     // Name
		buf.WriteString(fmt.Sprintf(", \"%s\"", nanoSec2Datetime(s.StartTimeNS).String2(6))) // start_time
		buf.WriteString(fmt.Sprintf(", \"%s\"", nanoSec2Datetime(s.EndTimeNS).String2(6)))   // end_time
		buf.WriteString(fmt.Sprintf(", %d", s.Duration))                                     // Duration
		buf.WriteString("),")
	}
	return string(buf.Next(buf.Len() - 1))
}

var logStackFormatter atomic.Value

func genLogBatchSql(in []IBuffer2SqlItem, buf *bytes.Buffer) any {
	buf.Reset()
	if len(in) == 0 {
		return ""
	}

	buf.WriteString(fmt.Sprintf("insert into %s.%s ", statsDatabase, logInfoTbl))
	buf.WriteString("(")
	buf.WriteString("`span_id`")
	buf.WriteString(", `statement_id`")
	buf.WriteString(", `node_id`")
	buf.WriteString(", `node_type`")
	buf.WriteString(", `timestamp`")
	buf.WriteString(", `level`")
	buf.WriteString(", `code_line`")
	buf.WriteString(", `message`")
	buf.WriteString(") values ")

	moNode := GetNodeResource()

	for _, item := range in {
		s, ok := item.(*MOLog)
		if !ok {
			panic("Not MOLog")
		}
		buf.WriteString("(")
		buf.WriteString(fmt.Sprintf("%d", s.SpanId))
		buf.WriteString(fmt.Sprintf(", %d", s.StatementId))
		buf.WriteString(fmt.Sprintf(", %d", moNode.NodeID))                                                         // node_id
		buf.WriteString(fmt.Sprintf(", \"%s\"", moNode.NodeType.String()))                                          // node_type
		buf.WriteString(fmt.Sprintf(", \"%s\"", nanoSec2Datetime(s.Timestamp).String2(6)))                          //Timestamp
		buf.WriteString(fmt.Sprintf(", \"%s\"", s.Level.String()))                                                  // log level
		buf.WriteString(fmt.Sprintf(", \"%s\"", quote(fmt.Sprintf(logStackFormatter.Load().(string), s.CodeLine)))) // CodeLine
		buf.WriteString(fmt.Sprintf(", \"%s\"", quote(s.Message)))                                                  // message
		buf.WriteString("),")
	}
	return string(buf.Next(buf.Len() - 1))
}

func genStatementBatchSql(in []IBuffer2SqlItem, buf *bytes.Buffer) any {
	buf.Reset()
	if len(in) == 0 {
		return ""
	}

	buf.WriteString(fmt.Sprintf("insert into %s.%s ", statsDatabase, statementInfoTbl))
	buf.WriteString("(")
	buf.WriteString("`statement_id`")
	buf.WriteString(", `transaction_id`")
	buf.WriteString(", `session_id`")
	buf.WriteString(", `account`")
	buf.WriteString(", `user`")
	buf.WriteString(", `host`")
	buf.WriteString(", `database`")
	buf.WriteString(", `statement`")
	buf.WriteString(", `statement_tag`")
	buf.WriteString(", `statement_fingerprint`")
	buf.WriteString(", `node_id`")
	buf.WriteString(", `node_type`")
	buf.WriteString(", `request_at`")
	buf.WriteString(", `status`")
	buf.WriteString(", `exec_plan`")
	buf.WriteString(") values ")

	moNode := GetNodeResource()

	for _, item := range in {
		s, ok := item.(*StatementInfo)
		if !ok {
			panic("Not StatementInfo")
		}
		buf.WriteString("(")
		buf.WriteString(fmt.Sprintf("%d", s.StatementID))
		buf.WriteString(fmt.Sprintf(", %d", s.TransactionID))
		buf.WriteString(fmt.Sprintf(", %d", s.SessionID))
		buf.WriteString(fmt.Sprintf(", \"%s\"", quote(s.Account)))
		buf.WriteString(fmt.Sprintf(", \"%s\"", quote(s.User)))
		buf.WriteString(fmt.Sprintf(", \"%s\"", quote(s.Host)))
		buf.WriteString(fmt.Sprintf(", \"%s\"", quote(s.Database)))
		buf.WriteString(fmt.Sprintf(", \"%s\"", quote(s.Statement)))
		buf.WriteString(fmt.Sprintf(", \"%s\"", quote(s.StatementFingerprint)))
		buf.WriteString(fmt.Sprintf(", \"%s\"", quote(s.StatementTag)))
		buf.WriteString(fmt.Sprintf(", %d", moNode.NodeID))
		buf.WriteString(fmt.Sprintf(", \"%s\"", moNode.NodeType.String()))
		buf.WriteString(fmt.Sprintf(", \"%s\"", nanoSec2Datetime(s.RequestAt).String2(6)))
		buf.WriteString(fmt.Sprintf(", \"%s\"", quote(s.Status.String())))
		buf.WriteString(fmt.Sprintf(", \"%s\"", quote(s.ExecPlan)))
		buf.WriteString("),")
	}
	return string(buf.Next(buf.Len() - 1))
}

var errorFormatter atomic.Value

func init() {
	errorFormatter.Store("%+v")
	logStackFormatter.Store("%+v")
}

func genErrorBatchSql(in []IBuffer2SqlItem, buf *bytes.Buffer) any {
	buf.Reset()
	if len(in) == 0 {
		return ""
	}

	buf.WriteString(fmt.Sprintf("insert into %s.%s ", statsDatabase, errorInfoTbl))
	buf.WriteString("(")
	buf.WriteString("`err_code`")
	buf.WriteString(", `stack`")
	buf.WriteString(", `timestamp`")
	buf.WriteString(", `node_id`")
	buf.WriteString(", `node_type`")
	buf.WriteString(") values ")

	moNode := GetNodeResource()

	for _, item := range in {
		s, ok := item.(*MOErrorHolder)
		if !ok {
			panic("Not MOErrorHolder")
		}
		buf.WriteString("(")
		buf.WriteString(fmt.Sprintf("\"%s\"", quote(s.Error.Error())))
		buf.WriteString(fmt.Sprintf(", \"%s\"", quote(fmt.Sprintf(errorFormatter.Load().(string), s.Error))))
		buf.WriteString(fmt.Sprintf(", \"%s\"", nanoSec2Datetime(s.Timestamp).String2(6)))
		buf.WriteString(fmt.Sprintf(", %d", moNode.NodeID))
		buf.WriteString(fmt.Sprintf(", \"%s\"", moNode.NodeType.String()))
		buf.WriteString("),")
	}
	return string(buf.Next(buf.Len() - 1))
}

var _ bp.ItemBuffer[bp.HasName, any] = &buffer2Sql{}

// buffer2Sql catch item, like trace/log/error, buffer
type buffer2Sql struct {
	bp.Reminder   // see bufferWithReminder
	buf           []IBuffer2SqlItem
	mux           sync.Mutex
	bufferType    string // see bufferWithType
	size          int64  // default: 1 MB
	sizeThreshold int64  // see bufferWithSizeThreshold

	genBatchFunc genBatchFunc
}

type genBatchFunc func([]IBuffer2SqlItem, *bytes.Buffer) any

var genBatchEmptySQL = genBatchFunc(func([]IBuffer2SqlItem, *bytes.Buffer) any { return "" })

func newBuffer2Sql(opts ...buffer2SqlOption) *buffer2Sql {
	b := &buffer2Sql{
		Reminder:      bp.NewConstantClock(5 * time.Second),
		sizeThreshold: 1 * MB,
		genBatchFunc:  genBatchEmptySQL,
	}
	for _, opt := range opts {
		opt.apply(b)
	}
	return b
}

func (b *buffer2Sql) Add(i bp.HasName) {
	b.mux.Lock()
	defer b.mux.Unlock()
	if item, ok := i.(IBuffer2SqlItem); !ok {
		panic("not implement interface IBuffer2SqlItem")
	} else {
		b.buf = append(b.buf, item)
		b.size += item.Size()
	}
}

func (b *buffer2Sql) Reset() {
	b.mux.Lock()
	defer b.mux.Unlock()
	b.buf = b.buf[0:0]
	b.size = 0
}

func (b *buffer2Sql) IsEmpty() bool {
	b.mux.Lock()
	defer b.mux.Unlock()
	return b.isEmpty()
}

func (b *buffer2Sql) isEmpty() bool {
	return len(b.buf) == 0
}

func (b *buffer2Sql) ShouldFlush() bool {
	return atomic.LoadInt64(&b.size) > b.sizeThreshold
}

func (b *buffer2Sql) Size() int64 {
	return atomic.LoadInt64(&b.size)
}

func (b *buffer2Sql) GetBufferType() string {
	return b.bufferType
}

func (b *buffer2Sql) GetBatch(buf *bytes.Buffer) any {
	_, span := Start(DefaultContext(), "GenBatch")
	defer span.End()
	b.mux.Lock()
	defer b.mux.Unlock()

	if b.isEmpty() {
		return ""
	}
	return b.genBatchFunc(b.buf, buf)
}

type buffer2SqlOption interface {
	apply(*buffer2Sql)
}

type buffer2SqlOptionFunc func(*buffer2Sql)

func (f buffer2SqlOptionFunc) apply(b *buffer2Sql) {
	f(b)
}

func bufferWithReminder(reminder bp.Reminder) buffer2SqlOption {
	return buffer2SqlOptionFunc(func(b *buffer2Sql) {
		b.Reminder = reminder
	})
}

func bufferWithType(name string) buffer2SqlOption {
	return buffer2SqlOptionFunc(func(b *buffer2Sql) {
		b.bufferType = name
	})
}

func bufferWithSizeThreshold(size int64) buffer2SqlOption {
	return buffer2SqlOptionFunc(func(b *buffer2Sql) {
		b.sizeThreshold = size
	})
}

func bufferWithGenBatchFunc(f genBatchFunc) buffer2SqlOption {
	return buffer2SqlOptionFunc(func(b *buffer2Sql) {
		b.genBatchFunc = f
	})
}

// nanoSec2Datetime implement container/types/datetime.go Datetime.String2
func nanoSec2Datetime(t util.TimeMono) types.Datetime {
	sec, nsec := t/1e9, t%1e9
	return types.Datetime((sec << 20) + nsec/1000)
}
