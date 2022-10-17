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
	"bytes"
	"context"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/util"
	bp "github.com/matrixorigin/matrixone/pkg/util/batchpipe"
	"github.com/matrixorigin/matrixone/pkg/util/errutil"
	"github.com/matrixorigin/matrixone/pkg/util/export"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"

	"github.com/google/uuid"
)

const defaultClock = 15 * time.Second

var errorFormatter atomic.Value
var insertSQLPrefix []string

func init() {
	errorFormatter.Store("%+v")
	logStackFormatter.Store("%+v")

	tables := []string{statementInfoTbl, spanInfoTbl, logInfoTbl, errorInfoTbl}
	for _, table := range tables {
		insertSQLPrefix = append(insertSQLPrefix, fmt.Sprintf("insert into %s.%s ", StatsDatabase, table))
	}
}

type IBuffer2SqlItem interface {
	bp.HasName
	Size() int64
	Free()
}

var _ bp.PipeImpl[bp.HasName, any] = &batchSqlHandler{}

type batchSqlHandler struct {
	defaultOpts []bufferOption
}

func NewBufferPipe2SqlWorker(opt ...bufferOption) bp.PipeImpl[bp.HasName, any] {
	return &batchSqlHandler{opt}
}

// NewItemBuffer implement batchpipe.PipeImpl
func (t batchSqlHandler) NewItemBuffer(name string) bp.ItemBuffer[bp.HasName, any] {
	var opts []bufferOption
	var f genBatchFunc
	logutil.Debugf("NewItemBuffer name: %s", name)
	switch name {
	case MOSpanType:
		f = genSpanBatchSql
	case MORawLogType:
		f = genLogBatchSql
	case MOLogType:
		f = genZapLogBatchSql
	case MOStatementType:
		f = genStatementBatchSql
		opts = append(opts, bufferWithFilterItemFunc(filterTraceInsertSql))
	case MOErrorType:
		f = genErrorBatchSql
	default:
		panic(moerr.NewInternalError("unknown type %s", name))
	}
	opts = append(opts, bufferWithGenBatchFunc(f), bufferWithType(name))
	opts = append(opts, t.defaultOpts...)
	return newBuffer2Sql(opts...)
}

// NewItemBatchHandler implement batchpipe.PipeImpl
func (t batchSqlHandler) NewItemBatchHandler(ctx context.Context) func(batch any) {
	var f = func(b any) {}
	sqlExecutor := GetTracerProvider().GetSqlExecutor()
	if sqlExecutor == nil {
		// fixme: handle error situation, should panic
		logutil.Errorf("[Trace] no SQL Executor.")
		return f
	}
	exec := sqlExecutor()
	if exec == nil {
		// fixme: handle error situation, should panic
		logutil.Errorf("[Trace] no SQL Executor.")
		return f
	}
	exec.ApplySessionOverride(ie.NewOptsBuilder().Database(StatsDatabase).Internal(true).Finish())
	f = func(b any) {
		// fixme: CollectCycle
		_, span := Start(DefaultContext(), "BatchHandle")
		defer span.End()
		batch := b.(string)
		if len(batch) == 0 {
			logutil.Warnf("meet empty sql")
			return
		}
		if err := exec.Exec(ctx, batch, ie.NewOptsBuilder().Finish()); err != nil {
			// fixme: error -> log -> exec.Exec -> ... cycle
			// fixme: handle error situation re-try
			logutil.Error(fmt.Sprintf("[Trace] faield to insert. sql: %s", batch), logutil.NoReportFiled())
			logutil.Error(fmt.Sprintf("[Trace] faield to insert. err: %v", err), logutil.NoReportFiled())
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
		logutil.Debugf("genSpanBatchSql empty")
		return ""
	}

	buf.WriteString(fmt.Sprintf("insert into %s.%s ", StatsDatabase, spanInfoTbl))
	buf.WriteString("(")
	buf.WriteString("`span_id`")
	buf.WriteString(", `statement_id`")
	buf.WriteString(", `parent_span_id`")
	buf.WriteString(", `node_uuid`")
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
		buf.WriteString(fmt.Sprintf(`"%s"`, s.SpanID.String()))
		buf.WriteString(fmt.Sprintf(`, "%s"`, s.TraceID.String()))
		buf.WriteString(fmt.Sprintf(`, "%s"`, s.parent.SpanContext().SpanID.String()))
		buf.WriteString(fmt.Sprintf(`, "%s"`, moNode.NodeUuid))                            // node_uuid
		buf.WriteString(fmt.Sprintf(`, "%s"`, moNode.NodeType))                            // node_type
		buf.WriteString(fmt.Sprintf(`, "%s"`, quote(s.tracer.provider.resource.String()))) // resource
		buf.WriteString(fmt.Sprintf(`, "%s"`, quote(s.Name.String())))                     // Name
		buf.WriteString(fmt.Sprintf(`, "%s"`, nanoSec2DatetimeString(s.StartTimeNS)))      // start_time
		buf.WriteString(fmt.Sprintf(`, "%s"`, nanoSec2DatetimeString(s.EndTimeNS)))        // end_time
		buf.WriteString(fmt.Sprintf(", %d", s.Duration))                                   // Duration
		buf.WriteString("),")
	}
	return string(buf.Next(buf.Len() - 1))
}

var logStackFormatter atomic.Value

func genLogBatchSql(in []IBuffer2SqlItem, buf *bytes.Buffer) any {
	buf.Reset()
	if len(in) == 0 {
		logutil.Debugf("genLogBatchSql empty")
		return ""
	}

	buf.WriteString(fmt.Sprintf("insert into %s.%s ", StatsDatabase, logInfoTbl))
	buf.WriteString("(")
	buf.WriteString("`span_id`")
	buf.WriteString(", `statement_id`")
	buf.WriteString(", `node_uuid`")
	buf.WriteString(", `node_type`")
	buf.WriteString(", `timestamp`")
	buf.WriteString(", `name`")
	buf.WriteString(", `level`")
	buf.WriteString(", `caller`")
	buf.WriteString(", `message`")
	buf.WriteString(", `extra`")
	buf.WriteString(") values ")

	moNode := GetNodeResource()

	for _, item := range in {
		s, ok := item.(*MOLog)
		if !ok {
			panic("Not MOLog")
		}
		buf.WriteString("(")
		buf.WriteString(fmt.Sprintf(`"%s"`, s.SpanID.String()))
		buf.WriteString(fmt.Sprintf(`, "%s"`, s.TraceID.String()))
		buf.WriteString(fmt.Sprintf(`, "%s"`, moNode.NodeUuid))                                                 // node_uuid
		buf.WriteString(fmt.Sprintf(`, "%s"`, moNode.NodeType))                                                 // node_type
		buf.WriteString(fmt.Sprintf(`, "%s"`, nanoSec2DatetimeString(s.Timestamp)))                             // timestamp
		buf.WriteString(fmt.Sprintf(`, "%s"`, quote(s.Name)))                                                   // log level
		buf.WriteString(fmt.Sprintf(`, "%s"`, s.Level.String()))                                                // log level
		buf.WriteString(fmt.Sprintf(`, "%s"`, quote(fmt.Sprintf(logStackFormatter.Load().(string), s.Caller)))) // caller
		buf.WriteString(fmt.Sprintf(`, "%s"`, quote(s.Message)))                                                // message
		buf.WriteString(fmt.Sprintf(`, "%s"`, quote(s.Extra)))                                                  // extra
		buf.WriteString("),")
	}
	return string(buf.Next(buf.Len() - 1))
}

func genZapLogBatchSql(in []IBuffer2SqlItem, buf *bytes.Buffer) any {
	buf.Reset()
	if len(in) == 0 {
		logutil.Debugf("genZapLogBatchSql empty")
		return ""
	}

	buf.WriteString(fmt.Sprintf("insert into %s.%s ", StatsDatabase, logInfoTbl))
	buf.WriteString("(")
	buf.WriteString("`span_id`")
	buf.WriteString(", `statement_id`")
	buf.WriteString(", `node_uuid`")
	buf.WriteString(", `node_type`")
	buf.WriteString(", `timestamp`")
	buf.WriteString(", `name`")
	buf.WriteString(", `level`")
	buf.WriteString(", `caller`")
	buf.WriteString(", `message`")
	buf.WriteString(", `extra`")
	buf.WriteString(") values ")

	moNode := GetNodeResource()

	for _, item := range in {
		s, ok := item.(*MOZapLog)
		if !ok {
			panic("Not MOZapLog")
		}

		buf.WriteString("(")
		buf.WriteString(fmt.Sprintf(`%q`, s.SpanContext.SpanID.String()))
		buf.WriteString(fmt.Sprintf(`, %q`, s.SpanContext.TraceID.String()))
		buf.WriteString(fmt.Sprintf(`, %q`, moNode.NodeUuid))                        // node_uuid
		buf.WriteString(fmt.Sprintf(`, %q`, moNode.NodeType))                        // node_type
		buf.WriteString(fmt.Sprintf(`, %q`, s.Timestamp.Format(timestampFormatter))) // timestamp
		buf.WriteString(fmt.Sprintf(`, %q`, s.LoggerName))                           // name
		buf.WriteString(fmt.Sprintf(`, %q`, s.Level.String()))                       // log level
		buf.WriteString(fmt.Sprintf(`, %q`, s.Caller))                               // caller
		buf.WriteString(fmt.Sprintf(`, %q`, s.Message))                              // message
		buf.WriteString(fmt.Sprintf(`, %q`, s.Extra))                                // extra
		buf.WriteString("),")
	}
	return string(buf.Next(buf.Len() - 1))
}

var genStatementBatchSql = func(in []IBuffer2SqlItem, buf *bytes.Buffer) any {
	buf.Reset()
	if len(in) == 0 {
		logutil.Debugf("genStatementBatchSql empty")
		return ""
	}

	buf.WriteString(fmt.Sprintf("insert into %s.%s ", StatsDatabase, statementInfoTbl))
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
	buf.WriteString(", `node_uuid`")
	buf.WriteString(", `node_type`")
	buf.WriteString(", `request_at`")
	buf.WriteString(", `response_at`")
	buf.WriteString(", `status`")
	buf.WriteString(", `error`")
	buf.WriteString(", `duration`")
	buf.WriteString(", `exec_plan`")
	buf.WriteString(") values ")

	moNode := GetNodeResource()

	for _, item := range in {
		s, ok := item.(*StatementInfo)
		if !ok {
			panic("Not StatementInfo")
		}
		s.mux.Lock()
		buf.WriteString("(")
		buf.WriteString(fmt.Sprintf(`%q`, uuid.UUID(s.StatementID).String()))
		buf.WriteString(fmt.Sprintf(`, %q`, uuid.UUID(s.TransactionID).String()))
		buf.WriteString(fmt.Sprintf(`, %q`, uuid.UUID(s.SessionID).String()))
		buf.WriteString(fmt.Sprintf(`, %q`, s.Account))
		buf.WriteString(fmt.Sprintf(`, %q`, s.User))
		buf.WriteString(fmt.Sprintf(`, %q`, s.Host))
		buf.WriteString(fmt.Sprintf(`, %q`, s.Database))
		buf.WriteString(fmt.Sprintf(`, %q`, s.Statement))
		buf.WriteString(fmt.Sprintf(`, %q`, s.StatementFingerprint))
		buf.WriteString(fmt.Sprintf(`, %q`, s.StatementTag))
		buf.WriteString(fmt.Sprintf(`, %q`, moNode.NodeUuid))
		buf.WriteString(fmt.Sprintf(`, %q`, moNode.NodeType))
		buf.WriteString(fmt.Sprintf(`, %q`, nanoSec2DatetimeString(s.RequestAt)))
		buf.WriteString(fmt.Sprintf(`, %q`, nanoSec2DatetimeString(s.ResponseAt)))
		buf.WriteString(fmt.Sprintf(`, %d`, s.Duration))
		buf.WriteString(fmt.Sprintf(`, %q`, s.Status.String()))
		if s.Error == nil {
			buf.WriteString(`, ""`)
		} else {
			buf.WriteString(fmt.Sprintf(`, %q`, s.Error))
		}
		buf.WriteString(fmt.Sprintf(`, %q`, s.ExecPlan2Json()))
		buf.WriteString("),")

		s.exported = true
		s.mux.Unlock()
	}
	return string(buf.Next(buf.Len() - 1))
}

func genErrorBatchSql(in []IBuffer2SqlItem, buf *bytes.Buffer) any {
	buf.Reset()
	if len(in) == 0 {
		logutil.Debugf("genErrorBatchSql empty")
		return ""
	}

	buf.WriteString(fmt.Sprintf("insert into %s.%s ", StatsDatabase, errorInfoTbl))
	buf.WriteString("(")
	buf.WriteString("`statement_id`")
	buf.WriteString(", `span_id`")
	buf.WriteString(", `node_uuid`")
	buf.WriteString(", `node_type`")
	buf.WriteString(", `err_code`")
	buf.WriteString(", `stack`")
	buf.WriteString(", `timestamp`")
	buf.WriteString(") values ")

	moNode := GetNodeResource()

	var span Span
	for _, item := range in {
		s, ok := item.(*MOErrorHolder)
		if !ok {
			panic("Not MOErrorHolder")
		}
		if ct := errutil.GetContextTracer(s.Error); ct != nil && ct.Context() != nil {
			span = SpanFromContext(ct.Context())
		} else {
			span = SpanFromContext(DefaultContext())
		}
		buf.WriteString("(")
		buf.WriteString(fmt.Sprintf(`"%s"`, span.SpanContext().TraceID.String()))
		buf.WriteString(fmt.Sprintf(`, "%s"`, span.SpanContext().SpanID.String()))
		buf.WriteString(fmt.Sprintf(`, "%s"`, moNode.NodeUuid))
		buf.WriteString(fmt.Sprintf(`, "%s"`, moNode.NodeType))
		buf.WriteString(fmt.Sprintf(`, "%s"`, quote(s.Error.Error())))
		buf.WriteString(fmt.Sprintf(`, "%s"`, quote(fmt.Sprintf(errorFormatter.Load().(string), s.Error))))
		buf.WriteString(fmt.Sprintf(`, "%s"`, nanoSec2DatetimeString(s.Timestamp)))
		buf.WriteString("),")
	}
	return string(buf.Next(buf.Len() - 1))
}

type batchCSVHandler struct {
	defaultOpts []bufferOption
}

func NewBufferPipe2CSVWorker(opt ...bufferOption) bp.PipeImpl[bp.HasName, any] {
	return &batchCSVHandler{opt}
}

// NewItemBuffer implement batchpipe.PipeImpl
func (t batchCSVHandler) NewItemBuffer(name string) bp.ItemBuffer[bp.HasName, any] {
	var opts []bufferOption
	var f genBatchFunc = genCsvData
	logutil.Debugf("NewItemBuffer name: %s", name)
	switch name {
	case MOSpanType:
	case MORawLogType:
	case MOLogType:
	case MOStatementType:
		opts = append(opts, bufferWithFilterItemFunc(filterTraceInsertSql))
	case MOErrorType:
	default:
		panic(moerr.NewInternalError("unknown type %s", name))
	}
	opts = append(opts, bufferWithGenBatchFunc(f), bufferWithType(name))
	opts = append(opts, t.defaultOpts...)
	return newBuffer2Sql(opts...)
}

type CSVRequests []*CSVRequest

type CSVRequest struct {
	writer  io.StringWriter
	content string
}

func NewCSVRequest(writer io.StringWriter, content string) *CSVRequest {
	return &CSVRequest{writer, content}
}

func (r *CSVRequest) Handle() (int, error) {
	return r.writer.WriteString(r.content)
}

func (r *CSVRequest) Content() string {
	return r.content
}

// NewItemBatchHandler implement batchpipe.PipeImpl
func (t batchCSVHandler) NewItemBatchHandler(ctx context.Context) func(b any) {
	var f = func(b any) {
		_, span := Start(DefaultContext(), "batchCSVHandler")
		defer span.End()
		req, ok := b.(*CSVRequest) // see genCsvData
		if !ok {
			panic(moerr.NewInternalError("batchCSVHandler meet unknown type: %v", reflect.ValueOf(b).Type()))
		}
		if len(req.content) == 0 {
			logutil.Warnf("meet empty csv content")
			return
		}
		if _, err := req.writer.WriteString(req.content); err != nil {
			logutil.Error(fmt.Sprintf("[Trace] faield to write csv: %s", req.content), logutil.NoReportFiled())
			logutil.Error(fmt.Sprintf("[Trace] faield to write. err: %v", err), logutil.NoReportFiled())
		}
	}
	return f
}

type CsvFields interface {
	bp.HasName
	CsvFields() []string
}

var QuoteFieldFunc = func(buf *bytes.Buffer, value string, enclose rune) string {
	replaceRules := map[rune]string{
		'"':  `""`,
		'\'': `\'`,
	}
	quotedClose, hasRule := replaceRules[enclose]
	if !hasRule {
		panic(moerr.NewInternalError("not support csv enclose: %c", enclose))
	}
	for _, c := range value {
		if c == enclose {
			buf.WriteString(quotedClose)
		} else {
			buf.WriteRune(c)
		}
	}
	return value
}

func genCsvData(in []IBuffer2SqlItem, buf *bytes.Buffer) any {
	buf.Reset()
	if len(in) == 0 {
		return NewCSVRequest(nil, "")
	}

	i, ok := in[0].(CsvFields)
	if !ok {
		panic("not MalCsv, dont support output CSV")
	}
	opts := export.CommonCsvOptions

	writer := GetTracerProvider().writerFactory(DefaultContext(), StatsDatabase, i)

	for _, i := range in {
		item, ok := i.(CsvFields)
		if !ok {
			panic("not MalCsv, dont support output CSV")
		}
		fields := item.CsvFields()
		for idx, field := range fields {
			if idx > 0 {
				buf.WriteRune(opts.FieldTerminator)
			}
			if strings.ContainsRune(field, opts.FieldTerminator) || strings.ContainsRune(field, opts.EncloseRune) || strings.ContainsRune(field, opts.Terminator) {
				buf.WriteRune(opts.EncloseRune)
				QuoteFieldFunc(buf, field, opts.EncloseRune)
				buf.WriteRune(opts.EncloseRune)
			} else {
				buf.WriteString(field)
			}
		}
		buf.WriteRune(opts.Terminator)
	}
	return NewCSVRequest(writer, buf.String())
}

func filterTraceInsertSql(i IBuffer2SqlItem) {
	s := i.(*StatementInfo)
	for _, prefix := range insertSQLPrefix {
		if strings.Contains(s.Statement, prefix) {
			logutil.Debugf("find insert system sql, short it.")
			s.Statement = prefix
		}
	}
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

	filterItemFunc filterItemFunc
	genBatchFunc   genBatchFunc
}

type filterItemFunc func(IBuffer2SqlItem)
type genBatchFunc func([]IBuffer2SqlItem, *bytes.Buffer) any

var noopFilterItemFunc = func(IBuffer2SqlItem) {}
var noopGenBatchSQL = genBatchFunc(func([]IBuffer2SqlItem, *bytes.Buffer) any { return "" })

func newBuffer2Sql(opts ...bufferOption) *buffer2Sql {
	b := &buffer2Sql{
		Reminder:       bp.NewConstantClock(defaultClock),
		buf:            make([]IBuffer2SqlItem, 0, 10240),
		sizeThreshold:  10 * mpool.MB,
		filterItemFunc: noopFilterItemFunc,
		genBatchFunc:   noopGenBatchSQL,
	}
	for _, opt := range opts {
		opt.apply(b)
	}
	logutil.Debugf("newBuffer2Sql, Reminder next: %v", b.Reminder.RemindNextAfter())
	if b.genBatchFunc == nil || b.filterItemFunc == nil || b.Reminder == nil {
		logutil.Debug("newBuffer2Sql meet nil elem")
		return nil
	}
	return b
}

func (b *buffer2Sql) Add(i bp.HasName) {
	b.mux.Lock()
	defer b.mux.Unlock()
	if item, ok := i.(IBuffer2SqlItem); !ok {
		panic("not implement interface IBuffer2SqlItem")
	} else {
		b.filterItemFunc(item)
		b.buf = append(b.buf, item)
		atomic.AddInt64(&b.size, item.Size())
	}
}

func (b *buffer2Sql) Reset() {
	b.mux.Lock()
	defer b.mux.Unlock()
	for _, i := range b.buf {
		i.Free()
	}
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
	b.mux.Lock()
	defer b.mux.Unlock()
	return b.size > b.sizeThreshold
}

func (b *buffer2Sql) Size() int64 {
	b.mux.Lock()
	defer b.mux.Unlock()
	return b.size
}

func (b *buffer2Sql) GetBufferType() string {
	return b.bufferType
}

func (b *buffer2Sql) GetBatch(buf *bytes.Buffer) any {
	// fixme: CollectCycle
	_, span := Start(DefaultContext(), "GenBatch")
	defer span.End()
	b.mux.Lock()
	defer b.mux.Unlock()

	if b.isEmpty() {
		return nil
	}
	return b.genBatchFunc(b.buf, buf)
}

type bufferOption interface {
	apply(*buffer2Sql)
}

type buffer2SqlOptionFunc func(*buffer2Sql)

func (f buffer2SqlOptionFunc) apply(b *buffer2Sql) {
	f(b)
}

func bufferWithReminder(reminder bp.Reminder) bufferOption {
	return buffer2SqlOptionFunc(func(b *buffer2Sql) {
		b.Reminder = reminder
	})
}

func bufferWithType(name string) bufferOption {
	return buffer2SqlOptionFunc(func(b *buffer2Sql) {
		b.bufferType = name
	})
}

func bufferWithSizeThreshold(size int64) bufferOption {
	return buffer2SqlOptionFunc(func(b *buffer2Sql) {
		b.sizeThreshold = size
	})
}

func bufferWithFilterItemFunc(f filterItemFunc) bufferOption {
	return buffer2SqlOptionFunc(func(b *buffer2Sql) {
		b.filterItemFunc = f
	})
}

func bufferWithGenBatchFunc(f genBatchFunc) bufferOption {
	return buffer2SqlOptionFunc(func(b *buffer2Sql) {
		b.genBatchFunc = f
	})
}

const timestampFormatter = "2006-01-02 15:04:05.000000"

// nanoSec2Datetime implement container/types/datetime.go Datetime.String2
func nanoSec2Datetime(t util.TimeMono) types.Datetime {
	sec, nsec := t/1e9, t%1e9
	// calculate like Datetime::Now() in datetime.go, but year = 0053
	return types.Datetime((sec << 20) + nsec/1000)
}

// nanoSec2Datetime
func nanoSec2DatetimeString(t util.TimeMono) string {
	sec, nsec := t/1e9, t%1e9
	// fixme: format() should use db's time-zone
	return time.Unix(int64(sec), int64(nsec)).Format(timestampFormatter)
}

// time2DatetimeString
func time2DatetimeString(t time.Time) string {
	return t.Format(timestampFormatter)
}
