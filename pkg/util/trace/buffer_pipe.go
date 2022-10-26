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
	"github.com/matrixorigin/matrixone/pkg/logutil"
	bp "github.com/matrixorigin/matrixone/pkg/util/batchpipe"
	"github.com/matrixorigin/matrixone/pkg/util/export"
)

const defaultClock = 15 * time.Second

var errorFormatter atomic.Value
var logStackFormatter atomic.Value
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
	case MOStatementType, SingleStatementTable.GetName():
		opts = append(opts, bufferWithFilterItemFunc(filterTraceInsertSql))
	case MOErrorType:
	case MOSpanType:
	case MOLogType:
	case MORawLogType:
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
	GetRow() *export.Row
	CsvFields(row *export.Row) []string
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

	ts := time.Now()
	row := i.GetRow()
	for _, i := range in {
		item, ok := i.(CsvFields)
		if !ok {
			panic("not MalCsv, dont support output CSV")
		}
		fields := item.CsvFields(row)
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

	writer := GetTracerProvider().writerFactory(DefaultContext(), StatsDatabase, i,
		export.WithTimestamp(ts),
		export.WithPathBuilder(row.Table.PathBuilder))
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
	for idx, i := range b.buf {
		i.Free()
		b.buf[idx] = nil
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

// time2DatetimeString
func time2DatetimeString(t time.Time) string {
	return t.Format(timestampFormatter)
}
