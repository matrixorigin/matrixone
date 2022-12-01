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
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
)

const defaultClock = 15 * time.Second

var errorFormatter atomic.Value
var logStackFormatter atomic.Value

func init() {
	errorFormatter.Store("%+v")
	logStackFormatter.Store("%+v")
}

type IBuffer2SqlItem interface {
	bp.HasName
	Size() int64
	Free()
}

var _ PipeImpl = (*batchCSVHandler)(nil)

type batchCSVHandler struct {
	defaultOpts []BufferOption
}

func NewBufferPipe2CSVWorker(opt ...BufferOption) bp.PipeImpl[bp.HasName, any] {
	return &batchCSVHandler{opt}
}

// NewItemBuffer implement batchpipe.PipeImpl
func (t batchCSVHandler) NewItemBuffer(name string) bp.ItemBuffer[bp.HasName, any] {
	var opts []BufferOption
	var f genBatchFunc = genCsvData
	logutil.Debugf("NewItemBuffer name: %s", name)
	ctx := DefaultContext()
	switch name {
	case MOStatementType, SingleStatementTable.GetName():
	case MOErrorType:
	case MOSpanType:
	case MOLogType:
	case MORawLogType:
	default:
		panic(moerr.NewInternalError(ctx, "unknown type %s", name))
	}
	opts = append(opts, BufferWithGenBatchFunc(f), BufferWithType(name))
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

	handle := func(b any) {
		req, ok := b.(*CSVRequest) // see genCsvData
		if !ok {
			panic(moerr.NewInternalError(ctx, "batchCSVHandler meet unknown type: %v", reflect.ValueOf(b).Type()))
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

	var f = func(batch any) {
		_, span := Start(DefaultContext(), "batchCSVHandler")
		defer span.End()
		switch b := batch.(type) {
		case *CSVRequest:
			handle(b)
		case CSVRequests:
			for _, req := range b {
				handle(req)
			}
		default:
			panic(moerr.NewNotSupported(ctx, "unknown batch type: %v", reflect.ValueOf(b).Type()))
		}
	}
	return f
}

type CsvFields interface {
	bp.HasName
	GetRow() *table.Row
	CsvFields(ctx context.Context, row *table.Row) []string
}

var QuoteFieldFunc = func(ctx context.Context, buf *bytes.Buffer, value string, enclose rune) string {
	replaceRules := map[rune]string{
		'"':  `""`,
		'\'': `\'`,
	}
	quotedClose, hasRule := replaceRules[enclose]
	if !hasRule {
		panic(moerr.NewInternalError(ctx, "not support csv enclose: %c", enclose))
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

type WriteFactoryConfig struct {
	Account     string
	Ts          time.Time
	PathBuilder table.PathBuilder
}

type FSWriterFactory func(ctx context.Context, db string, name bp.HasName, config WriteFactoryConfig) io.StringWriter

func genCsvData(ctx context.Context, in []IBuffer2SqlItem, buf *bytes.Buffer) any {
	buf.Reset()
	if len(in) == 0 {
		return NewCSVRequest(nil, "")
	}

	i, ok := in[0].(CsvFields)
	if !ok {
		panic("not MalCsv, dont support output CSV")
	}

	ts := time.Now()
	buffer := make(map[string]*bytes.Buffer, 2)
	writeValues := func(item CsvFields, row *table.Row) {
		fields := item.CsvFields(ctx, row)
		buf, exist := buffer[row.GetAccount()]
		if !exist {
			buf = bytes.NewBuffer(nil)
			buffer[row.GetAccount()] = buf
		}
		writeCsvOneLine(ctx, buf, fields)
	}

	row := i.GetRow()
	for _, i := range in {
		item, ok := i.(CsvFields)
		if !ok {
			panic("not MalCsv, dont support output CSV")
		}
		writeValues(item, row)
	}

	reqs := make(CSVRequests, 0, len(buffer))
	for account, buf := range buffer {
		writer := GetTracerProvider().writerFactory(DefaultContext(), row.Table.Database, row.Table,
			WriteFactoryConfig{Account: account, Ts: ts, PathBuilder: row.Table.PathBuilder})
		reqs = append(reqs, NewCSVRequest(writer, buf.String()))
	}

	return reqs
}

func writeCsvOneLine(ctx context.Context, buf *bytes.Buffer, fields []string) {
	opts := table.CommonCsvOptions
	for idx, field := range fields {
		if idx > 0 {
			buf.WriteRune(opts.FieldTerminator)
		}
		if strings.ContainsRune(field, opts.FieldTerminator) || strings.ContainsRune(field, opts.EncloseRune) || strings.ContainsRune(field, opts.Terminator) {
			buf.WriteRune(opts.EncloseRune)
			QuoteFieldFunc(ctx, buf, field, opts.EncloseRune)
			buf.WriteRune(opts.EncloseRune)
		} else {
			buf.WriteString(field)
		}
	}
	buf.WriteRune(opts.Terminator)
}

var _ bp.ItemBuffer[bp.HasName, any] = &buffer2Sql{}

// buffer2Sql catch item, like trace/log/error, buffer
type buffer2Sql struct {
	bp.Reminder   // see BufferWithReminder
	buf           []IBuffer2SqlItem
	mux           sync.Mutex
	bufferType    string // see BufferWithType
	size          int64  // default: 1 MB
	sizeThreshold int64  // see BufferWithSizeThreshold

	filterItemFunc filterItemFunc
	genBatchFunc   genBatchFunc
}

type filterItemFunc func(IBuffer2SqlItem)
type genBatchFunc func(context.Context, []IBuffer2SqlItem, *bytes.Buffer) any

var noopFilterItemFunc = func(IBuffer2SqlItem) {}
var noopGenBatchSQL = genBatchFunc(func(context.Context, []IBuffer2SqlItem, *bytes.Buffer) any { return "" })

func newBuffer2Sql(opts ...BufferOption) *buffer2Sql {
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

func (b *buffer2Sql) GetBatch(ctx context.Context, buf *bytes.Buffer) any {
	// fixme: CollectCycle
	ctx, span := Start(ctx, "GenBatch")
	defer span.End()
	b.mux.Lock()
	defer b.mux.Unlock()

	if b.isEmpty() {
		return nil
	}
	return b.genBatchFunc(ctx, b.buf, buf)
}

type BufferOption interface {
	apply(*buffer2Sql)
}

type buffer2SqlOptionFunc func(*buffer2Sql)

func (f buffer2SqlOptionFunc) apply(b *buffer2Sql) {
	f(b)
}

func BufferWithReminder(reminder bp.Reminder) BufferOption {
	return buffer2SqlOptionFunc(func(b *buffer2Sql) {
		b.Reminder = reminder
	})
}

func BufferWithType(name string) BufferOption {
	return buffer2SqlOptionFunc(func(b *buffer2Sql) {
		b.bufferType = name
	})
}

func BufferWithSizeThreshold(size int64) BufferOption {
	return buffer2SqlOptionFunc(func(b *buffer2Sql) {
		b.sizeThreshold = size
	})
}

func BufferWithFilterItemFunc(f filterItemFunc) BufferOption {
	return buffer2SqlOptionFunc(func(b *buffer2Sql) {
		b.filterItemFunc = f
	})
}

func BufferWithGenBatchFunc(f genBatchFunc) BufferOption {
	return buffer2SqlOptionFunc(func(b *buffer2Sql) {
		b.genBatchFunc = f
	})
}
