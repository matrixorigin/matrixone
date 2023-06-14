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
	"bytes"
	"context"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/util/export/etl"
	"github.com/matrixorigin/matrixone/pkg/util/trace"

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

var _ PipeImpl = (*batchETLHandler)(nil)

type batchETLHandler struct {
	defaultOpts []BufferOption
}

func NewBufferPipe2CSVWorker(opt ...BufferOption) bp.PipeImpl[bp.HasName, any] {
	return &batchETLHandler{opt}
}

// NewItemBuffer implement batchpipe.PipeImpl
func (t batchETLHandler) NewItemBuffer(name string) bp.ItemBuffer[bp.HasName, any] {
	var opts []BufferOption
	var f genBatchFunc = genETLData
	logutil.Debugf("NewItemBuffer name: %s", name)
	switch name {
	case MOStatementType, SingleStatementTable.GetName():
	case MOErrorType:
	case MOSpanType:
	case MOLogType:
	case MORawLogType:
	default:
		logutil.Warnf("batchETLHandler handle new type: %s", name)
	}
	opts = append(opts, BufferWithGenBatchFunc(f), BufferWithType(name))
	opts = append(opts, t.defaultOpts...)
	return NewItemBuffer(opts...)
}

// NewItemBatchHandler implement batchpipe.PipeImpl
func (t batchETLHandler) NewItemBatchHandler(ctx context.Context) func(b any) {

	handle := func(b any) {
		req, ok := b.(table.WriteRequest) // see genETLData
		if !ok {
			panic(moerr.NewInternalError(ctx, "batchETLHandler meet unknown type: %v", reflect.ValueOf(b).Type()))
		}
		if _, err := req.Handle(); err != nil {
			logutil.Error(fmt.Sprintf("[Trace] failed to write. err: %v", err), logutil.NoReportFiled())
		}
	}

	var f = func(batch any) {
		_, span := trace.Start(DefaultContext(), "batchETLHandler")
		defer span.End()
		switch b := batch.(type) {
		case table.WriteRequest:
			handle(b)
		case table.ExportRequests:
			for _, req := range b {
				handle(req)
			}
		default:
			panic(moerr.NewNotSupported(ctx, "unknown batch type: %v", reflect.ValueOf(b).Type()))
		}
	}
	return f
}

type WriteFactoryConfig struct {
	Account     string
	Ts          time.Time
	PathBuilder table.PathBuilder
}

func genETLData(ctx context.Context, in []IBuffer2SqlItem, buf *bytes.Buffer, factory table.WriterFactory) any {
	buf.Reset()
	if len(in) == 0 {
		return table.NewRowRequest(nil)
	}

	// Initialize aggregator
	var sessionId [16]byte
	sessionId[0] = 1
	aggregator := etl.NewAggregator(
		7*time.Second,
		func() etl.Item {
			return &StatementInfo{
				RequestAt: time.Now(),
			}
		},
		func(existing, new etl.Item) {
			e := existing.(*StatementInfo)
			n := new.(*StatementInfo)
			e.Duration += n.Duration
			e.Statement += n.Statement + ";"
		},
		func(i etl.Item) bool {
			statementInfo, ok := i.(*StatementInfo)
			if !ok {
				return false
			}
			if statementInfo.Duration > 200*time.Millisecond {
				return false
			}
			return ok
		},
	)

	ts := time.Now()
	writerMap := make(map[string]table.RowWriter, 2)
	writeValues := func(item table.RowField) {
		row := item.GetTable().GetRow(ctx)
		item.FillRow(ctx, row)
		account := row.GetAccount()
		w, exist := writerMap[account]
		if !exist {
			if factory == nil {
				factory = GetTracerProvider().writerFactory
			}
			w = factory(ctx, account, row.Table, ts)
			writerMap[row.GetAccount()] = w
		}
		w.WriteRow(row)
		if check, is := item.(table.NeedCheckWrite); is && check.NeedCheckWrite() {
			if writer, support := w.(table.AfterWrite); support {
				writer.AddAfter(check.GetCheckWriteHook())
			}
		}
		row.Free()
	}

	for _, i := range in {
		// Check if the item is a StatementInfo
		if statementInfo, ok := i.(*StatementInfo); ok {
			// If so, add it to the aggregator
			_, err := aggregator.AddItem(statementInfo)
			if err != nil {
				item, ok := i.(table.RowField)
				if !ok {
					panic("not MalCsv, dont support output CSV")
				}
				writeValues(item)
			}
		} else {
			// If not, process as before
			item, ok := i.(table.RowField)
			if !ok {
				panic("not MalCsv, dont support output CSV")
			}
			writeValues(item)
		}
	}

	// Get the aggregated results
	groupedResults := aggregator.GetResults()

	for _, i := range groupedResults {

		// If not, process as before
		item, ok := i.(table.RowField)
		if !ok {
			panic("not MalCsv, dont support output CSV")
		}
		writeValues(item)
	}

	reqs := make(table.ExportRequests, 0, len(writerMap))
	for _, ww := range writerMap {
		reqs = append(reqs, table.NewRowRequest(ww))
	}

	return reqs
}

var _ bp.ItemBuffer[bp.HasName, any] = &itemBuffer{}

// buffer catch item, like trace/log/error, buffer
type itemBuffer struct {
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
type genBatchFunc func(context.Context, []IBuffer2SqlItem, *bytes.Buffer, table.WriterFactory) any

var noopFilterItemFunc = func(IBuffer2SqlItem) {}
var noopGenBatchSQL = genBatchFunc(func(context.Context, []IBuffer2SqlItem, *bytes.Buffer, table.WriterFactory) any { return "" })

func NewItemBuffer(opts ...BufferOption) *itemBuffer {
	b := &itemBuffer{
		Reminder:       bp.NewConstantClock(defaultClock),
		buf:            make([]IBuffer2SqlItem, 0, 10240),
		sizeThreshold:  10 * mpool.MB,
		filterItemFunc: noopFilterItemFunc,
		genBatchFunc:   noopGenBatchSQL,
	}
	for _, opt := range opts {
		opt.apply(b)
	}
	logutil.Debugf("NewItemBuffer, Reminder next: %v", b.Reminder.RemindNextAfter())
	if b.genBatchFunc == nil || b.filterItemFunc == nil || b.Reminder == nil {
		logutil.Debug("NewItemBuffer meet nil elem")
		return nil
	}
	return b
}

func (b *itemBuffer) Add(i bp.HasName) {
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

func (b *itemBuffer) Reset() {
	b.mux.Lock()
	defer b.mux.Unlock()
	for idx, i := range b.buf {
		i.Free()
		b.buf[idx] = nil
	}
	b.buf = b.buf[0:0]
	b.size = 0
}

func (b *itemBuffer) IsEmpty() bool {
	b.mux.Lock()
	defer b.mux.Unlock()
	return b.isEmpty()
}

func (b *itemBuffer) isEmpty() bool {
	return len(b.buf) == 0
}

func (b *itemBuffer) ShouldFlush() bool {
	b.mux.Lock()
	defer b.mux.Unlock()
	return b.size > b.sizeThreshold
}

func (b *itemBuffer) Size() int64 {
	b.mux.Lock()
	defer b.mux.Unlock()
	return b.size
}

func (b *itemBuffer) GetBufferType() string {
	return b.bufferType
}

func (b *itemBuffer) GetBatch(ctx context.Context, buf *bytes.Buffer) any {
	ctx, span := trace.Start(ctx, "GenBatch")
	defer span.End()
	b.mux.Lock()
	defer b.mux.Unlock()

	if b.isEmpty() {
		return nil
	}
	return b.genBatchFunc(ctx, b.buf, buf, nil)
}

type BufferOption interface {
	apply(*itemBuffer)
}

type buffer2SqlOptionFunc func(*itemBuffer)

func (f buffer2SqlOptionFunc) apply(b *itemBuffer) {
	f(b)
}

func BufferWithReminder(reminder bp.Reminder) BufferOption {
	return buffer2SqlOptionFunc(func(b *itemBuffer) {
		b.Reminder = reminder
	})
}

func BufferWithType(name string) BufferOption {
	return buffer2SqlOptionFunc(func(b *itemBuffer) {
		b.bufferType = name
	})
}

func BufferWithSizeThreshold(size int64) BufferOption {
	return buffer2SqlOptionFunc(func(b *itemBuffer) {
		b.sizeThreshold = size
	})
}

func BufferWithFilterItemFunc(f filterItemFunc) BufferOption {
	return buffer2SqlOptionFunc(func(b *itemBuffer) {
		b.filterItemFunc = f
	})
}

func BufferWithGenBatchFunc(f genBatchFunc) BufferOption {
	return buffer2SqlOptionFunc(func(b *itemBuffer) {
		b.genBatchFunc = f
	})
}
