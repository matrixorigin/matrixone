// Copyright 2024 Matrix Origin
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
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	bp "github.com/matrixorigin/matrixone/pkg/util/batchpipe"
	db_holder "github.com/matrixorigin/matrixone/pkg/util/export/etl/db"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
)

const thresholdDelta = 10 * mpool.KB

var _ bp.ItemBuffer[bp.HasName, any] = &ContentBuffer{}
var _ Buffer = &ContentBuffer{}

// ContentBuffer cache item as csv content, not raw object.
type ContentBuffer struct {
	BufferConfig
	ctx context.Context
	buf *bytes.Buffer
	tbl *table.Table
	mux sync.Mutex

	// formatter init-ed while alloc buf
	formatter *db_holder.CSVWriter

	checkWriteHook []table.AckHook
}

func NewContentBuffer(opts ...BufferOption) *ContentBuffer {
	b := &ContentBuffer{
		ctx: context.Background(),
		BufferConfig: BufferConfig{
			Reminder:       bp.NewConstantClock(defaultClock),
			sizeThreshold:  table.DefaultWriterBufferSize,
			filterItemFunc: noopFilterItemFunc,
			genBatchFunc:   noopGenBatchSQL,
		},
	}
	for _, opt := range opts {
		opt.apply(&b.BufferConfig)
	}
	logutil.Debugf("NewContentBuffer, Reminder next: %v", b.Reminder.RemindNextAfter())
	// fixme: genBatchFunc useless in this buffer
	if b.genBatchFunc == nil || b.filterItemFunc == nil || b.Reminder == nil {
		logutil.Debug("NewItemBuffer meet nil elem")
		return nil
	}
	return b
}

func (b *ContentBuffer) reset() {
	if b.buf == nil {
		b.buf = bytes.NewBuffer(make([]byte, 0, b.sizeThreshold))
		if b.formatter == nil {
			b.formatter = db_holder.NewCSVWriterWithBuffer(b.ctx, b.buf)
		} else {
			b.formatter.ResetBuffer(b.buf)
		}
	}
}

func (b *ContentBuffer) Add(i bp.HasName) {
	b.mux.Lock()
	defer b.mux.Unlock()
	if b.buf == nil {
		b.reset()
	}
	if item, ok := i.(IBuffer2SqlItem); !ok {
		panic("not implement interface IBuffer2SqlItem")
	} else {
		b.filterItemFunc(item)

		rowFields, ok := i.(table.RowField)
		if !ok {
			panic("not MalCsv, dont support output CSV")
		}
		row := rowFields.GetTable().GetRow(b.ctx)
		defer row.Free()
		rowFields.FillRow(b.ctx, row)
		// fixme: support diff account diff buffer
		// account := row.GetAccount()
		if err := b.formatter.WriteRow(row); err != nil {
			logutil.Error("writer item failed",
				logutil.ErrorField(err),
				logutil.VarsField(fmt.Sprintf("%v", item)),
				logutil.Discardable())
			// need ALERT
			v2.TraceMOLoggerErrorWriteItemCounter.Inc()
		}

		item.Free()
		if b.tbl == nil {
			b.tbl = rowFields.GetTable()
		}
	}

	// keep checkWriteHook, push checkWriteHook to the writer while GenBatch
	if check, is := i.(table.NeedAck); is && check.NeedCheckAck() {
		b.checkWriteHook = append(b.checkWriteHook, check.GetAckHook())
	}
}

func (b *ContentBuffer) Reset() {
	b.mux.Lock()
	defer b.mux.Unlock()
	b.buf = nil
	b.tbl = nil
	b.reset()
}

func (b *ContentBuffer) IsEmpty() bool {
	b.mux.Lock()
	defer b.mux.Unlock()
	return b.isEmpty()
}

func (b *ContentBuffer) isEmpty() bool {
	if b.buf == nil {
		return true
	}
	b.formatter.Flush()
	return b.buf.Len() == 0
}

func (b *ContentBuffer) ShouldFlush() bool {
	b.mux.Lock()
	defer b.mux.Unlock()
	if b.buf == nil {
		return false
	}
	return b.buf.Len()+thresholdDelta > b.sizeThreshold
}

func (b *ContentBuffer) Size() int64 {
	b.mux.Lock()
	defer b.mux.Unlock()
	if b.buf == nil {
		return 0
	}
	return int64(b.buf.Len())
}

func (b *ContentBuffer) GetBufferType() string {
	return b.bufferType
}

func (b *ContentBuffer) GetBatch(ctx context.Context, buf *bytes.Buffer) any {
	ctx, span := trace.Start(ctx, "GenBatch")
	defer span.End()
	b.mux.Lock()
	defer b.mux.Unlock()

	if b.isEmpty() {
		return nil
	}
	// genBatchFunc Useless
	//b.genBatchFunc(ctx, nil, buf, nil)

	factory := GetTracerProvider().writerFactory
	w := factory.GetRowWriter(ctx, "sys", b.tbl, time.Now())

	// check Write Hook: for ReactWriter
	if writer, support := w.(table.AfterWrite); support {
		for idx, hook := range b.checkWriteHook {
			writer.AddAfter(hook)
			b.checkWriteHook[idx] = nil
		}
	}
	b.checkWriteHook = b.checkWriteHook[:0]

	return &contentWriteRequest{
		buffer: b.buf,
		writer: w,
	}
}

var _ table.WriteRequest = (*contentWriteRequest)(nil)

type contentWriteRequest struct {
	buffer *bytes.Buffer
	writer table.RowWriter
}

func (c *contentWriteRequest) Handle() (int, error) {
	if setter, ok := c.writer.(table.BufferSettable); ok && setter.NeedBuffer() {
		// FIXME: too complicated.
		setter.SetBuffer(c.buffer, nil)
	}
	return c.writer.FlushAndClose()
}
