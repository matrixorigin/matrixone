// Copyright 2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/util/batchpipe"
	bp "github.com/matrixorigin/matrixone/pkg/util/batchpipe"
	db_holder "github.com/matrixorigin/matrixone/pkg/util/export/etl/db"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
)

func TestContentBuffer_isEmpty(t *testing.T) {
	type fields struct {
		options []BufferOption
	}
	tests := []struct {
		name   string
		fields fields
		run    func(buf *ContentBuffer)
		want   bool
		nilReq bool
	}{
		{
			name: "empty",
			fields: fields{
				options: []BufferOption{BufferWithGenBatchFunc(noopGenBatchSQL), BufferWithType("test")},
			},
			run:    func(buf *ContentBuffer) { /*none op*/ },
			want:   true,
			nilReq: true,
		},
		{
			name: "not_empty",
			fields: fields{
				options: []BufferOption{BufferWithGenBatchFunc(noopGenBatchSQL), BufferWithType("test")},
			},
			run:    func(buf *ContentBuffer) { buf.Add(&StatementInfo{}) },
			want:   false,
			nilReq: false,
		},
	}
	ctx := context.TODO()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := NewContentBuffer(tt.fields.options...)
			tt.run(b)
			_ = b.ShouldFlush()
			_ = b.Size()
			assert.Equalf(t, tt.want, b.isEmpty(), "isEmpty()")

			// check bufferCount is correct count
			req := b.GetBatch(ctx, nil)
			if tt.nilReq {
				require.Nil(t, req)
			} else {
				require.NotNil(t, req)
				require.Equal(t, bufferCount.Load(), int32(1))
				require.Equal(t, ContentBufferBackOff{}.Count(), true)
				_, err := req.(*contentWriteRequest).Handle()
				require.NoError(t, err)
				require.Equal(t, bufferCount.Load(), int32(0))
			}
		})
	}
}

func TestContentBuffer_Add_Free_cause_panic(t *testing.T) {
	// issue: https://github.com/matrixorigin/matrixone/issues/19046
	type fields struct {
		BufferConfig   BufferConfig
		ctx            context.Context
		buf            *bytes.Buffer
		tbl            *table.Table
		formatter      *db_holder.CSVWriter
		checkWriteHook []table.AckHook
	}
	type args struct {
		i batchpipe.HasName
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name: "panic",
			fields: fields{
				BufferConfig: BufferConfig{
					bufferType:     "dummy",
					Reminder:       bp.NewConstantClock(defaultClock),
					sizeThreshold:  table.DefaultWriterBufferSize,
					filterItemFunc: noopFilterItemFunc,
					genBatchFunc:   noopGenBatchSQL,
				},
				ctx:            context.TODO(),
				buf:            nil,
				tbl:            nil,
				formatter:      nil, // go with buf, keep nil.
				checkWriteHook: nil,
			},
			args: args{
				i: NewItemSyncer(newDummyLog()),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &ContentBuffer{
				BufferConfig:   tt.fields.BufferConfig,
				ctx:            tt.fields.ctx,
				buf:            tt.fields.buf,
				tbl:            tt.fields.tbl,
				formatter:      tt.fields.formatter,
				checkWriteHook: tt.fields.checkWriteHook,
			}
			b.Add(tt.args.i)
		})
	}
}
