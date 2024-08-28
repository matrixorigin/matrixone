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

package cdc

import (
	"container/list"
	"context"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/tools"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
)

/*
Cdc process
	logtail replayer
=>  Queue[DecoderInput]
=>  Partitioner.Partition
=>  chan[DecoderInput]
=>  Decoder.Decode
=>  chan[DecoderOutput]
=>  Sinker.Sink
=>  Sink.Send

*/

// Partitioner partition the entry from queue by table
type Partitioner interface {
	Partition(entry tools.Pair[*disttae.TableCtx, *disttae.DecoderInput])
	Run(ctx context.Context, ar *ActiveRoutine)
}

// Decoder convert binary data into sql parts
type Decoder interface {
	Decode(ctx context.Context, cdcCtx *disttae.TableCtx, input *disttae.DecoderInput) *DecoderOutput
	Run(ctx context.Context, ar *ActiveRoutine)
	TableId() uint64
}

// Sinker manages and drains the sql parts
type Sinker interface {
	Sink(ctx context.Context, data *DecoderOutput) error
	Run(ctx context.Context, ar *ActiveRoutine)
}

// Sink represents the destination mysql or matrixone
type Sink interface {
	Send(ctx context.Context, data *DecoderOutput) error
	Close()
}

type OutputType int

const (
	OutputTypeCheckpoint OutputType = iota
)

type DecoderOutput struct {
	/*
		ts denotes the watermark of logtails in this batch of logtail.

		the algorithm of evaluating ts:
			Assume we already have a watermark OldWMark.
			To evaluate the NewWMark.

			Init: NewWMark = OldWMark

			for rowTs in Rows (Insert or Delete):
				if rowTs <= OldWMark:
					drop this row
				else
					NewWMark = max(NewWMark,rowTs)

			for commitTs in Objects(only cn):
				if commitTs <= OldWMark:
					drop this object
				else
					NewWMark = max(NewWMark,commitTs)

			for commitTs in Deltas(cn):
				if commitTs <= OldWMark:
					drop this delta
				else
					NewWMark = max(NewWMark,commitTs)

			for rowTs in rows of Deltas(dn):
				load rows of Deltas(dn);

				if rowTs <= OldWMark:
					drop this row
				else
					NewWMark = max(NewWMark,rowTs)
	*/

	outputTyp     OutputType
	ts            timestamp.Timestamp
	logtailTs     timestamp.Timestamp
	sqlOfRows     atomic.Value
	sqlOfObjects  atomic.Value
	sqlOfDeletes  atomic.Value
	checkpointBat *batch.Batch
	err           error
}

type RowType int

const (
	DeleteRow RowType = 1
	InsertRow RowType = 2
)

type RowIterator interface {
	Next() bool
	Row(row []any)
	Close() error
}

type DbTableInfo struct {
	DbName  string
	TblName string
	DbId    uint64
	TblId   uint64
	Rows    *Rows
}

// Rows denotes all rows have been read.
// Iterator hierarchy:
//
//	InsertRows iterator
//		--> AtomicBatch row iterator
//
//	DeleteRows iterator
//		--> AtomicBatch row iterator
//
// InsertRows holds the list of AtomicBatch for insert
// DeleteRows holds the list of AtomicBatch for delete
type Rows struct {
	InsertRows *list.List
	DeleteRows *list.List
}

func (rows *Rows) GetInsertRowIterator() RowIterator {
	return &rowIter{}
}

func (rows *Rows) GetDeleteRowIterator() RowIterator {
	return &rowIter{}
}

// AtomicBatch holds batches from [Tail_wip,...,Tail_done] or [Tail_done].
// These batches have atomicity
type AtomicBatch struct {
	MP       *mpool.MPool
	From, To types.TS
	List     *list.List
}

func (bat *AtomicBatch) Append(batch *batch.Batch) {
	if batch != nil {
		bat.List.PushBack(batch)
	}
}

func (bat *AtomicBatch) Close() {
	for le := bat.List.Front(); le != nil; le = le.Next() {
		data := le.Value.(*batch.Batch)
		data.Clean(bat.MP)
	}
}

func (bat *AtomicBatch) GetRowIterator() RowIterator {
	return &atomicBatchRowIter{
		offset: -1,
		ele:    bat.List.Front(),
	}
}

var _ RowIterator = new(atomicBatchRowIter)

type atomicBatchRowIter struct {
	offset int
	ele    *list.Element
}

func (iter *atomicBatchRowIter) Next() bool {
	if iter == nil || iter.ele == nil {
		return false
	}
	//step 1: check left rows
	data := iter.ele.(*batch.Batch)
	iter.offset++
	if iter.offset < data.Vecs[0].Length() {
		return true
	}
	//or step 2: move to next batch has data
	iter.offset = -1
	for ; iter.ele != nil; iter.ele = iter.ele.Next() {
		data = iter.ele.(*batch.Batch)
		if data.Vecs[0].Length() != 0 {
			break
		}
	}
	return iter.ele != nil && data.Vecs[0].Length() != 0
}

func (iter *atomicBatchRowIter) Row(row []any) {
	data = iter.ele.(*batch.Batch)
	vecCnt := len(data.Vecs)
	if vecCnt > len(row) {
		return
	}
	//TODO:
}

func (iter *atomicBatchRowIter) Close() error {
	iter.offset = -1
	iter.ele = nil
	return nil
}

var _ RowIterator = new(rowIter)

type rowIter struct {
	eleIter RowIterator
	ele     *list.Element
	list    *list.List
}

func (iter *rowIter) Next() bool {
	if iter == nil || iter.ele == nil || iter.eleIter == nil || iter.list == nil {
		return false
	}
	if iter.eleIter.Next() {
		return true
	}
	iter.eleIter = nil
	for ele = iter.ele.Next(); ele != nil; ele = ele.Next() {

	}

	return false
}

func (iter *rowIter) Row(row []any) {
	return 0, nil
}

func (iter *rowIter) Close() error {
	return nil
}
