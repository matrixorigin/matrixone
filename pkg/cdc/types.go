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
	"bytes"
	"context"
	"fmt"

	"github.com/tidwall/btree"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
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

type Reader interface {
	Run(ctx context.Context, ar *ActiveRoutine)
	Close()
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
	OutputTypeCheckpoint        OutputType = iota
	OutputTypeTailDone          OutputType = iota
	OutputTypeUnfinishedTailWIP OutputType = iota
)

func (t OutputType) String() string {
	switch t {
	case OutputTypeCheckpoint:
		return "Checkpoint"
	case OutputTypeTailDone:
		return "TailDone"
	case OutputTypeUnfinishedTailWIP:
		return "UnfinishedTailWIP"
	default:
		return "usp output type"
	}
}

type DecoderOutput struct {
	outputTyp      OutputType
	noMoreData     bool
	toTs           types.TS
	checkpointBat  *batch.Batch
	insertAtmBatch *AtomicBatch
	deleteAtmBatch *AtomicBatch
	err            error
}

type RowType int

const (
	DeleteRow RowType = 1
	InsertRow RowType = 2
)

type RowIterator interface {
	Next() bool
	Row(ctx context.Context, wantedColsIndices []int, row []any) error
	Close() error
}

type DbTableInfo struct {
	DbName                       string
	TblName                      string
	DbId                         uint64
	TblId                        uint64
	TsColIdx, CompositedPkColIdx int
}

func (info DbTableInfo) String() string {
	return fmt.Sprintf("%v %v %v %v %v %v",
		info.DbName,
		info.TblName,
		info.DbId,
		info.TblId,
		info.TsColIdx,
		info.CompositedPkColIdx,
	)
}

// Batches denotes all rows have been read.
type Batches struct {
	Inserts []*AtomicBatch
	Deletes []*AtomicBatch
}

// AtomicBatch holds batches from [Tail_wip,...,Tail_done] or [Tail_done].
// These batches have atomicity
type AtomicBatch struct {
	Mp                           *mpool.MPool
	From, To                     types.TS
	Batches                      []*batch.Batch
	Rows                         *btree.BTreeG[AtomicBatchRow]
	TsColIdx, CompositedPkColIdx int
}

func NewAtomicBatch(
	mp *mpool.MPool,
	from, to types.TS,
	tsColIdx, compositedPkColIdx int,
) *AtomicBatch {
	opts := btree.Options{
		Degree: 64,
	}
	ret := &AtomicBatch{
		Mp:                 mp,
		From:               from,
		To:                 to,
		Rows:               btree.NewBTreeGOptions(AtomicBatchRow.Less, opts),
		TsColIdx:           tsColIdx,
		CompositedPkColIdx: compositedPkColIdx,
	}
	return ret
}

type AtomicBatchRow struct {
	Ts     types.TS
	Pk     []byte
	Offset int
	Src    *batch.Batch
}

func (row AtomicBatchRow) Less(other AtomicBatchRow) bool {
	//ts asc
	if row.Ts.Less(&other.Ts) {
		return true
	}
	if row.Ts.Greater(&other.Ts) {
		return false
	}
	//pk asc
	return bytes.Compare(row.Pk, other.Pk) < 0
}

func (bat *AtomicBatch) Append(
	packer *types.Packer,
	batch *batch.Batch) {
	if batch != nil {
		bat.Batches = append(bat.Batches, batch)
		//ts columns
		tsVec := vector.MustFixedCol[types.TS](batch.Vecs[bat.TsColIdx])
		//composited pk columns
		compositedPkBytes := logtailreplay.EncodePrimaryKeyVector(batch.Vecs[bat.CompositedPkColIdx], packer)

		for i, ts := range tsVec {
			row := AtomicBatchRow{
				Ts:     ts,
				Pk:     compositedPkBytes[i],
				Offset: i,
				Src:    batch,
			}
			bat.Rows.Set(row)
		}
	}
}

func (bat *AtomicBatch) Close() {
	for _, oneBat := range bat.Batches {
		oneBat.Clean(bat.Mp)
	}
	bat.Rows.Clear()
	bat.Rows = nil
	bat.Batches = nil
	bat.Mp = nil
}

func (bat *AtomicBatch) GetRowIterator() RowIterator {
	return &atomicBatchRowIter{
		iter: bat.Rows.Iter(),
	}
}

var _ RowIterator = new(atomicBatchRowIter)

type atomicBatchRowIter struct {
	iter btree.IterG[AtomicBatchRow]
}

func (iter *atomicBatchRowIter) Next() bool {
	return iter.iter.Next()
}

func (iter *atomicBatchRowIter) Row(ctx context.Context, wantedColsIndices []int, row []any) (err error) {
	batchRow := iter.iter.Item()
	err = extractRowFromEveryVector2(
		ctx,
		batchRow.Src,
		wantedColsIndices,
		batchRow.Offset,
		row,
	)
	if err != nil {
		return
	}
	return
}

func (iter *atomicBatchRowIter) Close() error {
	iter.iter.Release()
	return nil
}

// TableCtx TODO: define suitable names
type TableCtx struct {
	db, table     string
	dbId, tableId uint64
	tblDef        *plan.TableDef
}

func (tctx *TableCtx) Db() string {
	return tctx.db
}

func (tctx *TableCtx) Table() string {
	return tctx.table
}

func (tctx *TableCtx) DBId() uint64 {
	return tctx.dbId
}

func (tctx *TableCtx) TableId() uint64 {
	return tctx.tableId
}

func (tctx *TableCtx) TableDef() *plan.TableDef {
	return tctx.tblDef
}
