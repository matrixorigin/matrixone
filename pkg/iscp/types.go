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

package iscp

import (
	"bytes"
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/cmd_util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/tidwall/btree"
)

type ConsumerType int8

const (
	ConsumerType_IndexSync ConsumerType = iota
	ConsumerType_CNConsumer

	ConsumerType_CustomizedStart = 1000
)

const (
	ISCPJobState_Invalid int8 = iota
	ISCPJobState_Pending
	ISCPJobState_Running
	ISCPJobState_Completed
	ISCPJobState_Error
	ISCPJobState_Canceled
)

type DataRetriever interface {
	Next() (iscpData *ISCPData)
	UpdateWatermark(executor.TxnExecutor, executor.StatementOption) error
	GetDataType() int8
}

// In an iteration, the table's data is propagated downstream.
// A newly created job will immediately trigger an iteration.
// Subsequent iterations are triggered according to the job's configuration.
// In an iteration, data is collected via CollectChanges and distributed to consumers through data retrievers.
// A single iteration can correspond to multiple consumers.
/*
              +--------------------+
              |   Source Table     |
              |  data [from, to]   |
              +--------+-----------+
                       |
               [Read a data batch]
                       |
        +--------------+--------------+--------------+
        |                             |              |
   +----v----+                  +-----v----+    +-----v----+
   |Consumer1|                  |Consumer2 |    |Consumer3 |
   | .Next() |                  | .Next()  |    | .Next()  |
   +---------+                  +----------+    +----------+
        |                             |              |
  Receives batch               Receives batch   Receives batch

*/

type IterationContext struct {
	accountID uint32
	tableID   uint64
	jobNames  []string
	jobIDs    []uint64
	fromTS    types.TS
	toTS      types.TS
}

type ISCPData struct {
	refcnt atomic.Int32

	insertBatch *AtomicBatch
	deleteBatch *AtomicBatch
	noMoreData  bool
	err         error
}

type JobStatus struct {
	TaskID    uint64
	From      types.TS
	To        types.TS
	StartAt   types.TS
	EndAt     types.TS
	ErrorCode int
	ErrorMsg  string
}

// Intra-System Change Propagation Task Executor
// iscp executor manages iterations, updates watermarks, and updates the iscp table.
type ISCPTaskExecutor struct {
	tables         *btree.BTreeG[*TableEntry]
	tableMu        sync.RWMutex
	packer         *types.Packer
	mp             *mpool.MPool
	cnUUID         string
	txnEngine      engine.Engine
	cnTxnClient    client.TxnClient
	iscpLogWm      types.TS

	rpcHandleFn func(
		ctx context.Context,
		meta txn.TxnMeta,
		req *cmd_util.GetChangedTableListReq,
		resp *cmd_util.GetChangedTableListResp,
	) (func(), error) // for test
	option *ISCPExecutorOption

	ctx    context.Context
	cancel context.CancelFunc

	worker Worker
	wg     sync.WaitGroup

	running   bool
	runningMu sync.Mutex
}

// Intra-System Change Propagation Job Entry
type JobEntry struct {
	tableInfo *TableEntry
	jobName   string
	jobSpec   *TriggerSpec
	jobID     uint64

	watermark          types.TS
	persistedWatermark types.TS
	state              int8
	dropAt             types.Timestamp
}

type JobKey struct {
	JobName string
	JobID   uint64
}

// Intra-System Change Propagation Table Entry
type TableEntry struct {
	exec      *ISCPTaskExecutor
	accountID uint32
	dbID      uint64
	tableID   uint64
	tableName string
	dbName    string
	jobs      map[JobKey]*JobEntry
	mu        sync.RWMutex
}

type JobID struct {
	DBName    string
	TableName string
	JobName   string
}

type JobSpec struct {
	Priority int8
	ConsumerInfo
	TriggerSpec
}

type Schedule struct {
	Interval time.Duration
	Share    bool
}

type TriggerSpec struct {
	JobType uint16
	Schedule
}

type ConsumerInfo struct {
	ConsumerType int8
	IndexName    string
	TableName    string
	DBName       string
	Columns      []string
	SrcTable     TableInfo
}

type TableInfo struct {
	DBName    string
	DBID      uint64
	TableName string
	TableID   uint64
}

type Consumer interface {
	Consume(context.Context, DataRetriever) error
}

type OutputType int8

const (
	OutputTypeSnapshot OutputType = iota
	OutputTypeTail
)

func (t OutputType) String() string {
	switch t {
	case OutputTypeSnapshot:
		return "Snapshot"
	case OutputTypeTail:
		return "Tail"
	default:
		return "usp output type"
	}
}

type RowType int

const (
	NoOp RowType = iota
	InsertRow
	DeleteRow
	UpsertRow
)

type RowIterator interface {
	Next() bool
	Row(ctx context.Context, row []any) error
	Close()
}

// AtomicBatch holds batches from [Tail_wip,...,Tail_done] or [Tail_done].
// These batches have atomicity
type AtomicBatch struct {
	Mp      *mpool.MPool
	Batches []*batch.Batch
	Rows    *btree.BTreeG[AtomicBatchRow]
}

func NewAtomicBatch(mp *mpool.MPool) *AtomicBatch {
	opts := btree.Options{
		Degree: 64,
	}
	ret := &AtomicBatch{
		Mp:   mp,
		Rows: btree.NewBTreeGOptions(AtomicBatchRow.Less, opts),
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
	if row.Ts.LT(&other.Ts) {
		return true
	}
	if row.Ts.GT(&other.Ts) {
		return false
	}
	//pk asc
	return bytes.Compare(row.Pk, other.Pk) < 0
}

func (bat *AtomicBatch) Append(
	packer *types.Packer,
	batch *batch.Batch,
	tsColIdx, compositedPkColIdx int,
) {

	if batch != nil {
		//ts columns
		tsVec := vector.MustFixedColWithTypeCheck[types.TS](batch.Vecs[tsColIdx])
		//composited pk columns
		compositedPkBytes := readutil.EncodePrimaryKeyVector(batch.Vecs[compositedPkColIdx], packer)

		for i, pk := range compositedPkBytes {
			// if ts is constant, then tsVec[0] is the ts for all rows
			ts := tsVec[0]
			if i < len(tsVec) {
				ts = tsVec[i]
			}

			row := AtomicBatchRow{
				Ts:     ts,
				Pk:     pk,
				Offset: i,
				Src:    batch,
			}
			bat.Rows.Set(row)
		}

		bat.Batches = append(bat.Batches, batch)
	}
}

func (bat *AtomicBatch) Close() {
	for _, oneBat := range bat.Batches {
		oneBat.Clean(bat.Mp)
	}
	if bat.Rows != nil {
		bat.Rows.Clear()
		bat.Rows = nil
	}
	bat.Batches = nil
	bat.Mp = nil
}

func (bat *AtomicBatch) GetRowIterator() RowIterator {
	return &atomicBatchRowIter{
		iter: bat.Rows.Iter(),
	}
}

//func (bat *AtomicBatch) DebugString(tableDef *plan.TableDef, isDelete bool) string {
//	ctx := context.Background()
//	keys := make([]string, 0, bat.Rows.Len())
//	iter := bat.Rows.Iter()
//	defer iter.Release()
//	for iter.Next() {
//		row := iter.Item()
//		s, err := getRowPkAndTsFromBat(ctx, row.Src, tableDef, isDelete, row.Offset)
//		if err != nil {
//			return ""
//		}
//		keys = append(keys, s)
//	}
//	return fmt.Sprintf("count=%d, key=%v", bat.Rows.Len(), keys)
//}

var _ RowIterator = new(atomicBatchRowIter)

type atomicBatchRowIter struct {
	iter btree.IterG[AtomicBatchRow]
}

func (iter *atomicBatchRowIter) Item() AtomicBatchRow {
	return iter.iter.Item()
}

func (iter *atomicBatchRowIter) Next() bool {
	return iter.iter.Next()
}

func (iter *atomicBatchRowIter) Row(ctx context.Context, row []any) error {
	batchRow := iter.iter.Item()
	return extractRowFromEveryVector(
		ctx,
		batchRow.Src,
		batchRow.Offset,
		row,
	)
}

func (iter *atomicBatchRowIter) Close() {
	iter.iter.Release()
}
