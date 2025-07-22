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
	"encoding/hex"
	"encoding/json"
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/tidwall/btree"
)

type ConsumerType int8

const (
	ConsumerType_IndexSync ConsumerType = iota
	ConsumerType_CNConsumer

	ConsumerType_CustomizedStart = 1000
)

type DataRetriever interface {
	Next() (iscpData *ISCPData)
	UpdateWatermark(executor.TxnExecutor, executor.StatementOption) error
	GetDataType() int8
}

// Intra-System Change Propagation Job Entry
type JobEntry struct {
	tableInfo    *TableEntry
	indexName    string
	inited       atomic.Bool
	consumer     Consumer
	consumerType int8
	watermark    types.TS
	err          error
	consumerInfo *ConsumerInfo
}

// Intra-System Change Propagation Table Entry
type TableEntry struct {
	exec      *ISCPTaskExecutor
	tableDef  *plan.TableDef
	accountID uint32
	dbID      uint64
	tableID   uint64
	tableName string
	dbName    string
	state     TableState
	sinkers   []*JobEntry
	mu        sync.RWMutex
}

type ConsumerInfo struct {
	ConsumerType int8
	TableName    string
	DbName       string
	IndexName    string
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

func (bat *AtomicBatch) RowCount() int {
	c := 0
	for _, b := range bat.Batches {
		rows := 0
		if b != nil && len(b.Vecs) > 0 {
			rows = b.Vecs[0].Length()
		}
		c += rows
	}

	if c != bat.Rows.Len() {
		logutil.Errorf("inconsistent row count, sum rows of batches: %d, rows of btree: %d\n", c, bat.Rows.Len())
	}
	return c
}

func (bat *AtomicBatch) Allocated() int {
	size := 0
	for _, b := range bat.Batches {
		size += b.Allocated()
	}
	return size
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

// JsonEncode encodes the object to json
func JsonEncode(value any) (string, error) {
	jbytes, err := json.Marshal(value)
	if err != nil {
		return "", err
	}

	return hex.EncodeToString(jbytes), nil
}

// JsonDecode decodes the json bytes to objects
func JsonDecode(jbytes string, value any) error {
	jRawBytes, err := hex.DecodeString(jbytes)
	if err != nil {
		return err
	}

	err = json.Unmarshal(jRawBytes, value)
	if err != nil {
		return err
	}
	return nil
}
