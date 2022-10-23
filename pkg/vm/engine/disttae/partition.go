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

package disttae

import (
	"bytes"
	"context"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/storage/memorystorage/memtable"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

const (
	index_PrimaryKey = memtable.Text("primary key")
	index_BlockID    = memtable.Text("block id")
	index_Time_OP    = memtable.Text("time, op")
)

func NewPartition() *Partition {
	return &Partition{
		data: memtable.NewTable[RowID, DataValue, *DataRow](),
	}
}

type RowID types.Rowid

func (r RowID) Less(than RowID) bool {
	return bytes.Compare(r[:], than[:]) < 0
}

type DataValue struct {
	op    Op
	value map[string]memtable.Nullable
}

type DataRow struct {
	rowID         RowID
	value         DataValue
	indexes       []memtable.Tuple
	uniqueIndexes []memtable.Tuple
}

type Op uint8

const (
	opInsert Op = iota + 1
	opDelete
)

func (d *DataRow) Key() RowID {
	return d.rowID
}

func (d *DataRow) Value() DataValue {
	return d.value
}

func (d *DataRow) Indexes() []memtable.Tuple {
	return d.indexes
}

func (d *DataRow) UniqueIndexes() []memtable.Tuple {
	return d.uniqueIndexes
}

var _ MVCC = new(Partition)

func (p *Partition) BlockList(ctx context.Context, ts timestamp.Timestamp,
	blocks []BlockMeta, entries []Entry) ([]BlockMeta, map[uint64][]int) {
	blks := make([]BlockMeta, 0, len(blocks))
	deletes := make(map[uint64][]int)
	p.IterDeletedRowIDs(ctx, ts, func(rowID RowID) bool {
		id, offset := catalog.DecodeRowid(types.Rowid(rowID))
		deletes[id] = append(deletes[id], int(offset))
		return true
	})
	for _, entry := range entries {
		if entry.typ == DELETE {
			vs := vector.MustTCols[types.Rowid](entry.bat.GetVector(0))
			for _, v := range vs {
				id, offset := catalog.DecodeRowid(v)
				deletes[id] = append(deletes[id], int(offset))
			}
		}
	}
	for i := range blocks {
		if _, ok := deletes[blocks[i].Info.BlockID]; !ok {
			blks = append(blks, blocks[i])
		}
	}
	return blks, deletes
}

func (*Partition) CheckPoint(ctx context.Context, ts timestamp.Timestamp) error {
	panic("unimplemented")
}

func (p *Partition) Get(key types.Rowid, ts timestamp.Timestamp) bool {
	t := memtable.Time{
		Timestamp: ts,
	}
	tx := memtable.NewTransaction(
		uuid.NewString(),
		t,
		memtable.SnapshotIsolation,
	)
	if _, err := p.data.Get(tx, RowID(key)); err != nil {
		return false
	}
	return true
}

func (p *Partition) Delete(ctx context.Context, b *api.Batch) error {
	bat, err := batch.ProtoBatchToBatch(b)
	if err != nil {
		return err
	}

	txID := uuid.NewString()

	iter := memtable.NewBatchIter(bat)
	for {
		tuple := iter()
		if len(tuple) == 0 {
			break
		}

		rowID := RowID(tuple[0].Value.(types.Rowid))
		ts := tuple[1].Value.(types.TS)
		t := memtable.Time{
			Timestamp: timestamp.Timestamp{
				PhysicalTime: ts.Physical(),
				LogicalTime:  ts.Logical(),
			},
		}
		tx := memtable.NewTransaction(txID, t, memtable.SnapshotIsolation)

		// indexes
		var indexes []memtable.Tuple
		// time, op
		indexes = append(indexes, memtable.Tuple{
			index_Time_OP,
			ts,
			memtable.Uint(opDelete),
		})

		err := p.data.Upsert(tx, &DataRow{
			rowID: rowID,
			value: DataValue{
				op: opDelete,
			},
			indexes: indexes,
		})
		if err != nil {
			return err
		}

		if err := tx.Commit(t); err != nil {
			return err
		}
	}

	return nil
}

func (p *Partition) Insert(ctx context.Context, primaryKeyIndex int,
	b *api.Batch, needCheck bool) error {
	bat, err := batch.ProtoBatchToBatch(b)
	if err != nil {
		return err
	}

	txID := uuid.NewString()

	iter := memtable.NewBatchIter(bat)
	for {
		tuple := iter()
		if len(tuple) == 0 {
			break
		}

		rowID := RowID(tuple[0].Value.(types.Rowid))
		ts := tuple[1].Value.(types.TS)
		t := memtable.Time{
			Timestamp: timestamp.Timestamp{
				PhysicalTime: ts.Physical(),
				LogicalTime:  ts.Logical(),
			},
		}
		tx := memtable.NewTransaction(txID, t, memtable.SnapshotIsolation)

		// check primary key
		var primaryKey any
		if primaryKeyIndex >= 0 {
			primaryKey = memtable.ToOrdered(tuple[primaryKeyIndex].Value)
			entries, err := p.data.Index(tx, memtable.Tuple{
				index_PrimaryKey,
				primaryKey,
			})
			if err != nil {
				return err
			}
			if len(entries) > 0 && needCheck {
				return moerr.NewDuplicate()
			}
		}

		dataValue := DataValue{
			op:    opInsert,
			value: make(map[string]memtable.Nullable),
		}
		for i := 2; i < len(tuple); i++ {
			dataValue.value[bat.Attrs[i]] = tuple[i]
		}

		// indexes
		var indexes []memtable.Tuple
		// primary key
		if primaryKey != nil {
			indexes = append(indexes, memtable.Tuple{
				index_PrimaryKey,
				primaryKey,
			})
		}
		// block id
		indexes = append(indexes, memtable.Tuple{
			index_BlockID,
			memtable.ToOrdered(rowIDToBlockID(rowID)),
		})
		// time, op
		indexes = append(indexes, memtable.Tuple{
			index_Time_OP,
			ts,
			memtable.Uint(opInsert),
		})

		err = p.data.Upsert(tx, &DataRow{
			rowID:   rowID,
			value:   dataValue,
			indexes: indexes,
		})
		if err != nil {
			return err
		}

		if err := tx.Commit(t); err != nil {
			return err
		}
	}

	return nil
}

func rowIDToBlockID(rowID RowID) uint64 {
	id, _ := catalog.DecodeRowid(types.Rowid(rowID))
	return id
}

func (p *Partition) DeleteByBlockID(ctx context.Context, ts timestamp.Timestamp, blockID uint64) error {
	tx := memtable.NewTransaction(uuid.NewString(), memtable.Time{
		Timestamp: ts,
	}, memtable.SnapshotIsolation)
	pivot := memtable.Tuple{
		index_BlockID,
		memtable.ToOrdered(blockID),
	}
	iter := p.data.NewIndexIter(tx, pivot, append(pivot, memtable.Min))
	defer iter.Close()
	for ok := iter.First(); ok; ok = iter.Next() {
		entry := iter.Item()
		if err := p.data.Delete(tx, entry.Key); err != nil {
			return err
		}
	}
	return nil
}

func (p *Partition) IterDeletedRowIDs(ctx context.Context, ts timestamp.Timestamp, fn func(rowID RowID) bool) {
	tx := memtable.NewTransaction(uuid.NewString(), memtable.Time{
		Timestamp: ts,
	}, memtable.SnapshotIsolation)
	min := memtable.Tuple{
		index_Time_OP,
		types.TS{},
	}
	max := memtable.Tuple{
		index_Time_OP,
		types.TimestampToTS(ts),
		memtable.Max,
	}
	iter := p.data.NewIndexIter(tx, min, max)
	defer iter.Close()
	deleted := make(map[RowID]bool)
	inserted := make(map[RowID]bool)
	for ok := iter.First(); ok; ok = iter.Next() {
		entry := iter.Item()
		rowID := entry.Key
		switch Op(entry.Index[2].(memtable.Uint)) {
		case opInsert:
			inserted[rowID] = true
		case opDelete:
			deleted[rowID] = true
		}
	}
	for rowID := range deleted {
		if !inserted[rowID] {
			if !fn(rowID) {
				break
			}
		}
	}
}

func (p *Partition) NewReader(
	ctx context.Context,
	readerNumber int,
	expr *plan.Expr,
	defs []engine.TableDef,
	tableDef *plan.TableDef,
	blks []ModifyBlockMeta,
	ts timestamp.Timestamp,
	fs fileservice.FileService,
	entries []Entry,
) ([]engine.Reader, error) {

	t := memtable.Time{
		Timestamp: ts,
	}
	tx := memtable.NewTransaction(
		uuid.NewString(),
		t,
		memtable.SnapshotIsolation,
	)

	inserts := make([]*batch.Batch, 0, len(entries))
	deletes := make(map[types.Rowid]uint8)
	for _, entry := range entries {
		if entry.typ == INSERT {
			inserts = append(inserts, entry.bat)
		} else {
			if entry.bat.GetVector(0).GetType().Oid == types.T_Rowid {
				vs := vector.MustTCols[types.Rowid](entry.bat.GetVector(0))
				for _, v := range vs {
					deletes[v] = 0
				}
			}
		}
	}

	readers := make([]engine.Reader, readerNumber)

	mp := make(map[string]types.Type)
	mp[catalog.Row_ID] = types.New(types.T_Rowid, 0, 0, 0)
	for _, def := range defs {
		attr, ok := def.(*engine.AttributeDef)
		if !ok {
			continue
		}
		mp[attr.Attr.Name] = attr.Attr.Type
	}
	readers[0] = &PartitionReader{
		typsMap:  mp,
		iter:     p.data.NewIter(tx),
		readTime: t,
		tx:       tx,
		expr:     expr,
		inserts:  inserts,
		deletes:  deletes,
	}
	if readerNumber == 1 {
		for i := range blks {
			readers = append(readers, &blockMergeReader{
				fs:       fs,
				ts:       ts,
				ctx:      ctx,
				tableDef: tableDef,
				sels:     make([]int64, 0, 1024),
				blks:     []ModifyBlockMeta{blks[i]},
			})
		}
		return []engine.Reader{&mergeReader{readers}}, nil
	}
	if len(blks) < readerNumber-1 {
		for i := range blks {
			readers[i+1] = &blockMergeReader{
				fs:       fs,
				ts:       ts,
				ctx:      ctx,
				tableDef: tableDef,
				sels:     make([]int64, 0, 1024),
				blks:     []ModifyBlockMeta{blks[i]},
			}
		}
		for j := len(blks) + 1; j < readerNumber; j++ {
			readers[j] = &emptyReader{}
		}
		return readers, nil
	}
	step := len(blks) / (readerNumber - 1)
	if step < 1 {
		step = 1
	}
	for i := 1; i < readerNumber; i++ {
		if i == readerNumber-1 {
			readers[i] = &blockMergeReader{
				fs:       fs,
				ts:       ts,
				ctx:      ctx,
				tableDef: tableDef,
				blks:     blks[i*step:],
				sels:     make([]int64, 0, 1024),
			}
		} else {
			readers[i] = &blockMergeReader{
				fs:       fs,
				ts:       ts,
				ctx:      ctx,
				tableDef: tableDef,
				blks:     blks[i*step : (i+1)*step],
				sels:     make([]int64, 0, 1024),
			}
		}
	}
	return readers, nil
}
