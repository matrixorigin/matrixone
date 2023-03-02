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
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/moprobe"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/storage/memorystorage/memorytable"
	"github.com/matrixorigin/matrixone/pkg/txn/storage/memorystorage/memtable"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

func NewPartition(
	columnsIndexDefs []ColumnsIndexDef,
) *Partition {
	lock := make(chan struct{}, 1)
	lock <- struct{}{}
	ret := &Partition{
		lock:             lock,
		data:             memtable.NewTable[RowID, DataValue, *DataRow](),
		columnsIndexDefs: columnsIndexDefs,
	}
	ret.state.Store(NewPartitionState())
	return ret
}

type RowID types.Rowid

func (r RowID) Less(than RowID) bool {
	return bytes.Compare(r[:], than[:]) < 0
}

type DataValue struct {
	op    uint8
	value map[string]memtable.Nullable
}

type DataRow struct {
	rowID         RowID
	value         DataValue
	indexes       []memtable.Tuple
	uniqueIndexes []memtable.Tuple
}

const (
	opInsert = iota + 1
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

func (*Partition) CheckPoint(ctx context.Context, ts timestamp.Timestamp) error {
	panic("unimplemented")
}

func (p *Partition) Get(key types.Rowid, ts timestamp.Timestamp) bool {
	t := memtable.Time{
		Timestamp: ts,
	}
	tx := memtable.NewTransaction(
		newMemTableTransactionID(),
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

	txID := newMemTableTransactionID()

	iter := memorytable.NewBatchIter(bat)
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
		// block id, time, op
		indexes = append(indexes, memtable.Tuple{
			index_BlockID_Time_OP,
			memtable.ToOrdered(rowIDToBlockID(rowID)),
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
		// the reason to ignore, see comments in Insert method
		if moerr.IsMoErrCode(err, moerr.ErrTxnWriteConflict) {
			continue
		}
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

	// As an example, lets probe this function.  First we want to find a tag so that
	// if several go routine call this function at the same time, we will not mix them.
	// the pointer b works.
	tag := int64(uintptr(unsafe.Pointer(b)))

	// enter probe, only need tag.  Adding an extra arg just for demo purpose.
	moprobe.DisttaePartitionInsert(tag, 1)

	// defer, this is the return probe.  Use same tag value
	defer moprobe.DisttaePartitionInsertRet(tag, 0x1020304050607080)

	bat, err := batch.ProtoBatchToBatch(b)
	if err != nil {
		return err
	}

	txID := newMemTableTransactionID()

	iter := memorytable.NewBatchIter(bat)
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
				return moerr.NewDuplicate(ctx)
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
		// block id, time, op
		indexes = append(indexes, memtable.Tuple{
			index_BlockID_Time_OP,
			memtable.ToOrdered(rowIDToBlockID(rowID)),
			ts,
			memtable.Uint(opInsert),
		})
		// columns indexes
		for _, def := range p.columnsIndexDefs {
			index := memtable.Tuple{
				def.Name,
			}
			for _, col := range def.Columns {
				index = append(index, memtable.ToOrdered(tuple[col].Value))
			}
			indexes = append(indexes, index)
		}

		err = p.data.Upsert(tx, &DataRow{
			rowID:   rowID,
			value:   dataValue,
			indexes: indexes,
		})
		// if conflict comes up here,  probably the checkpoint from dn
		// has duplicated history versions. As txn write conflict has been
		// checked in dn, so it is safe to ignore this error
		if moerr.IsMoErrCode(err, moerr.ErrTxnWriteConflict) {
			continue
		}
		if err != nil {
			return err
		}
		if err := tx.Commit(t); err != nil {
			return err
		}
	}

	return nil
}

func (p *Partition) GC(ts timestamp.Timestamp) error {
	// remove versions only visible before ts
	// assuming no transaction is reading or writing
	t := memtable.Time{
		Timestamp: ts,
	}
	err := p.data.FilterVersions(func(k RowID, versions []memtable.Version[DataValue]) (filtered []memtable.Version[DataValue], err error) {
		for _, version := range versions {
			if version.LockTime.IsZero() {
				// not deleted
				filtered = append(filtered, version)
				continue
			}
			if version.LockTime.Equal(t) ||
				version.LockTime.After(t) {
				// still visible after ts
				filtered = append(filtered, version)
				continue
			}
		}
		return
	})
	if err != nil {
		return err
	}
	return nil
}

func rowIDToBlockID(rowID RowID) uint64 {
	id, _ := catalog.DecodeRowid(types.Rowid(rowID))
	return id
}

func blockIDFromRowID(rowID types.Rowid) uint64 {
	id, _ := catalog.DecodeRowid(rowID)
	return id
}

func (p *Partition) DeleteByBlockID(ctx context.Context, ts timestamp.Timestamp, blockID uint64) error {
	tx := memtable.NewTransaction(newMemTableTransactionID(), memtable.Time{
		Timestamp: ts,
	}, memtable.SnapshotIsolation)
	min := memtable.Tuple{
		index_BlockID_Time_OP,
		memtable.ToOrdered(blockID),
		memtable.Min,
		memtable.Uint(opInsert),
	}
	max := memtable.Tuple{
		index_BlockID_Time_OP,
		memtable.ToOrdered(blockID),
		memtable.Max,
		memtable.Uint(opInsert),
	}
	iter := p.data.NewIndexIter(tx, min, max)
	defer iter.Close()
	for ok := iter.First(); ok; ok = iter.Next() {
		entry := iter.Item()
		if err := p.data.Delete(tx, entry.Key); err != nil {
			return err
		}
	}
	return tx.Commit(tx.Time)
}

func (p *Partition) NewReader(
	ctx context.Context,
	readerNumber int,
	index memtable.Tuple,
	defs []engine.TableDef,
	tableDef *plan.TableDef,
	skipBlocks map[uint64]uint8,
	blks []ModifyBlockMeta,
	ts timestamp.Timestamp,
	fs fileservice.FileService,
	entries []Entry,
) ([]engine.Reader, error) {

	t := memtable.Time{
		Timestamp: ts,
	}
	tx := memtable.NewTransaction(
		newMemTableTransactionID(),
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
	colIdxMp := make(map[string]int)
	if tableDef != nil {
		for i := range tableDef.Cols {
			colIdxMp[tableDef.Cols[i].Name] = i
		}
	}

	mp[catalog.Row_ID] = types.New(types.T_Rowid, 0, 0, 0)
	for _, def := range defs {
		attr, ok := def.(*engine.AttributeDef)
		if !ok {
			continue
		}
		mp[attr.Attr.Name] = attr.Attr.Type
	}

	var iter partitionIter
	if len(index) > 0 {
		iter = p.data.NewIndexIter(tx, index, index)
	} else {
		iter = p.data.NewIter(tx)
	}

	partReader := &PartitionReader{
		typsMap:         mp,
		readTime:        t,
		tx:              tx,
		inserts:         inserts,
		deletes:         deletes,
		skipBlocks:      skipBlocks,
		iter:            iter,
		colIdxMp:        colIdxMp,
		extendId2s3File: make(map[string]int),
		s3FileService:   fs,
	}
	if p.txn != nil {
		partReader.proc = p.txn.proc
	}
	readers[0] = partReader
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
				blks:     blks[(i-1)*step:],
				sels:     make([]int64, 0, 1024),
			}
		} else {
			readers[i] = &blockMergeReader{
				fs:       fs,
				ts:       ts,
				ctx:      ctx,
				tableDef: tableDef,
				blks:     blks[(i-1)*step : i*step],
				sels:     make([]int64, 0, 1024),
			}
		}
	}
	return readers, nil
}
