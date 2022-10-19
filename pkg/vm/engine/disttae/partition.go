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
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/storage/memorystorage/memtable"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

const (
	index_PrimaryKey = "primary key"
	index_BlockID    = "block id"
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

type DataValue map[string]memtable.Nullable

type DataRow struct {
	rowID   RowID
	value   DataValue
	indexes []memtable.Tuple
}

func (d *DataRow) Key() RowID {
	return d.rowID
}

func (d *DataRow) Value() DataValue {
	return d.value
}

func (d *DataRow) Indexes() []memtable.Tuple {
	return d.indexes
}

var _ MVCC = new(Partition)

func (*Partition) BlockList(ctx context.Context, ts timestamp.Timestamp, blocks []BlockMeta, entries []Entry) []BlockMeta {
	return nil
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

		err := p.data.Delete(tx, rowID)
		if err != nil {
			return err
		}

		if err := tx.Commit(t); err != nil {
			return err
		}
	}

	return nil
}

func (p *Partition) Insert(ctx context.Context, primaryKeyIndex int, b *api.Batch) error {
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
			primaryKey = memtable.ToOrdered(tuple[primaryKeyIndex])
			entries, err := p.data.Index(tx, memtable.Tuple{
				index_PrimaryKey,
				primaryKey,
			})
			if err != nil {
				return err
			}
			if len(entries) > 0 {
				return moerr.NewDuplicate()
			}
		}

		dataValue := make(DataValue)
		for i := 2; i < len(tuple); i++ {
			dataValue[bat.Attrs[i]] = tuple[i]
		}

		// indexes
		var indexes []memtable.Tuple
		if primaryKey != nil {
			indexes = append(indexes, memtable.Tuple{
				index_PrimaryKey,
				primaryKey,
			})
		}
		indexes = append(indexes, memtable.Tuple{
			index_BlockID,
			memtable.ToOrdered(rowIDToBlockID(rowID)),
		})

		err = p.data.Insert(tx, &DataRow{
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
	return types.DecodeUint64(rowID[:8]) //TODO use tae provided function
}

func (p *Partition) IterRowIDsByBlockID(ctx context.Context, ts timestamp.Timestamp, blockID uint64, fn func(rowID RowID) bool) {
	tx := memtable.NewTransaction(uuid.NewString(), memtable.Time{
		Timestamp: ts,
	}, memtable.SnapshotIsolation)
	iter := p.data.NewIndexIter(tx, memtable.Tuple{
		index_BlockID,
		memtable.ToOrdered(blockID),
	})
	defer iter.Close()
	for ok := iter.First(); ok; ok = iter.Next() {
		entry := iter.Item()
		rowID := entry.Key
		if !fn(rowID) {
			break
		}
	}
}

func (p *Partition) NewReader(
	ctx context.Context,
	readerNumber int,
	expr *plan.Expr,
	defs []engine.TableDef,
	blocks []BlockMeta,
	ts timestamp.Timestamp,
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
	for i := 1; i < readerNumber; i++ {
		readers[i] = &emptyReader{}
	}

	return readers, nil
}
