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
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/storage/memorystorage"
	"github.com/matrixorigin/matrixone/pkg/txn/storage/memorystorage/memtable"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
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
	rowID RowID
	value DataValue
}

func (d *DataRow) Key() RowID {
	return d.rowID
}

func (d *DataRow) Value() DataValue {
	return d.value
}

func (d *DataRow) Indexes() []memtable.Tuple {
	return nil
}

var _ MVCC = new(Partition)

func (*Partition) BlockList(ctx context.Context, ts timestamp.Timestamp, blocks []BlockMeta, entries [][]Entry) []BlockMeta {
	panic("unimplemented")
}

func (*Partition) CheckPoint(ctx context.Context, ts timestamp.Timestamp) error {
	panic("unimplemented")
}

func (p *Partition) Delete(ctx context.Context, b *api.Batch) error {
	bat, err := batch.ProtoBatchToBatch(b)
	if err != nil {
		return err
	}

	txID := uuid.NewString()

	iter := memorystorage.NewBatchIter(bat)
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

func (p *Partition) Insert(ctx context.Context, b *api.Batch) error {
	bat, err := batch.ProtoBatchToBatch(b)
	if err != nil {
		return err
	}

	txID := uuid.NewString()

	iter := memorystorage.NewBatchIter(bat)
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

		dataValue := make(DataValue)
		for i := 2; i < len(tuple); i++ {
			dataValue[bat.Attrs[i]] = tuple[i]
		}

		err := p.data.Insert(tx, &DataRow{
			rowID: rowID,
			value: dataValue,
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

func (p *Partition) NewReader(
	ctx context.Context,
	readerNumber int,
	expr *plan.Expr,
	blocks []BlockMeta,
	ts timestamp.Timestamp,
	entries [][]Entry,
) ([]engine.Reader, error) {

	t := memtable.Time{
		Timestamp: ts,
	}
	tx := memtable.NewTransaction(
		uuid.NewString(),
		t,
		memtable.SnapshotIsolation,
	)

	readers := make([]engine.Reader, readerNumber)

	readers[0] = &PartitionReader{
		iter:     p.data.NewIter(tx),
		readTime: t,
		tx:       tx,
		expr:     expr,
	}

	return readers, nil
}

func (*Partition) RowsCount(ctx context.Context, ts timestamp.Timestamp) int64 {
	panic("unimplemented")
}
