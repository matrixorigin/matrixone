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

package txnstorage

import (
	"database/sql"
	"errors"
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	apipb "github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/storage/txn/memtable"
)

type LogTailEntry = apipb.Entry

func (m *MemHandler) HandleGetLogTail(meta txn.TxnMeta, req apipb.SyncLogTailReq, resp *apipb.SyncLogTailResp) (err error) {
	tableID := ID(req.Table.TbId)

	// tx
	tx := m.getTx(meta)

	// table and db infos
	tableRow, err := m.relations.Get(tx, tableID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return moerr.NewInternalError("invalid relation id %v", tableID)
		}
		return err
	}
	dbRow, err := m.databases.Get(tx, tableRow.DatabaseID)
	if err != nil {
		return err
	}

	// time range
	from := timestamp.Timestamp{
		PhysicalTime: math.MinInt64,
	}
	if req.CnHave != nil {
		from = *req.CnHave
	}
	to := timestamp.Timestamp{
		PhysicalTime: math.MaxInt64,
	}
	if req.CnWant != nil {
		to = *req.CnWant
	}
	fromTime := Time{
		Timestamp: from,
	}
	toTime := Time{
		Timestamp: to,
	}

	// attributes
	attrs, err := m.attributes.Index(tx, Tuple{
		index_RelationID,
		tableID,
	})
	if err != nil {
		return err
	}
	attrsMap := make(map[string]*AttributeRow)
	insertNames := make([]string, 0, len(attrs))
	deleteNames := make([]string, 0, len(attrs))
	for _, attr := range attrs {
		attrsMap[attr.Value.Name] = attr.Value
		insertNames = append(insertNames, attr.Value.Name)
		if attr.Value.Primary || attr.Value.IsRowId {
			deleteNames = append(deleteNames, attr.Value.Name)
		}
	}

	// batches
	insertBatch := batch.New(false, insertNames)
	for i, name := range insertNames {
		insertBatch.Vecs[i] = vector.New(attrsMap[name].Type)
	}
	deleteBatch := batch.New(false, deleteNames)
	for i, name := range deleteNames {
		deleteBatch.Vecs[i] = vector.New(attrsMap[name].Type)
	}

	// iter
	// we don't use m.data.NewIter because we want to see deleted rows
	iter := m.data.NewPhysicalIter()
	defer iter.Close()
	tableKey := &memtable.PhysicalRow[DataKey, DataValue]{
		Key: DataKey{
			tableID:    tableID,
			primaryKey: Tuple{},
		},
	}
	for ok := iter.Seek(tableKey); ok; ok = iter.Next() {
		physicalRow := iter.Item()

		if physicalRow.Key.tableID != tableID {
			break
		}

		physicalRow.Versions.RLock()
		for i := len(physicalRow.Versions.List) - 1; i >= 0; i-- {
			value := physicalRow.Versions.List[i]

			if value.LockTx != nil &&
				value.LockTx.State.Load() == memtable.Committed &&
				value.LockTime.After(fromTime) &&
				value.LockTime.Before(toTime) {
				// committed delete
				namedRow := &NamedDataRow{
					Value:    *value.Value,
					AttrsMap: attrsMap,
				}
				if err := appendNamedRow(tx, m.mheap, deleteBatch, namedRow); err != nil {
					return err
				}
				break

			} else if value.BornTx.State.Load() == memtable.Committed &&
				value.BornTime.After(fromTime) &&
				value.BornTime.Before(toTime) {
				// committed insert
				namedRow := &NamedDataRow{
					Value:    *value.Value,
					AttrsMap: attrsMap,
				}
				if err := appendNamedRow(tx, m.mheap, insertBatch, namedRow); err != nil {
					return err
				}
				break

			}

		}
		physicalRow.Versions.RUnlock()

	}

	// entries
	entries := []*LogTailEntry{
		{
			EntryType:    apipb.Entry_Insert,
			Bat:          toPBBatch(insertBatch),
			TableId:      uint64(tableRow.ID),
			TableName:    tableRow.Name,
			DatabaseId:   uint64(dbRow.ID),
			DatabaseName: dbRow.Name,
		},
		{
			EntryType:    apipb.Entry_Delete,
			Bat:          toPBBatch(deleteBatch),
			TableId:      uint64(tableRow.ID),
			TableName:    tableRow.Name,
			DatabaseId:   uint64(dbRow.ID),
			DatabaseName: dbRow.Name,
		},
	}

	resp.Commands = entries

	return nil
}

func (c *CatalogHandler) HandleGetLogTail(meta txn.TxnMeta, req apipb.SyncLogTailReq, resp *apipb.SyncLogTailResp) (err error) {
	return c.upstream.HandleGetLogTail(meta, req, resp)
}

func toPBBatch(bat *batch.Batch) (ret *apipb.Batch) {
	ret = new(apipb.Batch)
	ret.Attrs = bat.Attrs
	for _, vec := range bat.Vecs {
		pbVector, err := vector.VectorToProtoVector(vec)
		if err != nil {
			panic(err)
		}
		ret.Vecs = append(ret.Vecs, pbVector)
	}
	return ret
}
