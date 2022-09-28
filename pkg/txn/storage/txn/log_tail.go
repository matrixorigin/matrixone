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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	apipb "github.com/matrixorigin/matrixone/pkg/pb/api"
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
	var fromTime *Time
	if req.CnHave != nil {
		fromTime = &Time{
			Timestamp: *req.CnHave,
		}
	}
	var toTime *Time
	if req.CnWant != nil {
		toTime = &Time{
			Timestamp: *req.CnWant,
		}
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
	appendInsert := func(row NamedRow) error {
		if err := appendNamedRow(tx, m, insertBatch, row); err != nil {
			return err
		}
		return nil
	}
	appendDelete := func(row NamedRow) error {
		if err := appendNamedRow(tx, m, deleteBatch, row); err != nil {
			return err
		}
		return nil
	}

	if tableID == ID(catalog.MO_DATABASE_ID) {
		// databases
		if err := logTailHandleSystemTable(
			m.databases,
			fromTime,
			toTime,
			appendInsert,
			appendDelete,
		); err != nil {
			return err
		}

	} else if tableID == ID(catalog.MO_TABLES_ID) {
		// relations
		if err := logTailHandleSystemTable(
			m.relations,
			fromTime,
			toTime,
			appendInsert,
			appendDelete,
		); err != nil {
			return err
		}

	} else if tableID == ID(catalog.MO_COLUMNS_ID) {
		// attributes
		if err := logTailHandleSystemTable(
			m.attributes,
			fromTime,
			toTime,
			appendInsert,
			appendDelete,
		); err != nil {
			return err
		}

	} else {
		// non-system table data
		iter := m.data.NewPhysicalIter()
		defer iter.Close()

		handleRow := func(
			physicalRow *memtable.PhysicalRow[DataKey, DataValue],
		) error {
			for i := len(physicalRow.Versions) - 1; i >= 0; i-- {
				value := physicalRow.Versions[i]

				if value.LockTx != nil &&
					value.LockTx.State.Load() == memtable.Committed &&
					(fromTime == nil || value.LockTime.After(*fromTime)) &&
					(toTime == nil || value.LockTime.Before(*toTime)) {
					// committed delete
					if err := appendDelete(&NamedDataRow{
						Value:    value.Value,
						AttrsMap: attrsMap,
					}); err != nil {
						return err
					}
					break

				} else if value.BornTx.State.Load() == memtable.Committed &&
					(fromTime == nil || value.BornTime.After(*fromTime)) &&
					(toTime == nil || value.BornTime.Before(*toTime)) {
					// committed insert
					if err := appendInsert(&NamedDataRow{
						Value:    value.Value,
						AttrsMap: attrsMap,
					}); err != nil {
						return err
					}
					break
				}

			}
			return nil
		}

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
			if err := handleRow(physicalRow); err != nil {
				return err
			}
		}

	}

	insertBatch.InitZsOne(insertBatch.Vecs[0].Length())
	deleteBatch.InitZsOne(deleteBatch.Vecs[0].Length())

	// entries
	if insertBatch.Length() > 0 {
		resp.Commands = append(resp.Commands, &LogTailEntry{
			EntryType:    apipb.Entry_Insert,
			Bat:          toPBBatch(insertBatch),
			TableId:      uint64(tableRow.ID),
			TableName:    tableRow.Name,
			DatabaseId:   uint64(dbRow.ID),
			DatabaseName: dbRow.Name,
		})
	}
	if deleteBatch.Length() > 0 {
		resp.Commands = append(resp.Commands, &LogTailEntry{
			EntryType:    apipb.Entry_Delete,
			Bat:          toPBBatch(deleteBatch),
			TableId:      uint64(tableRow.ID),
			TableName:    tableRow.Name,
			DatabaseId:   uint64(dbRow.ID),
			DatabaseName: dbRow.Name,
		})
	}

	return nil
}

func logTailHandleSystemTable[
	K memtable.Ordered[K],
	V NamedRow,
	R memtable.Row[K, V],
](
	table *memtable.Table[K, V, R],
	fromTime *Time,
	toTime *Time,
	appendInsert func(row NamedRow) error,
	appendDelete func(row NamedRow) error,
) error {

	handleRow := func(
		physicalRow *memtable.PhysicalRow[K, V],
	) error {
		for i := len(physicalRow.Versions) - 1; i >= 0; i-- {
			value := physicalRow.Versions[i]

			if value.LockTx != nil &&
				value.LockTx.State.Load() == memtable.Committed &&
				(fromTime == nil || value.LockTime.After(*fromTime)) &&
				(toTime == nil || value.LockTime.Before(*toTime)) {
				// committed delete
				if err := appendDelete(value.Value); err != nil {
					return err
				}
				break

			} else if value.BornTx.State.Load() == memtable.Committed &&
				(fromTime == nil || value.BornTime.After(*fromTime)) &&
				(toTime == nil || value.BornTime.Before(*toTime)) {
				// committed insert
				if err := appendInsert(value.Value); err != nil {
					return err
				}
				break
			}

		}
		return nil
	}

	iter := table.NewPhysicalIter()
	defer iter.Close()
	for ok := iter.First(); ok; ok = iter.Next() {
		physicalRow := iter.Item()
		if err := handleRow(physicalRow); err != nil {
			return err
		}
	}

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
