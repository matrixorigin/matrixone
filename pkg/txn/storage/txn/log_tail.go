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

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	apipb "github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	txnengine "github.com/matrixorigin/matrixone/pkg/vm/engine/txn"
)

type LogTailEntry = apipb.Entry

func (m *MemHandler) HandleGetLogTail(meta txn.TxnMeta, req txnengine.GetLogTailReq, resp *txnengine.GetLogTailResp) (err error) {

	// tx
	tx := m.getTx(meta)

	// table and db infos
	tableRow, err := m.relations.Get(tx, Text(req.TableID))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			resp.ErrRelationNotFound.ID = req.TableID
			return nil
		}
		return err
	}
	dbRow, err := m.databases.Get(tx, Text(tableRow.DatabaseID))
	if err != nil {
		return err
	}

	// time range
	from := timestamp.Timestamp{
		PhysicalTime: math.MinInt64,
	}
	if req.Request.CnHave != nil {
		from = *req.Request.CnHave
	}
	to := timestamp.Timestamp{
		PhysicalTime: math.MaxInt64,
	}
	if req.Request.CnWant != nil {
		to = *req.Request.CnWant
	}
	fromTime := Time{
		Timestamp: from,
	}
	toTime := Time{
		Timestamp: to,
	}

	// attributes
	rows, err := m.attributes.IndexRows(tx, Tuple{
		index_RelationID,
		Text(req.TableID),
	})
	if err != nil {
		return err
	}
	attrsMap := make(map[string]*AttributeRow)
	insertNames := make([]string, 0, len(rows))
	deleteNames := make([]string, 0, len(rows))
	for _, row := range rows {
		attrsMap[row.Name] = row
		insertNames = append(insertNames, row.Name)
		if row.Primary || row.IsRowId {
			deleteNames = append(deleteNames, row.Name)
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
	iter := m.data.rows.Iter()
	defer iter.Release()
	tableKey := &PhysicalRow[DataKey, DataRow]{
		Key: DataKey{
			tableID:    req.TableID,
			primaryKey: Tuple{},
		},
	}
	for ok := iter.Seek(tableKey); ok; ok = iter.Next() {
		physicalRow := iter.Item()

		if physicalRow.Key.tableID != req.TableID {
			break
		}

		values := physicalRow.Values
		values.RLock()
		for i := len(values.Values) - 1; i >= 0; i-- {
			value := values.Values[i]

			if value.LockTx != nil &&
				value.LockTx.State.Load() == Committed &&
				value.LockTime.After(fromTime) &&
				value.LockTime.Before(toTime) {
				// committed delete
				namedRow := &NamedDataRow{
					Row:      value.Value,
					AttrsMap: attrsMap,
				}
				if err := appendNamedRow(tx, m.mheap, deleteBatch, namedRow); err != nil {
					return err
				}
				break

			} else if value.BornTx.State.Load() == Committed &&
				value.BornTime.After(fromTime) &&
				value.BornTime.Before(toTime) {
				// committed insert
				namedRow := &NamedDataRow{
					Row:      value.Value,
					AttrsMap: attrsMap,
				}
				if err := appendNamedRow(tx, m.mheap, insertBatch, namedRow); err != nil {
					return err
				}
				break

			}

		}
		values.RUnlock()

	}

	// entries
	entries := []*LogTailEntry{
		{
			EntryType:    apipb.Entry_Insert,
			Bat:          toPBBatch(insertBatch),
			TableId:      tableRow.NumberID,
			TableName:    tableRow.Name,
			DatabaseId:   dbRow.NumberID,
			DatabaseName: dbRow.Name,
		},
		{
			EntryType:    apipb.Entry_Delete,
			Bat:          toPBBatch(deleteBatch),
			TableId:      tableRow.NumberID,
			TableName:    tableRow.Name,
			DatabaseId:   dbRow.NumberID,
			DatabaseName: dbRow.Name,
		},
	}

	resp.Response.Commands = entries

	return nil
}

func (c *CatalogHandler) HandleGetLogTail(meta txn.TxnMeta, req txnengine.GetLogTailReq, resp *txnengine.GetLogTailResp) (err error) {
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
