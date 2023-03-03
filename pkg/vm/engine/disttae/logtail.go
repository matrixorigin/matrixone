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
	"context"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
)

func consumeEntry(idx, primaryIdx int, tbl *txnTable,
	ctx context.Context, engine *Engine, partition *Partition, state *PartitionState, e *api.Entry) error {

	state.HandleLogtailEntry(ctx, e, primaryIdx)

	if e.EntryType == api.Entry_Insert {
		if isMetaTable(e.TableName) {
			vec, err := vector.ProtoVectorToVector(e.Bat.Vecs[catalog.BLOCKMETA_ID_IDX+MO_PRIMARY_OFF])
			if err != nil {
				return err
			}
			timeVec, err := vector.ProtoVectorToVector(e.Bat.Vecs[catalog.BLOCKMETA_COMMITTS_IDX+MO_PRIMARY_OFF])
			if err != nil {
				return err
			}
			vs := vector.MustTCols[uint64](vec)
			timestamps := vector.MustTCols[types.TS](timeVec)
			for i, v := range vs {
				if err := tbl.parts[idx].DeleteByBlockID(ctx, timestamps[i].ToTimestamp(), v); err != nil {
					if !moerr.IsMoErrCode(err, moerr.ErrTxnWriteConflict) {
						return err
					}
				}
			}
			return engine.getMetaPartitions(e.TableName)[idx].Insert(ctx, -1, e.Bat, false)
		}
		switch e.TableId {
		case catalog.MO_TABLES_ID:
			bat, _ := batch.ProtoBatchToBatch(e.Bat)
			tbl.db.txn.engine.catalog.InsertTable(bat)
		case catalog.MO_DATABASE_ID:
			bat, _ := batch.ProtoBatchToBatch(e.Bat)
			tbl.db.txn.engine.catalog.InsertDatabase(bat)
		case catalog.MO_COLUMNS_ID:
			bat, _ := batch.ProtoBatchToBatch(e.Bat)
			tbl.db.txn.engine.catalog.InsertColumns(bat)
		}
		if primaryIdx >= 0 {
			return partition.Insert(ctx, MO_PRIMARY_OFF+primaryIdx, e.Bat, false)
		}
		return partition.Insert(ctx, primaryIdx, e.Bat, false)
	}
	if isMetaTable(e.TableName) {
		return engine.getMetaPartitions(e.TableName)[idx].Delete(ctx, e.Bat)
	}
	switch e.TableId {
	case catalog.MO_TABLES_ID:
		bat, _ := batch.ProtoBatchToBatch(e.Bat)
		tbl.db.txn.engine.catalog.DeleteTable(bat)
	case catalog.MO_DATABASE_ID:
		bat, _ := batch.ProtoBatchToBatch(e.Bat)
		tbl.db.txn.engine.catalog.DeleteDatabase(bat)
	}
	return partition.Delete(ctx, e.Bat)
}
