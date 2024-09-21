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
	"fmt"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/trace"
)

func newColumnExpr(pos int, typ plan.Type, name string) *plan.Expr {
	return &plan.Expr{
		Typ: typ,
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				Name:   name,
				ColPos: int32(pos),
			},
		},
	}
}

func genWriteReqs(
	ctx context.Context,
	txnCommit *Transaction,
) ([]txn.TxnRequest, error) {
	writes, tablesInVain, op := txnCommit.writes, txnCommit.tablesInVain, txnCommit.op
	var pkChkByTN int8
	if v := ctx.Value(defines.PkCheckByTN{}); v != nil {
		pkChkByTN = v.(int8)
	}
	var tnID string
	var tn metadata.TNService
	entries := make([]*api.Entry, 0, len(writes))
	for _, e := range writes {
		if tnID == "" {
			tnID = e.tnStore.ServiceID
			tn = e.tnStore
		}
		if tnID != "" && tnID != e.tnStore.ServiceID {
			panic(fmt.Sprintf("txnCommit contains entries from different TNs, %s != %s", tnID, e.tnStore.ServiceID))
		}
		if e.bat == nil || e.bat.IsEmpty() {
			continue
		}
		e.pkChkByTN = pkChkByTN
		pe, err := toPBEntry(e)
		if err != nil {
			return nil, err
		}
		// --sql
		// create table t (a int);
		// begin;
		// alter table t comment 'will come back';
		// drop table t;
		// commit;
		//
		// the txn wrote a delete & insert batch due to alter, and the insert batch was cancelled by dropping.
		// the table should be dropped in TN, so we need to reset the delete batch to normal delete.
		isAlter, typ, id, name := noteSplitAlter(e.note)
		if _, deleted := tablesInVain[id]; deleted && isAlter && typ == DELETE {
			// reset to normal delete, this will lead to dropping table in TN
			e.note = noteForDrop(id, name)
		} else if isAlter {
			// To tell TN, this is an update due to alter, do not touch catalog
			pe.TableName = "alter"
		}
		entries = append(entries, pe)
	}

	if len(entries) == 0 {
		return nil, nil
	}
	trace.GetService(txnCommit.proc.GetService()).TxnCommit(op, entries)
	reqs := make([]txn.TxnRequest, 0, len(entries))
	payload, err := types.Encode(&api.PrecommitWriteCmd{EntryList: entries})
	if err != nil {
		return nil, err
	}
	for _, info := range tn.Shards {
		reqs = append(reqs, txn.TxnRequest{
			CNRequest: &txn.CNOpRequest{
				OpCode:  uint32(api.OpCode_OpPreCommit),
				Payload: payload,
				Target: metadata.TNShard{
					TNShardRecord: metadata.TNShardRecord{
						ShardID: info.ShardID,
					},
					ReplicaID: info.ReplicaID,
					Address:   tn.TxnServiceAddress,
				},
			},
			Options: &txn.TxnRequestOptions{
				RetryCodes: []int32{
					// tn shard not found
					int32(moerr.ErrTNShardNotFound),
				},
				RetryInterval: int64(time.Second),
			},
		})
	}
	return reqs, nil
}

func toPBEntry(e Entry) (*api.Entry, error) {
	var ebat *batch.Batch

	if e.typ == INSERT {
		ebat = batch.NewWithSize(0)
		if e.bat.Attrs[0] == catalog.BlockMeta_MetaLoc {
			ebat.Vecs = e.bat.Vecs
			ebat.Attrs = e.bat.Attrs

			// no need to commit the blk info to tn
			ebat.Vecs = ebat.Vecs[1:]
			ebat.Attrs = e.bat.Attrs[1:]
			ebat.SetRowCount(ebat.Vecs[0].Length())
		} else {
			//e.bat.Vecs[0] is rowid vector
			ebat.Vecs = e.bat.Vecs[1:]
			ebat.Attrs = e.bat.Attrs[1:]
		}
	} else {
		ebat = e.bat
	}
	typ := api.Entry_Insert
	if e.typ == DELETE {
		typ = api.Entry_Delete
		// ddl drop bat includes extra information to generate command in TN
		if e.tableId != catalog.MO_TABLES_ID &&
			e.tableId != catalog.MO_DATABASE_ID {
			ebat = batch.NewWithSize(0)
			if e.fileName == "" {
				if len(e.bat.Vecs) != 2 {
					panic(fmt.Sprintf("e.bat should contain 2 vectors, "+
						"one is rowid vector, the other is pk vector,"+
						"database name = %s, table name = %s", e.databaseName, e.tableName))
				}
				ebat.Vecs = e.bat.Vecs[:2]
				ebat.Attrs = e.bat.Attrs[:2]
			} else {
				ebat.Vecs = e.bat.Vecs[:1]
				ebat.Attrs = e.bat.Attrs[:1]
			}
		}

	} else if e.typ == ALTER {
		typ = api.Entry_Alter
	}
	bat, err := toPBBatch(ebat)
	if err != nil {
		return nil, err
	}
	return &api.Entry{
		Bat:          bat,
		EntryType:    typ,
		TableId:      e.tableId,
		DatabaseId:   e.databaseId,
		TableName:    e.tableName,
		DatabaseName: e.databaseName,
		FileName:     e.fileName,
		PkCheckByTn:  int32(e.pkChkByTN),
	}, nil
}

func toPBBatch(bat *batch.Batch) (*api.Batch, error) {
	rbat := new(api.Batch)
	rbat.Attrs = bat.Attrs
	for _, vec := range bat.Vecs {
		pbVector, err := vector.VectorToProtoVector(vec)
		if err != nil {
			return nil, err
		}
		rbat.Vecs = append(rbat.Vecs, pbVector)
	}
	return rbat, nil
}

func getSql(ctx context.Context) string {
	if v := ctx.Value(defines.SqlKey{}); v != nil {
		return v.(string)
	}
	return ""
}
func getTyp(ctx context.Context) string {
	if v := ctx.Value(defines.DatTypKey{}); v != nil {
		return v.(string)
	}
	return ""
}

func getAccessInfo(ctx context.Context) (uint32, uint32, uint32, error) {
	var accountId, userId, roleId uint32
	var err error

	accountId, err = defines.GetAccountId(ctx)
	if err != nil {
		return 0, 0, 0, err
	}
	userId = defines.GetUserId(ctx)
	roleId = defines.GetRoleId(ctx)
	return accountId, userId, roleId, nil
}

func genDatabaseKey(id uint32, name string) databaseKey {
	return databaseKey{
		name:      name,
		accountId: id,
	}
}

func genTableKey(aid uint32, name string, databaseId uint64, databaseName string) tableKey {
	return tableKey{
		name:       name,
		databaseId: databaseId,
		dbName:     databaseName,
		accountId:  aid,
	}
}

// fillRandomRowidAndZeroTs modifies the input batch and returns the proto batch as a shallow copy.
func fillRandomRowidAndZeroTs(bat *batch.Batch, m *mpool.MPool) (*api.Batch, error) {
	var attrs []string
	vecs := make([]*vector.Vector, 0, 2)

	{
		vec := vector.NewVec(types.T_Rowid.ToType())
		for i := 0; i < bat.RowCount(); i++ {
			val := types.RandomRowid()
			if err := vector.AppendFixed(vec, val, false, m); err != nil {
				vec.Free(m)
				return nil, err
			}
		}
		vecs = append(vecs, vec)
		attrs = append(attrs, catalog.Row_ID)
	}
	{
		var val types.TS

		vec := vector.NewVec(types.T_TS.ToType())
		for i := 0; i < bat.RowCount(); i++ {
			if err := vector.AppendFixed(vec, val, false, m); err != nil {
				vecs[0].Free(m)
				vec.Free(m)
				return nil, err
			}
		}
		vecs = append(vecs, vec)
		attrs = append(attrs, catalog.TableTailAttrCommitTs)
	}
	bat.Vecs = append(vecs, bat.Vecs...)
	bat.Attrs = append(attrs, bat.Attrs...)
	return batch.BatchToProtoBatch(bat)
}

func getColPks(aid uint32, dbName, tblName string, cols []*plan.ColDef, packer *types.Packer) [][]byte {
	pks := make([][]byte, 0, len(cols))
	for _, col := range cols {
		packer.Reset()
		packer.EncodeUint32(aid)
		packer.EncodeStringType([]byte(dbName))
		packer.EncodeStringType([]byte(tblName))
		packer.EncodeStringType([]byte(col.GetOriginCaseName()))
		pks = append(pks, packer.Bytes())
	}
	return pks
}
