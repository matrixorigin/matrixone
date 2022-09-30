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

package memorystorage

import (
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	apipb "github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	txnengine "github.com/matrixorigin/matrixone/pkg/vm/engine/memoryengine"
)

func (m *MemHandler) HandlePreCommit(meta txn.TxnMeta, req apipb.PrecommitWriteCmd,
	resp *apipb.SyncLogTailResp) (err error) {
	var e any

	es := req.EntryList
	for len(es) > 0 {
		e, es, err = catalog.ParseEntryList(es)
		if err != nil {
			return err
		}
		switch cmds := e.(type) {
		case []catalog.CreateDatabase:
			for _, cmd := range cmds {
				req := txnengine.CreateDatabaseReq{
					Name: cmd.Name,
					AccessInfo: txnengine.AccessInfo{
						UserID:    cmd.Owner,
						RoleID:    cmd.Creator,
						AccountID: cmd.AccountId,
					},
				}
				if err = m.HandleCreateDatabase(meta, req,
					new(txnengine.CreateDatabaseResp)); err != nil {
					return err
				}
			}
		case []catalog.CreateTable:
			for _, cmd := range cmds {
				req := txnengine.CreateRelationReq{
					Name:         cmd.Name,
					DatabaseName: cmd.DatabaseName,
					DatabaseID:   txnengine.ID(cmd.DatabaseId),
					Defs:         cmd.Defs,
				}
				if err = m.HandleCreateRelation(meta, req,
					new(txnengine.CreateRelationResp)); err != nil {
					return err
				}
			}
		case []catalog.DropDatabase:
			for _, cmd := range cmds {
				req := txnengine.DeleteDatabaseReq{
					Name: cmd.Name,
					AccessInfo: txnengine.AccessInfo{
						UserID:    req.UserId,
						RoleID:    req.RoleId,
						AccountID: req.AccountId,
					},
				}
				if err = m.HandleDeleteDatabase(meta, req,
					new(txnengine.DeleteDatabaseResp)); err != nil {
					return err
				}
			}
		case []catalog.DropTable:
			for _, cmd := range cmds {
				req := txnengine.DeleteRelationReq{
					Name:         cmd.Name,
					DatabaseName: cmd.DatabaseName,
					DatabaseID:   txnengine.ID(cmd.DatabaseId),
				}
				if err = m.HandleDeleteRelation(meta, req,
					new(txnengine.DeleteRelationResp)); err != nil {
					return err
				}
			}
		default:
			pe := e.(*apipb.Entry)
			bat, err := batch.ProtoBatchToBatch(pe.GetBat())
			if err != nil {
				return err
			}
			if pe.EntryType == apipb.Entry_Insert {
				req := txnengine.WriteReq{
					TableID:      txnengine.ID(pe.GetTableId()),
					DatabaseName: pe.GetDatabaseName(),
					TableName:    pe.GetTableName(),
					Batch:        bat,
				}
				if err = m.HandleWrite(meta, req,
					new(txnengine.WriteResp)); err != nil {
					return err
				}
			} else {
				req := txnengine.DeleteReq{
					TableID:      txnengine.ID(pe.GetTableId()),
					DatabaseName: pe.GetDatabaseName(),
					TableName:    pe.GetTableName(),
					ColumnName:   bat.Attrs[0],
					Vector:       bat.Vecs[0],
				}
				if err = m.HandleDelete(meta, req,
					new(txnengine.DeleteResp)); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (c *CatalogHandler) HandlePreCommit(meta txn.TxnMeta, req apipb.PrecommitWriteCmd, resp *apipb.SyncLogTailResp) (err error) {
	return c.upstream.HandlePreCommit(meta, req, resp)
}
