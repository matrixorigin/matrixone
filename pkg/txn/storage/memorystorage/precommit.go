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
	"context"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	apipb "github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	txnengine "github.com/matrixorigin/matrixone/pkg/vm/engine/memoryengine"
)

func (m *MemHandler) HandlePreCommit(ctx context.Context, meta txn.TxnMeta, req apipb.PrecommitWriteCmd,
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
					ID:   ID(cmd.DatabaseId),
					Name: cmd.Name,
					AccessInfo: txnengine.AccessInfo{
						UserID:    cmd.Owner,
						RoleID:    cmd.Creator,
						AccountID: cmd.AccountId,
					},
				}
				if err = m.HandleCreateDatabase(ctx, meta, req,
					new(txnengine.CreateDatabaseResp)); err != nil {
					return err
				}
			}
		case []catalog.CreateTable:
			for _, cmd := range cmds {
				req := txnengine.CreateRelationReq{
					ID:           ID(cmd.TableId),
					Name:         cmd.Name,
					DatabaseName: cmd.DatabaseName,
					DatabaseID:   txnengine.ID(cmd.DatabaseId),
					Defs:         cmd.Defs,
				}
				if err = m.HandleCreateRelation(ctx, meta, req,
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
				if err = m.HandleDeleteDatabase(ctx, meta, req,
					new(txnengine.DeleteDatabaseResp)); err != nil {
					return err
				}
			}
		case []catalog.DropOrTruncateTable:
			for _, cmd := range cmds {
				if cmd.IsDrop {
					req := txnengine.DeleteRelationReq{
						Name:         cmd.Name,
						DatabaseName: cmd.DatabaseName,
						DatabaseID:   txnengine.ID(cmd.DatabaseId),
					}
					if err = m.HandleDeleteRelation(ctx, meta, req,
						new(txnengine.DeleteRelationResp)); err != nil {
						return err
					}
				} else {
					req := txnengine.TruncateRelationReq{
						OldTableID:   txnengine.ID(cmd.Id),
						DatabaseName: cmd.DatabaseName,
						Name:         cmd.Name,
					}

					if err = m.HandleTruncateRelation(ctx, meta, req,
						new(txnengine.TruncateRelationResp)); err != nil {
						return err
					}
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
				if err = m.HandleWrite(ctx, meta, req,
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
				if err = m.HandleDelete(ctx, meta, req,
					new(txnengine.DeleteResp)); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (c *CatalogHandler) HandlePreCommit(ctx context.Context, meta txn.TxnMeta, req apipb.PrecommitWriteCmd, resp *apipb.SyncLogTailResp) (err error) {
	return c.upstream.HandlePreCommit(ctx, meta, req, resp)
}
