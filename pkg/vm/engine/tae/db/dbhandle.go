// Copyright 2021 - 2022 Matrix Origin
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

package db

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	apipb "github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"os"
	"syscall"
)

type Handle struct {
	db *DB
}

func NewTAEHandle(opt *options.Options) *Handle {
	//just for test
	path := "./store"
	tae, err := openTAE(path, opt)
	if err != nil {
		panic(err)
	}
	h := &Handle{
		db: tae,
	}
	return h
}
func (h *Handle) HandleCommit(meta txn.TxnMeta) (err error) {
	txn, err := h.db.GetTxnByMeta(nil, meta)
	if err != nil {
		return err
	}
	err = txn.Commit()
	return
}

func (h *Handle) HandleRollback(meta txn.TxnMeta) (err error) {
	txn, err := h.db.GetTxnByMeta(nil, meta)
	if err != nil {
		return err
	}
	err = txn.Commit()
	return
}

func (h *Handle) HandleCommitting(meta txn.TxnMeta) (err error) {
	txn, err := h.db.GetTxnByMeta(nil, meta)
	if err != nil {
		return err
	}
	err = txn.Committing()
	return
}

func (h *Handle) HandlePrepare(meta txn.TxnMeta) (pts timestamp.Timestamp, err error) {
	txn, err := h.db.GetTxnByMeta(nil, meta)
	if err != nil {
		return timestamp.Timestamp{}, err
	}
	ts, err := txn.Prepare()
	//TODO:: use a transfer function in txnts.go
	pts = timestamp.Timestamp{
		PhysicalTime: types.DecodeInt64(ts[4:12]),
		LogicalTime:  types.DecodeUint32(ts[:4])}
	return
}

func (h *Handle) HandleStartRecovery(ch chan txn.TxnMeta) {
	panic(moerr.NewNYI("HandleStartRecovery is not implemented yet"))
}

func (h *Handle) HandleClose() (err error) {
	panic(moerr.NewNYI("HandleClose is not implemented yet"))
}

func (h *Handle) HandleDestroy() (err error) {
	panic(moerr.NewNYI("HandleDestroy is not implemented yet"))
}

func (h *Handle) HandleGetLogTail(
	meta txn.TxnMeta,
	req apipb.SyncLogTailReq,
	resp *apipb.SyncLogTailResp) (err error) {
	//TODO:: Hanfeng will implement it in next PR.
	panic(moerr.NewNYI("HandleDestroy is not implemented yet"))
}

func (h *Handle) HandlePreCommit(
	meta txn.TxnMeta,
	req apipb.PrecommitWriteCmd,
	resp *apipb.SyncLogTailResp) (err error) {
	var e any

	es := req.EntryList
	for len(es) > 0 {
		//TODO::use pkg/catalog.ParseEntryList
		e, es, err = catalog.ParseEntryList(es)
		if err != nil {
			return err
		}
		switch cmds := e.(type) {
		//TODO:: use pkg/catalog.CreateDatabase
		case []catalog.CreateDatabase:
			for _, cmd := range cmds {
				req := CreateDatabaseReq{
					Name: cmd.Name,
					AccessInfo: AccessInfo{
						UserID:    cmd.Owner,
						RoleID:    cmd.Creator,
						AccountID: cmd.AccountId,
					},
				}
				if err = h.HandleCreateDatabase(meta, req,
					new(CreateDatabaseResp)); err != nil {
					return err
				}
			}
		case []catalog.CreateTable:
			for _, cmd := range cmds {
				req := CreateRelationReq{
					Name:         cmd.Name,
					DatabaseName: cmd.DatabaseName,
					DatabaseID:   cmd.DatabaseId,
					Defs:         cmd.Defs,
				}
				if err = h.HandleCreateRelation(meta, req,
					new(CreateRelationResp)); err != nil {
					return err
				}
			}
		case []catalog.DropDatabase:
			for _, cmd := range cmds {
				req := DeleteDatabaseReq{
					Name: cmd.Name,
					AccessInfo: AccessInfo{
						UserID:    req.UserId,
						RoleID:    req.RoleId,
						AccountID: req.AccountId,
					},
				}
				if err = h.HandleDeleteDatabase(meta, req,
					new(DeleteDatabaseResp)); err != nil {
					return err
				}
			}
		case []catalog.DropTable:
			for _, cmd := range cmds {
				req := DeleteRelationReq{
					Name:         cmd.Name,
					DatabaseName: cmd.DatabaseName,
					DatabaseID:   cmd.DatabaseId,
				}
				if err = h.HandleDeleteRelation(meta, req,
					new(DeleteRelationResp)); err != nil {
					return err
				}
			}
		case *apipb.Entry:
			//Handle DML
			moBat, err := batch.ProtoBatchToBatch(e.(*apipb.Entry).Bat)
			if err != nil {
				panic(err)
			}
			req := WriteReq{
				Type:         EntryType(e.(*apipb.Entry).EntryType),
				TableID:      e.(*apipb.Entry).TableId,
				DatabaseName: e.(*apipb.Entry).DatabaseName,
				TableName:    e.(*apipb.Entry).TableName,
				Batch:        moBat,
			}
			if err = h.HandleWrite(meta, req,
				new(WriteResp)); err != nil {
				return err
			}
		default:
			panic(moerr.NewNYI(""))
		}
	}
	return nil

}

//Handle DDL commands.

func (h *Handle) HandleCreateDatabase(
	meta txn.TxnMeta,
	req CreateDatabaseReq,
	resp *CreateDatabaseResp) (err error) {

	txn, err := h.db.GetOrCreateTxnWithMeta(nil, meta)
	if err != nil {
		return err
	}

	txn.BindAccessInfo(
		req.AccessInfo.AccountID,
		req.AccessInfo.UserID,
		req.AccessInfo.RoleID)

	db, err := txn.CreateDatabase(req.Name)
	if err != nil {
		return
	}
	resp.ID = db.GetID()
	return
}

func (h *Handle) HandleDeleteDatabase(
	meta txn.TxnMeta,
	req DeleteDatabaseReq,
	resp *DeleteDatabaseResp) (err error) {

	txn, err := h.db.GetOrCreateTxnWithMeta(nil, meta)
	if err != nil {
		return err
	}

	db, err := txn.DropDatabase(req.Name)
	if err != nil {
		return
	}
	resp.ID = db.GetID()
	return
}

func (h *Handle) HandleCreateRelation(
	meta txn.TxnMeta,
	req CreateRelationReq,
	resp *CreateRelationResp) (err error) {

	txn, err := h.db.GetOrCreateTxnWithMeta(nil, meta)
	if err != nil {
		return
	}

	db, err := txn.GetDatabase(req.DatabaseName)
	if err != nil {
		return
	}
	schema, err := catalog.DefsToSchema(req.Name, req.Defs)
	if err != nil {
		return
	}
	schema.BlockMaxRows = 40000
	schema.SegmentMaxBlocks = 20
	tb, err := db.CreateRelation(schema)
	if err != nil {
		return
	}
	resp.ID = tb.ID()
	return
}

func (h *Handle) HandleDeleteRelation(
	meta txn.TxnMeta,
	req DeleteRelationReq,
	resp *DeleteRelationResp) (err error) {

	txn, err := h.db.GetOrCreateTxnWithMeta(nil, meta)
	if err != nil {
		return
	}

	db, err := txn.GetDatabase(req.DatabaseName)
	if err != nil {
		return
	}
	tb, err := db.DropRelationByName(req.Name)
	if err != nil {
		return
	}
	resp.ID = tb.ID()
	return
}

//Handle DML commands

func (h *Handle) HandleWrite(
	meta txn.TxnMeta,
	req WriteReq,
	resp *WriteResp) (err error) {

	txn, err := h.db.GetOrCreateTxnWithMeta(nil, meta)
	if err != nil {
		return err
	}

	db, err := txn.GetDatabase(req.DatabaseName)
	if err != nil {
		return
	}

	tb, err := db.GetRelationByName(req.TableName)
	if err != nil {
		return
	}

	if req.Type == EntryInsert {
		//Append a block had been bulk-loaded into S3
		if req.FileName != "" {
			//TODO::Precommit a block from S3
			//tb.PrecommitAppendBlock()
			panic(moerr.NewNYI("Precommit a block is not implemented yet"))
		}
		//Add a batch into table
		taeBat := containers.MOToTAEBatch(
			req.Batch,
			tb.GetMeta().(*catalog.TableEntry).GetSchema().
				AllNullables(),
		)
		//TODO::use PrecommitAppend, instead of Append.
		err = tb.Append(taeBat)
		return
	}

	//TODO:: handle delete rows of block had been bulk-loaded into S3.

	//Delete a batch
	err = tb.DeleteByPhyAddrKeys(
		containers.MOToTAEVector(
			req.Batch.Vecs[0],
			false),
	)
	return

}

func openTAE(targetDir string, opt *options.Options) (tae *DB, err error) {

	if targetDir != "" {
		mask := syscall.Umask(0)
		if err := os.MkdirAll(targetDir, os.FileMode(0755)); err != nil {
			syscall.Umask(mask)
			logutil.Infof("Recreate dir error:%v\n", err)
			return nil, err
		}
		syscall.Umask(mask)
		tae, err = Open(targetDir+"/tae", nil)
		if err != nil {
			logutil.Infof("Open tae failed. error:%v", err)
			return nil, err
		}
		return tae, nil
	}

	tae, err = Open(targetDir, opt)
	if err != nil {
		logutil.Infof("Open tae failed. error:%v", err)
		return nil, err
	}
	return
}
