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

package rpc

import (
	"context"
	"os"
	"syscall"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	apipb "github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/moengine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
)

type Handle struct {
	eng moengine.TxnEngine
}

func NewTAEHandle(opt *options.Options) *Handle {
	//just for test
	path := "./store"
	tae, err := openTAE(path, opt)
	if err != nil {
		panic(err)
	}

	h := &Handle{
		eng: moengine.NewEngine(tae),
	}
	return h
}
func (h *Handle) HandleCommit(meta txn.TxnMeta) (err error) {
	txn, err := h.eng.GetTxnByID(meta.GetID())
	if err != nil {
		return err
	}
	err = txn.Commit()
	return
}

func (h *Handle) HandleRollback(meta txn.TxnMeta) (err error) {
	txn, err := h.eng.GetTxnByID(meta.GetID())
	if err != nil {
		return err
	}
	err = txn.Rollback()
	return
}

func (h *Handle) HandleCommitting(meta txn.TxnMeta) (err error) {
	txn, err := h.eng.GetTxnByID(meta.GetID())
	if err != nil {
		return err
	}
	err = txn.Committing()
	return
}

func (h *Handle) HandlePrepare(meta txn.TxnMeta) (pts timestamp.Timestamp, err error) {
	txn, err := h.eng.GetTxnByID(meta.GetID())
	if err != nil {
		return timestamp.Timestamp{}, err
	}
	ts, err := txn.Prepare()
	pts = ts.ToTimestamp()
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
	tae := h.eng.GetTAE(context.Background())
	res, err := db.LogtailHandler(tae, req)
	if err != nil {
		return err
	}
	*resp = res
	return nil
}

// TODO:: need to handle resp.
func (h *Handle) HandlePreCommit(
	meta txn.TxnMeta,
	req apipb.PrecommitWriteCmd,
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
				req := db.CreateDatabaseReq{
					Name: cmd.Name,
					AccessInfo: db.AccessInfo{
						UserID:    cmd.Owner,
						RoleID:    cmd.Creator,
						AccountID: cmd.AccountId,
					},
				}
				if err = h.HandleCreateDatabase(meta, req,
					new(db.CreateDatabaseResp)); err != nil {
					return err
				}
			}
		case []catalog.CreateTable:
			for _, cmd := range cmds {
				req := db.CreateRelationReq{
					Name:         cmd.Name,
					DatabaseName: cmd.DatabaseName,
					DatabaseID:   cmd.DatabaseId,
					Defs:         cmd.Defs,
				}
				if err = h.HandleCreateRelation(meta, req,
					new(db.CreateRelationResp)); err != nil {
					return err
				}
			}
		case []catalog.DropDatabase:
			for _, cmd := range cmds {
				req := db.DeleteDatabaseReq{
					Name: cmd.Name,
					AccessInfo: db.AccessInfo{
						UserID:    req.UserId,
						RoleID:    req.RoleId,
						AccountID: req.AccountId,
					},
				}
				if err = h.HandleDeleteDatabase(meta, req,
					new(db.DeleteDatabaseResp)); err != nil {
					return err
				}
			}
		case []catalog.DropTable:
			for _, cmd := range cmds {
				req := db.DeleteRelationReq{
					Name:         cmd.Name,
					DatabaseName: cmd.DatabaseName,
					DatabaseID:   cmd.DatabaseId,
				}
				if err = h.HandleDeleteRelation(meta, req,
					new(db.DeleteRelationResp)); err != nil {
					return err
				}
			}
		case *apipb.Entry:
			//Handle DML
			moBat, err := batch.ProtoBatchToBatch(e.(*apipb.Entry).Bat)
			if err != nil {
				panic(err)
			}
			req := db.WriteReq{
				Type:         db.EntryType(e.(*apipb.Entry).EntryType),
				TableID:      e.(*apipb.Entry).TableId,
				DatabaseName: e.(*apipb.Entry).DatabaseName,
				TableName:    e.(*apipb.Entry).TableName,
				Batch:        moBat,
			}
			if err = h.HandleWrite(meta, req,
				new(db.WriteResp)); err != nil {
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
	req db.CreateDatabaseReq,
	resp *db.CreateDatabaseResp) (err error) {

	txn, err := h.eng.GetOrCreateTxnWithMeta(nil, meta.GetID(),
		types.TimestampToTS(meta.GetSnapshotTS()))
	if err != nil {
		return err
	}
	err = h.eng.CreateDatabase(context.TODO(), req.Name, txn)
	if err != nil {
		return
	}
	db, err := h.eng.GetDatabase(context.TODO(), req.Name, txn)
	if err != nil {
		return
	}
	resp.ID = db.GetDatabaseID(context.TODO())
	return
}

func (h *Handle) HandleDeleteDatabase(
	meta txn.TxnMeta,
	req db.DeleteDatabaseReq,
	resp *db.DeleteDatabaseResp) (err error) {

	txn, err := h.eng.GetOrCreateTxnWithMeta(nil, meta.GetID(),
		types.TimestampToTS(meta.GetSnapshotTS()))
	if err != nil {
		return err
	}
	err = h.eng.DropDatabase(context.TODO(), req.Name, txn)
	if err != nil {
		return
	}

	db, err := h.eng.GetDatabase(context.TODO(), req.Name, txn)
	if err != nil {
		return
	}
	resp.ID = db.GetDatabaseID(context.TODO())
	return
}

func (h *Handle) HandleCreateRelation(
	meta txn.TxnMeta,
	req db.CreateRelationReq,
	resp *db.CreateRelationResp) (err error) {

	txn, err := h.eng.GetOrCreateTxnWithMeta(nil, meta.GetID(),
		types.TimestampToTS(meta.GetSnapshotTS()))
	if err != nil {
		return
	}
	db, err := h.eng.GetDatabase(context.TODO(), req.DatabaseName, txn)
	if err != nil {
		return
	}
	err = db.CreateRelation(context.TODO(), req.Name, req.Defs)
	if err != nil {
		return
	}

	tb, err := db.GetRelation(context.TODO(), req.Name)
	if err != nil {
		return
	}
	resp.ID = tb.GetRelationID(context.TODO())
	return
}

func (h *Handle) HandleDeleteRelation(
	meta txn.TxnMeta,
	req db.DeleteRelationReq,
	resp *db.DeleteRelationResp) (err error) {

	txn, err := h.eng.GetOrCreateTxnWithMeta(nil, meta.GetID(),
		types.TimestampToTS(meta.GetSnapshotTS()))
	if err != nil {
		return
	}
	db, err := h.eng.GetDatabase(context.TODO(), req.DatabaseName, txn)
	if err != nil {
		return
	}

	tb, err := db.GetRelation(context.TODO(), req.Name)
	if err != nil {
		return
	}
	resp.ID = tb.GetRelationID(context.TODO())

	err = db.DropRelation(context.TODO(), req.Name)
	if err != nil {
		return
	}
	return
}

// HandleWrite Handle DML commands
func (h *Handle) HandleWrite(
	meta txn.TxnMeta,
	req db.WriteReq,
	resp *db.WriteResp) (err error) {

	txn, err := h.eng.GetOrCreateTxnWithMeta(nil, meta.GetID(),
		types.TimestampToTS(meta.GetSnapshotTS()))
	if err != nil {
		return err
	}

	dbase, err := h.eng.GetDatabase(context.TODO(), req.DatabaseName, txn)
	if err != nil {
		return
	}

	tb, err := dbase.GetRelation(context.TODO(), req.TableName)
	if err != nil {
		return
	}

	if req.Type == db.EntryInsert {
		//Append a block had been bulk-loaded into S3
		if req.FileName != "" {
			//TODO::Precommit a block from S3
			//tb.AppendBlock()
			panic(moerr.NewNYI("Precommit a block is not implemented yet"))
		}
		//Add a batch into table
		//TODO::add a parameter to Append for PreCommit-Append?
		err = tb.Write(context.TODO(), req.Batch)
		return
	}

	//TODO:: handle delete rows of block had been bulk-loaded into S3.

	//Vecs[0]--> rowid
	//Vecs[1]--> PrimaryKey
	err = tb.DeleteByPhyAddrKeys(context.TODO(), req.Batch.GetVector(0))
	return

}

func openTAE(targetDir string, opt *options.Options) (tae *db.DB, err error) {

	if targetDir != "" {
		mask := syscall.Umask(0)
		if err := os.MkdirAll(targetDir, os.FileMode(0755)); err != nil {
			syscall.Umask(mask)
			logutil.Infof("Recreate dir error:%v\n", err)
			return nil, err
		}
		syscall.Umask(mask)
		tae, err = db.Open(targetDir+"/tae", nil)
		if err != nil {
			logutil.Infof("Open tae failed. error:%v", err)
			return nil, err
		}
		return tae, nil
	}

	tae, err = db.Open(targetDir, opt)
	if err != nil {
		logutil.Infof("Open tae failed. error:%v", err)
		return nil, err
	}
	return
}
