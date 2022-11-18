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
	"bytes"
	"context"
	"fmt"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/matrixorigin/matrixone/pkg/defines"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	apipb "github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/moengine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
)

// TODO::GC the abandoned txn.
type Handle struct {
	eng moengine.TxnEngine
	mu  struct {
		sync.RWMutex
		//map txn id to txnContext.
		txnCtxs map[string]*txnContext
	}
}

type txnContext struct {
	//createAt is used to GC the abandoned txn.
	createAt time.Time
	meta     txn.TxnMeta
	req      []any
	//res      []any
}

func (h *Handle) GetTxnEngine() moengine.TxnEngine {
	return h.eng
}

func NewTAEHandle(path string, opt *options.Options) *Handle {
	if path == "" {
		path = "./store"
	}
	tae, err := openTAE(path, opt)
	if err != nil {
		panic(err)
	}

	h := &Handle{
		eng: moengine.NewEngine(tae),
	}
	h.mu.txnCtxs = make(map[string]*txnContext)
	return h
}

func (h *Handle) HandleCommit(
	ctx context.Context,
	meta txn.TxnMeta) (err error) {
	h.mu.RLock()
	txnCtx, ok := h.mu.txnCtxs[string(meta.GetID())]
	h.mu.RUnlock()
	//Handle precommit-write command for 1PC
	var txn moengine.Txn
	if ok {
		for _, e := range txnCtx.req {
			switch req := e.(type) {
			case db.CreateDatabaseReq:
				err = h.HandleCreateDatabase(
					ctx,
					meta,
					req,
					&db.CreateDatabaseResp{},
				)
			case db.CreateRelationReq:
				err = h.HandleCreateRelation(
					ctx,
					meta,
					req,
					&db.CreateRelationResp{},
				)
			case db.DropDatabaseReq:
				err = h.HandleDropDatabase(
					ctx,
					meta,
					req,
					&db.DropDatabaseResp{},
				)
			case db.DropOrTruncateRelationReq:
				err = h.HandleDropOrTruncateRelation(
					ctx,
					meta,
					req,
					&db.DropOrTruncateRelationResp{},
				)
			case db.WriteReq:
				err = h.HandleWrite(
					ctx,
					meta,
					req,
					&db.WriteResp{},
				)
			default:
				panic(moerr.NewNYI("Pls implement me"))
			}
			//Need to rollback the txn.
			if err != nil {
				txn, _ = h.eng.GetTxnByID(meta.GetID())
				txn.Rollback()
				return
			}
		}
	}
	txn, err = h.eng.GetTxnByID(meta.GetID())
	if err != nil {
		return
	}
	//if txn is 2PC ,need to set commit timestamp passed by coordinator.
	if txn.Is2PC() {
		txn.SetCommitTS(types.TimestampToTS(meta.GetCommitTS()))
	}
	err = txn.Commit()

	//delete the txn's context.
	h.mu.Lock()
	delete(h.mu.txnCtxs, string(meta.GetID()))
	h.mu.Unlock()
	return
}

func (h *Handle) HandleRollback(
	ctx context.Context,
	meta txn.TxnMeta) (err error) {
	h.mu.Lock()
	_, ok := h.mu.txnCtxs[string(meta.GetID())]
	delete(h.mu.txnCtxs, string(meta.GetID()))
	h.mu.Unlock()
	//Rollback after pre-commit write.
	if ok {
		return
	}
	var txn moengine.Txn
	txn, err = h.eng.GetTxnByID(meta.GetID())

	if err != nil {
		return err
	}
	err = txn.Rollback()
	return
}

func (h *Handle) HandleCommitting(
	ctx context.Context,
	meta txn.TxnMeta) (err error) {
	var txn moengine.Txn
	txn, err = h.eng.GetTxnByID(meta.GetID())
	if err != nil {
		return err
	}
	txn.SetCommitTS(types.TimestampToTS(meta.GetCommitTS()))
	err = txn.Committing()
	return
}

func (h *Handle) HandlePrepare(
	ctx context.Context,
	meta txn.TxnMeta) (pts timestamp.Timestamp, err error) {
	h.mu.RLock()
	txnCtx, ok := h.mu.txnCtxs[string(meta.GetID())]
	h.mu.RUnlock()
	var txn moengine.Txn
	if ok {
		//handle pre-commit write for 2PC
		for _, e := range txnCtx.req {
			switch req := e.(type) {
			case db.CreateDatabaseReq:
				err = h.HandleCreateDatabase(
					ctx,
					meta,
					req,
					&db.CreateDatabaseResp{},
				)
			case db.CreateRelationReq:
				err = h.HandleCreateRelation(
					ctx,
					meta,
					req,
					&db.CreateRelationResp{},
				)
			case db.DropDatabaseReq:
				err = h.HandleDropDatabase(
					ctx,
					meta,
					req,
					&db.DropDatabaseResp{},
				)
			case db.DropOrTruncateRelationReq:
				err = h.HandleDropOrTruncateRelation(
					ctx,
					meta,
					req,
					&db.DropOrTruncateRelationResp{},
				)
			case db.WriteReq:
				err = h.HandleWrite(
					ctx,
					meta,
					req,
					&db.WriteResp{},
				)
			default:
				panic(moerr.NewNYI("Pls implement me"))
			}
			//need to rollback the txn
			if err != nil {
				txn, _ = h.eng.GetTxnByID(meta.GetID())
				txn.Rollback()
				return
			}
		}
	}
	txn, err = h.eng.GetTxnByID(meta.GetID())
	if err != nil {
		return timestamp.Timestamp{}, err
	}
	participants := make([]uint64, 0, len(meta.GetDNShards()))
	for _, shard := range meta.GetDNShards() {
		participants = append(participants, shard.GetShardID())
	}
	txn.SetParticipants(participants)
	var ts types.TS
	ts, err = txn.Prepare()
	pts = ts.ToTimestamp()
	//delete the txn's context.
	h.mu.Lock()
	delete(h.mu.txnCtxs, string(meta.GetID()))
	h.mu.Unlock()
	return
}

func (h *Handle) HandleStartRecovery(
	ctx context.Context,
	ch chan txn.TxnMeta) {
	//panic(moerr.NewNYI("HandleStartRecovery is not implemented yet"))
	//TODO:: 1.  Get the 2PC transactions which be in prepared or committing state from txn engine's recovery.
	//       2.  Feed these transaction into ch.
	close(ch)
}

func (h *Handle) HandleClose(ctx context.Context) (err error) {
	return h.eng.Close()
}

func (h *Handle) HandleDestroy(ctx context.Context) (err error) {
	return h.eng.Destroy()
}

func (h *Handle) HandleGetLogTail(
	ctx context.Context,
	meta txn.TxnMeta,
	req apipb.SyncLogTailReq,
	resp *apipb.SyncLogTailResp) (err error) {
	tae := h.eng.GetTAE(context.Background())
	res, err := logtail.HandleSyncLogTailReq(tae.BGCheckpointRunner, tae.LogtailMgr, tae.Catalog, req)
	if err != nil {
		return err
	}
	*resp = res
	return nil
}

func (h *Handle) HandleFlushTable(
	ctx context.Context,
	meta txn.TxnMeta,
	req db.FlushTable,
	resp *apipb.SyncLogTailResp) (err error) {

	// We use current TS instead of transaction ts.
	// Here, the point of this handle function is to trigger a flush
	// via mo_ctl.  We mimic the behaviour of a real background flush
	// currTs := types.TimestampToTS(meta.GetSnapshotTS())
	currTs := types.BuildTS(time.Now().UTC().UnixNano(), 0)

	err = h.eng.FlushTable(ctx,
		req.AccessInfo.AccountID,
		req.DatabaseID,
		req.TableID,
		currTs)
	return err
}

func (h *Handle) CacheTxnRequest(
	ctx context.Context,
	meta txn.TxnMeta,
	req any,
	rsp any) (err error) {
	h.mu.Lock()
	txnCtx, ok := h.mu.txnCtxs[string(meta.GetID())]
	if !ok {
		txnCtx = &txnContext{
			createAt: time.Now(),
			meta:     meta,
		}
		h.mu.txnCtxs[string(meta.GetID())] = txnCtx
	}
	h.mu.Unlock()
	txnCtx.req = append(txnCtx.req, req)
	return nil
}

// HandlePreCommitWrite only cache the req.
func (h *Handle) HandlePreCommitWrite(
	ctx context.Context,
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
					Name:       cmd.Name,
					CreateSql:  cmd.CreateSql,
					DatabaseId: cmd.DatabaseId,
					AccessInfo: db.AccessInfo{
						UserID:    cmd.Creator,
						RoleID:    cmd.Owner,
						AccountID: cmd.AccountId,
					},
				}
				if err = h.CacheTxnRequest(ctx, meta, req,
					new(db.CreateDatabaseResp)); err != nil {
					return err
				}
			}
		case []catalog.CreateTable:
			for _, cmd := range cmds {
				req := db.CreateRelationReq{
					AccessInfo: db.AccessInfo{
						UserID:    cmd.Creator,
						RoleID:    cmd.Owner,
						AccountID: cmd.AccountId,
					},
					Name:         cmd.Name,
					RelationId:   cmd.TableId,
					DatabaseName: cmd.DatabaseName,
					DatabaseID:   cmd.DatabaseId,
					Defs:         cmd.Defs,
				}
				if err = h.CacheTxnRequest(ctx, meta, req,
					new(db.CreateRelationResp)); err != nil {
					return err
				}
			}
		case []catalog.DropDatabase:
			for _, cmd := range cmds {
				req := db.DropDatabaseReq{
					Name: cmd.Name,
					ID:   cmd.Id,
				}
				if err = h.CacheTxnRequest(ctx, meta, req,
					new(db.DropDatabaseResp)); err != nil {
					return err
				}
			}
		case []catalog.DropOrTruncateTable:
			for _, cmd := range cmds {
				req := db.DropOrTruncateRelationReq{
					IsDrop:       cmd.IsDrop,
					Name:         cmd.Name,
					ID:           cmd.Id,
					NewId:        cmd.NewId,
					DatabaseName: cmd.DatabaseName,
					DatabaseID:   cmd.DatabaseId,
				}
				if err = h.CacheTxnRequest(ctx, meta, req,
					new(db.DropOrTruncateRelationResp)); err != nil {
					return err
				}
			}
		case *apipb.Entry:
			//Handle DML
			pe := e.(*apipb.Entry)
			moBat, err := batch.ProtoBatchToBatch(pe.GetBat())
			if err != nil {
				panic(err)
			}
			req := db.WriteReq{
				Type:         db.EntryType(pe.EntryType),
				DatabaseId:   pe.GetDatabaseId(),
				TableID:      pe.GetTableId(),
				DatabaseName: pe.GetDatabaseName(),
				TableName:    pe.GetTableName(),
				Batch:        moBat,
			}

			if err = h.CacheTxnRequest(ctx, meta, req,
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
	ctx context.Context,
	meta txn.TxnMeta,
	req db.CreateDatabaseReq,
	resp *db.CreateDatabaseResp) (err error) {
	_, span := trace.Start(ctx, "HandleCreateDatabase")
	defer span.End()

	txn, err := h.eng.GetOrCreateTxnWithMeta(nil, meta.GetID(),
		types.TimestampToTS(meta.GetSnapshotTS()))
	if err != nil {
		return err
	}

	logutil.Infof("[precommit] create database: %+v\n txn: %s\n", req, txn.String())
	defer func() {
		logutil.Infof("[precommit] create database end txn: %s\n", txn.String())
	}()

	ctx = context.WithValue(ctx, defines.TenantIDKey{}, req.AccessInfo.AccountID)
	ctx = context.WithValue(ctx, defines.UserIDKey{}, req.AccessInfo.UserID)
	ctx = context.WithValue(ctx, defines.RoleIDKey{}, req.AccessInfo.RoleID)
	err = h.eng.CreateDatabaseWithID(ctx, req.Name, req.CreateSql, req.DatabaseId, txn)
	if err != nil {
		return
	}
	resp.ID = req.DatabaseId
	return
}

func (h *Handle) HandleDropDatabase(
	ctx context.Context,
	meta txn.TxnMeta,
	req db.DropDatabaseReq,
	resp *db.DropDatabaseResp) (err error) {

	txn, err := h.eng.GetOrCreateTxnWithMeta(nil, meta.GetID(),
		types.TimestampToTS(meta.GetSnapshotTS()))
	if err != nil {
		return err
	}

	logutil.Infof("[precommit] drop database: %+v\n txn: %s\n", req, txn.String())
	defer func() {
		logutil.Infof("[precommit] drop database end: %s\n", txn.String())
	}()

	if err = h.eng.DropDatabaseByID(ctx, req.ID, txn); err != nil {
		return
	}
	resp.ID = req.ID
	return
}

func (h *Handle) HandleCreateRelation(
	ctx context.Context,
	meta txn.TxnMeta,
	req db.CreateRelationReq,
	resp *db.CreateRelationResp) (err error) {

	txn, err := h.eng.GetOrCreateTxnWithMeta(nil, meta.GetID(),
		types.TimestampToTS(meta.GetSnapshotTS()))
	if err != nil {
		return
	}

	logutil.Infof("[precommit] create relation: %+v\n txn: %s\n", req, txn.String())
	defer func() {
		logutil.Infof("[precommit] create relation end txn: %s\n", txn.String())
	}()

	ctx = context.WithValue(ctx, defines.TenantIDKey{}, req.AccessInfo.AccountID)
	ctx = context.WithValue(ctx, defines.UserIDKey{}, req.AccessInfo.UserID)
	ctx = context.WithValue(ctx, defines.RoleIDKey{}, req.AccessInfo.RoleID)
	db, err := h.eng.GetDatabase(ctx, req.DatabaseName, txn)
	if err != nil {
		return
	}

	err = db.CreateRelationWithID(ctx, req.Name, req.RelationId, req.Defs)
	if err != nil {
		return
	}

	resp.ID = req.RelationId
	return
}

func (h *Handle) HandleDropOrTruncateRelation(
	ctx context.Context,
	meta txn.TxnMeta,
	req db.DropOrTruncateRelationReq,
	resp *db.DropOrTruncateRelationResp) (err error) {

	txn, err := h.eng.GetOrCreateTxnWithMeta(nil, meta.GetID(),
		types.TimestampToTS(meta.GetSnapshotTS()))
	if err != nil {
		return
	}

	logutil.Infof("[precommit] drop/truncate relation: %+v\n txn: %s\n", req, txn.String())
	defer func() {
		logutil.Infof("[precommit] drop/truncate relation end txn: %s\n", txn.String())
	}()

	db, err := h.eng.GetDatabaseByID(ctx, req.DatabaseID, txn)
	if err != nil {
		return
	}

	if req.IsDrop {
		err = db.DropRelationByID(ctx, req.ID)
		if err != nil {
			return
		}
		return
	}
	err = db.TruncateRelationByID(ctx, req.ID, req.NewId)
	return err
}

// HandleWrite Handle DML commands
func (h *Handle) HandleWrite(
	ctx context.Context,
	meta txn.TxnMeta,
	req db.WriteReq,
	resp *db.WriteResp) (err error) {

	txn, err := h.eng.GetOrCreateTxnWithMeta(nil, meta.GetID(),
		types.TimestampToTS(meta.GetSnapshotTS()))
	if err != nil {
		return err
	}

	logutil.Infof("[precommit] handle write typ: %v, %d-%s, %d-%s\n txn: %s\n",
		req.Type, req.TableID,
		req.TableName, req.DatabaseId, req.DatabaseName,
		txn.String(),
	)
	logutil.Debugf("[precommit] write batch: %s\n", debugMoBatch(req.Batch))
	defer func() {
		logutil.Infof("[precommit] handle write end txn: %s\n", txn.String())
	}()

	dbase, err := h.eng.GetDatabaseByID(ctx, req.DatabaseId, txn)
	if err != nil {
		return
	}

	tb, err := dbase.GetRelationByID(ctx, req.TableID)
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
		err = tb.Write(ctx, req.Batch)
		return
	}

	//TODO:: handle delete rows of block had been bulk-loaded into S3.

	//Vecs[0]--> rowid
	//Vecs[1]--> PrimaryKey
	err = tb.DeleteByPhyAddrKeys(ctx, req.Batch.GetVector(0))
	return

}

func vec2Str[T any](vec []T, typ types.Type, originalLen int) string {
	var w bytes.Buffer
	_, _ = w.WriteString(fmt.Sprintf("[%d]: ", originalLen))
	first := true
	for i := 0; i < len(vec); i++ {
		if !first {
			_ = w.WriteByte(',')
		}
		_, _ = w.WriteString(common.TypeStringValue(typ, vec[i]))
		first = false
	}
	return w.String()
}

func moVec2String(v *vector.Vector, printN int) string {
	switch v.Typ.Oid {
	case types.T_bool:
		return vec2Str(vector.MustTCols[bool](v)[:printN], v.Typ, v.Length())
	case types.T_int8:
		return vec2Str(vector.MustTCols[int8](v)[:printN], v.Typ, v.Length())
	case types.T_int16:
		return vec2Str(vector.MustTCols[int16](v)[:printN], v.Typ, v.Length())
	case types.T_int32:
		return vec2Str(vector.MustTCols[int32](v)[:printN], v.Typ, v.Length())
	case types.T_int64:
		return vec2Str(vector.MustTCols[int64](v)[:printN], v.Typ, v.Length())
	case types.T_uint8:
		return vec2Str(vector.MustTCols[uint8](v)[:printN], v.Typ, v.Length())
	case types.T_uint16:
		return vec2Str(vector.MustTCols[uint16](v)[:printN], v.Typ, v.Length())
	case types.T_uint32:
		return vec2Str(vector.MustTCols[uint32](v)[:printN], v.Typ, v.Length())
	case types.T_uint64:
		return vec2Str(vector.MustTCols[uint64](v)[:printN], v.Typ, v.Length())
	case types.T_float32:
		return vec2Str(vector.MustTCols[float32](v)[:printN], v.Typ, v.Length())
	case types.T_float64:
		return vec2Str(vector.MustTCols[float64](v)[:printN], v.Typ, v.Length())
	case types.T_date:
		return vec2Str(vector.MustTCols[types.Date](v)[:printN], v.Typ, v.Length())
	case types.T_datetime:
		return vec2Str(vector.MustTCols[types.Datetime](v)[:printN], v.Typ, v.Length())
	case types.T_time:
		return vec2Str(vector.MustTCols[types.Time](v)[:printN], v.Typ, v.Length())
	case types.T_timestamp:
		return vec2Str(vector.MustTCols[types.Timestamp](v)[:printN], v.Typ, v.Length())
	case types.T_decimal64:
		return vec2Str(vector.MustTCols[types.Decimal64](v)[:printN], v.Typ, v.Length())
	case types.T_decimal128:
		return vec2Str(vector.MustTCols[types.Decimal128](v)[:printN], v.Typ, v.Length())
	case types.T_uuid:
		return vec2Str(vector.MustTCols[types.Uuid](v)[:printN], v.Typ, v.Length())
	case types.T_TS:
		return vec2Str(vector.MustTCols[types.TS](v)[:printN], v.Typ, v.Length())
	case types.T_Rowid:
		return vec2Str(vector.MustTCols[types.Rowid](v)[:printN], v.Typ, v.Length())
	}
	if v.Typ.IsVarlen() {
		return vec2Str(vector.MustBytesCols(v), types.T_varchar.ToType(), v.Length())
	}
	return fmt.Sprintf("unkown type vec... %v", v.Typ)
}

func debugMoBatch(moBat *batch.Batch) string {
	if !logutil.GetSkip1Logger().Core().Enabled(zap.DebugLevel) {
		return "not debug level"
	}
	printN := moBat.Length()
	if printN > logtail.PrintN {
		printN = logtail.PrintN
	}
	buf := new(bytes.Buffer)
	for i, vec := range moBat.Vecs {
		fmt.Fprintf(buf, "[%v] = %v\n", moBat.Attrs[i], moVec2String(vec, printN))
	}
	return buf.String()
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
		tae, err = db.Open(targetDir+"/tae", opt)
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
