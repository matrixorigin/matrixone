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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/google/shlex"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	apipb "github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	catalog2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/rpchandle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/moengine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"

	"go.uber.org/zap"
)

// TODO::GC the abandoned txn.
type Handle struct {
	eng moengine.TxnEngine
	mu  struct {
		sync.RWMutex
		//map txn id to txnContext.
		txnCtxs map[string]*txnContext
	}
	jobScheduler tasks.JobScheduler
}

var _ rpchandle.Handler = (*Handle)(nil)

type txnContext struct {
	//createAt is used to GC the abandoned txn.
	createAt time.Time
	meta     txn.TxnMeta
	reqs     []any
	//the table to create by this txn.
	toCreate map[uint64]*catalog2.Schema
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
		eng:          moengine.NewEngine(tae),
		jobScheduler: tasks.NewParallelJobScheduler(100),
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
	logutil.Infof("HandleCommit start : %X\n",
		string(meta.GetID()))
	defer func() {
		logutil.Infof("HandleCommit end : %X\n",
			string(meta.GetID()))
	}()
	//Handle precommit-write command for 1PC
	var txn moengine.Txn
	if ok {
		for _, e := range txnCtx.reqs {
			switch req := e.(type) {
			case *db.CreateDatabaseReq:
				err = h.HandleCreateDatabase(
					ctx,
					meta,
					req,
					&db.CreateDatabaseResp{},
				)
			case *db.CreateRelationReq:
				err = h.HandleCreateRelation(
					ctx,
					meta,
					req,
					&db.CreateRelationResp{},
				)
			case *db.DropDatabaseReq:
				err = h.HandleDropDatabase(
					ctx,
					meta,
					req,
					&db.DropDatabaseResp{},
				)
			case *db.DropOrTruncateRelationReq:
				err = h.HandleDropOrTruncateRelation(
					ctx,
					meta,
					req,
					&db.DropOrTruncateRelationResp{},
				)
			case *db.UpdateConstraintReq:
				err = h.HandleUpdateConstraint(
					ctx,
					meta,
					req,
					&db.UpdateConstraintResp{},
				)
			case *db.WriteReq:
				err = h.HandleWrite(
					ctx,
					meta,
					req,
					&db.WriteResp{},
				)
			default:
				panic(moerr.NewNYI(ctx, "Pls implement me"))
			}
			//Need to roll back the txn.
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
		for _, e := range txnCtx.reqs {
			switch req := e.(type) {
			case *db.CreateDatabaseReq:
				err = h.HandleCreateDatabase(
					ctx,
					meta,
					req,
					&db.CreateDatabaseResp{},
				)
			case *db.CreateRelationReq:
				err = h.HandleCreateRelation(
					ctx,
					meta,
					req,
					&db.CreateRelationResp{},
				)
			case *db.DropDatabaseReq:
				err = h.HandleDropDatabase(
					ctx,
					meta,
					req,
					&db.DropDatabaseResp{},
				)
			case *db.DropOrTruncateRelationReq:
				err = h.HandleDropOrTruncateRelation(
					ctx,
					meta,
					req,
					&db.DropOrTruncateRelationResp{},
				)
			case *db.UpdateConstraintReq:
				err = h.HandleUpdateConstraint(
					ctx,
					meta,
					req,
					&db.UpdateConstraintResp{},
				)
			case *db.WriteReq:
				err = h.HandleWrite(
					ctx,
					meta,
					req,
					&db.WriteResp{},
				)
			default:
				panic(moerr.NewNYI(ctx, "Pls implement me"))
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
	//TODO:: 1.  Get the 2PC transactions which be in prepared or
	//           committing state from txn engine's recovery.
	//       2.  Feed these transaction into ch.
	close(ch)
}

func (h *Handle) HandleClose(ctx context.Context) (err error) {
	//FIXME::should wait txn request's job done?
	h.jobScheduler.Stop()
	return h.eng.Close()
}

func (h *Handle) HandleDestroy(ctx context.Context) (err error) {
	//FIXME::should wait txn request's job done?
	h.jobScheduler.Stop()
	return h.eng.Destroy()
}

func (h *Handle) HandleGetLogTail(
	ctx context.Context,
	meta txn.TxnMeta,
	req apipb.SyncLogTailReq,
	resp *apipb.SyncLogTailResp) (err error) {
	tae := h.eng.GetTAE(context.Background())
	res, err := logtail.HandleSyncLogTailReq(
		ctx,
		tae.BGCheckpointRunner,
		tae.LogtailMgr,
		tae.Catalog,
		req,
		true)
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

func (h *Handle) HandleForceCheckpoint(
	ctx context.Context,
	meta txn.TxnMeta,
	req db.Checkpoint,
	resp *apipb.SyncLogTailResp) (err error) {

	timeout := req.FlushDuration

	currTs := types.BuildTS(time.Now().UTC().UnixNano(), 0)

	err = h.eng.ForceCheckpoint(ctx,
		currTs, timeout)
	return err
}

func (h *Handle) HandleInspectDN(
	ctx context.Context,
	meta txn.TxnMeta,
	req db.InspectDN,
	resp *db.InspectResp) (err error) {
	tae := h.eng.GetTAE(context.Background())
	args, _ := shlex.Split(req.Operation)
	logutil.Info("Inspect", zap.Strings("args", args))
	b := &bytes.Buffer{}

	inspectCtx := &inspectContext{
		db:     tae,
		acinfo: &req.AccessInfo,
		args:   args,
		out:    b,
		resp:   resp,
	}
	RunInspect(inspectCtx)
	resp.Message = b.String()
	return nil
}

func (h *Handle) startLoadJobs(
	ctx context.Context,
	meta txn.TxnMeta,
	req *db.WriteReq,
) (err error) {
	var locations []string
	var isNull []bool
	var jobIds []string
	var columnIdx int
	if req.Type == db.EntryInsert {
		//for loading ZM index of primary key
		locations = append(locations, req.MetaLocs...)
		columnIdx = req.Schema.GetSingleSortKeyIdx()
		isNull = append(isNull, false)
		req.Jobs = make([]*tasks.Job, len(req.MetaLocs))
		req.JobRes = make([]*tasks.JobResult, len(req.Jobs))
		for i := range req.MetaLocs {
			jobIds = append(jobIds,
				fmt.Sprintf("load-zm-%s", req.MetaLocs[i]))
		}
	} else {
		//for loading deleted rowid.
		locations = append(locations, req.DeltaLocs...)
		columnIdx = 0
		isNull = append(isNull, false)
		req.Jobs = make([]*tasks.Job, len(req.DeltaLocs))
		req.JobRes = make([]*tasks.JobResult, len(req.Jobs))
		for i := range req.DeltaLocs {
			jobIds = append(jobIds,
				fmt.Sprintf("load-deleted-rowid-%s", req.DeltaLocs[i]))
		}
	}
	//start loading jobs asynchronously,should create a new root context.
	nctx := context.Background()
	if deadline, ok := ctx.Deadline(); ok {
		nctx, req.Cancel = context.WithTimeout(nctx, time.Until(deadline))
	}
	for i, v := range locations {
		nctx = context.WithValue(nctx, db.LocationKey{}, v)
		req.Jobs[i] = tasks.NewJob(
			jobIds[i],
			nctx,
			func(ctx context.Context) (jobR *tasks.JobResult) {
				jobR = &tasks.JobResult{}
				loc, ok := ctx.Value(db.LocationKey{}).(string)
				if !ok {
					panic(moerr.NewInternalErrorNoCtx("Miss Location"))
				}
				//reader, err := blockio.NewReader(ctx,
				//	h.eng.GetTAE(ctx).Fs, req.MetaLocs[i])
				_, id, _, _, err := blockio.DecodeLocation(loc)
				if err != nil {
					jobR.Err = err
					return
				}
				reader, err := blockio.NewObjectReader(
					h.eng.GetTAE(ctx).Fs.Service, loc)
				if err != nil {
					jobR.Err = err
					return
				}
				if req.Type == db.EntryDelete {
					bat, err := reader.LoadColumns(
						ctx,
						[]uint16{uint16(columnIdx)},
						[]uint32{id},
						nil,
					)
					if err != nil {
						jobR.Err = err
						return
					}
					jobR.Res = containers.NewVectorWithSharedMemory(bat[0].Vecs[0], isNull[0])
					return
				}
				zmIndex, err := reader.LoadZoneMaps(
					ctx,
					[]uint16{uint16(columnIdx)},
					[]uint32{id},
					nil,
				)
				if err != nil {
					jobR.Err = err
					return
				}
				jobR.Res = zmIndex[0][columnIdx]
				return
			},
		)
		if err = h.jobScheduler.Schedule(req.Jobs[i]); err != nil {
			return err
		}
	}
	return
}

// EvaluateTxnRequest only evaluate the request ,do not change the state machine of TxnEngine.
func (h *Handle) EvaluateTxnRequest(
	ctx context.Context,
	meta txn.TxnMeta,
) (err error) {
	h.mu.RLock()
	txnCtx := h.mu.txnCtxs[string(meta.GetID())]
	h.mu.RUnlock()
	for _, e := range txnCtx.reqs {
		if r, ok := e.(*db.WriteReq); ok {
			if r.FileName != "" {
				if r.Type == db.EntryDelete {
					//start to load deleted row ids
					err = h.startLoadJobs(ctx, meta, r)
					if err != nil {
						return
					}
				}
			}
		}
	}
	return
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
			toCreate: make(map[uint64]*catalog2.Schema),
		}
		h.mu.txnCtxs[string(meta.GetID())] = txnCtx
	}
	h.mu.Unlock()
	txnCtx.reqs = append(txnCtx.reqs, req)
	if r, ok := req.(*db.CreateRelationReq); ok {
		// Does this place need
		schema, err := moengine.DefsToSchema(r.Name, r.Defs)
		if err != nil {
			return err
		}
		txnCtx.toCreate[r.RelationId] = schema
	}
	return nil
}

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
				req := &db.CreateDatabaseReq{
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
				req := &db.CreateRelationReq{
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
		case []catalog.UpdateConstraint:
			for _, cmd := range cmds {
				req := &db.UpdateConstraintReq{
					TableName:    cmd.TableName,
					TableId:      cmd.TableId,
					DatabaseName: cmd.DatabaseName,
					DatabaseId:   cmd.DatabaseId,
					Constraint:   cmd.Constraint,
				}
				if err = h.CacheTxnRequest(ctx, meta, req,
					new(db.UpdateConstraintResp)); err != nil {
					return err
				}
			}
		case []catalog.DropDatabase:
			for _, cmd := range cmds {
				req := &db.DropDatabaseReq{
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
				req := &db.DropOrTruncateRelationReq{
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
			req := &db.WriteReq{
				Type:         db.EntryType(pe.EntryType),
				DatabaseId:   pe.GetDatabaseId(),
				TableID:      pe.GetTableId(),
				DatabaseName: pe.GetDatabaseName(),
				TableName:    pe.GetTableName(),
				FileName:     pe.GetFileName(),
				Batch:        moBat,
			}
			if req.FileName != "" {
				rows := catalog.GenRows(req.Batch)
				for _, row := range rows {
					if req.Type == db.EntryInsert {
						//req.Blks[i] = row[catalog.BLOCKMETA_ID_ON_FS_IDX].(uint64)
						//req.MetaLocs[i] = string(row[catalog.BLOCKMETA_METALOC_ON_FS_IDX].([]byte))
						req.MetaLocs = append(req.MetaLocs,
							string(row[0].([]byte)))
					} else {
						//req.DeltaLocs[i] = string(row[0].([]byte))
						req.DeltaLocs = append(req.DeltaLocs,
							string(row[0].([]byte)))
					}
				}
			}
			if err = h.CacheTxnRequest(ctx, meta, req,
				new(db.WriteResp)); err != nil {
				return err
			}
		default:
			panic(moerr.NewNYI(ctx, ""))
		}
	}
	//evaluate all the txn requests.
	return h.EvaluateTxnRequest(ctx, meta)
}

//Handle DDL commands.

func (h *Handle) HandleCreateDatabase(
	ctx context.Context,
	meta txn.TxnMeta,
	req *db.CreateDatabaseReq,
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
	req *db.DropDatabaseReq,
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
	req *db.CreateRelationReq,
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
	req *db.DropOrTruncateRelationReq,
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
	req *db.WriteReq,
	resp *db.WriteResp) (err error) {
	defer func() {
		if req.Cancel != nil {
			req.Cancel()
		}
	}()
	txn, err := h.eng.GetOrCreateTxnWithMeta(nil, meta.GetID(),
		types.TimestampToTS(meta.GetSnapshotTS()))
	if err != nil {
		return
	}
	txn.SetPKDedupSkip(txnif.PKDedupSkipWorkSpace)

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
		//Add blocks which had been bulk-loaded into S3 into table.
		if req.FileName != "" {
			err = tb.AddBlksWithMetaLoc(
				ctx,
				nil,
				req.MetaLocs)
			return
		}
		//Appends a batch of data into table.
		err = tb.Write(ctx, req.Batch)
		return
	}
	//handle delete
	if req.FileName != "" {
		//wait for loading deleted row-id done.
		for i, job := range req.Jobs {
			req.JobRes[i] = job.WaitDone()
			if req.JobRes[i].Err != nil {
				return req.JobRes[i].Err
			}
			rowidVec := req.JobRes[i].Res.(containers.Vector)
			err = tb.DeleteByPhyAddrKeys(ctx, containers.UnmarshalToMoVec(rowidVec))
			if err != nil {
				rowidVec.Close()
				return
			}
			rowidVec.Close()
		}
		return
	}
	err = tb.DeleteByPhyAddrKeys(ctx, req.Batch.GetVector(0))
	return
}

func (h *Handle) HandleUpdateConstraint(
	ctx context.Context,
	meta txn.TxnMeta,
	req *db.UpdateConstraintReq,
	resp *db.UpdateConstraintResp) (err error) {
	txn, err := h.eng.GetOrCreateTxnWithMeta(nil, meta.GetID(),
		types.TimestampToTS(meta.GetSnapshotTS()))
	if err != nil {
		return err
	}

	cstr := req.Constraint
	req.Constraint = nil
	logutil.Infof("[precommit] update cstr: %+v cstr %d bytes\n txn: %s\n", req, len(cstr), txn.String())

	dbase, err := h.eng.GetDatabaseByID(ctx, req.DatabaseId, txn)
	if err != nil {
		return
	}

	tbl, err := dbase.GetRelationByID(ctx, req.TableId)
	if err != nil {
		return
	}

	tbl.UpdateConstraintWithBin(ctx, cstr)

	return nil
}

func vec2Str[T any](vec []T, v *vector.Vector) string {
	var w bytes.Buffer
	_, _ = w.WriteString(fmt.Sprintf("[%d]: ", v.Length()))
	first := true
	for i := 0; i < len(vec); i++ {
		if !first {
			_ = w.WriteByte(',')
		}
		if v.GetNulls().Contains(uint64(i)) {
			_, _ = w.WriteString(common.TypeStringValue(*v.GetType(), types.Null{}))
		} else {
			_, _ = w.WriteString(common.TypeStringValue(*v.GetType(), vec[i]))
		}
		first = false
	}
	return w.String()
}

func moVec2String(v *vector.Vector, printN int) string {
	switch v.GetType().Oid {
	case types.T_bool:
		return vec2Str(vector.MustFixedCol[bool](v)[:printN], v)
	case types.T_int8:
		return vec2Str(vector.MustFixedCol[int8](v)[:printN], v)
	case types.T_int16:
		return vec2Str(vector.MustFixedCol[int16](v)[:printN], v)
	case types.T_int32:
		return vec2Str(vector.MustFixedCol[int32](v)[:printN], v)
	case types.T_int64:
		return vec2Str(vector.MustFixedCol[int64](v)[:printN], v)
	case types.T_uint8:
		return vec2Str(vector.MustFixedCol[uint8](v)[:printN], v)
	case types.T_uint16:
		return vec2Str(vector.MustFixedCol[uint16](v)[:printN], v)
	case types.T_uint32:
		return vec2Str(vector.MustFixedCol[uint32](v)[:printN], v)
	case types.T_uint64:
		return vec2Str(vector.MustFixedCol[uint64](v)[:printN], v)
	case types.T_float32:
		return vec2Str(vector.MustFixedCol[float32](v)[:printN], v)
	case types.T_float64:
		return vec2Str(vector.MustFixedCol[float64](v)[:printN], v)
	case types.T_date:
		return vec2Str(vector.MustFixedCol[types.Date](v)[:printN], v)
	case types.T_datetime:
		return vec2Str(vector.MustFixedCol[types.Datetime](v)[:printN], v)
	case types.T_time:
		return vec2Str(vector.MustFixedCol[types.Time](v)[:printN], v)
	case types.T_timestamp:
		return vec2Str(vector.MustFixedCol[types.Timestamp](v)[:printN], v)
	case types.T_decimal64:
		return vec2Str(vector.MustFixedCol[types.Decimal64](v)[:printN], v)
	case types.T_decimal128:
		return vec2Str(vector.MustFixedCol[types.Decimal128](v)[:printN], v)
	case types.T_uuid:
		return vec2Str(vector.MustFixedCol[types.Uuid](v)[:printN], v)
	case types.T_TS:
		return vec2Str(vector.MustFixedCol[types.TS](v)[:printN], v)
	case types.T_Rowid:
		return vec2Str(vector.MustFixedCol[types.Rowid](v)[:printN], v)
	}
	if v.GetType().IsVarlen() {
		return vec2Str(vector.MustBytesCol(v)[:printN], v)
	}
	return fmt.Sprintf("unkown type vec... %v", *v.GetType())
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
