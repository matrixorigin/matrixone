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
	"strings"
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
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	catalog2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/gc"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/rpchandle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"go.uber.org/zap"
)

const (
	MAX_ALLOWED_TXN_LATENCY = time.Millisecond * 300
	MAX_TXN_COMMIT_LATENCY  = time.Minute * 2
)

type Handle struct {
	db *db.DB
	mu struct {
		sync.RWMutex
		//map txn id to txnContext.
		txnCtxs map[string]*txnContext
	}

	GCManager *gc.Manager
}

var _ rpchandle.Handler = (*Handle)(nil)

type txnContext struct {
	//createAt is used to GC the abandoned txn.
	createAt time.Time
	deadline time.Time
	meta     txn.TxnMeta
	reqs     []any
	//the table to create by this txn.
	toCreate map[uint64]*catalog2.Schema
}

func (h *Handle) GetDB() *db.DB {
	return h.db
}

func NewTAEHandle(ctx context.Context, path string, opt *options.Options) *Handle {
	if path == "" {
		path = "./store"
	}
	tae, err := openTAE(ctx, path, opt)
	if err != nil {
		panic(err)
	}

	h := &Handle{
		db: tae,
	}
	h.mu.txnCtxs = make(map[string]*txnContext)

	// clean h.mu.txnCtxs by interval
	h.GCManager = gc.NewManager(
		gc.WithCronJob(
			"clean-txn-cache",
			MAX_TXN_COMMIT_LATENCY,
			func(ctx context.Context) error {
				return h.GCCache(time.Now())
			},
		),
	)
	h.GCManager.Start()

	return h
}

// TODO: vast items within h.mu.txnCtxs would incur performance penality.
func (h *Handle) GCCache(now time.Time) error {
	logutil.Infof("GC rpc handle txn cache")
	h.mu.Lock()
	defer h.mu.Unlock()
	for id, txn := range h.mu.txnCtxs {
		if txn.deadline.Before(now) {
			delete(h.mu.txnCtxs, id)
		}
	}
	return nil
}

func (h *Handle) HandleCommit(
	ctx context.Context,
	meta txn.TxnMeta) (cts timestamp.Timestamp, err error) {
	start := time.Now()
	h.mu.RLock()
	txnCtx, ok := h.mu.txnCtxs[string(meta.GetID())]
	h.mu.RUnlock()
	common.DoIfDebugEnabled(func() {
		logutil.Debugf("HandleCommit start : %X",
			string(meta.GetID()))
	})
	s := ""
	defer func() {
		if ok {
			//delete the txn's context.
			h.mu.Lock()
			delete(h.mu.txnCtxs, string(meta.GetID()))
			h.mu.Unlock()
		}
		common.DoIfInfoEnabled(func() {
			if time.Since(start) > MAX_ALLOWED_TXN_LATENCY {
				logutil.Info("Commit with long latency", zap.Duration("duration", time.Since(start)), zap.String("debug", meta.DebugString()))
			}
		})
		if s != "" {
			logutil.Info("trace span", zap.String("lantency", s))
		}
	}()
	var txn txnif.AsyncTxn
	if ok {
		//Handle precommit-write command for 1PC
		txn, err = h.db.GetOrCreateTxnWithMeta(nil, meta.GetID(),
			types.TimestampToTS(meta.GetSnapshotTS()))
		if err != nil {
			return
		}
		err = h.handleRequests(ctx, txn, txnCtx)
		if err != nil {
			return
		}
	}
	activeDuration := time.Since(start)
	_, enable, threshold := trace.IsMOCtledSpan(trace.SpanKindTNRPCHandle)
	if enable && activeDuration > threshold && txn.GetContext() != nil {
		txn.GetStore().SetContext(context.WithValue(txn.GetContext(), common.ActiveHandleCommit, &common.DurationRecords{Duration: activeDuration}))
	}
	txn, err = h.db.GetTxnByID(meta.GetID())
	if err != nil {
		return
	}
	//if txn is 2PC ,need to set commit timestamp passed by coordinator.
	if txn.Is2PC() {
		txn.SetCommitTS(types.TimestampToTS(meta.GetCommitTS()))
	}

	v2.TxnBeforeCommitDurationHistogram.Observe(time.Since(start).Seconds())

	err = txn.Commit(ctx)
	cts = txn.GetCommitTS().ToTimestamp()

	if enable {
		vDuration := txn.GetContext().Value(common.ActiveHandleCommit)
		if vDuration != nil {
			duration := vDuration.(*common.DurationRecords)
			s = fmt.Sprintf("%s[active: %v]", s, duration.Duration)
		}
		vHandleRequestDuration := txn.GetContext().Value(common.ActiveHandleRequests)
		if vHandleRequestDuration != nil {
			handleRequestDuration := vHandleRequestDuration.(*handleRequestsTraceValue)
			s = fmt.Sprintf("%s[handle request: %v, createDB %d, createTbl %d, dropDB %d, dropTbl %d, alterTable %d, write %d]", s,
				handleRequestDuration.Duration,
				handleRequestDuration.CreateDB,
				handleRequestDuration.CreateRelation,
				handleRequestDuration.DropDB,
				handleRequestDuration.DropOrTruncateRelation,
				handleRequestDuration.AlterTable,
				handleRequestDuration.Write)
		}
		vDuration = txn.GetContext().Value(common.DequeuePreparing)
		if vDuration != nil {
			duration := vDuration.(*common.DurationRecords)
			s = fmt.Sprintf("%s[dequeue preparing: %v]", s, duration.Duration)
		}
		vDuration = txn.GetContext().Value(common.StorePrePrepare)
		if vDuration != nil {
			duration := vDuration.(*common.DurationRecords)
			s = fmt.Sprintf("%s[pre prepare: %v]", s, duration.Duration)
		}
		vDuration = txn.GetContext().Value(common.StorePrepareCommit)
		if vDuration != nil {
			duration := vDuration.(*common.DurationRecords)
			s = fmt.Sprintf("%s[prepare commit: %v]", s, duration.Duration)
		}
		vDuration = txn.GetContext().Value(common.StorePreApplyCommit)
		if vDuration != nil {
			duration := vDuration.(*common.DurationRecords)
			s = fmt.Sprintf("%s[pre apply commit: %v]", s, duration.Duration)
		}
		vDuration = txn.GetContext().Value(common.PrepareWAL)
		if vDuration != nil {
			duration := vDuration.(*common.DurationRecords)
			s = fmt.Sprintf("%s[prepare wal: %v]", s, duration.Duration)
		}
		vDuration = txn.GetContext().Value(common.PrepareLogtail)
		if vDuration != nil {
			duration := vDuration.(*common.DurationRecords)
			s = fmt.Sprintf("%s[prepare logtail: %v]", s, duration.Duration)
		}
		vDuration = txn.GetContext().Value(common.DequeuePrepared)
		if vDuration != nil {
			duration := vDuration.(*common.DurationRecords)
			s = fmt.Sprintf("%s[dequeue prepared: %v]", s, duration.Duration)
		}
		vDuration = txn.GetContext().Value(common.StoreApplyCommit)
		if vDuration != nil {
			duration := vDuration.(*common.DurationRecords)
			s = fmt.Sprintf("%s[apply commit: %v]", s, duration.Duration)
		}
	}

	if moerr.IsMoErrCode(err, moerr.ErrTAENeedRetry) {
		for {
			txn, err = h.db.StartTxnWithStartTSAndSnapshotTS(nil,
				types.TimestampToTS(meta.GetSnapshotTS()))
			if err != nil {
				return
			}
			logutil.Infof("retry txn %X with new txn %X", string(meta.GetID()), txn.GetID())
			//Handle precommit-write command for 1PC
			h.handleRequests(ctx, txn, txnCtx)
			//if txn is 2PC ,need to set commit timestamp passed by coordinator.
			if txn.Is2PC() {
				txn.SetCommitTS(types.TimestampToTS(meta.GetCommitTS()))
			}
			err = txn.Commit(ctx)
			cts = txn.GetCommitTS().ToTimestamp()
			if !moerr.IsMoErrCode(err, moerr.ErrTAENeedRetry) {
				break
			}
		}
	}
	return
}

type handleRequestsTraceValue struct {
	Duration               time.Duration
	CreateDB               int
	CreateRelation         int
	DropDB                 int
	DropOrTruncateRelation int
	AlterTable             int
	Write                  int
}

func (h *Handle) handleRequests(
	ctx context.Context,
	txn txnif.AsyncTxn,
	txnCtx *txnContext,
) (err error) {
	t0 := time.Now()
	var createDB, createRelation, dropDB, dropRelation, alterTable, write int
	defer func() {
		handleRequestDuration := time.Since(t0)
		_, enable, threshold := trace.IsMOCtledSpan(trace.SpanKindTNRPCHandle)
		if enable && handleRequestDuration > threshold && txn.GetContext() != nil {
			txn.GetStore().SetContext(context.WithValue(
				txn.GetContext(),
				common.ActiveHandleRequests,
				&handleRequestsTraceValue{
					Duration:               handleRequestDuration,
					CreateDB:               createDB,
					CreateRelation:         createRelation,
					DropDB:                 dropDB,
					DropOrTruncateRelation: dropRelation,
					AlterTable:             alterTable,
					Write:                  write,
				}))
		}
	}()
	for _, e := range txnCtx.reqs {
		switch req := e.(type) {
		case *db.CreateDatabaseReq:
			err = h.HandleCreateDatabase(
				ctx,
				txn,
				req,
				&db.CreateDatabaseResp{},
			)
			createDB++
		case *db.CreateRelationReq:
			err = h.HandleCreateRelation(
				ctx,
				txn,
				req,
				&db.CreateRelationResp{},
			)
			createRelation++
		case *db.DropDatabaseReq:
			err = h.HandleDropDatabase(
				ctx,
				txn,
				req,
				&db.DropDatabaseResp{},
			)
			dropDB++
		case *db.DropOrTruncateRelationReq:
			err = h.HandleDropOrTruncateRelation(
				ctx,
				txn,
				req,
				&db.DropOrTruncateRelationResp{},
			)
			dropRelation++
		case *api.AlterTableReq:
			err = h.HandleAlterTable(
				ctx,
				txn,
				req,
				&db.WriteResp{},
			)
			alterTable++
		case *db.WriteReq:
			err = h.HandleWrite(
				ctx,
				txn,
				req,
				&db.WriteResp{},
			)
			if moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry) && (strings.HasPrefix(req.TableName, "bmsql") || strings.HasPrefix(req.TableName, "sbtest")) {
				for _, rreq := range txnCtx.reqs {
					if crreq, ok := rreq.(*db.WriteReq); ok {
						logutil.Infof("[precommit] dup handle write typ: %v, %d-%s, %s txn: %s",
							crreq.Type, crreq.TableID,
							crreq.TableName, common.MoBatchToString(crreq.Batch, 3),
							txn.String(),
						)
					}
				}
			}
			write++
		default:
			err = moerr.NewNotSupported(ctx, "unknown txn request type: %T", req)
		}
		//Need to roll back the txn.
		if err != nil {
			txn.Rollback(ctx)
			return
		}
	}
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
	txn, err := h.db.GetTxnByID(meta.GetID())

	if err != nil {
		return err
	}
	err = txn.Rollback(ctx)
	return
}

func (h *Handle) HandleCommitting(
	ctx context.Context,
	meta txn.TxnMeta) (err error) {
	txn, err := h.db.GetTxnByID(meta.GetID())
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
	var txn txnif.AsyncTxn
	defer func() {
		if ok {
			//delete the txn's context.
			h.mu.Lock()
			delete(h.mu.txnCtxs, string(meta.GetID()))
			h.mu.Unlock()
		}
	}()
	if ok {
		//handle pre-commit write for 2PC
		txn, err = h.db.GetOrCreateTxnWithMeta(nil, meta.GetID(),
			types.TimestampToTS(meta.GetSnapshotTS()))
		if err != nil {
			return
		}
		h.handleRequests(ctx, txn, txnCtx)
	}
	txn, err = h.db.GetTxnByID(meta.GetID())
	if err != nil {
		return timestamp.Timestamp{}, err
	}
	participants := make([]uint64, 0, len(meta.GetTNShards()))
	for _, shard := range meta.GetTNShards() {
		participants = append(participants, shard.GetShardID())
	}
	txn.SetParticipants(participants)
	var ts types.TS
	ts, err = txn.Prepare(ctx)
	pts = ts.ToTimestamp()
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
	if h.GCManager != nil {
		h.GCManager.Stop()
	}
	return h.db.Close()
}

func (h *Handle) HandleDestroy(ctx context.Context) (err error) {
	//FIXME::should wait txn request's job done?
	return
}

func (h *Handle) HandleGetLogTail(
	ctx context.Context,
	meta txn.TxnMeta,
	req *api.SyncLogTailReq,
	resp *api.SyncLogTailResp) (closeCB func(), err error) {
	res, closeCB, err := logtail.HandleSyncLogTailReq(
		ctx,
		h.db.BGCheckpointRunner,
		h.db.LogtailMgr,
		h.db.Catalog,
		*req,
		true)
	if err != nil {
		return
	}
	*resp = res
	return
}

func (h *Handle) HandleFlushTable(
	ctx context.Context,
	meta txn.TxnMeta,
	req *db.FlushTable,
	resp *api.SyncLogTailResp) (cb func(), err error) {

	// We use current TS instead of transaction ts.
	// Here, the point of this handle function is to trigger a flush
	// via mo_ctl.  We mimic the behaviour of a real background flush
	// currTs := types.TimestampToTS(meta.GetSnapshotTS())
	currTs := types.BuildTS(time.Now().UTC().UnixNano(), 0)

	err = h.db.FlushTable(
		ctx,
		req.AccessInfo.AccountID,
		req.DatabaseID,
		req.TableID,
		currTs)
	return nil, err
}

func (h *Handle) HandleForceCheckpoint(
	ctx context.Context,
	meta txn.TxnMeta,
	req *db.Checkpoint,
	resp *api.SyncLogTailResp) (cb func(), err error) {

	timeout := req.FlushDuration

	currTs := types.BuildTS(time.Now().UTC().UnixNano(), 0)

	err = h.db.ForceCheckpoint(ctx, currTs, timeout)
	return nil, err
}

func (h *Handle) HandleBackup(
	ctx context.Context,
	meta txn.TxnMeta,
	req *db.Checkpoint,
	resp *api.SyncLogTailResp) (cb func(), err error) {

	timeout := req.FlushDuration

	backupTime := time.Now().UTC()
	currTs := types.BuildTS(backupTime.UnixNano(), 0)
	var locations string
	locations += backupTime.Format(time.DateTime) + ";"
	location, err := h.db.ForceCheckpointForBackup(ctx, currTs, timeout)
	if err != nil {
		return nil, err
	}
	data := h.db.BGCheckpointRunner.GetAllCheckpoints()
	locations += location + ";"
	for i := range data {
		locations += data[i].GetLocation().String()
		locations += ":"
		locations += fmt.Sprintf("%d", data[i].GetVersion())
		locations += ";"
	}
	resp.CkpLocation = locations
	return nil, err
}

func (h *Handle) HandleInspectTN(
	ctx context.Context,
	meta txn.TxnMeta,
	req *db.InspectTN,
	resp *db.InspectResp) (cb func(), err error) {
	args, _ := shlex.Split(req.Operation)
	common.DoIfDebugEnabled(func() {
		logutil.Debug("Inspect", zap.Strings("args", args))
	})
	b := &bytes.Buffer{}

	inspectCtx := &inspectContext{
		db:     h.db,
		acinfo: &req.AccessInfo,
		args:   args,
		out:    b,
		resp:   resp,
	}
	RunInspect(ctx, inspectCtx)
	resp.Message = b.String()
	return nil, nil
}

func (h *Handle) prefetchDeleteRowID(ctx context.Context, req *db.WriteReq) error {
	if len(req.DeltaLocs) == 0 {
		return nil
	}
	//for loading deleted rowid.
	columnIdx := 0
	pkIdx := 1
	//start loading jobs asynchronously,should create a new root context.
	loc, err := blockio.EncodeLocationFromString(req.DeltaLocs[0])
	if err != nil {
		return err
	}
	pref, err := blockio.BuildPrefetchParams(h.db.Runtime.Fs.Service, loc)
	if err != nil {
		return err
	}
	for _, key := range req.DeltaLocs {
		var location objectio.Location
		location, err = blockio.EncodeLocationFromString(key)
		if err != nil {
			return err
		}
		pref.AddBlockWithType([]uint16{uint16(columnIdx), uint16(pkIdx)}, []uint16{location.ID()}, uint16(objectio.SchemaTombstone))
	}
	return blockio.PrefetchWithMerged(pref)
}

func (h *Handle) prefetchMetadata(ctx context.Context,
	req *db.WriteReq) (int, error) {
	if len(req.MetaLocs) == 0 {
		return 0, nil
	}
	//start loading jobs asynchronously,should create a new root context.
	objCnt := 0
	var objectName objectio.ObjectNameShort
	for _, meta := range req.MetaLocs {
		loc, err := blockio.EncodeLocationFromString(meta)
		if err != nil {
			return 0, err
		}
		if !objectio.IsSameObjectLocVsShort(loc, &objectName) {
			err := blockio.PrefetchMeta(h.db.Runtime.Fs.Service, loc)
			if err != nil {
				return 0, err
			}
			objCnt++
			objectName = *loc.Name().Short()
		}
	}
	return objCnt, nil
}

// EvaluateTxnRequest only evaluate the request ,do not change the state machine of TxnEngine.
func (h *Handle) EvaluateTxnRequest(
	ctx context.Context,
	meta txn.TxnMeta,
) error {
	h.mu.RLock()
	txnCtx := h.mu.txnCtxs[string(meta.GetID())]
	h.mu.RUnlock()

	metaLocCnt := 0
	deltaLocCnt := 0

	defer func() {
		if metaLocCnt != 0 {
			v2.TxnCNCommittedMetaLocationQuantityGauge.Set(float64(metaLocCnt))
		}

		if deltaLocCnt != 0 {
			v2.TxnCNCommittedDeltaLocationQuantityGauge.Set(float64(deltaLocCnt))
		}
	}()

	for _, e := range txnCtx.reqs {
		if r, ok := e.(*db.WriteReq); ok {
			if r.FileName != "" {
				if r.Type == db.EntryDelete {
					// start to load deleted row ids
					deltaLocCnt += len(r.DeltaLocs)
					if err := h.prefetchDeleteRowID(ctx, r); err != nil {
						return err
					}
				} else if r.Type == db.EntryInsert {
					objCnt, err := h.prefetchMetadata(ctx, r)
					if err != nil {
						return err
					}
					metaLocCnt += objCnt
				}
			}
		}
	}
	return nil
}

func (h *Handle) CacheTxnRequest(
	ctx context.Context,
	meta txn.TxnMeta,
	req any,
	rsp any) (err error) {
	h.mu.Lock()
	txnCtx, ok := h.mu.txnCtxs[string(meta.GetID())]
	if !ok {
		now := time.Now()
		txnCtx = &txnContext{
			createAt: now,
			deadline: now.Add(MAX_TXN_COMMIT_LATENCY),
			meta:     meta,
			toCreate: make(map[uint64]*catalog2.Schema),
		}
		h.mu.txnCtxs[string(meta.GetID())] = txnCtx
	}
	h.mu.Unlock()
	txnCtx.reqs = append(txnCtx.reqs, req)
	if r, ok := req.(*db.CreateRelationReq); ok {
		// Does this place need
		schema, err := DefsToSchema(r.Name, r.Defs)
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
	req *api.PrecommitWriteCmd,
	resp *api.SyncLogTailResp) (err error) {
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
					DatTyp: cmd.DatTyp,
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
				// TODO: debug for #11917
				if strings.Contains(req.Name, "sbtest") {
					logutil.Infof("create table: %s.%s\n", req.DatabaseName, req.Name)
				}
				if err = h.CacheTxnRequest(ctx, meta, req,
					new(db.CreateRelationResp)); err != nil {
					return err
				}
			}
		case []catalog.UpdateConstraint:
			for _, cmd := range cmds {
				req := api.NewUpdateConstraintReq(
					cmd.DatabaseId,
					cmd.TableId,
					string(cmd.Constraint))
				if err = h.CacheTxnRequest(ctx, meta, req, nil); err != nil {
					return err
				}
			}
		case []*api.AlterTableReq:
			for _, cmd := range cmds {
				if err = h.CacheTxnRequest(ctx, meta, cmd, nil); err != nil {
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
		case *api.Entry:
			//Handle DML
			pe := e.(*api.Entry)
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
				PkCheck:      db.PKCheckType(pe.GetPkCheckByTn()),
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
			return moerr.NewNYI(ctx, "pre commit write type: %T", cmds)
		}
	}
	//evaluate all the txn requests.
	return h.EvaluateTxnRequest(ctx, meta)
}

//Handle DDL commands.

func (h *Handle) HandleCreateDatabase(
	ctx context.Context,
	txn txnif.AsyncTxn,
	req *db.CreateDatabaseReq,
	resp *db.CreateDatabaseResp) (err error) {
	_, span := trace.Start(ctx, "HandleCreateDatabase")
	defer span.End()

	common.DoIfInfoEnabled(func() {
		logutil.Infof("[precommit] create database: %+v txn: %s", req, txn.String())
	})
	defer func() {
		common.DoIfDebugEnabled(func() {
			logutil.Debugf("[precommit] create database end txn: %s", txn.String())
		})
	}()

	ctx = context.WithValue(ctx, defines.TenantIDKey{}, req.AccessInfo.AccountID)
	ctx = context.WithValue(ctx, defines.UserIDKey{}, req.AccessInfo.UserID)
	ctx = context.WithValue(ctx, defines.RoleIDKey{}, req.AccessInfo.RoleID)
	ctx = context.WithValue(ctx, defines.DatTypKey{}, req.DatTyp)
	if _, err = txn.CreateDatabaseWithCtx(
		ctx,
		req.Name,
		req.CreateSql,
		req.DatTyp,
		req.DatabaseId); err != nil {
		return
	}
	resp.ID = req.DatabaseId
	return
}

func (h *Handle) HandleDropDatabase(
	ctx context.Context,
	txn txnif.AsyncTxn,
	req *db.DropDatabaseReq,
	resp *db.DropDatabaseResp) (err error) {

	common.DoIfInfoEnabled(func() {
		logutil.Infof("[precommit] drop database: %+v txn: %s", req, txn.String())
	})
	defer func() {
		common.DoIfDebugEnabled(func() {
			logutil.Debugf("[precommit] drop database end: %s", txn.String())
		})
	}()

	if _, err = txn.DropDatabaseByID(req.ID); err != nil {
		return
	}
	resp.ID = req.ID
	return
}

func (h *Handle) HandleCreateRelation(
	ctx context.Context,
	txn txnif.AsyncTxn,
	req *db.CreateRelationReq,
	resp *db.CreateRelationResp) (err error) {

	common.DoIfInfoEnabled(func() {
		logutil.Infof("[precommit] create relation: %+v txn: %s", req, txn.String())
	})
	defer func() {
		// do not turn it on in prod. This print outputs multiple duplicate lines
		common.DoIfDebugEnabled(func() {
			logutil.Debugf("[precommit] create relation end txn: %s", txn.String())
		})
	}()

	ctx = context.WithValue(ctx, defines.TenantIDKey{}, req.AccessInfo.AccountID)
	ctx = context.WithValue(ctx, defines.UserIDKey{}, req.AccessInfo.UserID)
	ctx = context.WithValue(ctx, defines.RoleIDKey{}, req.AccessInfo.RoleID)
	dbH, err := txn.GetDatabaseWithCtx(ctx, req.DatabaseName)
	if err != nil {
		return
	}

	if err = CreateRelation(ctx, dbH, req.Name, req.RelationId, req.Defs); err != nil {
		return
	}

	resp.ID = req.RelationId
	return
}

func (h *Handle) HandleDropOrTruncateRelation(
	ctx context.Context,
	txn txnif.AsyncTxn,
	req *db.DropOrTruncateRelationReq,
	resp *db.DropOrTruncateRelationResp) (err error) {

	common.DoIfInfoEnabled(func() {
		logutil.Infof("[precommit] drop/truncate relation: %+v txn: %s", req, txn.String())
	})
	defer func() {
		common.DoIfDebugEnabled(func() {
			logutil.Debugf("[precommit] drop/truncate relation end txn: %s", txn.String())
		})
	}()

	db, err := txn.GetDatabaseByID(req.DatabaseID)
	if err != nil {
		return
	}

	if req.IsDrop {
		_, err = db.DropRelationByID(req.ID)
		return
	}
	_, err = db.TruncateByID(req.ID, req.NewId)
	return err
}

// HandleWrite Handle DML commands
func (h *Handle) HandleWrite(
	ctx context.Context,
	txn txnif.AsyncTxn,
	req *db.WriteReq,
	resp *db.WriteResp) (err error) {
	defer func() {
		if req.Cancel != nil {
			req.Cancel()
		}
	}()
	ctx = perfcounter.WithCounterSetFrom(ctx, h.db.Opts.Ctx)
	switch req.PkCheck {
	case db.FullDedup:
		txn.SetDedupType(txnif.FullDedup)
	case db.IncrementalDedup:
		if h.db.Opts.IncrementalDedup {
			txn.SetDedupType(txnif.IncrementalDedup)
		} else {
			txn.SetDedupType(txnif.FullSkipWorkSpaceDedup)
		}
	case db.FullSkipWorkspaceDedup:
		txn.SetDedupType(txnif.FullSkipWorkSpaceDedup)
	}
	common.DoIfDebugEnabled(func() {
		logutil.Debugf("[precommit] handle write typ: %v, %d-%s, %d-%s txn: %s",
			req.Type, req.TableID,
			req.TableName, req.DatabaseId, req.DatabaseName,
			txn.String(),
		)
		logutil.Debugf("[precommit] write batch: %s", common.DebugMoBatch(req.Batch))
	})
	var dbase handle.Database
	var tb handle.Relation
	defer func() {
		common.DoIfDebugEnabled(func() {
			logutil.Debugf("[precommit] handle write end txn: %s", txn.String())
		})
		if err != nil && moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry) && (strings.HasPrefix(req.TableName, "bmsql") || strings.HasPrefix(req.TableName, "sbtest")) {
			logutil.Infof("[precommit] dup handle catalog on dup %s ", tb.GetMeta().(*catalog2.TableEntry).PPString(common.PPL1, 0, ""))
		}
	}()

	dbase, err = txn.GetDatabaseByID(req.DatabaseId)
	if err != nil {
		return
	}

	tb, err = dbase.GetRelationByID(req.TableID)
	if err != nil {
		return
	}

	if req.Type == db.EntryInsert {
		//Add blocks which had been bulk-loaded into S3 into table.
		if req.FileName != "" {
			locations := make([]objectio.Location, 0)
			for _, metLoc := range req.MetaLocs {
				location, err := blockio.EncodeLocationFromString(metLoc)
				if err != nil {
					return err
				}
				locations = append(locations, location)
			}
			err = tb.AddBlksWithMetaLoc(ctx, locations)
			return
		}
		//check the input batch passed by cn is valid.
		len := 0
		for i, vec := range req.Batch.Vecs {
			if vec == nil {
				logutil.Errorf("the vec:%d in req.Batch is nil", i)
				panic("invalid vector : vector is nil")
			}
			if vec.Length() == 0 {
				logutil.Errorf("the vec:%d in req.Batch is empty", i)
				panic("invalid vector: vector is empty")
			}
			if i == 0 {
				len = vec.Length()
			}
			if vec.Length() != len {
				logutil.Errorf("the length of vec:%d in req.Batch is not equal to the first vec", i)
				panic("invalid batch : the length of vectors in batch is not the same")
			}
		}
		//Appends a batch of data into table.
		err = AppendDataToTable(ctx, tb, req.Batch)
		return
	}

	//handle delete
	if req.FileName != "" {
		//wait for loading deleted row-id done.
		nctx := context.Background()
		if deadline, ok := ctx.Deadline(); ok {
			_, req.Cancel = context.WithTimeout(nctx, time.Until(deadline))
		}
		rowidIdx := 0
		pkIdx := 1
		for _, key := range req.DeltaLocs {
			var location objectio.Location
			location, err = blockio.EncodeLocationFromString(key)
			if err != nil {
				return err
			}
			var ok bool
			var bat *batch.Batch
			bat, err = blockio.LoadTombstoneColumns(
				ctx,
				[]uint16{uint16(rowidIdx), uint16(pkIdx)},
				nil,
				h.db.Runtime.Fs.Service,
				location,
				nil,
			)
			if err != nil {
				return
			}
			blkids := getBlkIDsFromRowids(bat.Vecs[0])
			id := tb.GetMeta().(*catalog2.TableEntry).AsCommonID()
			if len(blkids) == 1 {
				for blkID := range blkids {
					id.BlockID = blkID
				}
				ok, err = tb.TryDeleteByDeltaloc(id, location)
				if err != nil {
					return
				}
				if ok {
					continue
				}
				logutil.Warnf("blk %v try delete by deltaloc failed", id.BlockID.String())
			} else {
				logutil.Warnf("multiply blocks in one deltalocation")
			}
			rowIDVec := containers.ToTNVector(bat.Vecs[0], common.WorkspaceAllocator)
			defer rowIDVec.Close()
			pkVec := containers.ToTNVector(bat.Vecs[1], common.WorkspaceAllocator)
			//defer pkVec.Close()
			if err = tb.DeleteByPhyAddrKeys(rowIDVec, pkVec); err != nil {
				return
			}
		}
		return
	}
	if len(req.Batch.Vecs) != 2 {
		panic(fmt.Sprintf("req.Batch.Vecs length is %d, should be 2", len(req.Batch.Vecs)))
	}
	rowIDVec := containers.ToTNVector(req.Batch.GetVector(0), common.WorkspaceAllocator)
	defer rowIDVec.Close()
	pkVec := containers.ToTNVector(req.Batch.GetVector(1), common.WorkspaceAllocator)
	//defer pkVec.Close()
	err = tb.DeleteByPhyAddrKeys(rowIDVec, pkVec)
	return
}

func getBlkIDsFromRowids(vec *vector.Vector) map[types.Blockid]struct{} {
	rowids := vector.MustFixedCol[types.Rowid](vec)
	blkids := make(map[types.Blockid]struct{})
	for _, rowid := range rowids {
		blkID := *rowid.BorrowBlockID()
		blkids[blkID] = struct{}{}
	}
	return blkids
}

func (h *Handle) HandleAlterTable(
	ctx context.Context,
	txn txnif.AsyncTxn,
	req *api.AlterTableReq,
	resp *db.WriteResp) (err error) {
	common.DoIfInfoEnabled(func() {
		logutil.Debugf("[precommit] alter table: %v txn: %s", req.String(), txn.String())
	})

	dbase, err := txn.GetDatabaseByID(req.DbId)
	if err != nil {
		return
	}

	tbl, err := dbase.GetRelationByID(req.TableId)
	if err != nil {
		return
	}

	return tbl.AlterTable(ctx, req)
}

func (h *Handle) HandleAddFaultPoint(
	ctx context.Context,
	meta txn.TxnMeta,
	req *db.FaultPoint,
	resp *api.SyncLogTailResp) (func(), error) {
	if req.Name == db.EnableFaultInjection {
		fault.Enable()
		return nil, nil
	} else if req.Name == db.DisableFaultInjection {
		fault.Disable()
		return nil, nil
	}
	return nil, h.db.AddFaultPoint(ctx, req.Name, req.Freq, req.Action, req.Iarg, req.Sarg)
}

func (h *Handle) HandleTraceSpan(ctx context.Context,
	meta txn.TxnMeta,
	req *db.TraceSpan,
	resp *api.SyncLogTailResp) (func(), error) {

	return nil, nil
}

//var visitBlkEntryForStorageUsage = func(h *Handle, resp *db.StorageUsageResp, lastCkpEndTS types.TS) {
//	processor := new(catalog2.LoopProcessor)
//	processor.SegmentFn = func(blkEntry *catalog2.SegmentEntry) error {
//
//		return nil
//	}
//
//	h.db.Catalog.RecurLoop(processor)
//}

func (h *Handle) HandleStorageUsage(ctx context.Context, meta txn.TxnMeta,
	req *db.StorageUsage, resp *db.StorageUsageResp) (func(), error) {

	// get all checkpoints.
	//var ckp *checkpoint.CheckpointEntry
	// [g_ckp, i_ckp, i_ckp, ...] (if g exist)
	allCkp := h.db.BGCheckpointRunner.GetAllCheckpoints()
	for idx := range allCkp {
		resp.CkpEntries = append(resp.CkpEntries, &db.CkpMetaInfo{
			Version:  allCkp[idx].GetVersion(),
			Location: allCkp[idx].GetLocation(),
		})
	}

	resp.Succeed = true

	// TODO
	// exist a gap!
	// collecting block entries that have been not been checkpoint yet
	//if lastCkpTS.Less(types.BuildTS(time.Now().UTC().UnixNano(), 0)) {
	//	visitBlkEntryForStorageUsage(h, resp, lastCkpTS)
	//}

	return nil, nil
}

func openTAE(ctx context.Context, targetDir string, opt *options.Options) (tae *db.DB, err error) {

	if targetDir != "" {
		mask := syscall.Umask(0)
		if err := os.MkdirAll(targetDir, os.FileMode(0755)); err != nil {
			syscall.Umask(mask)
			logutil.Infof("Recreate dir error:%v", err)
			return nil, err
		}
		syscall.Umask(mask)
		tae, err = db.Open(ctx, targetDir+"/tae", opt)
		if err != nil {
			logutil.Warnf("Open tae failed. error:%v", err)
			return nil, err
		}
		return tae, nil
	}

	tae, err = db.Open(ctx, targetDir, opt)
	if err != nil {
		logutil.Warnf("Open tae failed. error:%v", err)
		return nil, err
	}
	return
}
