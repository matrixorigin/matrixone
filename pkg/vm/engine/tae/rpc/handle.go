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
	"fmt"
	"os"
	"reflect"
	"regexp"
	"runtime"
	"sync/atomic"
	"syscall"
	"time"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/cmd_util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
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
	db        *db.DB
	txnCtxs   *common.Map[string, *txnContext]
	GCManager *gc.Manager

	interceptMatchRegexp atomic.Pointer[regexp.Regexp]
}

var _ rpchandle.Handler = (*Handle)(nil)

type txnContext struct {
	//createAt is used to GC the abandoned txn.
	createAt time.Time
	deadline time.Time
	meta     txn.TxnMeta
	reqs     []any
}

//#region Open

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
	h.txnCtxs = common.NewMap[string, *txnContext](runtime.GOMAXPROCS(0))
	h.interceptMatchRegexp.Store(regexp.MustCompile(`.*bmsql_stock.*`))
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

//#endregion Open

//#region Handle Operations

func (h *Handle) GetDB() *db.DB {
	return h.db
}

func (h *Handle) IsInterceptTable(name string) bool {
	printMatchRegexp := h.getInterceptMatchRegexp()
	if printMatchRegexp == nil {
		return false
	}
	return printMatchRegexp.MatchString(name)
}

func (h *Handle) getInterceptMatchRegexp() *regexp.Regexp {
	return h.interceptMatchRegexp.Load()
}

func (h *Handle) UpdateInterceptMatchRegexp(name string) {
	if name == "" {
		h.interceptMatchRegexp.Store(nil)
		return
	}
	h.interceptMatchRegexp.Store(regexp.MustCompile(fmt.Sprintf(`.*%s.*`, name)))
}

// TODO: vast items within h.mu.txnCtxs would incur performance penality.
func (h *Handle) GCCache(now time.Time) error {

	var (
		cnt, deleteCnt int
	)

	h.txnCtxs.DeleteIf(func(k string, v *txnContext) bool {
		cnt++
		ok := v.deadline.Before(now)
		if ok {
			deleteCnt++
		}
		return ok
	})
	logutil.Info(
		"GC-RPC-Cache",
		zap.Int("total", cnt),
		zap.Int("deleted", deleteCnt),
	)
	return nil
}

func (h *Handle) CacheTxnRequest(
	ctx context.Context,
	meta txn.TxnMeta,
	req any) (err error) {
	txnCtx, ok := h.txnCtxs.Load(util.UnsafeBytesToString(meta.GetID()))
	if !ok {
		now := time.Now()
		txnCtx = &txnContext{
			createAt: now,
			deadline: now.Add(MAX_TXN_COMMIT_LATENCY),
			meta:     meta,
		}
		h.txnCtxs.Store(util.UnsafeBytesToString(meta.GetID()), txnCtx)
	}
	v := reflect.ValueOf(req)
	if v.Kind() == reflect.Slice {
		for i := 0; i < v.Len(); i++ {
			txnCtx.reqs = append(txnCtx.reqs, v.Index(i).Interface())
		}
	} else {
		txnCtx.reqs = append(txnCtx.reqs, req)
	}
	return nil
}

func (h *Handle) tryLockMergeForBulkDelete(reqs []any, txn txnif.AsyncTxn) (releaseF []func(), err error) {
	delM := make(map[uint64]*struct {
		dbID uint64
		rows uint64
	})
	for _, e := range reqs {
		if req, ok := e.(*cmd_util.WriteReq); ok && req.Type == cmd_util.EntryDelete {
			if req.FileName == "" && req.Batch == nil {
				continue
			}

			delM[req.TableID] = &struct {
				dbID uint64
				rows uint64
			}{dbID: req.DatabaseId, rows: 0}

			if req.FileName != "" {
				for _, stats := range req.TombstoneStats {
					delM[req.TableID].rows += uint64(stats.Rows())
				}
			}
			if req.Batch != nil {
				delM[req.TableID].rows += uint64(req.Batch.RowCount())
			}
		}
	}

	for id, info := range delM {
		if info.rows <= h.db.Opts.BulkTomestoneTxnThreshold {
			continue
		}

		dbHandle, err := txn.GetDatabaseByID(info.dbID)
		if err != nil {
			return nil, err
		}

		relation, err := dbHandle.GetRelationByID(id)
		if err != nil {
			return nil, err
		}

		err = h.db.MergeScheduler.StopMerge(relation.GetMeta().(*catalog.TableEntry), true)
		if err != nil {
			return nil, err
		}
		logutil.Info("LockMerge Bulk Delete",
			zap.Uint64("tid", id), zap.Uint64("rows", info.rows), zap.String("txn", txn.String()))
		release := func() {
			err = h.db.MergeScheduler.StartMerge(id, true)
			if err != nil {
				return
			}
			logutil.Info("LockMerge Bulk Delete Committed",
				zap.Uint64("tid", id), zap.Uint64("rows", info.rows), zap.String("txn", txn.String()))
		}
		releaseF = append(releaseF, release)
	}

	return
}

func (h *Handle) handleRequests(
	ctx context.Context,
	txn txnif.AsyncTxn,
	txnCtx *txnContext,
) (releaseF []func(), hasDDL bool, err error) {
	var (
		inMemoryInsertRows        int
		persistedMemoryInsertRows int
		inMemoryTombstoneRows     int
		persistedTombstoneRows    int
	)
	releaseF, err = h.tryLockMergeForBulkDelete(txnCtx.reqs, txn)
	if err != nil {
		logutil.Warn("failed to lock merging", zap.Error(err))
	}
	for _, e := range txnCtx.reqs {
		switch req := e.(type) {
		case *pkgcatalog.CreateDatabaseReq:
			hasDDL = true
			err = h.HandleCreateDatabase(ctx, txn, req)
		case *pkgcatalog.DropDatabaseReq:
			hasDDL = true
			err = h.HandleDropDatabase(ctx, txn, req)
		case *pkgcatalog.CreateTableReq:
			hasDDL = true
			err = h.HandleCreateRelation(ctx, txn, req)
		case *pkgcatalog.DropTableReq:
			hasDDL = true
			err = h.HandleDropRelation(ctx, txn, req)
		case *api.AlterTableReq:
			hasDDL = true
			err = h.HandleAlterTable(ctx, txn, req)
		case *cmd_util.WriteReq:
			var r1, r2, r3, r4 int
			r1, r2, r3, r4, err = h.HandleWrite(ctx, txn, req)
			if err == nil {
				inMemoryInsertRows += r1
				persistedMemoryInsertRows += r2
				inMemoryTombstoneRows += r3
				persistedTombstoneRows += r4
			}
		default:
			err = moerr.NewNotSupportedf(ctx, "unknown txn request type: %T", req)
		}
		//Need to roll back the txn.
		if err != nil {
			txn.Rollback(ctx)
			return
		}
	}
	if inMemoryInsertRows+inMemoryTombstoneRows+persistedTombstoneRows+persistedMemoryInsertRows > 100000 {
		logutil.Info(
			"BIG-COMMIT-TRACE-LOG",
			zap.Int("in-memory-rows", inMemoryInsertRows),
			zap.Int("persisted-rows", persistedMemoryInsertRows),
			zap.Int("in-memory-tombstones", inMemoryTombstoneRows),
			zap.Int("persisted-tombstones", persistedTombstoneRows),
			zap.String("txn", txn.String()),
		)
	}
	return
}

//#endregion

//#region Impl TxnStorage interface
//order by call frequency

// HandlePreCommitWrite impls TxnStorage:Write
func (h *Handle) HandlePreCommitWrite(
	ctx context.Context,
	meta txn.TxnMeta,
	req *api.PrecommitWriteCmd,
	_ *api.TNStringResponse /*no response*/) (err error) {
	var e any
	es := req.EntryList
	for len(es) > 0 {
		e, es, err = pkgcatalog.ParseEntryList(es)
		if err != nil {
			return err
		}
		switch cmds := e.(type) {
		case *pkgcatalog.CreateDatabaseReq, *pkgcatalog.CreateTableReq,
			*pkgcatalog.DropDatabaseReq, *pkgcatalog.DropTableReq,
			[]*api.AlterTableReq:
			if err = h.CacheTxnRequest(ctx, meta, cmds); err != nil {
				return err
			}
		case *api.Entry:
			//Handle DML
			pe := e.(*api.Entry)
			moBat, err := batch.ProtoBatchToBatch(pe.GetBat())
			if err != nil {
				panic(err)
			}
			req := &cmd_util.WriteReq{
				Type:         cmd_util.EntryType(pe.EntryType),
				DatabaseId:   pe.GetDatabaseId(),
				TableID:      pe.GetTableId(),
				DatabaseName: pe.GetDatabaseName(),
				TableName:    pe.GetTableName(),
				FileName:     pe.GetFileName(),
				Batch:        moBat,
				PkCheck:      cmd_util.PKCheckType(pe.GetPkCheckByTn()),
			}

			if req.FileName != "" {
				col := req.Batch.Vecs[0]
				for i := 0; i < req.Batch.RowCount(); i++ {
					stats := objectio.ObjectStats(col.GetBytesAt(i))
					if req.Type == cmd_util.EntryInsert {
						req.DataObjectStats = append(req.DataObjectStats, stats)
					} else {
						req.TombstoneStats = append(req.TombstoneStats, stats)
					}
				}
			}
			if err = h.CacheTxnRequest(ctx, meta, req); err != nil {
				return err
			}
		default:
			return moerr.NewNYIf(ctx, "pre commit write type: %T", cmds)
		}
	}
	//evaluate all the txn requests.
	return h.TryPrefetchTxn(ctx, &meta)
}

// HandlePreCommitWrite impls TxnStorage:Commit
func (h *Handle) HandleCommit(
	ctx context.Context,
	meta txn.TxnMeta,
) (cts timestamp.Timestamp, err error) {
	start := time.Now()
	txnCtx, ok := h.txnCtxs.Load(util.UnsafeBytesToString(meta.GetID()))
	var (
		txn      txnif.AsyncTxn
		releaseF []func()
		hasDDL   bool = false
	)
	defer func() {
		for _, f := range releaseF {
			f()
		}
		if ok {
			//delete the txn's context.
			h.txnCtxs.Delete(util.UnsafeBytesToString(meta.GetID()))
		}
		common.DoIfInfoEnabled(func() {
			_, _, injected := fault.TriggerFault(objectio.FJ_CommitSlowLog)
			if time.Since(start) > MAX_ALLOWED_TXN_LATENCY || err != nil || hasDDL || injected {
				var tnTxnInfo string
				if txn != nil {
					tnTxnInfo = txn.String()
				}
				logger := logutil.Warn
				msg := "HandleCommit-SLOW-LOG"
				if err != nil {
					logger = logutil.Error
					msg = "HandleCommit-Error"
				} else if hasDDL {
					logger = logutil.Info
					msg = "HandleCommit-With-DDL"
				}
				logger(
					msg,
					zap.Error(err),
					zap.Duration("commit-latency", time.Since(start)),
					zap.String("cn-txn", meta.DebugString()),
					zap.String("tn-txn", tnTxnInfo),
				)
			}
		})
	}()
	if ok {
		//Handle precommit-write command for 1PC
		txn, err = h.db.GetOrCreateTxnWithMeta(nil, meta.GetID(),
			types.TimestampToTS(meta.GetSnapshotTS()))
		if err != nil {
			return
		}
		releaseF, hasDDL, err = h.handleRequests(ctx, txn, txnCtx)
		if err != nil {
			return
		}
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
	if cts.PhysicalTime == txnif.UncommitTS.Physical() {
		panic("bad committs causing hung")
	}

	if moerr.IsMoErrCode(err, moerr.ErrTAENeedRetry) {
		for {
			for _, f := range releaseF {
				f()
			}
			txn, err = h.db.StartTxnWithStartTSAndSnapshotTS(nil,
				types.TimestampToTS(meta.GetSnapshotTS()))
			if err != nil {
				return
			}
			logutil.Info(
				"TAE-RETRY-TXN",
				zap.String("old-txn", string(meta.GetID())),
				zap.String("new-txn", txn.GetID()),
			)
			//Handle precommit-write command for 1PC
			releaseF, hasDDL, err = h.handleRequests(ctx, txn, txnCtx)
			if err != nil && !moerr.IsMoErrCode(err, moerr.ErrTAENeedRetry) {
				break
			}
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

// HandleGetLogTail impls TxnStorage:Read
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

func (h *Handle) HandleRollback(
	ctx context.Context,
	meta txn.TxnMeta) (err error) {
	_, ok := h.txnCtxs.LoadAndDelete(util.UnsafeBytesToString(meta.GetID()))

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

//#endregion

//#region WriteDB: Handle DDL & DML

func (h *Handle) HandleCreateDatabase(
	ctx context.Context,
	txn txnif.AsyncTxn,
	req *pkgcatalog.CreateDatabaseReq) (err error) {
	_, span := trace.Start(ctx, "HandleCreateDatabase")
	defer span.End()

	defer func() {
		common.DoIfDebugEnabled(func() {
			logutil.Debugf("[precommit] create database end txn: %s", txn.String())
		})
	}()

	// modify memory structure
	for i, c := range req.Cmds {
		common.DoIfInfoEnabled(func() {
			logutil.Info(
				"PreCommit-CreateDB",
				zap.String("txn", txn.String()),
				zap.String("i/cnt", fmt.Sprintf("%d/%d", i+1, len(req.Cmds))),
				zap.String("cmd", fmt.Sprintf("%+v", c)),
			)
		})
		ctx = defines.AttachAccount(ctx, c.AccountId, c.Creator, c.Owner)
		ctx = context.WithValue(ctx, defines.DatTypKey{}, c.DatTyp)
		if _, err = txn.CreateDatabaseWithCtx(
			ctx,
			c.Name,
			c.CreateSql,
			c.DatTyp,
			c.DatabaseId); err != nil {
			return
		}
	}

	// Write to mo_database table
	catalog, _ := txn.GetDatabaseByID(pkgcatalog.MO_CATALOG_ID)
	databaseTbl, _ := catalog.GetRelationByID(pkgcatalog.MO_DATABASE_ID)
	err = AppendDataToTable(ctx, databaseTbl, req.Bat)

	return
}

func (h *Handle) HandleDropDatabase(
	ctx context.Context,
	txn txnif.AsyncTxn,
	req *pkgcatalog.DropDatabaseReq) (err error) {
	defer func() {
		common.DoIfDebugEnabled(func() {
			logutil.Debugf("[precommit] drop database end: %s", txn.String())
		})
	}()
	for i, c := range req.Cmds {
		common.DoIfInfoEnabled(func() {
			logutil.Info(
				"PreCommit-DropDB",
				zap.String("txn", txn.String()),
				zap.String("i/cnt", fmt.Sprintf("%d/%d", i+1, len(req.Cmds))),
				zap.String("cmd", fmt.Sprintf("%+v", c)),
			)
		})
		if _, err = txn.DropDatabaseByID(c.Id); err != nil {
			return
		}
	}

	// Delete in mo_database table
	catalog, _ := txn.GetDatabaseByID(pkgcatalog.MO_CATALOG_ID)
	databaseTbl, _ := catalog.GetRelationByID(pkgcatalog.MO_DATABASE_ID)
	rowIDVec := containers.ToTNVector(req.Bat.GetVector(0), common.WorkspaceAllocator)
	defer rowIDVec.Close()
	pkVec := containers.ToTNVector(req.Bat.GetVector(1), common.WorkspaceAllocator)
	defer pkVec.Close()
	err = databaseTbl.DeleteByPhyAddrKeys(rowIDVec, pkVec, handle.DT_Normal)

	return
}

func (h *Handle) HandleCreateRelation(
	ctx context.Context,
	txn txnif.AsyncTxn,
	req *pkgcatalog.CreateTableReq) error {
	defer func() {
		common.DoIfDebugEnabled(func() {
			logutil.Debugf("[precommit] create relation end txn: %s", txn.String())
		})
	}()

	for i, c := range req.Cmds {
		common.DoIfInfoEnabled(func() {
			logutil.Info(
				"PreCommit-CreateTBL",
				zap.String("txn", txn.String()),
				zap.String("i/cnt", fmt.Sprintf("%d/%d", i+1, len(req.Cmds))),
				zap.String("cmd", fmt.Sprintf("%+v", c)),
			)
		})
		ctx = defines.AttachAccount(ctx, c.AccountId, c.Creator, c.Owner)
		dbH, err := txn.GetDatabaseWithCtx(ctx, c.DatabaseName)
		if err != nil {
			return err
		}

		if err = CreateRelation(ctx, dbH, c.Name, c.TableId, c.Defs); err != nil {
			return err
		}
	}

	catalog, _ := txn.GetDatabaseByID(pkgcatalog.MO_CATALOG_ID)
	tablesTbl, _ := catalog.GetRelationByID(pkgcatalog.MO_TABLES_ID)
	if err := AppendDataToTable(ctx, tablesTbl, req.TableBat); err != nil {
		return err
	}
	columnsTbl, _ := catalog.GetRelationByID(pkgcatalog.MO_COLUMNS_ID)
	for _, bat := range req.ColumnBat {
		if err := AppendDataToTable(ctx, columnsTbl, bat); err != nil {
			return err
		}
	}

	return nil
}

func (h *Handle) HandleDropRelation(
	ctx context.Context,
	txn txnif.AsyncTxn,
	req *pkgcatalog.DropTableReq) error {
	defer func() {
		common.DoIfDebugEnabled(func() {
			logutil.Debugf("[precommit] drop/truncate relation end txn: %s", txn.String())
		})
	}()

	for i, c := range req.Cmds {
		common.DoIfInfoEnabled(func() {
			logutil.Info(
				"PreCommit-DropTBL",
				zap.String("txn", txn.String()),
				zap.String("i/cnt", fmt.Sprintf("%d/%d", i+1, len(req.Cmds))),
				zap.String("cmd", fmt.Sprintf("%+v", c)),
			)
		})
		db, err := txn.GetDatabaseByID(c.DatabaseId)
		if err != nil {
			return err
		}

		if !c.IsDrop {
			panic("truncate table should be splitted into drop and create")
		}
		if _, err = db.DropRelationByID(c.Id); err != nil {
			return err
		}
	}

	catalog, _ := txn.GetDatabaseByID(pkgcatalog.MO_CATALOG_ID)
	tablesTbl, _ := catalog.GetRelationByID(pkgcatalog.MO_TABLES_ID)
	rowIDVec := containers.ToTNVector(req.TableBat.GetVector(0), common.WorkspaceAllocator)
	defer rowIDVec.Close()
	pkVec := containers.ToTNVector(req.TableBat.GetVector(1), common.WorkspaceAllocator)
	defer pkVec.Close()
	if err := tablesTbl.DeleteByPhyAddrKeys(rowIDVec, pkVec, handle.DT_Normal); err != nil {
		return err
	}
	columnsTbl, _ := catalog.GetRelationByID(pkgcatalog.MO_COLUMNS_ID)
	for _, bat := range req.ColumnBat {
		rowIDVec := containers.ToTNVector(bat.GetVector(0), common.WorkspaceAllocator)
		defer rowIDVec.Close()
		pkVec := containers.ToTNVector(bat.GetVector(1), common.WorkspaceAllocator)
		defer pkVec.Close()
		if err := columnsTbl.DeleteByPhyAddrKeys(rowIDVec, pkVec, handle.DT_Normal); err != nil {
			return err
		}
	}

	return nil
}

// HandleWrite Handle DML commands
func (h *Handle) HandleWrite(
	ctx context.Context,
	txn txnif.AsyncTxn,
	req *cmd_util.WriteReq,
) (
	inMemoryInsertRows int,
	persistedMemoryInsertRows int,
	inMemoryTombstoneRows int,
	persistedTombstoneRows int,
	err error,
) {
	defer func() {
		if req.Cancel != nil {
			req.Cancel()
		}
	}()
	ctx = perfcounter.WithCounterSetFrom(ctx, h.db.Opts.Ctx)
	switch req.PkCheck {
	case cmd_util.FullDedup:
		txn.SetDedupType(txnif.DedupPolicy_CheckAll)
	case cmd_util.IncrementalDedup:
		if h.db.Opts.IncrementalDedup {
			txn.SetDedupType(txnif.DedupPolicy_CheckIncremental)
		} else {
			txn.SetDedupType(txnif.DedupPolicy_SkipWorkspace)
		}
	case cmd_util.FullSkipWorkspaceDedup:
		txn.SetDedupType(txnif.DedupPolicy_SkipWorkspace)
	}
	common.DoIfDebugEnabled(func() {
		logutil.Debugf("[precommit] handle write typ: %v, %d-%s, %d-%s txn: %s",
			req.Type, req.TableID,
			req.TableName, req.DatabaseId, req.DatabaseName,
			txn.String(),
		)
		logutil.Debugf("[precommit] write batch: %s", common.DebugMoBatch(req.Batch))
	})
	defer func() {
		common.DoIfDebugEnabled(func() {
			logutil.Debugf("[precommit] handle write end txn: %s", txn.String())
		})
	}()

	dbase, err := txn.GetDatabaseByID(req.DatabaseId)
	if err != nil {
		return
	}

	tb, err := dbase.GetRelationByID(req.TableID)
	if err != nil {
		return
	}

	if req.Type == cmd_util.EntryInsert {
		//Add blocks which had been bulk-loaded into S3 into table.
		if req.FileName != "" {
			statsVec := req.Batch.Vecs[0]
			for i := 0; i < statsVec.Length(); i++ {
				s := objectio.ObjectStats(statsVec.GetBytesAt(i))
				if !s.GetCNCreated() {
					logutil.Fatalf("the `CNCreated` mask not set: %s", s.String())
				}
				persistedMemoryInsertRows += int(s.Rows())
			}
			err = tb.AddDataFiles(
				ctx,
				containers.ToTNVector(statsVec, common.WorkspaceAllocator),
			)
			return
		}
		//check the input batch passed by cn is valid.
		for i, vec := range req.Batch.Vecs {
			if vec == nil {
				logutil.Fatal(
					"INVALID-INSERT-BATCH-NIL-VEC",
					zap.Int("idx", i),
					zap.Uint64("table-id", req.TableID),
					zap.String("table-name", req.TableName),
					zap.String("txn", txn.String()),
				)
			}
			if i == 0 {
				inMemoryInsertRows = vec.Length()
			}
			if vec.Length() != inMemoryInsertRows {
				logutil.Fatal(
					"INVALID-INSERT-BATCH-DIFF-LENGTH",
					zap.Int("idx", i),
					zap.Uint64("table-id", req.TableID),
					zap.String("table-name", req.TableName),
					zap.String("rows", fmt.Sprintf("%d/%d", vec.Length(), inMemoryInsertRows)),
					zap.String("txn", txn.String()),
				)
			}
		}

		// TODO: debug for #13342, remove me later
		if h.IsInterceptTable(tb.Schema(false).(*catalog.Schema).Name) {
			schema := tb.Schema(false).(*catalog.Schema)
			if schema.HasPK() {
				pkDef := schema.GetSingleSortKey()
				idx := pkDef.Idx
				isCompositeKey := pkDef.IsCompositeColumn()
				for i := 0; i < req.Batch.Vecs[0].Length(); i++ {
					if isCompositeKey {
						pkbuf := req.Batch.Vecs[idx].GetBytesAt(i)
						tuple, _ := types.Unpack(pkbuf)
						logutil.Info(
							"op1",
							zap.String("txn", txn.String()),
							zap.String("pk", common.TypeStringValue(*req.Batch.Vecs[idx].GetType(), pkbuf, false)),
							zap.Any("detail", tuple.SQLStrings(nil)),
						)
					} else {
						logutil.Info(
							"op1",
							zap.String("txn", txn.String()),
							zap.String("pk", common.MoVectorToString(req.Batch.Vecs[idx], i)),
						)
					}
				}
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
			_, req.Cancel = context.WithTimeoutCause(nctx, time.Until(deadline), moerr.CauseHandleWrite)
		}
		rowidIdx := 0
		pkIdx := 1

		var (
			ok        bool
			loc       objectio.Location
			vectors   []containers.Vector
			closeFunc func()
		)

		for _, stats := range req.TombstoneStats {
			persistedTombstoneRows += int(stats.Rows())
			id := tb.GetMeta().(*catalog.TableEntry).AsCommonID()

			if ok, err = tb.AddPersistedTombstoneFile(id, stats); err != nil {
				logutil.Errorf("try delete by stats faild: %s, %v", stats.String(), err)
				return
			} else if ok {
				continue
			}

			logutil.Errorf("try delete by stats faild: %s, try to delete by row id and pk",
				stats.String())

			for i := range stats.BlkCnt() {
				loc = stats.BlockLocation(uint16(i), objectio.BlockMaxRows)
				vectors, closeFunc, err = blockio.LoadColumns2(
					ctx,
					[]uint16{uint16(rowidIdx), uint16(pkIdx)},
					nil,
					h.db.Runtime.Fs.Service,
					loc,
					fileservice.Policy(0),
					false,
					nil,
				)

				if err = tb.DeleteByPhyAddrKeys(vectors[0], vectors[1], handle.DT_Normal); err != nil {
					logutil.Errorf("delete by phyaddr keys faild: %s, %s, [idx]%d, %v",
						stats.String(), loc.String(), i, err)

					closeFunc()
					return
				}

				closeFunc()
			}
		}
		return
	}

	if len(req.Batch.Vecs) != 2 {
		panic(fmt.Sprintf("req.Batch.Vecs length is %d, should be 2", len(req.Batch.Vecs)))
	}
	rowIDVec := containers.ToTNVector(req.Batch.GetVector(0), common.WorkspaceAllocator)
	pkVec := containers.ToTNVector(req.Batch.GetVector(1), common.WorkspaceAllocator)
	inMemoryTombstoneRows += rowIDVec.Length()
	//defer pkVec.Close()
	// TODO: debug for #13342, remove me later
	_, _, injected := fault.TriggerFault(objectio.FJ_CommitDelete)
	if h.IsInterceptTable(tb.Schema(false).(*catalog.Schema).Name) || injected {
		schema := tb.Schema(false).(*catalog.Schema)
		if schema.HasPK() {
			rowids := vector.MustFixedColNoTypeCheck[types.Rowid](rowIDVec.GetDownstreamVector())
			isCompositeKey := schema.GetSingleSortKey().IsCompositeColumn()
			for i := 0; i < len(rowids); i++ {
				if isCompositeKey {
					pkbuf := req.Batch.Vecs[1].GetBytesAt(i)
					tuple, _ := types.Unpack(pkbuf)
					logutil.Info(
						"op2",
						zap.String("txn", txn.String()),
						zap.String("pk", common.TypeStringValue(*req.Batch.Vecs[1].GetType(), pkbuf, false)),
						zap.String("rowid", rowids[i].String()),
						zap.Any("detail", tuple.SQLStrings(nil)),
					)
				} else {
					logutil.Info(
						"op2",
						zap.String("txn", txn.String()),
						zap.String("pk", common.MoVectorToString(req.Batch.Vecs[1], i)),
						zap.String("rowid", rowids[i].String()),
					)
				}
			}
		}
	}
	err = tb.DeleteByPhyAddrKeys(rowIDVec, pkVec, handle.DT_Normal)
	return
}

func (h *Handle) HandleAlterTable(
	ctx context.Context,
	txn txnif.AsyncTxn,
	req *api.AlterTableReq,
) (err error) {
	var (
		dbase handle.Database
		tbl   handle.Relation
	)

	defer func() {
		logger := logutil.Info
		if err != nil {
			logger = logutil.Error
		}
		logger(
			"PreCommit-AlterTBL",
			zap.String("req", req.String()),
			zap.String("txn", txn.String()),
			zap.Error(err),
		)
	}()

	if dbase, err = txn.GetDatabaseByID(req.DbId); err != nil {
		return
	}

	if tbl, err = dbase.GetRelationByID(req.TableId); err != nil {
		return
	}

	err = tbl.AlterTable(ctx, req)
	return
}

//#endregion
