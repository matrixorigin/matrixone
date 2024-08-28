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
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/gc"
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
	logutil.Infof("GC rpc handle txn cache")
	h.txnCtxs.DeleteIf(func(k string, v *txnContext) bool {
		return v.deadline.Before(now)
	})
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

func (h *Handle) handleRequests(
	ctx context.Context,
	txn txnif.AsyncTxn,
	txnCtx *txnContext,
) (err error) {
	for _, e := range txnCtx.reqs {
		switch req := e.(type) {
		case *pkgcatalog.CreateDatabaseReq:
			err = h.HandleCreateDatabase(ctx, txn, req)
		case *pkgcatalog.DropDatabaseReq:
			err = h.HandleDropDatabase(ctx, txn, req)
		case *pkgcatalog.CreateTableReq:
			err = h.HandleCreateRelation(ctx, txn, req)
		case *pkgcatalog.DropTableReq:
			err = h.HandleDropRelation(ctx, txn, req)
		case *api.AlterTableReq:
			err = h.HandleAlterTable(ctx, txn, req)
		case *db.WriteReq:
			err = h.HandleWrite(ctx, txn, req)
		default:
			err = moerr.NewNotSupportedf(ctx, "unknown txn request type: %T", req)
		}
		//Need to roll back the txn.
		if err != nil {
			txn.Rollback(ctx)
			return
		}
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
				loc := req.Batch.Vecs[0]
				for i := 0; i < req.Batch.RowCount(); i++ {
					if req.Type == db.EntryInsert {
						req.MetaLocs = append(req.MetaLocs, loc.GetStringAt(i))
					} else {
						req.DeltaLocs = append(req.DeltaLocs, loc.GetStringAt(i))
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
	return h.TryPrefetchTxn(ctx, meta)
}

// HandlePreCommitWrite impls TxnStorage:Commit
func (h *Handle) HandleCommit(
	ctx context.Context,
	meta txn.TxnMeta) (cts timestamp.Timestamp, err error) {
	start := time.Now()
	txnCtx, ok := h.txnCtxs.Load(util.UnsafeBytesToString(meta.GetID()))
	common.DoIfDebugEnabled(func() {
		logutil.Debugf("HandleCommit start : %X",
			string(meta.GetID()))
	})
	defer func() {
		if ok {
			//delete the txn's context.
			h.txnCtxs.Delete(util.UnsafeBytesToString(meta.GetID()))
		}
		common.DoIfInfoEnabled(func() {
			if time.Since(start) > MAX_ALLOWED_TXN_LATENCY {
				logutil.Warn(
					"SLOW-LOG",
					zap.Duration("commit-latency", time.Since(start)),
					zap.String("txn", meta.DebugString()),
				)
			}
		})
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
			err = h.handleRequests(ctx, txn, txnCtx)
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
			logutil.Infof(
				"[precommit] create database: %+v (%v/%v) txn: %s",
				c, i+1, len(req.Cmds), txn.String(),
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
	// logutil.Infof("yyyy create db %v", common.MoBatchToString(req.Bat, 5))
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
			logutil.Infof(
				"[precommit] drop database: %+v (%v/%v) txn: %s",
				c, i+1, len(req.Cmds), txn.String(),
			)
		})
		if _, err = txn.DropDatabaseByID(c.Id); err != nil {
			return
		}
	}

	// Delete in mo_database table
	// logutil.Infof("yyyy drop db %v", common.MoBatchToString(req.Bat, 5))
	catalog, _ := txn.GetDatabaseByID(pkgcatalog.MO_CATALOG_ID)
	databaseTbl, _ := catalog.GetRelationByID(pkgcatalog.MO_DATABASE_ID)
	rowIDVec := containers.ToTNVector(req.Bat.GetVector(0), common.WorkspaceAllocator)
	defer rowIDVec.Close()
	pkVec := containers.ToTNVector(req.Bat.GetVector(1), common.WorkspaceAllocator)
	defer pkVec.Close()
	err = databaseTbl.DeleteByPhyAddrKeys(rowIDVec, pkVec)

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
			logutil.Infof(
				"[precommit] create table: %v (%v/%v) txn: %s",
				c, i+1, len(req.Cmds), txn.String(),
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

	// if len(req.Cmds) > 0 {
	// 	logutil.Infof("yyyy create table %v", common.MoBatchToString(req.TableBat, 5))
	// } else {
	// 	logutil.Infof("yyyy [alter] insert table %v", common.MoBatchToString(req.TableBat, 5))
	// }
	catalog, _ := txn.GetDatabaseByID(pkgcatalog.MO_CATALOG_ID)
	tablesTbl, _ := catalog.GetRelationByID(pkgcatalog.MO_TABLES_ID)
	if err := AppendDataToTable(ctx, tablesTbl, req.TableBat); err != nil {
		return err
	}
	// if len(req.Cmds) > 0 {
	// 	logutil.Infof("yyyy create table cols %v", common.MoBatchToString(req.ColumnBat[0], 5))
	// } else {
	// 	logutil.Infof("yyyy [alter] insert columns %v", common.MoBatchToString(req.TableBat, 5))
	// }
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
			logutil.Infof(
				"[precommit] drop/truncate table: %+v (%v/%v) txn: %s",
				c, i+1, len(req.Cmds), txn.String(),
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

	// if len(req.Cmds) > 0 {
	// 	logutil.Infof("yyyy drop table %v", common.MoBatchToString(req.TableBat, 5))
	// } else {
	// 	logutil.Infof("yyyy [alter] delete table%v", common.MoBatchToString(req.TableBat, 5))
	// }
	catalog, _ := txn.GetDatabaseByID(pkgcatalog.MO_CATALOG_ID)
	tablesTbl, _ := catalog.GetRelationByID(pkgcatalog.MO_TABLES_ID)
	rowIDVec := containers.ToTNVector(req.TableBat.GetVector(0), common.WorkspaceAllocator)
	defer rowIDVec.Close()
	pkVec := containers.ToTNVector(req.TableBat.GetVector(1), common.WorkspaceAllocator)
	defer pkVec.Close()
	if err := tablesTbl.DeleteByPhyAddrKeys(rowIDVec, pkVec); err != nil {
		return err
	}
	// if len(req.Cmds) > 0 {
	// 	logutil.Infof("yyyy drop table cols %v", common.MoBatchToString(req.ColumnBat[0], 5))
	// } else {
	// 	logutil.Infof("yyyy [alter] delete columns %v", common.MoBatchToString(req.TableBat, 5))
	// }
	columnsTbl, _ := catalog.GetRelationByID(pkgcatalog.MO_COLUMNS_ID)
	for _, bat := range req.ColumnBat {
		rowIDVec := containers.ToTNVector(bat.GetVector(0), common.WorkspaceAllocator)
		defer rowIDVec.Close()
		pkVec := containers.ToTNVector(bat.GetVector(1), common.WorkspaceAllocator)
		defer pkVec.Close()
		if err := columnsTbl.DeleteByPhyAddrKeys(rowIDVec, pkVec); err != nil {
			return err
		}
	}

	return nil
}

// HandleWrite Handle DML commands
func (h *Handle) HandleWrite(
	ctx context.Context,
	txn txnif.AsyncTxn,
	req *db.WriteReq) (err error) {
	defer func() {
		if req.Cancel != nil {
			req.Cancel()
		}
	}()
	ctx = perfcounter.WithCounterSetFrom(ctx, h.db.Opts.Ctx)
	switch req.PkCheck {
	case db.FullDedup:
		txn.SetDedupType(txnif.DedupPolicy_CheckAll)
	case db.IncrementalDedup:
		if h.db.Opts.IncrementalDedup {
			txn.SetDedupType(txnif.DedupPolicy_Incremental)
		} else {
			txn.SetDedupType(txnif.DedupPolicy_SkipWorkspace)
		}
	case db.FullSkipWorkspaceDedup:
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

	if req.Type == db.EntryInsert {
		//Add blocks which had been bulk-loaded into S3 into table.
		if req.FileName != "" {
			metalocations := make(map[string]struct{})
			for _, metLoc := range req.MetaLocs {
				location, err := blockio.EncodeLocationFromString(metLoc)
				if err != nil {
					return err
				}
				metalocations[location.Name().String()] = struct{}{}
			}
			statsCNVec := req.Batch.Vecs[1]
			statsVec := containers.ToTNVector(statsCNVec, common.WorkspaceAllocator)
			for i := 0; i < statsVec.Length(); i++ {
				s := objectio.ObjectStats(statsVec.Get(i).([]byte))
				if !s.GetCNCreated() {
					logutil.Fatal("the `CNCreated` mask not set")
				}
				delete(metalocations, s.ObjectName().String())
			}
			if len(metalocations) != 0 {
				logutil.Warn(
					"TAE-EMPTY-STATS",
					zap.Any("locations", metalocations),
					zap.String("table", req.TableName),
				)
				err = moerr.NewInternalError(ctx, "object stats doesn't match meta locations")
				return
			}
			err = tb.AddObjsWithMetaLoc(ctx, statsVec)
			return
		}
		//check the input batch passed by cn is valid.
		len := 0
		for i, vec := range req.Batch.Vecs {
			if vec == nil {
				logutil.Errorf("the vec:%d in req.Batch is nil", i)
				panic("invalid vector : vector is nil")
			}
			if i == 0 {
				len = vec.Length()
			}
			if vec.Length() != len {
				logutil.Errorf("the length of vec:%d in req.Batch is not equal to the first vec", i)
				panic("invalid batch : the length of vectors in batch is not the same")
			}
		}
		// TODO: debug for #13342, remove me later
		if h.IsInterceptTable(tb.Schema(false).(*catalog.Schema).Name) {
			if tb.Schema(false).(*catalog.Schema).HasPK() {
				idx := tb.Schema(false).(*catalog.Schema).GetSingleSortKeyIdx()
				for i := 0; i < req.Batch.Vecs[0].Length(); i++ {
					logutil.Info(
						"op1",
						zap.String("start-ts", txn.GetStartTS().ToString()),
						zap.String("pk", common.MoVectorToString(req.Batch.Vecs[idx], i)),
					)
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
			var vectors []containers.Vector
			var closeFunc func()
			//Extend lifetime of vectors is within the function.
			//No NeedCopy. closeFunc is required after use.
			//closeFunc is not nil.
			vectors, closeFunc, err = blockio.LoadColumns2(
				ctx,
				[]uint16{uint16(rowidIdx), uint16(pkIdx)},
				nil,
				h.db.Runtime.Fs.Service,
				location,
				fileservice.Policy(0),
				false,
				nil,
			)
			if err != nil {
				return
			}
			defer closeFunc()
			blkids := getBlkIDsFromRowids(vectors[0].GetDownstreamVector())
			id := tb.GetMeta().(*catalog.TableEntry).AsCommonID()
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
			rowIDVec := vectors[0]
			defer rowIDVec.Close()
			pkVec := vectors[1]
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
	// TODO: debug for #13342, remove me later
	if h.IsInterceptTable(tb.Schema(false).(*catalog.Schema).Name) {
		if tb.Schema(false).(*catalog.Schema).HasPK() {
			for i := 0; i < rowIDVec.Length(); i++ {
				rowID := objectio.HackBytes2Rowid(req.Batch.Vecs[0].GetRawBytesAt(i))
				logutil.Info(
					"op2",
					zap.String("start-ts", txn.GetStartTS().ToString()),
					zap.String("pk", common.MoVectorToString(req.Batch.Vecs[1], i)),
					zap.String("rowid", rowID.String()),
				)
			}
		}
	}
	err = tb.DeleteByPhyAddrKeys(rowIDVec, pkVec)
	return
}

func (h *Handle) HandleAlterTable(
	ctx context.Context,
	txn txnif.AsyncTxn,
	req *api.AlterTableReq) (err error) {
	logutil.Infof("[precommit] alter table: %v txn: %s", req.String(), txn.String())

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

//#endregion
