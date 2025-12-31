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
	"errors"
	"fmt"
	"os"
	"regexp"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/matrixorigin/matrixone/pkg/queryservice/client"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/cmd_util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/merge"
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

	interceptMatchRegexp atomic.Pointer[regexp.Regexp]

	client client.QueryClient
}

var _ rpchandle.Handler = (*Handle)(nil)

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

func NewTAEHandle(ctx context.Context, path string, client client.QueryClient, opt *options.Options) *Handle {
	if path == "" {
		path = "./store"
	}
	tae, err := openTAE(ctx, path, opt)
	if err != nil {
		panic(err)
	}

	h := &Handle{
		db:     tae,
		client: client,
	}

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

type txnCommitRequestsIter struct {
	cursor         int
	curNorReq      *api.PrecommitWriteCmd
	commitRequests *txn.TxnCommitRequest
}

func (h *Handle) newTxnCommitRequestsIter(
	cr *txn.TxnCommitRequest,
	meta txn.TxnMeta,
) *txnCommitRequestsIter {
	if cr == nil {
		return nil
	}
	return &txnCommitRequestsIter{
		cursor:         0,
		commitRequests: cr,
	}
}

func (cri *txnCommitRequestsIter) Next() bool {
	return cri.cursor < len(cri.commitRequests.Payload)
}

func (cri *txnCommitRequestsIter) Entry() (entry any, err error) {
	cnReq := cri.commitRequests.Payload[cri.cursor].CNRequest

	if cri.curNorReq == nil {
		cri.curNorReq = new(api.PrecommitWriteCmd)
	}

	if len(cri.curNorReq.EntryList) == 0 {
		if err = cri.curNorReq.UnmarshalBinary(cnReq.Payload); err != nil {
			return
		}
	}

	entry, cri.curNorReq.EntryList, err = pkgcatalog.ParseEntryList(cri.curNorReq.EntryList)
	if len(cri.curNorReq.EntryList) == 0 {
		cri.cursor++
	}

	return
}

func (h *Handle) handleRequests(
	ctx context.Context,
	txn txnif.AsyncTxn,
	commitRequests *txn.TxnCommitRequest,
	response *txn.TxnResponse,
	txnMeta txn.TxnMeta,
) (bigDelete []uint64, hasDDL bool, err error) {

	var (
		entry any

		iter *txnCommitRequestsIter

		inMemoryInsertRows        int
		persistedMemoryInsertRows int
		inMemoryTombstoneRows     int
		persistedTombstoneRows    int
		postFuncs                 []func()
	)

	defer func() {
		if err != nil {
			txn.Rollback(ctx)
		}
	}()

	if iter = h.newTxnCommitRequestsIter(commitRequests, txnMeta); iter == nil {
		return
	}

	bigDelete = make([]uint64, 0)
	var delM map[uint64]uint64 // tableID -> rows

	for iter.Next() {
		if entry, err = iter.Entry(); err != nil {
			return
		}

		switch req := entry.(type) {
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
		case []*api.AlterTableReq:
			hasDDL = true
			for _, r := range req {
				if err = h.HandleAlterTable(ctx, txn, r); err != nil {
					return
				}
			}

		case *cmd_util.WriteReq, *api.Entry:
			var wr *cmd_util.WriteReq
			if ae, ok := req.(*api.Entry); ok {
				wr = h.apiEntryToWriteEntry(ctx, txnMeta, ae, true)
			} else {
				wr = req.(*cmd_util.WriteReq)
			}

			if delM == nil {
				delM = make(map[uint64]uint64)
			}

			if wr.Type == cmd_util.EntryDelete {
				if wr.FileName != "" {
					for _, stats := range wr.TombstoneStats {
						delM[wr.TableID] += uint64(stats.Rows())
					}
				}
				if wr.Batch != nil {
					delM[wr.TableID] += uint64(wr.Batch.RowCount())
				}
			}

			var r1, r2, r3, r4 int
			var postFs []func()
			r1, r2, r3, r4, postFs, err = h.HandleWrite(ctx, txn, wr)
			postFuncs = append(postFuncs, postFs...)
			if err == nil {
				inMemoryInsertRows += r1
				persistedMemoryInsertRows += r2
				inMemoryTombstoneRows += r3
				persistedTombstoneRows += r4
			}

		default:
			err = moerr.NewNotSupportedf(ctx, "unknown txn request type: %T", req)
		}
		if err != nil {
			return
		}
	}

	totalAffectedRows := inMemoryInsertRows +
		persistedMemoryInsertRows +
		inMemoryTombstoneRows +
		persistedTombstoneRows
	if totalAffectedRows > 100000 {
		logutil.Info(
			"tn.handle.big.commit.trace.log",
			zap.Int("in-memory-rows", inMemoryInsertRows),
			zap.Int("persisted-rows", persistedMemoryInsertRows),
			zap.Int("in-memory-tombstones", inMemoryTombstoneRows),
			zap.Int("persisted-tombstones", persistedTombstoneRows),
			zap.String("txn", txn.String()),
		)
	}
	for tableID, rows := range delM {
		if rows > h.db.Opts.BulkTomestoneTxnThreshold {
			bigDelete = append(bigDelete, tableID)
		}
	}
	if len(postFuncs) > 0 {
		if hasDDL {
			// the target table of merge settings might not put into scheduler yet,
			// so we need to delay the post func execution
			time.AfterFunc(5*time.Second, func() {
				for _, f := range postFuncs {
					f()
				}
			})
		} else {
			for _, f := range postFuncs {
				f()
			}
		}
	}
	return
}

//#endregion

//#region Impl TxnStorage interface
//order by call frequency

func (h *Handle) apiEntryToWriteEntry(
	ctx context.Context,
	meta txn.TxnMeta,
	pe *api.Entry,
	prefetch bool,
) *cmd_util.WriteReq {

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

	if prefetch {
		if req.Type == cmd_util.EntryDelete {
			if err = h.prefetchDeleteRowID(ctx, req, &meta); err != nil {
				return nil
			}
		} else {
			if err = h.prefetchMetadata(ctx, req, &meta); err != nil {
				return nil
			}
		}
	}

	return req
}

// HandlePreCommitWrite impls TxnStorage:Write
// Deprecated: This method is no longer used in production. The new commit flow uses
// HandleCommit with TxnCommitRequest directly. This is kept only for interface compatibility.
func (h *Handle) HandlePreCommitWrite(
	ctx context.Context,
	meta txn.TxnMeta,
	req *api.PrecommitWriteCmd,
	_ *api.TNStringResponse /*no response*/) (err error) {
	// Do nothing - this method is deprecated
	return nil
}

// HandlePreCommitWrite impls TxnStorage:Commit
func (h *Handle) HandleCommit(
	ctx context.Context,
	meta txn.TxnMeta,
	response *txn.TxnResponse,
	commitRequests *txn.TxnCommitRequest,
) (cts timestamp.Timestamp, err error) {
	start := time.Now()

	var (
		txn      txnif.AsyncTxn
		releaseF []func()
		hasDDL   bool = false
	)
	defer func() {
		for _, f := range releaseF {
			f()
		}

		common.DoIfInfoEnabled(func() {
			_, _, injected := fault.TriggerFault(objectio.FJ_CommitSlowLog)
			if time.Since(start) > MAX_ALLOWED_TXN_LATENCY || err != nil || hasDDL || injected {
				var tnTxnInfo string
				if txn != nil {
					tnTxnInfo = txn.String()
				}
				logger := logutil.Warn
				msg := "tn.handle.commit.slow.log"
				if err != nil {
					logger = logutil.Error
					msg = "tn.handle.commit.error"
				} else if hasDDL {
					logger = logutil.Info
					msg = "tn.handle.commit.with.ddl"
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

	if txn, err = h.db.GetOrCreateTxnWithMeta(
		nil, meta.GetID(), types.TimestampToTS(meta.GetSnapshotTS())); err != nil {
		return
	}

	var bigDeleteTbls []uint64
	if bigDeleteTbls, hasDDL, err = h.handleRequests(
		ctx, txn, commitRequests, response, meta); err != nil {
		return
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
	if err == nil && len(bigDeleteTbls) > 0 {
		h.db.Runtime.BigDeleteHinter.RecordBigDel(bigDeleteTbls, types.TimestampToTS(cts))
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
				"tn.handle.retry.txn",
				zap.String("old-txn", string(meta.GetID())),
				zap.String("new-txn", txn.GetID()),
			)
			//Handle precommit-write command for 1PC
			bigDeleteTbls, hasDDL, err = h.handleRequests(ctx, txn, commitRequests, response, meta)
			if err != nil && !moerr.IsMoErrCode(err, moerr.ErrTAENeedRetry) {
				break
			}
			//if txn is 2PC ,need to set commit timestamp passed by coordinator.
			if txn.Is2PC() {
				txn.SetCommitTS(types.TimestampToTS(meta.GetCommitTS()))
			}
			err = txn.Commit(ctx)
			cts = txn.GetCommitTS().ToTimestamp()
			if err == nil && len(bigDeleteTbls) > 0 {
				h.db.Runtime.BigDeleteHinter.RecordBigDel(bigDeleteTbls, types.TimestampToTS(cts))
			}
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
		h.db,
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
	txn, err := h.db.GetTxnByID(meta.GetID())
	if err != nil {
		return err
	}
	err = txn.Rollback(ctx)
	return
}

func (h *Handle) HandleClose(ctx context.Context) (err error) {
	//FIXME::should wait txn request's job done?
	//if h.GCJob != nil {
	//	h.GCJob.Stop()
	//}
	if h.client != nil {
		err = h.client.Close()
		if err != nil {
			return err
		}
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

	for i, c := range req.Cmds {
		common.DoIfInfoEnabled(func() {
			logutil.Info(
				"tn.handle.create.database",
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
	req *pkgcatalog.DropDatabaseReq,
) (err error) {
	for i, c := range req.Cmds {
		common.DoIfInfoEnabled(func() {
			logutil.Info(
				"tn.handle.drop.database",
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
	mp := common.WorkspaceAllocator
	rowIDVec := containers.ToTNVector(req.Bat.GetVector(0), mp)
	defer rowIDVec.Close()
	pkVec := containers.ToTNVector(req.Bat.GetVector(1), mp)
	defer pkVec.Close()
	err = databaseTbl.DeleteByPhyAddrKeys(rowIDVec, pkVec, handle.DT_Normal)

	return
}

func (h *Handle) HandleCreateRelation(
	ctx context.Context,
	txn txnif.AsyncTxn,
	req *pkgcatalog.CreateTableReq,
) error {
	for i, c := range req.Cmds {
		common.DoIfInfoEnabled(func() {
			logutil.Info(
				"tn.handle.create.relation",
				zap.String("txn", txn.String()),
				zap.String("i/cnt", fmt.Sprintf("%d/%d", i+1, len(req.Cmds))),
				zap.String("cmd", fmt.Sprintf("%+v", c)),
			)
		})
		ctx = defines.AttachAccount(ctx, c.AccountId, c.Creator, c.Owner)
		txn.BindAccessInfo(c.AccountId, c.Creator, c.Owner)
		dbH, err := txn.GetDatabaseByID(c.DatabaseId)
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
	req *pkgcatalog.DropTableReq,
) error {
	for i, c := range req.Cmds {
		common.DoIfInfoEnabled(func() {
			logutil.Info(
				"tn.handle.drop.relation",
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
	mp := common.WorkspaceAllocator
	dt := handle.DT_Normal
	rowIDVec := containers.ToTNVector(req.TableBat.GetVector(0), mp)
	defer rowIDVec.Close()
	pkVec := containers.ToTNVector(req.TableBat.GetVector(1), mp)
	defer pkVec.Close()
	if err := tablesTbl.DeleteByPhyAddrKeys(rowIDVec, pkVec, dt); err != nil {
		return err
	}
	columnsTbl, _ := catalog.GetRelationByID(pkgcatalog.MO_COLUMNS_ID)
	for _, bat := range req.ColumnBat {
		rowIDVec := containers.ToTNVector(bat.GetVector(0), mp)
		defer rowIDVec.Close()
		pkVec := containers.ToTNVector(bat.GetVector(1), mp)
		defer pkVec.Close()
		if err := columnsTbl.DeleteByPhyAddrKeys(rowIDVec, pkVec, dt); err != nil {
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
	postFunc []func(),
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
		err = errors.Join(err, moerr.NewBadDB(ctx, fmt.Sprintf("%d-%s",
			req.DatabaseId,
			req.DatabaseName)))
		return
	}

	tb, err := dbase.GetRelationByID(req.TableID)
	if err != nil {
		err = errors.Join(err, moerr.NewBadDB(ctx, fmt.Sprintf("%d-%s",
			req.TableID,
			req.TableName)))
		return
	}

	if req.Type == cmd_util.EntryInsert {
		//Add blocks which had been bulk-loaded into S3 into table.
		if req.FileName != "" {
			statsVec := req.Batch.Vecs[0]
			for i := 0; i < statsVec.Length(); i++ {
				s := objectio.ObjectStats(statsVec.GetBytesAt(i))
				// do not check because clone will send reusable objects to tn
				// if !s.GetCNCreated() {
				// logutil.Infof("the `CNCreated` mask not set: %s", s.String())
				// }
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
		//if h.IsInterceptTable(tb.Schema(false).(*catalog.Schema).Name) {
		//	schema := tb.Schema(false).(*catalog.Schema)
		//	if schema.HasPK() {
		//		pkDef := schema.GetSingleSortKey()
		//		idx := pkDef.Idx
		//		isCompositeKey := pkDef.IsCompositeColumn()
		//		for i := 0; i < req.Batch.Vecs[0].Length(); i++ {
		//			if isCompositeKey {
		//				pkbuf := req.Batch.Vecs[idx].GetBytesAt(i)
		//				tuple, _ := types.Unpack(pkbuf)
		//				logutil.Info(
		//					"op1",
		//					zap.String("txn", txn.String()),
		//					zap.String("pk", common.TypeStringValue(*req.Batch.Vecs[idx].GetType(), pkbuf, false)),
		//					zap.Any("detail", tuple.SQLStrings(nil)),
		//				)
		//			} else {
		//				logutil.Info(
		//					"op1",
		//					zap.String("txn", txn.String()),
		//					zap.String("pk", common.MoVectorToString(req.Batch.Vecs[idx], i)),
		//				)
		//			}
		//		}
		//	}
		//
		//}
		//Appends a batch of data into table.
		if req.DatabaseId == pkgcatalog.MO_CATALOG_ID && req.TableName == pkgcatalog.MO_MERGE_SETTINGS {
			postFunc = append(postFunc, parse_merge_settings_set(req.Batch, h.db.MergeScheduler))
		}

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
				vectors, closeFunc, err = ioutil.LoadColumns2(
					ctx,
					[]uint16{uint16(rowidIdx), uint16(pkIdx)},
					nil,
					h.db.Runtime.Fs,
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
	if req.DatabaseId == pkgcatalog.MO_CATALOG_ID && req.TableName == pkgcatalog.MO_MERGE_SETTINGS {
		postFunc = append(postFunc, parse_merge_settings_unset(pkVec, h.db.MergeScheduler))
	}
	inMemoryTombstoneRows += rowIDVec.Length()
	//defer pkVec.Close()
	// TODO: debug for #13342, remove me later
	//_, _, injected := fault.TriggerFault(objectio.FJ_CommitDelete)
	//if h.IsInterceptTable(tb.Schema(false).(*catalog.Schema).Name) || injected {
	//	schema := tb.Schema(false).(*catalog.Schema)
	//	if schema.HasPK() {
	//		rowids := vector.MustFixedColNoTypeCheck[types.Rowid](rowIDVec.GetDownstreamVector())
	//		isCompositeKey := schema.GetSingleSortKey().IsCompositeColumn()
	//		for i := 0; i < len(rowids); i++ {
	//			if isCompositeKey {
	//				pkbuf := req.Batch.Vecs[1].GetBytesAt(i)
	//				tuple, _ := types.Unpack(pkbuf)
	//				logutil.Info(
	//					"op2",
	//					zap.String("txn", txn.String()),
	//					zap.String("pk", common.TypeStringValue(*req.Batch.Vecs[1].GetType(), pkbuf, false)),
	//					zap.String("rowid", rowids[i].String()),
	//					zap.Any("detail", tuple.SQLStrings(nil)),
	//				)
	//			} else {
	//				logutil.Info(
	//					"op2",
	//					zap.String("txn", txn.String()),
	//					zap.String("pk", common.MoVectorToString(req.Batch.Vecs[1], i)),
	//					zap.String("rowid", rowids[i].String()),
	//				)
	//			}
	//		}
	//	}
	//}
	err = tb.DeleteByPhyAddrKeys(rowIDVec, pkVec, handle.DT_Normal)
	return
}

func parse_merge_settings_set(
	bat *batch.Batch,
	scheduler *merge.MergeScheduler,
) func() {
	settings := make([]*merge.MergeSettings, 0, 1)
	tids := make([]uint64, 0, 1)
	merge.DecodeMergeSettingsBatchAnd(bat, func(tid uint64, setting *merge.MergeSettings) {
		settings = append(settings, setting)
		tids = append(tids, tid)
	})
	return func() {
		for i, tid := range tids {
			err := scheduler.SendConfig(tid, settings[i])
			if err != nil {
				logutil.Error("MergeExecutorEvent",
					zap.String("event", "send config"),
					zap.Uint64("table-id", tid),
					zap.Error(err),
					zap.String("settings", settings[i].String()),
				)
			}
		}
	}
}

func parse_merge_settings_unset(
	pkVec containers.Vector,
	scheduler *merge.MergeScheduler,
) func() {
	tids := make([]uint64, 0, 1)
	for i := 0; i < pkVec.Length(); i++ {
		bs := pkVec.Get(i).([]byte)
		tuple, _, _, err := types.DecodeTuple(bs)
		if err != nil {
			logutil.Error("MergeExecutorEvent",
				zap.String("event", "unmarshal settings pk tuple"),
				zap.Int("idx", i),
				zap.Error(err),
			)
		}
		tids = append(tids, tuple[1].(uint64))
	}
	return func() {
		for _, tid := range tids {
			scheduler.SendConfig(tid, nil)
		}
	}
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
			"tn.handle.alter.relation",
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
