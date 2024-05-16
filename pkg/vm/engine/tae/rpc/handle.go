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
	"regexp"
	"runtime"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/exp/slices"

	"github.com/google/shlex"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/util"
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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/merge"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/gc"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/rpchandle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/jobs"
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

// TODO: vast items within h.mu.txnCtxs would incur performance penality.
func (h *Handle) GCCache(now time.Time) error {
	logutil.Infof("GC rpc handle txn cache")
	h.txnCtxs.DeleteIf(func(k string, v *txnContext) bool {
		return v.deadline.Before(now)
	})
	return nil
}

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
				logutil.Info("Commit with long latency", zap.Duration("duration", time.Since(start)), zap.String("debug", meta.DebugString()))
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

	if moerr.IsMoErrCode(err, moerr.ErrTAENeedRetry) {
		for {
			txn, err = h.db.StartTxnWithStartTSAndSnapshotTS(nil,
				types.TimestampToTS(meta.GetSnapshotTS()))
			if err != nil {
				return
			}
			logutil.Infof("retry txn %X with new txn %X", string(meta.GetID()), txn.GetID())
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

func (h *Handle) handleRequests(
	ctx context.Context,
	txn txnif.AsyncTxn,
	txnCtx *txnContext,
) (err error) {
	var createDB, createRelation, dropDB, dropRelation, alterTable, write int
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
	txnCtx, ok := h.txnCtxs.Load(util.UnsafeBytesToString(meta.GetID()))
	var txn txnif.AsyncTxn
	defer func() {
		if ok {
			//delete the txn's context.
			h.txnCtxs.Delete(util.UnsafeBytesToString(meta.GetID()))
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

func (h *Handle) HandleCommitMerge(
	ctx context.Context,
	meta txn.TxnMeta,
	req *api.MergeCommitEntry,
	resp *db.InspectResp) (cb func(), err error) {

	defer func() {
		if err != nil {
			e := moerr.DowncastError(err)
			logutil.Error("mergeblocks err handle commit merge",
				zap.String("table", fmt.Sprintf("%v-%v", req.TblId, req.TableName)),
				zap.String("start-ts", req.StartTs.DebugString()),
				zap.String("error", e.Display()))
		}

	}()
	txn, err := h.db.GetOrCreateTxnWithMeta(nil, meta.GetID(),
		types.TimestampToTS(meta.GetSnapshotTS()))
	if err != nil {
		return
	}
	ids := make([]objectio.ObjectId, 0, len(req.MergedObjs))
	for _, o := range req.MergedObjs {
		stat := objectio.ObjectStats(o)
		ids = append(ids, *stat.ObjectName().ObjectId())
	}
	merge.ActiveCNObj.RemoveActiveCNObj(ids)
	if req.Err != "" {
		resp.Message = req.Err
		err = moerr.NewInternalError(ctx, "merge err in cn: %s", req.Err)
		return
	}

	defer func() {
		if err != nil {
			txn.Rollback(ctx)
			resp.Message = err.Error()
			merge.CleanUpUselessFiles(req, h.db.Runtime.Fs.Service)
		}
	}()

	if len(req.BookingLoc) > 0 {
		// load transfer info from s3
		if req.Booking != nil {
			logutil.Error("mergeblocks err booking loc is not empty, but booking is not nil")
		}
		if len(req.BookingLoc) == objectio.LocationLen {
			loc := objectio.Location(req.BookingLoc)
			var bat *batch.Batch
			var release func()
			bat, release, err = blockio.LoadTombstoneColumns(ctx, []uint16{0}, nil, h.db.Runtime.Fs.Service, loc, nil)
			if err != nil {
				return
			}
			req.Booking = &api.BlkTransferBooking{}
			err = req.Booking.Unmarshal(bat.Vecs[0].GetBytesAt(0))
			if err != nil {
				release()
				return
			}
			release()
			h.db.Runtime.Fs.Service.Delete(ctx, loc.Name().String())
			bat = nil
		} else {
			// it has to copy to concat
			idx := 0
			locations := req.BookingLoc
			data := make([]byte, 0, 2<<30)
			for ; idx < len(locations); idx += objectio.LocationLen {
				loc := objectio.Location(locations[idx : idx+objectio.LocationLen])
				var bat *batch.Batch
				var release func()
				bat, release, err = blockio.LoadTombstoneColumns(ctx, []uint16{0}, nil, h.db.Runtime.Fs.Service, loc, nil)
				if err != nil {
					return
				}
				data = append(data, bat.Vecs[0].GetBytesAt(0)...)
				release()
				h.db.Runtime.Fs.Service.Delete(ctx, loc.Name().String())
				bat = nil
			}
			req.Booking = &api.BlkTransferBooking{}
			if err = req.Booking.Unmarshal(data); err != nil {
				return
			}
		}
	}

	_, err = jobs.HandleMergeEntryInTxn(txn, req, h.db.Runtime)
	if err != nil {
		return
	}
	err = txn.Commit(ctx)
	if err == nil {
		b := &bytes.Buffer{}
		b.WriteString("merged success\n")
		for _, o := range req.CreatedObjs {
			stat := objectio.ObjectStats(o)
			b.WriteString(fmt.Sprintf("%v, rows %v, blks %v, osize %v, csize %v",
				stat.ObjectName().String(), stat.Rows(), stat.BlkCnt(),
				common.HumanReadableBytes(int(stat.OriginSize())),
				common.HumanReadableBytes(int(stat.Size())),
			))
			b.WriteByte('\n')
		}
		resp.Message = b.String()
	}
	return nil, err
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

func (h *Handle) HandleForceGlobalCheckpoint(
	ctx context.Context,
	meta txn.TxnMeta,
	req *db.Checkpoint,
	resp *api.SyncLogTailResp) (cb func(), err error) {

	timeout := req.FlushDuration

	currTs := types.BuildTS(time.Now().UTC().UnixNano(), 0)

	err = h.db.ForceGlobalCheckpoint(ctx, currTs, timeout)
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

func (h *Handle) HandleInterceptCommit(
	ctx context.Context,
	meta txn.TxnMeta,
	req *db.InterceptCommit,
	resp *api.SyncLogTailResp) (cb func(), err error) {

	name := req.TableName
	h.UpdateInterceptMatchRegexp(name)
	return nil, err
}

func (h *Handle) HandleInspectTN(
	ctx context.Context,
	meta txn.TxnMeta,
	req *db.InspectTN,
	resp *db.InspectResp) (cb func(), err error) {
	defer func() {
		if e := recover(); e != nil {
			err = moerr.ConvertPanicError(ctx, e)
			logutil.Error(
				"panic in inspect dn",
				zap.String("cmd", req.Operation),
				zap.String("error", err.Error()))
		}
	}()
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
	txnCtx, _ := h.txnCtxs.Load(util.UnsafeBytesToString(meta.GetID()))

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
	txnCtx, ok := h.txnCtxs.Load(util.UnsafeBytesToString(meta.GetID()))
	if !ok {
		now := time.Now()
		txnCtx = &txnContext{
			createAt: now,
			deadline: now.Add(MAX_TXN_COMMIT_LATENCY),
			meta:     meta,
			toCreate: make(map[uint64]*catalog2.Schema),
		}
		h.txnCtxs.Store(util.UnsafeBytesToString(meta.GetID()), txnCtx)
	}
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
				logutil.Infof("dropOrTruncateRelation isDrop: %v, name: %s, id: %d, newId: %d, databaseName: %s, databaseId: %d\n",
					req.IsDrop, req.Name, req.ID, req.NewId, req.DatabaseName, req.DatabaseID)
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

	ctx = defines.AttachAccount(ctx, req.AccessInfo.AccountID, req.AccessInfo.UserID, req.AccessInfo.RoleID)
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

	ctx = defines.AttachAccount(ctx, req.AccessInfo.AccountID, req.AccessInfo.UserID, req.AccessInfo.RoleID)
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

func PrintTuple(tuple types.Tuple) string {
	res := "("
	for i, t := range tuple {
		switch t := t.(type) {
		case int32:
			res += fmt.Sprintf("%v", t)
		}
		if i != len(tuple)-1 {
			res += ","
		}
	}
	res += ")"
	return res
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
				delete(metalocations, s.ObjectName().String())
			}
			if len(metalocations) != 0 {
				logutil.Warnf("tbl %v, not receive stats of following locations %v", req.TableName, metalocations)
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
		// TODO: debug for #13342, remove me later
		if h.IsInterceptTable(tb.Schema().(*catalog2.Schema).Name) {
			if tb.Schema().(*catalog2.Schema).HasPK() {
				idx := tb.Schema().(*catalog2.Schema).GetSingleSortKeyIdx()
				for i := 0; i < req.Batch.Vecs[0].Length(); i++ {
					logutil.Infof("op1 %v, %v", txn.GetStartTS().ToString(), common.MoVectorToString(req.Batch.Vecs[idx], i))
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
			vectors, closeFunc, err = blockio.LoadTombstoneColumns2(
				ctx,
				[]uint16{uint16(rowidIdx), uint16(pkIdx)},
				nil,
				h.db.Runtime.Fs.Service,
				location,
				false,
				nil,
			)
			if err != nil {
				return
			}
			defer closeFunc()
			blkids := getBlkIDsFromRowids(vectors[0].GetDownstreamVector())
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
	if h.IsInterceptTable(tb.Schema().(*catalog2.Schema).Name) {
		if tb.Schema().(*catalog2.Schema).HasPK() {
			for i := 0; i < rowIDVec.Length(); i++ {
				rowID := objectio.HackBytes2Rowid(req.Batch.Vecs[0].GetRawBytesAt(i))
				logutil.Infof("op2 %v %v %v", txn.GetStartTS().ToString(), common.MoVectorToString(req.Batch.Vecs[1], i), rowID.String())
			}
		}
	}
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

func traverseCatalogForNewAccounts(c *catalog2.Catalog, memo *logtail.TNUsageMemo, ids []uint32) {
	if len(ids) == 0 {
		return
	}
	processor := new(catalog2.LoopProcessor)
	processor.DatabaseFn = func(entry *catalog2.DBEntry) error {
		if entry.HasDropCommitted() {
			return nil
		}

		accId := entry.GetTenantID()
		if !slices.Contains(ids, accId) {
			return nil
		}

		tblIt := entry.MakeTableIt(true)
		for tblIt.Valid() {
			insUsage := logtail.UsageData{
				AccId: uint64(accId), DbId: entry.ID, TblId: tblIt.Get().GetPayload().ID}

			tblEntry := tblIt.Get().GetPayload()
			if tblEntry.HasDropCommitted() {
				tblIt.Next()
				continue
			}

			objIt := tblEntry.MakeObjectIt(true)
			for objIt.Valid() {
				objEntry := objIt.Get().GetPayload()
				// PXU TODO
				if !objEntry.IsAppendable() && !objEntry.HasDropCommitted() && objEntry.IsCommitted() {
					insUsage.Size += uint64(objEntry.GetCompSize())
				}
				objIt.Next()
			}

			if insUsage.Size > 0 {
				memo.UpdateNewAccCache(insUsage, false)
			}

			tblIt.Next()
		}
		return nil
	}

	c.RecurLoop(processor)
}

func (h *Handle) HandleStorageUsage(ctx context.Context, meta txn.TxnMeta,
	req *db.StorageUsageReq, resp *db.StorageUsageResp) (func(), error) {
	memo := h.db.GetUsageMemo()

	start := time.Now()
	defer func() {
		v2.TaskStorageUsageReqDurationHistogram.Observe(time.Since(start).Seconds())
	}()

	memo.EnterProcessing()
	defer func() {
		resp.Magic = logtail.StorageUsageMagic
		memo.LeaveProcessing()
	}()

	if !memo.HasUpdate() {
		resp.Succeed = true
		return nil, nil
	}

	usages := memo.GatherAllAccSize()

	newIds := make([]uint32, 0)
	for _, id := range req.AccIds {
		if usages != nil {
			if size, exist := usages[uint64(id)]; exist {
				memo.AddReqTrace(uint64(id), size, start, "req")
				resp.AccIds = append(resp.AccIds, int32(id))
				resp.Sizes = append(resp.Sizes, size)
				delete(usages, uint64(id))
				continue
			}
		}
		// new account which haven't been collect
		newIds = append(newIds, uint32(id))
	}

	for accId, size := range usages {
		memo.AddReqTrace(uint64(accId), size, start, "oth")
		resp.AccIds = append(resp.AccIds, int32(accId))
		resp.Sizes = append(resp.Sizes, size)
	}

	// new accounts
	traverseCatalogForNewAccounts(h.db.Catalog, memo, newIds)

	for idx := range newIds {
		if size, exist := memo.GatherNewAccountSize(uint64(newIds[idx])); exist {
			resp.AccIds = append(resp.AccIds, int32(newIds[idx]))
			resp.Sizes = append(resp.Sizes, size)
			memo.AddReqTrace(uint64(newIds[idx]), size, start, "new")
		}
	}

	memo.ClearNewAccCache()

	resp.Succeed = true

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
