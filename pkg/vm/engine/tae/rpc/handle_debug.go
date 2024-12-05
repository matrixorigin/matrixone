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
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/google/shlex"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/cmd_util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	catalog2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/merge"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/jobs"
	"go.uber.org/zap"
)

///
///
/// impls TxnStorage:Debug
///
///

func (h *Handle) HandleAddFaultPoint(
	ctx context.Context,
	meta txn.TxnMeta,
	req *cmd_util.FaultPoint,
	resp *api.SyncLogTailResp) (func(), error) {
	if req.Name == cmd_util.EnableFaultInjection {
		fault.Enable()
		return nil, nil
	} else if req.Name == cmd_util.DisableFaultInjection {
		fault.Disable()
		return nil, nil
	}
	return nil, h.db.AddFaultPoint(ctx, req.Name, req.Freq, req.Action, req.Iarg, req.Sarg)
}

func (h *Handle) HandleTraceSpan(ctx context.Context,
	meta txn.TxnMeta,
	req *cmd_util.TraceSpan,
	resp *api.SyncLogTailResp) (func(), error) {

	return nil, nil
}

func (h *Handle) HandleSnapshotRead(
	ctx context.Context,
	meta txn.TxnMeta,
	req *cmd_util.SnapshotReadReq,
	resp *cmd_util.SnapshotReadResp,
) (func(), error) {
	now := time.Now()
	defer func() {
		v2.TaskSnapshotReadReqDurationHistogram.Observe(time.Since(now).Seconds())
	}()
	maxEnd := types.TS{}
	maxCheckpoint := h.db.BGCheckpointRunner.MaxIncrementalCheckpoint()
	if maxCheckpoint != nil {
		maxEnd = maxCheckpoint.GetEnd()
	}
	snapshot := types.TimestampToTS(*req.Snapshot)
	if snapshot.GT(&maxEnd) {
		resp.Succeed = false
		return nil, nil
	}
	checkpoints, err := checkpoint.ListSnapshotCheckpoint(
		ctx,
		"",
		h.db.Runtime.Fs.Service,
		snapshot,
		h.db.BGCheckpointRunner.GetCheckpointMetaFiles())
	if err != nil {
		resp.Succeed = false
		return nil, err
	}
	resp.Succeed = true
	resp.Entries = make([]*cmd_util.CheckpointEntryResp, 0, len(checkpoints))
	for _, ckp := range checkpoints {
		start := ckp.GetStart().ToTimestamp()
		end := ckp.GetEnd().ToTimestamp()
		resp.Entries = append(resp.Entries, &cmd_util.CheckpointEntryResp{
			Start:     &start,
			End:       &end,
			Location1: ckp.GetLocation(),
			Location2: ckp.GetTNLocation(),
			EntryType: int32(ckp.GetType()),
			Version:   ckp.GetVersion(),
		})
	}
	return nil, nil
}

func (h *Handle) HandleStorageUsage(ctx context.Context, meta txn.TxnMeta,
	req *cmd_util.StorageUsageReq, resp *cmd_util.StorageUsageResp_V3) (func(), error) {
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

	//specialSize := memo.GatherSpecialTableSize()
	specialSize := uint64(0)
	usages := memo.GatherAllAccSize()
	for accId := range usages {
		if accId != uint64(catalog.System_Account) {
			usages[accId][0] += specialSize
		}
	}

	newIds := make([]uint64, 0)
	for _, id := range req.AccIds {
		if usages != nil {
			if size, exist := usages[uint64(id)]; exist {
				memo.AddReqTrace(uint64(id), size[0], start, "req")
				resp.AccIds = append(resp.AccIds, int64(id))
				resp.Sizes = append(resp.Sizes, size[0])
				resp.SnapshotSizes = append(resp.SnapshotSizes, size[1])
				delete(usages, uint64(id))
				continue
			}
		}
		// new account which haven't been collect
		newIds = append(newIds, uint64(id))
	}

	for accId, size := range usages {
		memo.AddReqTrace(uint64(accId), size[0], start, "oth")
		resp.AccIds = append(resp.AccIds, int64(accId))
		resp.Sizes = append(resp.Sizes, size[0])
		resp.SnapshotSizes = append(resp.SnapshotSizes, size[1])
	}

	//var notReadyNewAcc []uint64

	// new accounts
	traverseCatalogForNewAccounts(h.db.Catalog, memo, newIds)
	for idx := range newIds {
		if size, snapshotSize, exist := memo.GatherNewAccountSize(uint64(newIds[idx])); exist {
			size += specialSize
			resp.AccIds = append(resp.AccIds, int64(newIds[idx]))
			resp.Sizes = append(resp.Sizes, size)
			resp.SnapshotSizes = append(resp.SnapshotSizes, snapshotSize)
			memo.AddReqTrace(uint64(newIds[idx]), size, start, "new, ready")
		}
		//else {
		//	notReadyNewAcc = append(notReadyNewAcc, newIds[idx])
		//}
	}

	memo.ClearNewAccCache()

	//for idx := range notReadyNewAcc {
	//	resp.AccIds = append(resp.AccIds, int64(notReadyNewAcc[idx]))
	//	resp.Sizes = append(resp.Sizes, specialSize)
	//	memo.AddReqTrace(uint64(newIds[idx]), specialSize, start, "new, not ready, only special")
	//}

	abstract := memo.GatherObjectAbstractForAllAccount()
	for _, acc := range resp.AccIds {
		resp.ObjCnts = append(resp.ObjCnts, uint64(abstract[uint64(acc)].TotalObjCnt))
		resp.BlkCnts = append(resp.BlkCnts, uint64(abstract[uint64(acc)].TotalBlkCnt))
		resp.RowCnts = append(resp.RowCnts, uint64(abstract[uint64(acc)].TotalRowCnt))
	}

	resp.Succeed = true

	return nil, nil
}

func (h *Handle) HandleGetChangedTableList(
	ctx context.Context,
	meta txn.TxnMeta,
	req *cmd_util.GetChangedTableListReq,
	resp *cmd_util.GetChangedTableListResp,
) (func(), error) {

	isTheTblIWant := func(tblId uint64, commit types.TS) bool {
		if slices.Index(resp.TableIds, tblId) != -1 {
			// already exist
			return false
		}

		if idx := slices.Index(req.TableIds, tblId); idx == -1 {
			// not the tbl I want to check
			return false
		} else {
			ts := types.TimestampToTS(*req.From[idx])
			if commit.LT(&ts) {
				return false
			}
		}

		return true
	}

	minFrom := slices.MinFunc(req.From, func(a, b *timestamp.Timestamp) int {
		return a.Compare(*b)
	})
	from := types.TimestampToTS(*minFrom)
	now := types.BuildTS(time.Now().UnixNano(), 0)

	var (
		err     error
		dbEntry *catalog2.DBEntry
		data    = &logtail.CheckpointData{}
	)

	logErr := func(e error, hint string) {
		logutil.Info("handle get changed table list failed",
			zap.Error(e),
			zap.String("hint", hint))
	}

	ckps := h.GetDB().BGCheckpointRunner.GetAllCheckpoints()
	for i := 0; i < len(ckps); i++ {
		if ckps[i] == nil {
			continue
		}

		ckpEnd := ckps[i].GetEnd()
		if ckpEnd.LT(&from) {
			continue
		}

		if data, err = ckps[i].PrefetchMetaIdx(ctx, h.GetDB().Runtime.Fs); err != nil {
			logErr(err, ckps[i].String())
			return nil, err
		}

		if err = ckps[i].ReadMetaIdx(ctx, h.GetDB().Runtime.Fs, data); err != nil {
			logErr(err, ckps[i].String())
			return nil, err
		}

		if err = ckps[i].Prefetch(ctx, h.GetDB().Runtime.Fs, data); err != nil {
			logErr(err, ckps[i].String())
			return nil, err
		}

		if err = ckps[i].Read(ctx, h.GetDB().Runtime.Fs, data); err != nil {
			logErr(err, ckps[i].String())
			return nil, err
		}

		dataObjBat := data.GetObjectBatchs()
		tombstoneObjBat := data.GetTombstoneObjectBatchs()

		bats := []*containers.Batch{dataObjBat, tombstoneObjBat}

		for j := range bats {
			for k := range bats[j].Length() {
				dbIdVec := bats[j].GetVectorByName(logtail.SnapshotAttr_DBID)
				tblIdVec := bats[j].GetVectorByName(logtail.SnapshotAttr_TID)
				commitVec := bats[j].GetVectorByName(objectio.DefaultCommitTS_Attr)
				if dbIdVec.Length() <= k || tblIdVec.Length() <= k || commitVec.Length() <= k {
					logutil.Error("dbId/tblId/commit vector length not match",
						zap.String("dbId vector", dbIdVec.String()),
						zap.String("tblId vector", tblIdVec.String()),
						zap.String("commit vector", commitVec.String()))

					// some wrong, return quickly?
					//resp.AccIds = req.AccIds
					//resp.TableIds = req.TableIds
					//resp.DatabaseIds = req.DatabaseIds
					//tt := now.ToTimestamp()
					//resp.Newest = &tt

					return nil, nil
				}

				dbId := dbIdVec.Get(k).(uint64)
				tblId := tblIdVec.Get(k).(uint64)
				commit := commitVec.Get(k).(types.TS)

				if !isTheTblIWant(tblId, commit) {
					continue
				}

				dbEntry, err = h.GetDB().Catalog.GetDatabaseByID(dbId)
				if err != nil {
					logErr(err, fmt.Sprintf("get db entry failed: %d", dbId))
					return nil, err
				}

				resp.TableIds = append(resp.TableIds, tblId)
				resp.DatabaseIds = append(resp.DatabaseIds, dbId)
				resp.AccIds = append(resp.AccIds, uint64(dbEntry.GetTenantID()))
			}
		}
	}

	rr := h.db.LogtailMgr.GetReader(from, now)

	for i := range req.TableIds {
		tree := rr.GetDirtyByTable(req.DatabaseIds[i], req.TableIds[i])
		if tree.IsEmpty() {
			continue
		}

		// prev() for ut
		if !isTheTblIWant(req.TableIds[i], types.MaxTs().Prev()) {
			continue
		}

		resp.TableIds = append(resp.TableIds, req.TableIds[i])
		resp.DatabaseIds = append(resp.DatabaseIds, req.DatabaseIds[i])
		resp.AccIds = append(resp.AccIds, req.AccIds[i])
	}

	tt := now.ToTimestamp()
	resp.Newest = &tt

	return nil, nil
}

func (h *Handle) HandleFlushTable(
	ctx context.Context,
	meta txn.TxnMeta,
	req *cmd_util.FlushTable,
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
	req *cmd_util.Checkpoint,
	resp *api.SyncLogTailResp) (cb func(), err error) {

	timeout := req.FlushDuration

	currTs := types.BuildTS(time.Now().UTC().UnixNano(), 0)

	err = h.db.ForceGlobalCheckpoint(ctx, currTs, timeout, 0)
	return nil, err
}

func (h *Handle) HandleForceCheckpoint(
	ctx context.Context,
	meta txn.TxnMeta,
	req *cmd_util.Checkpoint,
	resp *api.SyncLogTailResp) (cb func(), err error) {

	timeout := req.FlushDuration

	currTs := types.BuildTS(time.Now().UTC().UnixNano(), 0)

	err = h.db.ForceCheckpoint(ctx, currTs, timeout)
	return nil, err
}

func (h *Handle) HandleBackup(
	ctx context.Context,
	meta txn.TxnMeta,
	req *cmd_util.Checkpoint,
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
	compact := h.db.BGCheckpointRunner.GetCompacted()
	data := h.db.BGCheckpointRunner.GetAllCheckpointsForBackup(compact)
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

func (h *Handle) HandleDiskCleaner(
	ctx context.Context,
	meta txn.TxnMeta,
	req *cmd_util.DiskCleaner,
	resp *api.SyncLogTailResp) (cb func(), err error) {

	op := req.Op
	key := req.Key
	value := req.Value
	if op == cmd_util.RemoveChecker {
		return nil, h.db.DiskCleaner.GetCleaner().RemoveChecker(key)
	}
	switch key {
	case cmd_util.CheckerKeyTTL:
		// Set a ttl, checkpoints whose endTS is less than this ttl can be consumed
		var ttl time.Duration
		ttl, err = time.ParseDuration(value)
		if err != nil {
			logutil.Errorf("parse ttl failed: %v", err)
			return nil, err
		}
		// ttl should be at least 1 hour,
		if ttl < time.Hour {
			logutil.Errorf("ttl should be at least 1 hour")
			return nil, moerr.NewInvalidArgNoCtx(key, value)
		}
		h.db.DiskCleaner.GetCleaner().AddChecker(
			func(item any) bool {
				checkpoint := item.(*checkpoint.CheckpointEntry)
				ts := types.BuildTS(time.Now().UTC().UnixNano()-int64(ttl), 0)
				endTS := checkpoint.GetEnd()
				return !endTS.GE(&ts)
			}, cmd_util.CheckerKeyTTL)
		return
	case cmd_util.CheckerKeyMinTS:
		// Set a minTS, checkpoints whose endTS is less than this minTS can be consumed
		var ts types.TS
		var pTime int64
		var lTime uint64
		tmp := strings.Split(value, "-")
		if len(tmp) != 2 {
			return nil, moerr.NewInvalidArgNoCtx(key, value)
		}

		pTime, err = strconv.ParseInt(tmp[0], 10, 64)
		if err != nil {
			return nil, err
		}

		lTime, err = strconv.ParseUint(tmp[1], 10, 32)
		if err != nil {
			return nil, err
		}
		ts = types.BuildTS(pTime, uint32(lTime))
		h.db.DiskCleaner.GetCleaner().AddChecker(
			func(item any) bool {
				ckp := item.(*checkpoint.CheckpointEntry)
				end := ckp.GetEnd()
				return !end.GE(&ts)
			}, cmd_util.CheckerKeyMinTS)
		return
	default:
		return nil, moerr.NewInvalidArgNoCtx(key, value)
	}
}

func (h *Handle) HandleInterceptCommit(
	ctx context.Context,
	meta txn.TxnMeta,
	req *cmd_util.InterceptCommit,
	resp *api.SyncLogTailResp) (cb func(), err error) {

	name := req.TableName
	h.UpdateInterceptMatchRegexp(name)
	return nil, err
}

func (h *Handle) HandleInspectTN(
	ctx context.Context,
	meta txn.TxnMeta,
	req *cmd_util.InspectTN,
	resp *cmd_util.InspectResp) (cb func(), err error) {
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

func (h *Handle) HandleCommitMerge(
	ctx context.Context,
	meta txn.TxnMeta,
	req *api.MergeCommitEntry,
	resp *api.TNStringResponse,
) (err error) {

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
	txn.GetMemo().IsFlushOrMerge = true
	if err != nil {
		return
	}
	ids := make([]objectio.ObjectId, 0, len(req.MergedObjs))
	for _, o := range req.MergedObjs {
		stat := objectio.ObjectStats(o)
		ids = append(ids, *stat.ObjectName().ObjectId())
	}
	h.GetDB().MergeScheduler.RemoveCNActiveObjects(ids)
	if req.Err != "" {
		resp.ReturnStr = req.Err
		err = moerr.NewInternalErrorf(ctx, "merge err in cn: %s", req.Err)
		return
	}

	defer func() {
		if err != nil {
			resp.ReturnStr = err.Error()
			merge.CleanUpUselessFiles(req, h.db.Runtime.Fs.Service)
		}
	}()

	transferMaps, err := marshalTransferMaps(ctx, req, h.db.Runtime.SID(), h.db.Runtime.Fs.Service)
	if err != nil {
		return err
	}
	_, err = jobs.HandleMergeEntryInTxn(ctx, txn, txn.String(), req, transferMaps, h.db.Runtime, false)
	if err != nil {
		return
	}
	b := new(bytes.Buffer)
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
	resp.ReturnStr = b.String()
	return
}

func (h *Handle) HandleGetLatestCheckpoint(
	_ context.Context,
	_ txn.TxnMeta,
	_ *cmd_util.Checkpoint,
	resp *api.CheckpointResp,
) (cb func(), err error) {
	var locations string
	data := h.db.BGCheckpointRunner.GetAllCheckpoints()
	for i := range data {
		locations += data[i].GetLocation().String()
		locations += ":"
		locations += fmt.Sprintf("%d", data[i].GetVersion())
		locations += ";"
		if resp.TruncateLsn < data[i].GetTruncateLsn() {
			resp.TruncateLsn = data[i].GetTruncateLsn()
		}
	}
	resp.Location = locations
	return nil, err
}

func marshalTransferMaps(
	ctx context.Context,
	req *api.MergeCommitEntry,
	sid string,
	fs fileservice.FileService,
) (api.TransferMaps, error) {
	if len(req.BookingLoc) > 0 {
		// load transfer info from s3
		if req.Booking != nil {
			logutil.Error("mergeblocks err booking loc is not empty, but booking is not nil")
		}

		blkCnt := types.DecodeInt32(util.UnsafeStringToBytes(req.BookingLoc[0]))
		booking := make(api.TransferMaps, blkCnt)
		for i := range blkCnt {
			rowCnt := types.DecodeInt32(util.UnsafeStringToBytes(req.BookingLoc[i+1]))
			booking[i] = make(api.TransferMap, rowCnt)
		}
		req.BookingLoc = req.BookingLoc[blkCnt+1:]
		locations := req.BookingLoc
		for _, filepath := range locations {
			reader, err := blockio.NewFileReader(sid, fs, filepath)
			if err != nil {
				return nil, err
			}
			bats, releases, err := reader.LoadAllColumns(ctx, nil, nil)
			if err != nil {
				return nil, err
			}

			for _, bat := range bats {
				for i := range bat.RowCount() {
					srcBlk := vector.GetFixedAtNoTypeCheck[int32](bat.Vecs[0], i)
					srcRow := vector.GetFixedAtNoTypeCheck[uint32](bat.Vecs[1], i)
					destObj := vector.GetFixedAtNoTypeCheck[uint8](bat.Vecs[2], i)
					destBlk := vector.GetFixedAtNoTypeCheck[uint16](bat.Vecs[3], i)
					destRow := vector.GetFixedAtNoTypeCheck[uint32](bat.Vecs[4], i)

					booking[srcBlk][srcRow] = api.TransferDestPos{
						ObjIdx: destObj,
						BlkIdx: destBlk,
						RowIdx: destRow,
					}
				}
			}
			releases()
			_ = fs.Delete(ctx, filepath)
		}
		return booking, nil
	} else if req.Booking != nil {
		booking := make(api.TransferMaps, len(req.Booking.Mappings))
		for i := range booking {
			booking[i] = make(api.TransferMap, len(req.Booking.Mappings[i].M))
		}
		for i, m := range req.Booking.Mappings {
			for r, pos := range m.M {
				booking[i][uint32(r)] = api.TransferDestPos{
					ObjIdx: uint8(pos.ObjIdx),
					BlkIdx: uint16(pos.BlkIdx),
					RowIdx: uint32(pos.RowIdx),
				}
			}
		}
		return booking, nil
	}
	return nil, nil
}
