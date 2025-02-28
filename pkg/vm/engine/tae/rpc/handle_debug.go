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
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/cmd_util"
	catalog2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/merge"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/jobs"
	"go.uber.org/zap"
)

const (
	DefaultTimeout = time.Minute * 3 / 2
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
	return nil, h.db.AddFaultPoint(ctx, req.Name, req.Freq, req.Action, req.Iarg, req.Sarg, req.Constant)
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
		h.db.Runtime.Fs,
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

func getChangedListFromCheckpoints(
	ctx context.Context,
	from types.TS,
	to types.TS,
	h *Handle,
	isTheTblIWant func(exists []uint64, tblId uint64, ts types.TS) bool,
) (accIds, dbIds, tblIds []uint64, err error) {

	var (
		dbEntry *catalog2.DBEntry
	)

	logErr := func(e error, hint string) {
		logutil.Info("handle get changed table list from ckp failed",
			zap.Error(e),
			zap.String("hint", hint))
	}

	ckps := h.GetDB().BGCheckpointRunner.GetAllCheckpoints()
	tblIds = make([]uint64, 0)
	readers := make([]*logtail.CKPReader, len(ckps))
	for i := 0; i < len(ckps); i++ {

		if ckps[i] == nil {
			continue
		}
		if !ckps[i].HasOverlap(from, to) {
			continue
		}
		ioutil.Prefetch(
			h.GetDB().Runtime.SID(),
			h.GetDB().Runtime.Fs,
			ckps[i].GetLocation(),
		)
	}
	for i := 0; i < len(ckps); i++ {

		if ckps[i] == nil {
			continue
		}
		if !ckps[i].HasOverlap(from, to) {
			continue
		}
		readers[i] = logtail.NewCKPReader(
			ckps[i].GetVersion(),
			ckps[i].GetLocation(),
			common.CheckpointAllocator,
			h.GetDB().Runtime.Fs,
		)
		if err = readers[i].ReadMeta(ctx); err != nil {
			return
		}
		readers[i].PrefetchData(h.GetDB().Runtime.SID())
	}
	for i := 0; i < len(ckps); i++ {

		if ckps[i] == nil {
			continue
		}
		if !ckps[i].HasOverlap(from, to) {
			continue
		}
		readers[i].ForEachRow(
			ctx,
			func(
				accout uint32,
				dbid, tid uint64,
				objectType int8,
				objectStats objectio.ObjectStats,
				create, delete types.TS,
				rowID types.Rowid,
			) error {
				commit := create
				if !delete.IsEmpty() {
					commit = delete
				}
				if !isTheTblIWant(tblIds, tid, commit) {
					return nil
				}
				dbEntry, err = h.GetDB().Catalog.GetDatabaseByID(dbid)
				if err != nil {
					logErr(err, fmt.Sprintf("get db entry failed: %d", dbid))
					return nil
				}

				tblIds = append(tblIds, tid)
				dbIds = append(dbIds, dbid)
				accIds = append(accIds, uint64(dbEntry.GetTenantID()))
				return nil
			},
		)
	}
	return
}

func getChangedListFromDirtyTree(
	ctx context.Context,
	from types.TS,
	to types.TS,
	h *Handle,
	isTheTblIWant func(exists []uint64, tblId uint64, ts types.TS) bool,
) (accIds, dbIds, tblIds []uint64, err error) {

	var (
		dbEntry  *catalog2.DBEntry
		truncate = h.db.LogtailMgr.GetTruncateTS()
		reader   *logtail.Reader
	)

	if truncate.GT(&from) {
		from = truncate
	}

	reader = h.db.LogtailMgr.GetReader(from, to)
	tree, _ := reader.GetDirty()

	for _, v := range tree.Tables {
		if v == nil || v.IsEmpty() {
			continue
		}

		// prev() for ut
		if !isTheTblIWant(tblIds, v.ID, types.MaxTs().Prev()) {
			continue
		}

		dbEntry, err = h.GetDB().Catalog.GetDatabaseByID(v.DbID)
		if err != nil {
			continue
		}

		tblIds = append(tblIds, v.ID)
		dbIds = append(dbIds, v.DbID)
		accIds = append(accIds, uint64(dbEntry.GetTenantID()))
	}

	return
}

func (h *Handle) HandleGetChangedTableList(
	ctx context.Context,
	meta txn.TxnMeta,
	req *cmd_util.GetChangedTableListReq,
	resp *cmd_util.GetChangedTableListResp,
) (func(), error) {

	var (
		to   types.TS
		from types.TS
	)

	defer func() {
		tt := to.ToTimestamp()
		resp.Newest = &tt
	}()

	if len(req.TableIds) == 0 && len(req.TS) == 0 {
		to = types.BuildTS(time.Now().UnixNano(), 0)
		return nil, nil
	}

	if req.Type == cmd_util.CheckChanged {
		to = types.BuildTS(time.Now().UnixNano(), 0)
		minFrom := slices.MinFunc(req.TS, func(a, b *timestamp.Timestamp) int {
			return a.Compare(*b)
		})
		from = types.TimestampToTS(*minFrom)
	} else {
		to = types.TimestampToTS(*req.TS[1])
		from = types.TimestampToTS(*req.TS[0])
	}

	var (
		err    error
		accIds []uint64
		dbIds  []uint64
		tblIds []uint64
	)

	isTheTblIWant := func(innerExist []uint64, tblId uint64, commit types.TS) bool {
		if slices.Index(tblIds, tblId) != -1 || slices.Index(innerExist, tblId) != -1 {
			// already exist
			return false
		}

		if req.Type == cmd_util.CheckChanged {
			if idx := slices.Index(req.TableIds, tblId); idx == -1 {
				// not the tbl I want to check
				return false
			} else {
				ts := types.TimestampToTS(*req.TS[idx])
				if commit.LT(&ts) {
					return false
				}
			}

			return true

		} else if req.Type == cmd_util.CollectChanged {
			// collecting changed list
			skip := types.MaxTs().Prev()
			if !commit.Equal(&skip) && (commit.LT(&from) || commit.GT(&to)) {
				return false
			}

			return true
		}

		return false
	}

	accIds, dbIds, tblIds, err = getChangedListFromCheckpoints(ctx, from, to, h, isTheTblIWant)
	if err != nil {
		return nil, err
	}

	accIds2, dbIds2, tblIds2, err := getChangedListFromDirtyTree(ctx, from, to, h, isTheTblIWant)
	if err != nil {
		return nil, err
	}

	accIds = append(accIds, accIds2...)
	dbIds = append(dbIds, dbIds2...)
	tblIds = append(tblIds, tblIds2...)

	resp.TableIds = append(resp.TableIds, tblIds...)
	resp.AccIds = append(resp.AccIds, accIds...)
	resp.DatabaseIds = append(resp.DatabaseIds, dbIds...)

	common.DoIfDebugEnabled(func() {
		buf := bytes.Buffer{}
		for i := range resp.TableIds {
			buf.WriteString(fmt.Sprintf("%d-%d-%d; ",
				resp.AccIds[i], resp.DatabaseIds[i], resp.TableIds[i]))
		}
		logutil.Infof("handle get changed table list ret: %s", buf.String())
	})

	return nil, nil
}

func (h *Handle) HandleFlushTable(
	ctx context.Context,
	meta txn.TxnMeta,
	req *cmd_util.FlushTable,
	resp *api.SyncLogTailResp,
) (cb func(), err error) {
	err = h.db.FlushTable(
		ctx,
		req.AccessInfo.AccountID,
		req.DatabaseID,
		req.TableID,
		h.db.TxnMgr.Now(),
	)
	return
}

func (h *Handle) HandleForceGlobalCheckpoint(
	ctx context.Context,
	meta txn.TxnMeta,
	req *cmd_util.Checkpoint,
	resp *api.SyncLogTailResp,
) (cb func(), err error) {
	var (
		timeout = req.FlushDuration
		now     = h.db.TxnMgr.Now()
	)

	if timeout == 0 {
		timeout = DefaultTimeout
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	err = h.db.ForceGlobalCheckpoint(ctx, now, 0)
	return
}

func (h *Handle) HandleForceCheckpoint(
	ctx context.Context,
	meta txn.TxnMeta,
	req *cmd_util.Checkpoint,
	resp *api.SyncLogTailResp,
) (cb func(), err error) {
	var (
		timeout = req.FlushDuration
		now     = h.db.TxnMgr.Now()
	)

	if timeout == 0 {
		timeout = DefaultTimeout
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	err = h.db.ForceCheckpoint(ctx, now)
	return
}

func (h *Handle) HandleBackup(
	ctx context.Context,
	meta txn.TxnMeta,
	req *cmd_util.Checkpoint,
	resp *api.SyncLogTailResp,
) (cb func(), err error) {
	var (
		timeout    = req.FlushDuration
		backupTime = time.Now().UTC()
		currTs     = types.BuildTS(backupTime.UnixNano(), 0)
		locations  string
		location   string
	)

	locations += backupTime.Format(time.DateTime) + ";"

	if timeout == 0 {
		timeout = DefaultTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	if location, err = h.db.ForceCheckpointForBackup(ctx, currTs); err != nil {
		return
	}

	compactEntry := h.db.BGCheckpointRunner.GetCompacted()
	entries := h.db.BGCheckpointRunner.GetAllCheckpointsForBackup(compactEntry)
	locations += location + ";"
	for i := range entries {
		locations += entries[i].GetLocation().String()
		locations += ":"
		locations += fmt.Sprintf("%d", entries[i].GetVersion())
		locations += ";"
	}
	resp.CkpLocation = locations
	return
}

func (h *Handle) HandleDiskCleaner(
	ctx context.Context,
	meta txn.TxnMeta,
	req *cmd_util.DiskCleaner,
	resp *api.SyncLogTailResp,
) (cb func(), err error) {
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
			merge.CleanUpUselessFiles(req, h.db.Runtime.Fs)
		}
	}()

	transferMaps, err := marshalTransferMaps(ctx, req, h.db.Runtime.SID(), h.db.Runtime.Fs)
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
			reader, err := ioutil.NewFileReader(fs, filepath)
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

func (h *Handle) HandleFaultInject(
	ctx context.Context,
	meta txn.TxnMeta,
	req *cmd_util.FaultInjectReq,
	resp *api.TNStringResponse,
) (cb func(), err error) {
	resp.ReturnStr = fault.HandleFaultInject(ctx, req.Method, req.Parameter)
	return nil, nil
}
