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
	"time"

	"github.com/google/shlex"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
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
				resp.AccIds = append(resp.AccIds, int64(id))
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
		resp.AccIds = append(resp.AccIds, int64(accId))
		resp.Sizes = append(resp.Sizes, size)
	}

	// new accounts
	traverseCatalogForNewAccounts(h.db.Catalog, memo, newIds)

	for idx := range newIds {
		if size, exist := memo.GatherNewAccountSize(uint64(newIds[idx])); exist {
			resp.AccIds = append(resp.AccIds, int64(newIds[idx]))
			resp.Sizes = append(resp.Sizes, size)
			memo.AddReqTrace(uint64(newIds[idx]), size, start, "new")
		}
	}

	memo.ClearNewAccCache()

	resp.Succeed = true

	return nil, nil
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
