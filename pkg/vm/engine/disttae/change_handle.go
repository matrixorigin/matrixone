// Copyright 2022 Matrix Origin
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

package disttae

import (
	"context"
	"fmt"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/cmd_util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"go.uber.org/zap"
	"sync"
)

const DefaultLoadParallism = 20

// NewPartitionStateChangesHandler is the function used to create a ChangeHandler
// from the partition state. It is a variable so tests can stub it.
var NewPartitionStateChangesHandler = logtailreplay.NewChangesHandler

var newPartitionChangesHandle = func(
	ctx context.Context,
	tbl *txnTable,
	from, to types.TS,
	skipDeletes bool,
	snapshotReadPolicy engine.SnapshotReadPolicy,
	mp *mpool.MPool,
) (engine.ChangesHandle, error) {
	return NewPartitionChangesHandle(
		ctx, tbl, from, to, skipDeletes, snapshotReadPolicy, mp,
	)
}

func GetPartitionStateStart(
	ctx context.Context,
	rel engine.Relation,
) (types.TS, error) {
	var tbl *txnTable
	var ok bool
	if tbl, ok = rel.(*txnTable); !ok {
		tbl = rel.(*txnTableDelegate).origin
	}
	state, err := tbl.getPartitionState(ctx)
	if err != nil {
		return types.TS{}, err
	}
	return state.GetStart(), nil
}

func (tbl *txnTable) CollectChanges(
	ctx context.Context,
	from, to types.TS,
	skipDeletes bool,
	mp *mpool.MPool,
) (engine.ChangesHandle, error) {
	// In-memory logtail rows are stored in physical seqnum order, while
	// table_changes exposes the current logical schema order. Capture the
	// current logical-to-physical mapping before constructing any deferred
	// range handle; normal table_changes callers do not pass through the
	// compaction paths that normally initialize this cache.
	if tbl.tableDef != nil {
		tbl.ensureSeqnumsAndTypesExpectRowid()
	}
	if from.IsEmpty() && !useBoundedVisibleStateRange(ctx) {
		return NewCheckpointChangesHandle(ctx, tbl, to, mp)
	}
	return newPartitionChangesHandle(
		ctx,
		tbl,
		from,
		to,
		skipDeletes,
		engine.SnapshotReadPolicyFromContext(ctx),
		mp,
	)
}

// useBoundedVisibleStateRange keeps opt-in visible-state callers on the
// range-aware path even when their lower watermark is empty. In particular,
// table_changes supplies both this policy and a ChangeRangeLimit; routing it
// through CheckpointChangesHandle would bypass that limit while decoding
// legacy persisted columns.
func useBoundedVisibleStateRange(ctx context.Context) bool {
	return engine.SnapshotReadPolicyFromContext(ctx) == engine.SnapshotReadPolicyVisibleState &&
		engine.ChangeRangeLimitFromContext(ctx).Enabled()
}

type PartitionChangesHandle struct {
	currentChangeHandle engine.ChangesHandle
	currentPSFrom       types.TS
	currentPSTo         types.TS
	closeMu             sync.Mutex
	handleIdx           int

	fromTs types.TS
	toTs   types.TS
	tbl    *txnTable

	skipDeletes         bool
	primarySeqnum       int
	snapshotReadPolicy  engine.SnapshotReadPolicy
	preserveAllVersions bool
	mp                  *mpool.MPool
	fs                  fileservice.FileService

	bufferedBatches     []queuedChangeBatch
	currentRangeDrained bool
	visibleResources    engine.VisibleStateRecoveryResources
	visibleStartRel     engine.Relation
}

type queuedChangeBatch struct {
	data          *batch.Batch
	tombstone     *batch.Batch
	hint          engine.ChangesHandle_Hint
	reservedBytes int64
}

func NewPartitionChangesHandle(
	ctx context.Context,
	tbl *txnTable,
	from, to types.TS,
	skipDeletes bool,
	snapshotReadPolicy engine.SnapshotReadPolicy,
	mp *mpool.MPool,
) (*PartitionChangesHandle, error) {
	if to.IsEmpty() || from.GT(&to) {
		return nil, moerr.NewInternalErrorNoCtx("invalid timestamp")
	}
	handle := &PartitionChangesHandle{
		tbl:                 tbl,
		fromTs:              from,
		toTs:                to,
		skipDeletes:         skipDeletes,
		primarySeqnum:       tbl.primarySeqnum,
		snapshotReadPolicy:  snapshotReadPolicy,
		preserveAllVersions: engine.CollectChangesPreserveAllVersionsFromContext(ctx),
		mp:                  mp,
		fs:                  tbl.getTxn().engine.fs,
	}
	if snapshotReadPolicy == engine.SnapshotReadPolicyVisibleState {
		handle.visibleResources = engine.VisibleStateRecoveryResourcesFromContext(ctx)
		handle.visibleStartRel = engine.VisibleStateStartRelationFromContext(ctx)
	}
	end, err := handle.getNextChangeHandle(ctx)
	if err != nil {
		return nil, err
	}
	if end {
		return nil, moerr.NewInternalErrorNoCtx(fmt.Sprintf("logic error:from %s to %s", from.ToString(), to.ToString()))
	}
	return handle, err
}

func (h *PartitionChangesHandle) Next(ctx context.Context, mp *mpool.MPool) (data, tombstone *batch.Batch, hint engine.ChangesHandle_Hint, err error) {
	// DATA BRANCH supplies governed recovery resources and needs failure-atomic
	// replay so a missing compacted predecessor can be rebuilt from the two
	// visible boundary snapshots. Bounded table_changes callers intentionally
	// omit these resources and keep the streaming range path below.
	if h.snapshotReadPolicy == engine.SnapshotReadPolicyVisibleState && h.visibleResources != nil {
		return h.nextWithSnapshotRecovery(ctx, mp)
	}
	return h.nextReplay(ctx, mp)
}

func (h *PartitionChangesHandle) collectChangesContext(ctx context.Context) context.Context {
	if h.preserveAllVersions {
		return engine.WithCollectChangesPreserveAllVersions(ctx)
	}
	return ctx
}

func (h *PartitionChangesHandle) nextReplay(ctx context.Context, mp *mpool.MPool) (data, tombstone *batch.Batch, hint engine.ChangesHandle_Hint, err error) {
	for {
		data, tombstone, hint, err = h.currentChangeHandle.Next(ctx, mp)
		if err != nil {
			return
		}
		if data != nil || tombstone != nil {
			return
		}
		var end bool
		end, err = h.getNextChangeHandle(
			ctx,
		)
		if err != nil {
			return
		}
		if end {
			return
		}
	}
}

func (h *PartitionChangesHandle) nextWithSnapshotRecovery(ctx context.Context, mp *mpool.MPool) (data, tombstone *batch.Batch, hint engine.ChangesHandle_Hint, err error) {
	hint = engine.ChangesHandle_Tail_done
	for {
		if len(h.bufferedBatches) > 0 {
			next := h.bufferedBatches[0]
			h.bufferedBatches[0] = queuedChangeBatch{}
			h.bufferedBatches = h.bufferedBatches[1:]
			if len(h.bufferedBatches) == 0 {
				h.bufferedBatches = nil
			}
			h.releaseBufferedReservation(next.reservedBytes)
			return next.data, next.tombstone, next.hint, nil
		}
		if h.currentRangeDrained {
			var end bool
			end, err = h.getNextChangeHandle(ctx)
			if err != nil || end {
				return nil, nil, hint, err
			}
			h.currentRangeDrained = false
		}
		if err = h.bufferCurrentRange(ctx, mp); err != nil {
			return nil, nil, hint, err
		}
	}
}

func (h *PartitionChangesHandle) bufferCurrentRange(ctx context.Context, mp *mpool.MPool) (err error) {
	var queued []queuedChangeBatch
	snapshotStateRangeTried := false
	visibleStateTried := false
	cleanQueued := func() {
		for i := range queued {
			if queued[i].data != nil {
				queued[i].data.Clean(mp)
			}
			if queued[i].tombstone != nil {
				queued[i].tombstone.Clean(mp)
			}
			h.releaseBufferedReservation(queued[i].reservedBytes)
		}
		queued = nil
	}
	for {
		data, tombstone, hint, nextErr := h.currentChangeHandle.Next(ctx, mp)
		if nextErr != nil {
			if !isVisibleStateRecoveryError(nextErr) {
				cleanQueued()
				return nextErr
			}
			cleanQueued()
			if !snapshotStateRangeTried {
				snapshotStateRangeTried = true
				swapErr := h.swapCurrentHandleToSnapshotStateRange(ctx)
				if swapErr == nil {
					continue
				}
				if !isVisibleStateRecoveryError(swapErr) {
					return swapErr
				}
			}
			if !visibleStateTried {
				visibleStateTried = true
				if swapErr := h.swapCurrentHandleToVisibleState(ctx); swapErr != nil {
					return swapErr
				}
				continue
			}
			return nextErr
		}
		if data == nil && tombstone == nil {
			h.bufferedBatches = append(h.bufferedBatches, queued...)
			h.currentRangeDrained = true
			return nil
		}
		reservedBytes := bufferedChangeBatchBytes(data, tombstone)
		if err = h.visibleResources.ReserveBuffer(reservedBytes); err != nil {
			if data != nil {
				data.Clean(mp)
			}
			if tombstone != nil {
				tombstone.Clean(mp)
			}
			cleanQueued()
			return err
		}
		queued = append(queued, queuedChangeBatch{
			data: data, tombstone: tombstone, hint: hint, reservedBytes: reservedBytes,
		})
	}
}

const bufferedChangeBatchOverhead = int64(256)

func bufferedChangeBatchBytes(data, tombstone *batch.Batch) int64 {
	bytes := bufferedChangeBatchOverhead
	if data != nil {
		bytes += int64(data.Size())
	}
	if tombstone != nil {
		bytes += int64(tombstone.Size())
	}
	return bytes
}

func (h *PartitionChangesHandle) releaseBufferedReservation(bytes int64) {
	if bytes > 0 && h.visibleResources != nil {
		h.visibleResources.ReleaseBuffer(bytes)
	}
}

func (h *PartitionChangesHandle) loadCheckpointEntries(
	ctx context.Context,
	from types.TS,
) (
	checkpointEntries []*checkpoint.CheckpointEntry,
	minTS types.TS,
	maxTS types.TS,
	err error,
) {
	ctxWithDeadline, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	response, err := RequestSnapshotRead(ctxWithDeadline, h.tbl, &from)
	if err != nil {
		return nil, types.MaxTs(), types.TS{}, err
	}
	minTS = types.MaxTs()
	maxTS = types.TS{}
	resp, ok := response.(*cmd_util.SnapshotReadResp)
	if !ok || !resp.Succeed || len(resp.Entries) == 0 {
		return nil, minTS, maxTS, nil
	}
	checkpointEntries = make([]*checkpoint.CheckpointEntry, 0, len(resp.Entries))
	for _, entry := range resp.Entries {
		logutil.Debug("ChangesHandle-Split-CheckpointEntry", zap.String("entry", entry.String()))
		start := types.TimestampToTS(*entry.Start)
		end := types.TimestampToTS(*entry.End)
		if start.LT(&minTS) {
			minTS = start
		}
		if end.GT(&maxTS) {
			maxTS = end
		}
		checkpointEntry := checkpoint.NewCheckpointEntry("", start, end, checkpoint.EntryType(entry.EntryType))
		checkpointEntry.SetLocation(entry.Location1, entry.Location2)
		checkpointEntries = append(checkpointEntries, checkpointEntry)
	}
	return checkpointEntries, minTS, maxTS, nil
}

func (h *PartitionChangesHandle) getNextChangeHandle(ctx context.Context) (end bool, err error) {
	if h.currentPSTo.EQ(&h.toTs) {
		return true, nil
	}
	var nextFrom types.TS
	if h.currentPSFrom.IsEmpty() {
		nextFrom = h.fromTs
	} else {
		nextFrom = h.currentPSTo.Next()
	}
	if h.snapshotReadPolicy == engine.SnapshotReadPolicyVisibleState && h.visibleResources == nil {
		// Visible-state callers require the exact net effect of the requested
		// range. Build that range from its end snapshot up front and stream the
		// resulting handle one batch at a time. This avoids replaying first and
		// retaining the whole range in case a later object has been GC-ed.
		h.currentPSFrom = nextFrom
		h.currentPSTo = h.toTs
		h.handleIdx++
		if err = h.swapCurrentHandleToSnapshotStateRange(ctx); err != nil {
			return false, err
		}
		return false, nil
	}
	ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	state, err := h.tbl.getPartitionState(ctxWithTimeout)
	if err != nil {
		return false, err
	}
	stateStart := state.GetStart()
	if stateStart.LE(&nextFrom) {
		h.currentPSTo = h.toTs
		h.currentPSFrom = nextFrom
		if h.handleIdx != 0 {
			err = h.closeCurrentChangeHandle()
			if err != nil {
				return
			}
			logutil.Debug("ChangesHandle-Split change handles",
				zap.String("from", h.fromTs.ToString()),
				zap.String("to", h.toTs.ToString()),
				zap.String("ps from", h.currentPSFrom.ToString()),
				zap.String("ps to", h.currentPSTo.ToString()),
				zap.Int("handle idx", h.handleIdx),
			)
		}
		h.handleIdx++
		h.currentChangeHandle, err = NewPartitionStateChangesHandler(
			ctx,
			state,
			h.currentPSFrom,
			h.currentPSTo,
			h.skipDeletes,
			objectio.BlockMaxRows,
			h.primarySeqnum,
			h.mp,
			h.fs,
		)
		if err != nil {
			// If the partition state references GC-ed object files,
			// fall through to snapshot-read recovery for this range.
			// Only FileNotFound is recoverable; a real ErrStaleRead means
			// the partition state's logical range doesn't cover the request.
			if moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
				logutil.Warn("ChangesHandle-Split partition state file missing, falling back to snapshot read",
					zap.Uint64("table-id", h.tbl.tableId),
					zap.String("nextFrom", nextFrom.ToString()),
					zap.String("stateStart", stateStart.ToString()),
					zap.Error(err),
				)
				_ = h.closeCurrentChangeHandle()
				err = nil
			} else {
				return
			}
		} else {
			return
		}
	}

	logutil.Info("ChangesHandle-Split request snapshot read",
		zap.String("from", nextFrom.ToString()),
	)
	if h.snapshotReadPolicy == engine.SnapshotReadPolicyVisibleState {
		h.currentPSFrom = nextFrom
		h.currentPSTo = h.toTs
		h.handleIdx++
		_, err = initializeVisibleStateRange(
			func() error { return h.swapCurrentHandleToSnapshotStateRange(ctx) },
			func(error) error { return h.swapCurrentHandleToVisibleState(ctx) },
		)
		return false, err
	}
	var checkpointEntries []*checkpoint.CheckpointEntry
	var minTS, maxTS types.TS
	checkpointEntries, minTS, maxTS, err = h.loadCheckpointEntries(ctx, nextFrom)
	if err != nil {
		return
	}
	if nextFrom.LT(&minTS) || nextFrom.GT(&maxTS) {
		logutil.Info("ChangesHandle-Split stale read",
			zap.Uint64("table-id", h.tbl.tableId),
			zap.String("nextFrom", nextFrom.ToString()),
			zap.String("stateStart", stateStart.ToString()),
			zap.String("minTS", minTS.ToString()),
			zap.String("maxTS", maxTS.ToString()),
			zap.Int("checkpointEntries", len(checkpointEntries)),
			zap.Bool("nextFrom<minTS", nextFrom.LT(&minTS)),
			zap.Bool("nextFrom>maxTS", nextFrom.GT(&maxTS)),
		)
		return false, moerr.NewErrStaleReadNoCtx(minTS.ToString(), nextFrom.ToString())
	}
	h.currentPSFrom = nextFrom
	h.currentPSTo = maxTS
	if h.toTs.LT(&maxTS) {
		h.currentPSTo = h.toTs
	}
	logutil.Debug("ChangesHandle-Split change handles",
		zap.String("from", h.fromTs.ToString()),
		zap.String("to", h.toTs.ToString()),
		zap.String("ps from", h.currentPSFrom.ToString()),
		zap.String("ps to", h.currentPSTo.ToString()),
		zap.Int("handle idx", h.handleIdx),
	)
	h.handleIdx++
	err = h.closeCurrentChangeHandle()
	if err != nil {
		return
	}
	h.currentChangeHandle, err = logtailreplay.NewChangesHandlerWithCheckpointRangeRecovery(
		ctx,
		h.tbl.tableId,
		h.tbl.proc.Load().GetService(),
		checkpointEntries,
		h.currentPSFrom,
		h.currentPSTo,
		h.skipDeletes,
		objectio.BlockMaxRows,
		h.primarySeqnum,
		h.mp,
		h.fs,
	)
	if err != nil {
		return
	}
	return false, nil
}

func isVisibleStateRecoveryError(err error) bool {
	// The current range reader maps both physical object loss and compacted
	// blocks without evaluable per-row commit timestamps to ErrFileNotFound.
	return moerr.IsMoErrCode(err, moerr.ErrFileNotFound)
}

func initializeVisibleStateRange(
	initSnapshotRange func() error,
	initVisibleState func(snapshotErr error) error,
) (usedVisibleState bool, err error) {
	if err = initSnapshotRange(); err == nil {
		return false, nil
	}
	if !isVisibleStateRecoveryError(err) {
		return false, err
	}
	return true, initVisibleState(err)
}

func (h *PartitionChangesHandle) swapCurrentHandleToSnapshotStateRange(ctx context.Context) (err error) {
	ctx = h.collectChangesContext(ctx)
	if h.snapshotReadPolicy != engine.SnapshotReadPolicyVisibleState {
		return nil
	}
	snapshotTbl, err := h.getTxnTableAt(ctx, h.currentPSTo)
	if err != nil {
		return err
	}
	if snapshotTbl == nil {
		return moerr.NewErrStaleReadNoCtx(h.currentPSTo.ToString(), h.currentPSFrom.ToString())
	}
	if snapshotTbl.tableDef != nil {
		snapshotTbl.ensureSeqnumsAndTypesExpectRowid()
	}
	state, err := snapshotTbl.getPartitionState(ctx)
	if err != nil {
		return err
	}
	if err = h.closeCurrentChangeHandle(); err != nil {
		return err
	}
	pkFilter := engine.PKFilterFromContext(ctx)
	rangeLimit := engine.ChangeRangeLimitFromContext(ctx)
	spillConfig := engine.ChangeRangeSpillFromContext(ctx)
	debugLabel := engine.CollectChangesDebugLabelFromContext(ctx)
	retainRowID := engine.RetainRowIDFromContext(ctx)
	preserveAllVersions := h.preserveAllVersions
	rangeFrom, rangeTo := h.currentPSFrom, h.currentPSTo
	skipDeletes, primarySeqnum, primaryIdx := h.skipDeletes, snapshotTbl.primarySeqnum, snapshotTbl.primaryIdx
	logicalSeqnums := append([]uint16(nil), snapshotTbl.seqnums...)
	rangeMP, rangeFS := h.mp, h.fs
	h.currentChangeHandle = &deferredChangesHandle{
		build: func(nextCtx context.Context) (engine.ChangesHandle, error) {
			nextCtx = engine.WithPKFilter(nextCtx, pkFilter)
			nextCtx = engine.WithChangeRangeLimit(nextCtx, rangeLimit)
			nextCtx = engine.WithChangeRangeSpill(nextCtx, spillConfig)
			nextCtx = engine.WithCollectChangesDebugLabel(nextCtx, debugLabel)
			nextCtx = engine.WithRetainRowID(nextCtx, retainRowID)
			if preserveAllVersions {
				nextCtx = engine.WithCollectChangesPreserveAllVersions(nextCtx)
			}
			return logtailreplay.NewChangesHandlerWithPartitionStateRangeAndPrimaryIdx(
				nextCtx,
				state,
				rangeFrom,
				rangeTo,
				skipDeletes,
				objectio.BlockMaxRows,
				primarySeqnum,
				primaryIdx,
				logicalSeqnums,
				rangeMP,
				rangeFS,
			)
		},
	}
	return nil
}

func (h *PartitionChangesHandle) swapCurrentHandleToVisibleState(ctx context.Context) (err error) {
	if h.snapshotReadPolicy != engine.SnapshotReadPolicyVisibleState {
		return nil
	}
	if err = h.closeCurrentChangeHandle(); err != nil {
		return err
	}
	h.currentChangeHandle, err = NewVisibleStateChangesHandle(
		h.collectChangesContext(ctx),
		h.tbl,
		h.currentPSFrom,
		h.currentPSTo,
		h.skipDeletes,
		objectio.BlockMaxRows,
		h.mp,
		h.visibleResources,
		h.visibleStateStartRelation(),
	)
	return err
}

func (h *PartitionChangesHandle) visibleStateStartRelation() engine.Relation {
	if h.currentPSFrom.EQ(&h.fromTs) {
		return h.visibleStartRel
	}
	return nil
}

// deferredChangesHandle keeps CollectChanges construction cheap. The
// partition-state range is materialized only when the consumer requests its
// first batch, where any caller-provided range limit applies.
type deferredChangesHandle struct {
	build    func(context.Context) (engine.ChangesHandle, error)
	handle   engine.ChangesHandle
	buildErr error
}

func (h *deferredChangesHandle) Next(
	ctx context.Context,
	mp *mpool.MPool,
) (data, tombstone *batch.Batch, hint engine.ChangesHandle_Hint, err error) {
	if h.handle == nil && h.buildErr == nil {
		h.handle, h.buildErr = h.build(ctx)
		h.build = nil
	}
	if h.buildErr != nil {
		return nil, nil, engine.ChangesHandle_Tail_done, h.buildErr
	}
	return h.handle.Next(ctx, mp)
}

func (h *deferredChangesHandle) Close() error {
	if h == nil || h.handle == nil {
		return nil
	}
	err := h.handle.Close()
	h.handle = nil
	return err
}

func (h *PartitionChangesHandle) getTxnTableAt(ctx context.Context, at types.TS) (*txnTable, error) {
	_, _, rel, err := h.tbl.eng.GetRelationById(
		ctx,
		h.tbl.db.op.CloneSnapshotOp(at.ToTimestamp()),
		h.tbl.tableId,
	)
	if err != nil {
		return nil, err
	}
	if rel == nil {
		return nil, nil
	}
	if t, ok := rel.(*txnTable); ok {
		return t, nil
	}
	if t, ok := rel.(*txnTableDelegate); ok {
		return t.origin, nil
	}
	return nil, moerr.NewInternalErrorNoCtx("unexpected relation type in snapshot")
}
func (h *PartitionChangesHandle) Close() error {
	if h == nil {
		return nil
	}
	for i := range h.bufferedBatches {
		if h.bufferedBatches[i].data != nil {
			h.bufferedBatches[i].data.Clean(h.mp)
		}
		if h.bufferedBatches[i].tombstone != nil {
			h.bufferedBatches[i].tombstone.Clean(h.mp)
		}
		h.releaseBufferedReservation(h.bufferedBatches[i].reservedBytes)
	}
	h.bufferedBatches = nil
	return h.closeCurrentChangeHandle()
}

func (h *PartitionChangesHandle) closeCurrentChangeHandle() (err error) {
	if h == nil {
		return nil
	}
	h.closeMu.Lock()
	defer h.closeMu.Unlock()
	if h.currentChangeHandle != nil {
		err = h.currentChangeHandle.Close()
		h.currentChangeHandle = nil
	}
	return
}

type CheckpointChangesHandle struct {
	end    types.TS
	table  *txnTable
	fs     fileservice.FileService
	reader engine.Reader
	attrs  []string
	isEnd  bool

	sid         string
	blockList   objectio.BlockInfoSlice
	prefetchIdx int
	readIdx     int

	duration      time.Duration
	dataLength    int
	lastPrintTime time.Time
	retainRowID   bool
}

func NewCheckpointChangesHandle(
	ctx context.Context,
	table *txnTable,
	end types.TS,
	mp *mpool.MPool,
) (*CheckpointChangesHandle, error) {
	handle := &CheckpointChangesHandle{
		end:         end,
		table:       table,
		fs:          table.getTxn().engine.fs,
		sid:         table.proc.Load().GetService(),
		retainRowID: engine.RetainRowIDFromContext(ctx),
	}
	err := handle.initReader(ctx)
	return handle, err
}
func (h *CheckpointChangesHandle) prefetch() {
	blkCount := h.blockList.Len()
	for i := 0; i < DefaultLoadParallism; i++ {
		if h.prefetchIdx >= blkCount {
			return
		}
		blk := h.blockList.Get(h.prefetchIdx)
		err := ioutil.Prefetch(h.sid, h.fs, blk.MetaLoc[:])
		if err != nil {
			logutil.Warnf("ChangesHandle: prefetch failed: %v", err)
		}
		h.prefetchIdx++
	}
}
func (h *CheckpointChangesHandle) Next(
	ctx context.Context, mp *mpool.MPool,
) (
	data *batch.Batch,
	tombstone *batch.Batch,
	hint engine.ChangesHandle_Hint,
	err error,
) {
	if time.Since(h.lastPrintTime) > time.Minute {
		h.lastPrintTime = time.Now()
		if h.dataLength != 0 {
			logutil.Infof("ChangesHandle-Slow, data length %d, duration %v", h.dataLength, h.duration)
		}
	}
	select {
	case <-ctx.Done():
		return
	default:
	}
	hint = engine.ChangesHandle_Snapshot
	if h.isEnd {
		return nil, nil, hint, nil
	}
	tblDef := h.table.GetTableDef(ctx)
	if h.readIdx >= h.prefetchIdx {
		h.prefetch()
	}

	t0 := time.Now()
	buildBatch := func() *batch.Batch {
		bat := batch.NewWithSize(len(tblDef.Cols))
		for i, col := range tblDef.Cols {
			bat.Attrs = append(bat.Attrs, col.Name)
			typ := plan2.ExprType2Type(&col.Typ)
			bat.Vecs[i] = vector.NewVec(typ)
		}
		return bat
	}
	data = buildBatch()
	h.isEnd, err = h.reader.Read(
		ctx,
		h.attrs,
		nil,
		mp,
		data,
	)
	h.readIdx++
	if h.isEnd {
		return nil, nil, hint, nil
	}
	if err != nil {
		return
	}

	committs, err := vector.NewConstFixed(types.T_TS.ToType(), h.end, data.Vecs[0].Length(), mp)
	if err != nil {
		data.Clean(mp)
		return
	}
	rowIDIdx := -1
	for i, vec := range data.Vecs {
		if vec != nil && vec.GetType().Oid == types.T_Rowid {
			rowIDIdx = i
			break
		}
	}
	if h.retainRowID {
		if rowIDIdx < 0 {
			data.Clean(mp)
			committs.Free(mp)
			err = moerr.NewInternalErrorNoCtx("checkpoint changes handle missing rowid vector")
			return
		}
		rowIDVec := data.Vecs[rowIDIdx]
		rewrittenVecs := make([]*vector.Vector, 0, len(data.Vecs)+1)
		rewrittenVecs = append(rewrittenVecs, rowIDVec)
		for i, vec := range data.Vecs {
			if i == rowIDIdx {
				continue
			}
			rewrittenVecs = append(rewrittenVecs, vec)
		}
		rewrittenVecs = append(rewrittenVecs, committs)
		data.Vecs = rewrittenVecs

		rewrittenAttrs := make([]string, 0, len(data.Attrs)+1)
		rewrittenAttrs = append(rewrittenAttrs, catalog.Row_ID)
		for i, attr := range data.Attrs {
			if i == rowIDIdx {
				continue
			}
			rewrittenAttrs = append(rewrittenAttrs, attr)
		}
		rewrittenAttrs = append(rewrittenAttrs, objectio.DefaultCommitTS_Attr)
		data.Attrs = rewrittenAttrs
	} else {
		if rowIDIdx >= 0 {
			data.Vecs[rowIDIdx].Free(mp)
			data.Vecs = append(data.Vecs[:rowIDIdx], data.Vecs[rowIDIdx+1:]...)
			data.Attrs = append(data.Attrs[:rowIDIdx], data.Attrs[rowIDIdx+1:]...)
		}
		data.Vecs = append(data.Vecs, committs)
		data.Attrs = append(data.Attrs, objectio.DefaultCommitTS_Attr)
	}
	h.duration += time.Since(t0)
	h.dataLength += data.Vecs[0].Length()
	return
}
func (h *CheckpointChangesHandle) Close() error {
	if h.reader != nil {
		h.reader.Close()
	}
	return nil
}
func (h *CheckpointChangesHandle) initReader(ctx context.Context) (err error) {
	tblDef := h.table.GetTableDef(ctx)
	h.attrs = make([]string, 0)
	for _, col := range tblDef.Cols {
		h.attrs = append(h.attrs, col.Name)
	}

	var part *logtailreplay.PartitionState
	if part, err = h.table.getPartitionState(ctx); err != nil {
		return
	}

	var blockList objectio.BlockInfoSlice
	if _, err = readutil.TryFastFilterBlocks(
		ctx,
		h.end.ToTimestamp(),
		tblDef,
		engine.DefaultRangesParam,
		part,
		nil,
		nil,
		&blockList,
		h.table.PrefetchAllMeta,
		h.fs,
	); err != nil {
		return
	}
	relData := readutil.NewBlockListRelationData(
		1,
		readutil.WithPartitionState(part))
	h.blockList = blockList
	for i, end := 0, blockList.Len(); i < end; i++ {
		relData.AppendBlockInfo(blockList.Get(i))
	}

	readers, err := h.table.BuildReaders(
		ctx,
		h.table.proc.Load(),
		nil,
		relData,
		1,
		0,
		false,
		engine.Policy_CheckCommittedOnly,
		engine.FilterHint{},
	)
	if err != nil {
		return
	}
	h.reader = readers[0]

	return
}
