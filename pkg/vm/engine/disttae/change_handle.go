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
	if from.IsEmpty() {
		return NewCheckpointChangesHandle(ctx, tbl, to, mp)
	}
	return NewPartitionChangesHandle(
		ctx,
		tbl,
		from,
		to,
		skipDeletes,
		engine.SnapshotReadPolicyFromContext(ctx),
		mp,
	)
}

type queuedChangeBatch struct {
	data      *batch.Batch
	tombstone *batch.Batch
	hint      engine.ChangesHandle_Hint
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

	skipDeletes        bool
	primarySeqnum      int
	snapshotReadPolicy engine.SnapshotReadPolicy
	mp                 *mpool.MPool
	fs                 fileservice.FileService

	bufferedBatches     []queuedChangeBatch
	currentRangeDrained bool
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
		tbl:                tbl,
		fromTs:             from,
		toTs:               to,
		skipDeletes:        skipDeletes,
		primarySeqnum:      tbl.primarySeqnum,
		snapshotReadPolicy: snapshotReadPolicy,
		mp:                 mp,
		fs:                 tbl.getTxn().engine.fs,
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
	// The normal path keeps the existing replay behavior. The VisibleState
	// policy enables snapshot-recovery on FileNotFound (via SnapshotStateRange).
	if h.snapshotReadPolicy == engine.SnapshotReadPolicyVisibleState {
		return h.nextWithSnapshotRecovery(ctx, mp)
	}
	return h.nextReplay(ctx, mp)
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

// nextWithSnapshotRecovery drains one logical sub-range at a time. The whole
// sub-range is buffered before anything is returned so that a late
// FileNotFound can discard partial output and rebuild the same range via
// SnapshotStateRange without exposing inconsistent batches.
func (h *PartitionChangesHandle) nextWithSnapshotRecovery(ctx context.Context, mp *mpool.MPool) (data, tombstone *batch.Batch, hint engine.ChangesHandle_Hint, err error) {
	hint = engine.ChangesHandle_Tail_done
	for {
		if len(h.bufferedBatches) > 0 {
			next := h.bufferedBatches[0]
			h.bufferedBatches = h.bufferedBatches[1:]
			return next.data, next.tombstone, next.hint, nil
		}
		if h.currentRangeDrained {
			var end bool
			end, err = h.getNextChangeHandle(ctx)
			if err != nil {
				return nil, nil, hint, err
			}
			if end {
				return nil, nil, hint, nil
			}
			h.currentRangeDrained = false
		}
		if err = h.bufferCurrentRange(ctx, mp); err != nil {
			return nil, nil, hint, err
		}
	}
}

// bufferCurrentRange eagerly consumes the current sub-range into memory so
// that a mid-iteration FileNotFound can discard partial output and rebuild
// the same range via SnapshotStateRange recovery.
func (h *PartitionChangesHandle) bufferCurrentRange(ctx context.Context, mp *mpool.MPool) (err error) {
	var queued []queuedChangeBatch
	snapshotStateRangeTried := false
	cleanQueued := func() {
		for i := range queued {
			if queued[i].data != nil {
				queued[i].data.Clean(mp)
			}
			if queued[i].tombstone != nil {
				queued[i].tombstone.Clean(mp)
			}
		}
	}
	for {
		data, tombstone, hint, nextErr := h.currentChangeHandle.Next(ctx, mp)
		if nextErr != nil {
			if moerr.IsMoErrCode(nextErr, moerr.ErrFileNotFound) {
				// A late FileNotFound means the replay handle for this sub-range is
				// no longer trustworthy. Drop buffered output for the whole range,
				// then rebuild from the end-snapshot partition state with
				// delete-chain object rewrite.
				cleanQueued()
				queued = nil
				if !snapshotStateRangeTried {
					snapshotStateRangeTried = true
					swapErr := h.swapCurrentHandleToSnapshotStateRange(ctx)
					if swapErr == nil {
						continue
					}
					logutil.Error("ChangesHandle-SnapshotStateRange rebuild failed",
						zap.Uint64("table-id", h.tbl.tableId),
						zap.String("from", h.currentPSFrom.ToString()),
						zap.String("to", h.currentPSTo.ToString()),
						zap.Error(swapErr),
					)
				}
				cleanQueued()
				return nextErr
			}
			cleanQueued()
			return nextErr
		}
		if data == nil && tombstone == nil {
			h.bufferedBatches = append(h.bufferedBatches, queued...)
			h.currentRangeDrained = true
			return nil
		}
		queued = append(queued, queuedChangeBatch{
			data:      data,
			tombstone: tombstone,
			hint:      hint,
		})
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
	ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	state, err := h.tbl.getPartitionState(ctxWithTimeout)
	if err != nil {
		return
	}
	var nextFrom types.TS
	if h.currentPSFrom.IsEmpty() {
		nextFrom = h.fromTs
	} else {
		nextFrom = h.currentPSTo.Next()
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
		logutil.Debug("ChangesHandle-Split change handles",
			zap.String("from", h.fromTs.ToString()),
			zap.String("to", h.toTs.ToString()),
			zap.String("ps from", h.currentPSFrom.ToString()),
			zap.String("ps to", h.currentPSTo.ToString()),
			zap.Int("handle idx", h.handleIdx),
		)
		h.handleIdx++
		snapshotRangeStart := time.Now()
		if err = h.swapCurrentHandleToSnapshotStateRange(ctx); err != nil {
			logutil.Error("ChangesHandle-SnapshotStateRange init failed",
				zap.Uint64("table-id", h.tbl.tableId),
				zap.String("from", h.currentPSFrom.ToString()),
				zap.String("to", h.currentPSTo.ToString()),
				zap.Duration("snapshot-range-attempt", time.Since(snapshotRangeStart)),
				zap.Error(err),
			)
			return false, err
		}
		logutil.Info("ChangesHandle-SnapshotStateRange-Ready",
			zap.Uint64("table-id", h.tbl.tableId),
			zap.String("from", h.currentPSFrom.ToString()),
			zap.String("to", h.currentPSTo.ToString()),
			zap.Duration("duration", time.Since(snapshotRangeStart)),
		)
		return false, nil
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
	h.currentChangeHandle, err = logtailreplay.NewChangesHandlerWithCheckpointEntries(
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

func (h *PartitionChangesHandle) swapCurrentHandleToSnapshotStateRange(ctx context.Context) (err error) {
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
	state, err := snapshotTbl.getPartitionState(ctx)
	if err != nil {
		return err
	}
	if err = h.closeCurrentChangeHandle(); err != nil {
		return err
	}
	h.currentChangeHandle, err = logtailreplay.NewChangesHandlerWithPartitionStateRange(
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
}

func NewCheckpointChangesHandle(
	ctx context.Context,
	table *txnTable,
	end types.TS,
	mp *mpool.MPool,
) (*CheckpointChangesHandle, error) {
	handle := &CheckpointChangesHandle{
		end:   end,
		table: table,
		fs:    table.getTxn().engine.fs,
		sid:   table.proc.Load().GetService(),
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
	rowidVec := data.Vecs[len(data.Vecs)-1]
	rowidVec.Free(mp)
	data.Vecs[len(data.Vecs)-1] = committs
	data.Attrs[len(data.Attrs)-1] = objectio.DefaultCommitTS_Attr
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
