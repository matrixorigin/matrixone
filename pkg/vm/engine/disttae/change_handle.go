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
)

const DefaultLoadParallism = 20

func (tbl *txnTable) CollectChanges(
	ctx context.Context,
	from, to types.TS,
	skipDeletes bool,
	mp *mpool.MPool,
) (engine.ChangesHandle, error) {
	if from.IsEmpty() {
		return NewCheckpointChangesHandle(ctx, tbl, to, mp)
	}
	return NewPartitionChangesHandle(ctx, tbl, from, to, skipDeletes, mp)
}

type PartitionChangesHandle struct {
	currentChangeHandle engine.ChangesHandle
	currentPSFrom       types.TS
	currentPSTo         types.TS

	fromTs types.TS
	toTs   types.TS
	tbl    *txnTable

	skipDeletes   bool
	primarySeqnum int
	mp            *mpool.MPool
	fs            fileservice.FileService
}

func NewPartitionChangesHandle(
	ctx context.Context,
	tbl *txnTable,
	from, to types.TS,
	skipDeletes bool,
	mp *mpool.MPool,
) (*PartitionChangesHandle, error) {
	handle := &PartitionChangesHandle{
		tbl:           tbl,
		fromTs:        from,
		toTs:          to,
		skipDeletes:   skipDeletes,
		primarySeqnum: tbl.primarySeqnum,
		mp:            mp,
		fs:            tbl.getTxn().engine.fs,
	}
	end, err := handle.getNextChangeHandle(ctx)
	if end {
		panic(fmt.Sprintf("logic error: from %s to %s", from.ToString(), to.ToString()))
	}
	return handle, err
}

func (h *PartitionChangesHandle) Next(ctx context.Context, mp *mpool.MPool) (data, tombstone *batch.Batch, hint engine.ChangesHandle_Hint, err error) {
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

func (h *PartitionChangesHandle) getNextChangeHandle(ctx context.Context) (end bool, err error) {
	if h.currentPSTo.EQ(&h.toTs) {
		return true, nil
	}
	state, err := h.tbl.getPartitionState(ctx)
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
	if stateStart.LT(&nextFrom) {
		h.currentPSTo = h.toTs
		h.currentPSFrom = nextFrom
		h.currentChangeHandle, err = logtailreplay.NewChangesHandler(
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
			return
		}
		return
	}

	response, err := RequestSnapshotRead(ctx, h.tbl, &nextFrom)
	if err != nil {
		return
	}
	resp, ok := response.(*cmd_util.SnapshotReadResp)
	var checkpointEntries []*checkpoint.CheckpointEntry
	minTS := types.MaxTs()
	maxTS := types.TS{}
	if ok && resp.Succeed && len(resp.Entries) > 0 {
		checkpointEntries = make([]*checkpoint.CheckpointEntry, 0, len(resp.Entries))
		entries := resp.Entries
		for _, entry := range entries {
			start := types.TimestampToTS(*entry.Start)
			end := types.TimestampToTS(*entry.End)
			if start.LT(&minTS) {
				minTS = start
			}
			if end.GT(&maxTS) {
				maxTS = end
			}
			entryType := entry.EntryType
			checkpointEntry := checkpoint.NewCheckpointEntry("", start, end, checkpoint.EntryType(entryType))
			checkpointEntry.SetLocation(entry.Location1, entry.Location2)
			checkpointEntries = append(checkpointEntries, checkpointEntry)
		}
	}
	h.currentPSFrom = minTS
	if h.fromTs.GT(&minTS) {
		h.currentPSFrom = h.fromTs
	}
	h.currentPSTo = maxTS
	if h.toTs.LT(&maxTS) {
		h.currentPSTo = h.toTs
	}
	logutil.Info("ChangesHandle-Split change handles",
		zap.String("from", h.fromTs.ToString()),
		zap.String("to", h.toTs.ToString()),
		zap.String("ps from", h.currentPSFrom.ToString()),
		zap.String("ps to", h.currentPSTo.ToString()),
	)
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

func (h *PartitionChangesHandle) Close() error {
	h.currentChangeHandle.Close()
	h.currentChangeHandle = nil
	return nil
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
