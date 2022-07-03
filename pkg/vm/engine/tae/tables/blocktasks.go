// Copyright 2021 Matrix Origin
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

package tables

import (
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
)

func (blk *dataBlock) CheckpointWALClosure(currTs uint64) tasks.FuncT {
	return func() error {
		return blk.CheckpointWAL(currTs)
	}
}

func (blk *dataBlock) SyncBlockDataClosure(ts uint64, rows uint32) tasks.FuncT {
	return func() error {
		return blk.SyncBlockData(ts, rows)
	}
}

func (blk *dataBlock) FlushColumnDataClosure(ts uint64, colIdx int, colData containers.Vector, sync bool) tasks.FuncT {
	return func() error {
		return blk.FlushColumnData(ts, colIdx, colData, sync)
	}
}

func (blk *dataBlock) ABlkFlushDataClosure(
	ts uint64,
	bat *containers.Batch,
	masks map[uint16]*roaring.Bitmap,
	vals map[uint16]map[uint32]any,
	deletes *roaring.Bitmap) tasks.FuncT {
	return func() error {
		return blk.ABlkFlushData(ts, bat, masks, vals, deletes)
	}
}

func (blk *dataBlock) CheckpointWAL(currTs uint64) (err error) {
	if blk.meta.IsAppendable() {
		return blk.ABlkCheckpointWAL(currTs)
	}
	return blk.BlkCheckpointWAL(currTs)
}

func (blk *dataBlock) BlkCheckpointWAL(currTs uint64) (err error) {
	defer func() {
		logutil.Info("[Done]", common.ReprerField("blk", blk.meta),
			common.OperationField("ckp-wal"),
			common.ErrorField(err),
			common.AnyField("curr", currTs))
	}()
	ckpTs := blk.GetMaxCheckpointTS()
	logutil.Info("[Start]", common.ReprerField("blk", blk.meta),
		common.OperationField("ckp-wal"),
		common.AnyField("ckped", ckpTs),
		common.AnyField("curr", currTs))
	if currTs <= ckpTs {
		return
	}
	view, err := blk.CollectChangesInRange(ckpTs+1, currTs)
	if err != nil {
		return
	}
	cnt := 0
	for _, column := range view.Columns {
		idxes := column.LogIndexes
		cnt += len(idxes)
		if err = blk.scheduler.Checkpoint(idxes); err != nil {
			return
		}
		// for _, index := range idxes {
		// 	logutil.Infof("Ckp2Index  %s", index.String())
		// }
	}
	if err = blk.scheduler.Checkpoint(view.DeleteLogIndexes); err != nil {
		return
	}
	blk.SetMaxCheckpointTS(currTs)
	return
}

func (blk *dataBlock) ABlkCheckpointWAL(currTs uint64) (err error) {
	defer func() {
		if err != nil {
			logutil.Warn("[Done]", common.ReprerField("blk", blk.meta),
				common.OperationField("ckp-wal"),
				common.ErrorField(err))
		}
	}()
	ckpTs := blk.GetMaxCheckpointTS()
	logutil.Info("[Start]", common.ReprerField("blk", blk.meta),
		common.OperationField("ckp-wal"),
		common.AnyField("ckpTs", ckpTs),
		common.AnyField("curr", currTs))
	if currTs <= ckpTs {
		return
	}
	indexes, err := blk.CollectAppendLogIndexes(ckpTs+1, currTs+1)
	if err != nil {
		return
	}
	view, err := blk.CollectChangesInRange(ckpTs+1, currTs+1)
	if err != nil {
		return
	}
	for _, column := range view.Columns {
		idxes := column.LogIndexes
		if err = blk.scheduler.Checkpoint(idxes); err != nil {
			return
		}
		// for _, index := range idxes {
		// 	logutil.Infof("Ckp1Index  %s", index.String())
		// }
	}
	if err = blk.scheduler.Checkpoint(indexes); err != nil {
		return
	}
	if err = blk.scheduler.Checkpoint(view.DeleteLogIndexes); err != nil {
		return
	}
	logutil.Info("[Done]", common.ReprerField("blk", blk.meta),
		common.OperationField("ckp-wal"),
		common.CountField(len(indexes)))
	// for _, index := range indexes {
	// 	logutil.Infof("Ckp1Index  %s", index.String())
	// }
	// for _, index := range view.DeleteLogIndexes {
	// 	logutil.Infof("Ckp1Index  %s", index.String())
	// }
	blk.SetMaxCheckpointTS(currTs)
	return
}

func (blk *dataBlock) FlushColumnData(ts uint64, colIdx int, colData containers.Vector, sync bool) (err error) {
	if err = blk.file.WriteColumnVec(ts, colIdx, colData); err != nil {
		return err
	}
	if sync {
		err = blk.file.Sync()
	}
	return
}

func (blk *dataBlock) SyncBlockData(ts uint64, rows uint32) (err error) {
	if err = blk.file.WriteRows(rows); err != nil {
		return
	}
	if err = blk.file.WriteTS(ts); err != nil {
		return
	}
	return blk.file.Sync()
}

func (blk *dataBlock) ForceCompact() (err error) {
	if !blk.meta.IsAppendable() {
		panic("todo")
	}
	ts := blk.mvcc.LoadMaxVisible()
	if blk.node.GetBlockMaxFlushTS() >= ts {
		return
	}
	h, err := blk.bufMgr.TryPin(blk.node, time.Second)
	if err != nil {
		return
	}
	defer h.Close()
	// Why check again? May be a flush was executed in between
	if blk.node.GetBlockMaxFlushTS() >= ts {
		return
	}
	blk.mvcc.RLock()
	maxRow, _, err := blk.mvcc.GetMaxVisibleRowLocked(ts)
	blk.mvcc.RUnlock()
	if err != nil {
		return
	}
	bat, err := blk.node.GetDataCopy(maxRow)
	if err != nil {
		return
	}
	defer bat.Close()
	needCkp := true
	if err = blk.node.flushData(ts, bat, new(forceCompactOp)); err != nil {
		if err == data.ErrStaleRequest {
			err = nil
			needCkp = false
		} else {
			return
		}
	}
	if needCkp {
		_, err = blk.scheduler.ScheduleScopedFn(nil, tasks.CheckpointTask, blk.meta.AsCommonID(), blk.CheckpointWALClosure(ts))
	}
	return
}

func (blk *dataBlock) ABlkFlushData(
	ts uint64,
	bat *containers.Batch,
	masks map[uint16]*roaring.Bitmap,
	vals map[uint16]map[uint32]any,
	deletes *roaring.Bitmap) (err error) {
	flushTs := blk.node.GetBlockMaxFlushTS()
	if ts <= flushTs {
		logutil.Info("[Cancelled]",
			common.ReprerField("blk", blk.meta),
			common.OperationField("flush"),
			common.ReasonField("State Request: Already Flushed"))
		return data.ErrStaleRequest
	}
	// ckpTs := blk.GetMaxCheckpointTS()
	// if ts <= ckpTs {
	// 	logutil.Info("[Cancelled]",
	// 		common.ReprerField("blk", blk.meta),
	// 		common.OperationField("flush"),
	// 		common.ReasonField("State Request: Already Flushed"))
	// 	return data.ErrStaleRequest
	// }

	if err := blk.file.WriteSnapshot(bat, ts, masks, vals, deletes); err != nil {
		return err
	}
	if err = blk.file.Sync(); err != nil {
		return
	}
	blk.node.SetBlockMaxFlushTS(ts)
	blk.resetNice()
	logutil.Info("[Done]",
		common.ReprerField("blk", blk.meta),
		common.OperationField("flush"),
		common.AnyField("maxRow", bat.Length()),
		common.AnyField("maxTs", ts))
	return
}
