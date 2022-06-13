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
	gvec "github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
)

func (blk *dataBlock) CheckpointWALClosure(endTs uint64) tasks.FuncT {
	return func() error {
		return blk.CheckpointWAL(endTs)
	}
}

func (blk *dataBlock) SyncBlockDataClosure(ts uint64, rows uint32) tasks.FuncT {
	return func() error {
		return blk.SyncBlockData(ts, rows)
	}
}

func (blk *dataBlock) FlushColumnDataClosure(ts uint64, colIdx int, colData *gvec.Vector, sync bool) tasks.FuncT {
	return func() error {
		return blk.FlushColumnData(ts, colIdx, colData, sync)
	}
}

func (blk *dataBlock) ABlkFlushDataClosure(ts uint64, bat batch.IBatch, masks map[uint16]*roaring.Bitmap, vals map[uint16]map[uint32]any, deletes *roaring.Bitmap) tasks.FuncT {
	return func() error {
		return blk.ABlkFlushData(ts, bat, masks, vals, deletes)
	}
}

func (blk *dataBlock) CheckpointWAL(endTs uint64) (err error) {
	if blk.meta.IsAppendable() {
		return blk.ABlkCheckpointWAL(endTs)
	}
	return blk.BlkCheckpointWAL(endTs)
}

func (blk *dataBlock) BlkCheckpointWAL(endTs uint64) (err error) {
	ckpTs := blk.GetMaxCheckpointTS()
	logutil.Infof("BlkCheckpointWAL | %s | [%d/%d]", blk.meta.Repr(), ckpTs, endTs)
	if endTs <= ckpTs {
		return
	}
	view, err := blk.CollectChangesInRange(ckpTs+1, endTs)
	if err != nil {
		return
	}
	cnt := 0
	for _, idxes := range view.ColLogIndexes {
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
	// logutil.Infof("BLK | [%d,%d] | CNT=[%d] | Checkpointed | %s", ckpTs+1, endTs, cnt, blk.meta.String())
	blk.SetMaxCheckpointTS(endTs)
	return
}

func (blk *dataBlock) ABlkCheckpointWAL(endTs uint64) (err error) {
	ckpTs := blk.GetMaxCheckpointTS()
	logutil.Infof("ABlkCheckpointWAL | %s | [%d/%d]", blk.meta.Repr(), ckpTs, endTs)
	if endTs <= ckpTs {
		return
	}
	indexes, err := blk.CollectAppendLogIndexes(ckpTs+1, endTs+1)
	if err != nil {
		return
	}
	view, err := blk.CollectChangesInRange(ckpTs+1, endTs+1)
	if err != nil {
		return
	}
	for _, idxes := range view.ColLogIndexes {
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
	logutil.Infof("ABLK | [%d,%d] | CNT=[%d] | Checkpointed | %s", ckpTs+1, endTs, len(indexes), blk.meta.String())
	// for _, index := range indexes {
	// 	logutil.Infof("Ckp1Index  %s", index.String())
	// }
	// for _, index := range view.DeleteLogIndexes {
	// 	logutil.Infof("Ckp1Index  %s", index.String())
	// }
	blk.SetMaxCheckpointTS(endTs)
	return
}

func (blk *dataBlock) FlushColumnData(ts uint64, colIdx int, colData *gvec.Vector, sync bool) (err error) {
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
	view, err := blk.node.GetColumnsView(maxRow)
	if err != nil {
		return
	}
	needCkp := true
	if err = blk.node.flushData(ts, view); err != nil {
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

func (blk *dataBlock) ABlkFlushData(ts uint64, bat batch.IBatch, masks map[uint16]*roaring.Bitmap, vals map[uint16]map[uint32]any, deletes *roaring.Bitmap) (err error) {
	flushTs := blk.node.GetBlockMaxFlushTS()
	if ts <= flushTs {
		logutil.Infof("FLUSH ABLK | [%s] | CANCELLED | (Stale Request: Already Flushed)", blk.meta.String())
		return data.ErrStaleRequest
	}
	ckpTs := blk.GetMaxCheckpointTS()
	if ts <= ckpTs {
		logutil.Infof("FLUSH ABLK | [%s] | CANCELLED | (Stale Request: Already Compacted)", blk.meta.String())
		return data.ErrStaleRequest
	}

	if err := blk.file.WriteIBatch(bat, ts, masks, vals, nil); err != nil {
		return err
	}
	if deletes != nil {
		var buf []byte
		if buf, err = deletes.ToBytes(); err != nil {
			return
		}
		if err = blk.file.WriteDeletes(buf); err != nil {
			return
		}
	}
	if err = blk.file.Sync(); err != nil {
		return
	}
	blk.node.SetBlockMaxFlushTS(ts)
	blk.resetNice()
	logutil.Infof("FLUSH ABLK | [%s] | Done | MaxRow=%d | MaxTs=%d", blk.meta.String(), bat.Length(), ts)
	return
}
