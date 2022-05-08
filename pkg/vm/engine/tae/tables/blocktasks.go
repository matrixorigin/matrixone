package tables

import (
	"github.com/RoaringBitmap/roaring"
	gvec "github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/updates"
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

func (blk *dataBlock) ABlkFlushDataClosure(ts uint64, bat batch.IBatch, masks map[uint16]*roaring.Bitmap, vals map[uint16]map[uint32]interface{}, deletes *roaring.Bitmap) tasks.FuncT {
	return func() error {
		return blk.ABlkFlushData(ts, bat, masks, vals, deletes)
	}
}

func (blk *dataBlock) CheckpointWAL(endTs uint64) (err error) {
	if blk.meta.IsAppendable() {
		return blk.ABlkCheckpointWAL(endTs)
	}
	// TODO
	return
}

func (blk *dataBlock) ABlkCheckpointWAL(endTs uint64) (err error) {
	ckpTs := blk.GetMaxCheckpointTS()
	if endTs <= ckpTs {
		return
	}
	indexes := blk.CollectAppendLogIndexes(ckpTs+1, endTs)
	view := blk.CollectChangesInRange(ckpTs+1, endTs).(*updates.BlockView)
	for _, idxes := range view.ColLogIndexes {
		blk.scheduler.Checkpoint(idxes)
	}
	blk.scheduler.Checkpoint(indexes)
	logutil.Infof("ABLK | [%d,%d] | CNT=[%d] | Checkpointed ", ckpTs+1, endTs, len(indexes))
	// for _, index := range indexes {
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
	h := blk.bufMgr.Pin(blk.node)
	defer h.Close()
	// Why check again? May be a flush was executed in between
	if blk.node.GetBlockMaxFlushTS() >= ts {
		return
	}
	blk.mvcc.RLock()
	maxRow, _ := blk.mvcc.GetMaxVisibleRowLocked(ts)
	blk.mvcc.RUnlock()
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
		blk.scheduler.ScheduleScopedFn(nil, tasks.CheckpointTask, blk.meta.AsCommonID(), blk.CheckpointWALClosure(ts))
	}
	return
}

func (blk *dataBlock) ABlkFlushData(ts uint64, bat batch.IBatch, masks map[uint16]*roaring.Bitmap, vals map[uint16]map[uint32]interface{}, deletes *roaring.Bitmap) (err error) {
	flushTs := blk.node.GetBlockMaxFlushTS()
	if ts <= flushTs {
		logutil.Infof("FLUSH ABLK | [%s] | CANCELLED | (Stale Request: Already Flushed)", blk.meta.String())
		return data.ErrStaleRequest
	}
	ckpTs := blk.GetMaxCheckpointTS()
	if ts <= ckpTs {
		logutil.Infof("FLUSH ABLK | [%s] | CANCELLED | (State Request: Already Compacted)", blk.meta.String())
		return data.ErrStaleRequest
	}

	if err := blk.file.WriteIBatch(bat, ts, masks, vals, deletes); err != nil {
		return err
	}
	if err = blk.file.Sync(); err != nil {
		return
	}
	blk.node.SetBlockMaxFlushTS(ts)
	blk.resetNice()
	logutil.Infof("FLUSH ABLK | [%s] | Done | MaxRow=%d | MaxTs=%d", blk.meta.String(), bat.Length(), ts)
	return
}
