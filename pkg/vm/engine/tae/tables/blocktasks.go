package tables

import (
	"github.com/RoaringBitmap/roaring"
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
	// for i, index := range indexes {
	// 	logutil.Infof("Checkpoint %d: %s", i, index.String())
	// }
	blk.SetMaxCheckpointTS(endTs)
	return
}

func (blk *dataBlock) ABlkFlushData(ts uint64, bat batch.IBatch, masks map[uint16]*roaring.Bitmap, vals map[uint16]map[uint32]interface{}, deletes *roaring.Bitmap) (err error) {
	flushTs := blk.node.GetBlockMaxFlushTS()
	if ts <= flushTs {
		logutil.Infof("FLUSH ABLK | [%s] | CANCELLED | (Stale Request: Already Flushed)")
		return data.ErrStaleRequest
	}
	ckpTs := blk.GetMaxCheckpointTS()
	if ts <= ckpTs {
		logutil.Infof("FLUSH ABLK | [%s] | CANCELLED | (State Request: Already Compacted)")
		return data.ErrStaleRequest
	}

	if err := blk.file.WriteIBatch(bat, ts, masks, vals, deletes); err != nil {
		return err
	}
	if err = blk.file.Sync(); err != nil {
		return
	}
	blk.node.SetBlockMaxFlushTS(ts)
	return
}
