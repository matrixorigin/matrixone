package tables

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
)

func (blk *dataBlock) CheckpointWALClosure(endTs uint64) tasks.FuncT {
	closure := func(ts uint64) func() error {
		return func() error {
			return blk.CheckpointWAL(ts)
		}
	}
	return closure(endTs)
}

func (blk *dataBlock) ABlkFlushDataClosure(ts uint64, data batch.IBatch, masks map[uint16]*roaring.Bitmap, vals map[uint16]map[uint32]interface{}, deletes *roaring.Bitmap) tasks.FuncT {
	closure := func(ts uint64, data batch.IBatch, masks map[uint16]*roaring.Bitmap, vals map[uint16]map[uint32]interface{}, deletes *roaring.Bitmap) tasks.FuncT {
		return func() error {
			return blk.ABlkFlushData(ts, data, masks, vals, deletes)
		}
	}
	return closure(ts, data, masks, vals, deletes)
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
	blk.scheduler.Checkpoint(indexes)
	for i, index := range indexes {
		logutil.Infof("Checkpoint %d: %s", i, index.String())
	}
	blk.SetMaxCheckpointTS(endTs)
	return
}

func (blk *dataBlock) ABlkFlushData(ts uint64, data batch.IBatch, masks map[uint16]*roaring.Bitmap, vals map[uint16]map[uint32]interface{}, deletes *roaring.Bitmap) (err error) {
	if err := blk.file.WriteIBatch(data, ts, masks, vals, deletes); err != nil {
		return err
	}
	return blk.file.Sync()
}
