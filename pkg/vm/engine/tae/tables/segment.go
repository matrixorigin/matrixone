package tables

import (
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/file"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/jobs"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
)

type dataSegment struct {
	meta      *catalog.SegmentEntry
	file      file.Segment
	bufMgr    base.INodeManager
	scheduler tasks.TaskScheduler
}

func newSegment(meta *catalog.SegmentEntry, factory file.SegmentFileFactory, bufMgr base.INodeManager) *dataSegment {
	segFile := factory("xxx", meta.GetID())
	seg := &dataSegment{
		meta:      meta,
		file:      segFile,
		bufMgr:    bufMgr,
		scheduler: meta.GetScheduler(),
	}
	return seg
}

func (segment *dataSegment) GetSegmentFile() file.Segment {
	return segment.file
}

func (segment *dataSegment) GetID() uint64 { return segment.meta.GetID() }

func (segment *dataSegment) BatchDedup(txn txnif.AsyncTxn, pks *vector.Vector) (err error) {
	// TODO: segment level index
	return data.ErrPossibleDuplicate
	// blkIt := segment.meta.MakeBlockIt(false)
	// for blkIt.Valid() {
	// 	block := blkIt.Get().GetPayload().(*catalog.BlockEntry)
	// 	if err = block.GetBlockData().BatchDedup(txn, pks); err != nil {
	// 		return
	// 	}
	// 	blkIt.Next()
	// }
	// return nil
}

func (segment *dataSegment) MutationInfo() string { return "" }

func (segment *dataSegment) RunCalibration()    {}
func (segment *dataSegment) EstimateScore() int { return 0 }

func (segment *dataSegment) BuildCompactionTaskFactory() (factory tasks.TxnTaskFactory, taskType tasks.TaskType, scopes []common.ID, err error) {
	if segment.meta.IsAppendable() {
		segment.meta.RLock()
		dropped := segment.meta.IsDroppedCommitted()
		inTxn := segment.meta.HasActiveTxn()
		segment.meta.RUnlock()
		if dropped || inTxn {
			return
		}
		filter := catalog.NewComposedFilter()
		filter.AddBlockFilter(catalog.NonAppendableBlkFilter)
		filter.AddCommitFilter(catalog.ActiveWithNoTxnFilter)
		blks := segment.meta.CollectBlockEntries(filter.FilteCommit, filter.FilteBlock)
		if len(blks) < int(segment.meta.GetTable().GetSchema().SegmentMaxBlocks) {
			return
		}
		for _, blk := range blks {
			scopes = append(scopes, *blk.AsCommonID())
		}
		factory = jobs.MergeBlocksTaskFactory(blks, nil, segment.scheduler)
		taskType = tasks.DataCompactionTask
		return
	}
	return
}
