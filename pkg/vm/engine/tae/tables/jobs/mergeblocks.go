package jobs

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
)

var MergeBlocksTaskFactory = func(metas []*catalog.BlockEntry) tasks.TxnTaskFactory {
	return func(ctx *tasks.Context, txn txnif.AsyncTxn) (tasks.Task, error) {
		return NewMergeBlocksTask(ctx, txn, metas)
	}
}

type mergeBlocksTask struct {
	*tasks.BaseTask
	txn       txnif.AsyncTxn
	metas     []*catalog.BlockEntry
	compacted []handle.Block
	created   []handle.Block
	newSeg    handle.Segment
}

func NewMergeBlocksTask(ctx *tasks.Context, txn txnif.AsyncTxn, metas []*catalog.BlockEntry) (task *mergeBlocksTask, err error) {
	task = &mergeBlocksTask{
		txn:       txn,
		metas:     metas,
		created:   make([]handle.Block, 0),
		compacted: make([]handle.Block, 0),
	}
	dbName := metas[0].GetSegment().GetTable().GetDB().GetName()
	database, err := txn.GetDatabase(dbName)
	if err != nil {
		return
	}
	relName := metas[0].GetSchema().Name
	rel, err := database.GetRelationByName(relName)
	if err != nil {
		return
	}
	for _, meta := range metas {
		seg, err := rel.GetSegment(meta.GetSegment().GetID())
		if err != nil {
			return nil, err
		}
		blk, err := seg.GetBlock(meta.GetID())
		if err != nil {
			return nil, err
		}
		task.compacted = append(task.compacted, blk)
	}
	task.BaseTask = tasks.NewBaseTask(task, tasks.MergeBlocksTask, ctx)
	return
}

func (task *mergeBlocksTask) Execute() (err error) {
	// 1. Get total rows of all blocks to be compacted: 10000
	// 2. Decide created blocks layout: []int{3000,3000,3000,1000}
	// 3. Merge sort blocks and split it into created blocks
	// 4. Record all mappings: []int
	// 5. PrepareMergeBlock(mappings)
	return
}
