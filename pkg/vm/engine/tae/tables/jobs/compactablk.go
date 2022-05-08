package jobs

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
)

var CompactABlockTaskFactory = func(meta *catalog.BlockEntry, scheduler tasks.TaskScheduler) tasks.TxnTaskFactory {
	return func(ctx *tasks.Context, txn txnif.AsyncTxn) (tasks.Task, error) {
		return NewCompactABlockTask(ctx, txn, meta, scheduler)
	}
}

type compactABlockTask struct {
	*tasks.BaseTask
	txn       txnif.AsyncTxn
	meta      *catalog.BlockEntry
	scopes    []common.ID
	scheduler tasks.TaskScheduler
}

func NewCompactABlockTask(ctx *tasks.Context, txn txnif.AsyncTxn, meta *catalog.BlockEntry, scheduler tasks.TaskScheduler) (task *compactABlockTask, err error) {
	task = &compactABlockTask{
		txn:       txn,
		meta:      meta,
		scheduler: scheduler,
	}
	task.scopes = append(task.scopes, *meta.AsCommonID())
	task.BaseTask = tasks.NewBaseTask(task, tasks.DataCompactionTask, ctx)
	return
}

func (task *compactABlockTask) Scopes() []common.ID { return task.scopes }

func (task *compactABlockTask) Execute() (err error) {
	dataBlock := task.meta.GetBlockData()
	return dataBlock.ForceCompact()
}
