package tasks

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/ops/base"
)

type FuncT = func() error

type TaskType uint16

var taskIdAlloctor *common.IdAlloctor

const (
	NoopTask TaskType = iota
	TxnTask
	IOTask
	MockTask
	CustomizedTask

	CompactBlockTask
	MergeBlocksTask
	CheckpointDataTask
	CheckpointCatalogTask
	CheckpointWalTask
)

func init() {
	taskIdAlloctor = common.NewIdAlloctor(1)
}

type TxnTaskFactory = func(ctx *Context, txn txnif.AsyncTxn) (Task, error)

func NextTaskId() uint64 {
	return taskIdAlloctor.Alloc()
}

type Task interface {
	base.IOp
	ID() uint64
	Type() TaskType
	Cancel() error
}

type ScopedTask interface {
	Task
	Scope() *common.ID
}

var DefaultScopeSharder = func(scope *common.ID) int {
	if scope == nil {
		return 0
	}
	return int(scope.TableID + scope.SegmentID)
}

func IsSameScope(left, right *common.ID) bool {
	return left.TableID == right.TableID && left.SegmentID == right.SegmentID
}

type FnTask struct {
	*BaseTask
	Fn FuncT
}

func NewFnTask(ctx *Context, taskType TaskType, fn FuncT) *FnTask {
	task := &FnTask{
		Fn: fn,
	}
	task.BaseTask = NewBaseTask(task, taskType, ctx)
	return task
}

func (task *FnTask) Execute() error {
	return task.Fn()
}

type ScopedFnTask struct {
	*FnTask
	scope *common.ID
}

func NewScopedFnTask(ctx *Context, taskType TaskType, scope *common.ID, fn FuncT) *ScopedFnTask {
	task := &ScopedFnTask{
		FnTask: NewFnTask(ctx, taskType, fn),
		scope:  scope,
	}
	return task
}

func (task *ScopedFnTask) Scope() *common.ID { return task.scope }
