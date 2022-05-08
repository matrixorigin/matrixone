package tasks

import (
	"errors"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/ops/base"
)

var (
	ErrBadTaskRequestPara    = errors.New("tae scheduler: bad task request parameters")
	ErrScheduleScopeConflict = errors.New("tae scheduler: scope conflict")
)

type FuncT = func() error

type TaskType uint16

var taskIdAlloctor *common.IdAlloctor

const (
	NoopTask TaskType = iota
	MockTask
	CustomizedTask

	DataCompactionTask
	CheckpointTask
	IOTask
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

type MScopedTask interface {
	Task
	Scopes() []common.ID
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

type MultiScopedFnTask struct {
	*FnTask
	scopes []common.ID
}

func NewMultiScopedFnTask(ctx *Context, taskType TaskType, scopes []common.ID, fn FuncT) *MultiScopedFnTask {
	task := &MultiScopedFnTask{
		FnTask: NewFnTask(ctx, taskType, fn),
		scopes: scopes,
	}
	return task
}

func (task *MultiScopedFnTask) Scopes() []common.ID { return task.scopes }
