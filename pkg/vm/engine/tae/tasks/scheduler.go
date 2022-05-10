package tasks

import (
	"errors"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	iops "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks/ops/base"
	ops "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks/worker"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

var (
	ErrDispatcherNotFound = errors.New("tae sched: dispatcher not found")
	ErrSchedule           = errors.New("tae sched: cannot schedule")
)

type Scheduler interface {
	Start()
	Stop()
	Schedule(Task) error
}

type TaskScheduler interface {
	Scheduler
	ScheduleTxnTask(ctx *Context, taskType TaskType, factory TxnTaskFactory) (Task, error)
	ScheduleMultiScopedTxnTask(ctx *Context, taskType TaskType, scopes []common.ID, factory TxnTaskFactory) (Task, error)
	ScheduleMultiScopedFn(ctx *Context, taskType TaskType, scopes []common.ID, fn FuncT) (Task, error)
	ScheduleFn(ctx *Context, taskType TaskType, fn func() error) (Task, error)
	ScheduleScopedFn(ctx *Context, taskType TaskType, scope *common.ID, fn func() error) (Task, error)
	Checkpoint(indexes []*wal.Index) error

	GetCheckpointedLSN() uint64
	GetPenddingLSNCnt() uint64
	GetSafeTS() uint64
}

type BaseScheduler struct {
	ops.OpWorker
	idAlloc     *common.IdAlloctor
	Dispatchers map[TaskType]Dispatcher
}

func NewBaseScheduler(name string) *BaseScheduler {
	scheduler := &BaseScheduler{
		OpWorker:    *ops.NewOpWorker(name),
		idAlloc:     common.NewIdAlloctor(1),
		Dispatchers: make(map[TaskType]Dispatcher),
	}
	scheduler.ExecFunc = scheduler.doDispatch
	return scheduler
}

func (s *BaseScheduler) RegisterDispatcher(t TaskType, dispatcher Dispatcher) {
	s.Dispatchers[t] = dispatcher
}

func (s *BaseScheduler) Schedule(task Task) error {
	// task.AttachID(s.idAlloc())
	if !s.SendOp(task) {
		return ErrSchedule
	}
	return nil
}

func (s *BaseScheduler) doDispatch(op iops.IOp) {
	task := op.(Task)
	dispatcher := s.Dispatchers[task.Type()]
	if dispatcher == nil {
		logutil.Errorf("No dispatcher found for %d[T] Task", task.Type())
		panic(ErrDispatcherNotFound)
	}
	dispatcher.Dispatch(task)
}

func (s *BaseScheduler) Stop() {
	s.OpWorker.Stop()
	for _, d := range s.Dispatchers {
		d.Close()
	}
}
