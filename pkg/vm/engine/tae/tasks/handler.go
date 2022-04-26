package tasks

import (
	"errors"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	ops "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/worker"
)

var (
	ErrTaskHandleEnqueue = errors.New("tae: task handle enqueue")
)

type BaseTaskHandler struct {
	ops.OpWorker
}

func NewBaseEventHandler(name string) *BaseTaskHandler {
	h := &BaseTaskHandler{
		OpWorker: *ops.NewOpWorker(name),
	}
	return h
}

func (h *BaseTaskHandler) Enqueue(task Task) {
	if !h.SendOp(task) {
		task.SetError(ErrTaskHandleEnqueue)
		err := task.Cancel()
		if err != nil {
			logutil.Warnf("%v", err)
		}
	}
}

func (h *BaseTaskHandler) Execute(task Task) {
	h.ExecFunc(task)
}

func (h *BaseTaskHandler) Close() error {
	h.Stop()
	return nil
}

type singleWorkerHandler struct {
	BaseTaskHandler
}

func NewSingleWorkerHandler(name string) *singleWorkerHandler {
	h := &singleWorkerHandler{
		BaseTaskHandler: *NewBaseEventHandler(name),
	}
	return h
}
