package sched

import (
	"errors"
	iops "matrixone/pkg/vm/engine/aoe/storage/ops/base"
	ops "matrixone/pkg/vm/engine/aoe/storage/worker"

	log "github.com/sirupsen/logrus"
)

var (
	ErrEventHandleEnqueue = errors.New("aoe: event handle enqueue")
)

type mockEventHandler struct {
	BaseEventHandler
}

func newMockEventHandler(name string) *mockEventHandler {
	h := &mockEventHandler{
		BaseEventHandler: *NewBaseEventHandler(name),
	}
	h.ExecFunc = h.doHandle
	return h
}

func (h *mockEventHandler) doHandle(op iops.IOp) {
	e := op.(Event)
	log.Infof("Handling event type %v, id %d", e.Type(), e.ID())
}

type BaseEventHandler struct {
	ops.OpWorker
}

func NewBaseEventHandler(name string) *BaseEventHandler {
	h := &BaseEventHandler{
		OpWorker: *ops.NewOpWorker(name),
	}
	return h
}

func (h *BaseEventHandler) Enqueue(e Event) {
	if !h.SendOp(e) {
		e.SetError(ErrEventHandleEnqueue)
		e.Cancel()
	}
}

func (h *BaseEventHandler) ExecuteEvent(e Event) {
	h.ExecFunc(e)
}

func (h *BaseEventHandler) Close() error {
	h.Stop()
	return nil
}

type singleWorkerHandler struct {
	BaseEventHandler
}

func NewSingleWorkerHandler(name string) *singleWorkerHandler {
	h := &singleWorkerHandler{
		BaseEventHandler: *NewBaseEventHandler(name),
	}
	return h
}
