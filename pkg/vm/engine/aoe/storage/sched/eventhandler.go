package sched

import (
	log "github.com/sirupsen/logrus"
	ops "matrixone/pkg/vm/engine/aoe/storage/worker"
)

type EventHandler interface {
	Handle(Event)
}

type mockEventHandler struct{}

func (h *mockEventHandler) Handle(e Event) {
	log.Infof("Handling event type %v, id %d", e.Type(), e.ID())
}

type BaseEventHandler struct {
	ops.OpWorker
}

func NewBaseEventHandler(name string, execFunc ops.OpExecFunc) *BaseEventHandler {
	h := &BaseEventHandler{
		OpWorker: *ops.NewOpWorker(name),
	}
	if execFunc != nil {
		h.ExecFunc = execFunc
	}
	return h
}
