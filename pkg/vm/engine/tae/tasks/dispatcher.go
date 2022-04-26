package tasks

import (
	"errors"
	"io"
)

var (
	ErrDispatchWrongEvent = errors.New("aoe: wrong event type")
)

type Dispatcher interface {
	io.Closer
	Dispatch(Task)
}

type TaskHandler interface {
	io.Closer
	Start()
	Enqueue(Task)
	Execute(Task)
}

type BaseDispatcher struct {
	handlers map[TaskType]TaskHandler
}

func NewBaseDispatcher() *BaseDispatcher {
	d := &BaseDispatcher{
		handlers: make(map[TaskType]TaskHandler),
	}
	return d
}

func (d *BaseDispatcher) Dispatch(task Task) {
	handler, ok := d.handlers[task.Type()]
	if !ok {
		panic(ErrDispatchWrongEvent)
	}
	handler.Enqueue(task)
}

func (d *BaseDispatcher) RegisterHandler(t TaskType, h TaskHandler) {
	d.handlers[t] = h
}

func (d *BaseDispatcher) Close() error {
	for _, h := range d.handlers {
		h.Close()
	}
	return nil
}
