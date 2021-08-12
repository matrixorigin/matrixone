package sched

import (
	"errors"
	// log "github.com/sirupsen/logrus"
)

var (
	ErrDispatchWrongEvent = errors.New("aoe: wrong event type")
)

type mockDispatcher struct {
	BaseDispatcher
}

func newMockDispatcher() *mockDispatcher {
	d := &mockDispatcher{
		BaseDispatcher: *NewBaseDispatcher(),
	}
	handler := newMockEventHandler("meh")
	handler.Start()
	d.RegisterHandler(MockEvent, handler)
	return d
}

type BaseDispatcher struct {
	handlers map[EventType]EventHandler
}

func NewBaseDispatcher() *BaseDispatcher {
	d := &BaseDispatcher{
		handlers: make(map[EventType]EventHandler),
	}
	return d
}

func (d *BaseDispatcher) Dispatch(e Event) {
	handler, ok := d.handlers[e.Type()]
	if !ok {
		panic(ErrDispatchWrongEvent)
	}
	handler.Enqueue(e)
}

func (d *BaseDispatcher) RegisterHandler(t EventType, h EventHandler) {
	d.handlers[t] = h
}

func (d *BaseDispatcher) Close() error {
	for _, h := range d.handlers {
		h.Close()
	}
	return nil
}
