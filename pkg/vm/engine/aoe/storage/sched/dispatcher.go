package sched

import "errors"

var (
	ErrDispatchWrongEvent = errors.New("aoe: wrong event type")
)

type Dispatcher interface {
	Dispatch(Event)
}

type mockDispatcher struct {
	BaseDispatcher
}

func newMockDispatcher() *mockDispatcher {
	d := &mockDispatcher{
		BaseDispatcher: *NewBaseDispatcher(),
	}
	d.RegisterHandler(MockEvent, &mockEventHandler{})
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
	handler.Handle(e)
}

func (d *BaseDispatcher) RegisterHandler(t EventType, h EventHandler) {
	d.handlers[t] = h
}
