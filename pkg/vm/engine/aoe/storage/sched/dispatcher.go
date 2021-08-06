package sched

type Dispatcher interface {
	Dispatch(Event)
	Stop()
}

type mockDispatcher struct{}

func (d *mockDispatcher) Dispatch(e Event) {}
func (d *mockDispatcher) Stop()            {}
