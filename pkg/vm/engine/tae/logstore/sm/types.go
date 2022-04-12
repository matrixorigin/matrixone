package sm

type OnFinCB = func()
type EnqueueOp = func(interface{}) interface{}
type OnItemsCB = func(...interface{})

type Closable interface {
	IsClosed() bool
	TryClose() bool
}

type Queue interface {
	Start()
	Stop()
	Enqueue(interface{}) (interface{}, error)
}

type StateMachine interface {
	Start()
	Stop()
	EnqueueRecevied(interface{}) (interface{}, error)
	EnqueueCheckpoint(interface{}) (interface{}, error)
}
