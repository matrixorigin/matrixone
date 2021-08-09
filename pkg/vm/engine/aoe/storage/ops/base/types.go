package base

import "time"

type Observer interface {
	OnExecDone(IOp)
}

type IOpInternal interface {
	PreExecute() error
	Execute() error
	PostExecute() error
}

type IOp interface {
	OnExec() error
	SetError(err error)
	GetError() error
	WaitDone() error
	Waitable() bool
	GetCreateTime() time.Time
	GetStartTime() time.Time
	GetEndTime() time.Time
	GetExecutTime() int64
	AddObserver(Observer)
}
