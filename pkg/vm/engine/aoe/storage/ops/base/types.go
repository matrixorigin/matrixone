package base

import "time"

type IOpInternal interface {
	PreExecute() error
	Execute() error
	PostExecute() error
}

type IOp interface {
	OnExec() error
	SetError(err error)
	GetCreateTime() time.Time
	GetStartTime() time.Time
	GetEndTime() time.Time
	GetExecutTime() int64
}
