package base

import ()

type IOpInternal interface {
	PreExecute() error
	Execute() error
	PostExecute() error
}

type IOp interface {
	OnExec() error
	SetError(err error)
}
