package base

import (
	ops "matrixone/pkg/vm/engine/aoe/storage/ops/base"
)

type IOpWorker interface {
	Start()
	Stop()
	SendOp(ops.IOp) bool
	StopReceiver()
	WaitStop()
	StatsString() string
}

type IHeartbeater interface {
	Start()
	Stop()
}

type IHBHandle interface {
	OnExec()
	OnStopped()
}
