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
