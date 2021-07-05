package gci

import (
	iops "matrixone/pkg/vm/engine/aoe/storage/ops/base"
	iw "matrixone/pkg/vm/engine/aoe/storage/worker/base"
)

type RequestType int32

const (
	GCDropTable RequestType = 0
)

const (
	DefaultInterval int64 = 10
)

type WorkerCfg struct {
	Interval int64
	Executor iw.IOpWorker
}

type IRequest interface {
	iops.IOp
	GetNext() IRequest
	IncIteration()
	GetIteration() uint32
}

type IAcceptor interface {
	Accept(IRequest)
	Start()
	Stop()
}

type IWorker interface {
	IAcceptor
	iw.IOpWorker
}
