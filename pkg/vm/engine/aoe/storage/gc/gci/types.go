package gci

import (
	iops "matrixone/pkg/vm/engine/aoe/storage/ops/base"
	iw "matrixone/pkg/vm/engine/aoe/storage/worker/base"
	"time"
)

type RequestType int32

const (
	GCDropTable RequestType = 0
)

const (
	DefaultInterval = 10 * time.Millisecond
)

type WorkerCfg struct {
	Interval time.Duration
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
