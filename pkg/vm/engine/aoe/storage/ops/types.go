package ops

import (
	iops "matrixone/pkg/vm/engine/aoe/storage/ops/base"
	iworker "matrixone/pkg/vm/engine/aoe/storage/worker/base"
	"time"
	// log "github.com/sirupsen/logrus"
)

type OpDoneCB = func(iops.IOp)

type Op struct {
	Impl       iops.IOpInternal
	ErrorC     chan error
	Worker     iworker.IOpWorker
	Err        error
	Result     interface{}
	CreateTime time.Time
	StartTime  time.Time
	EndTime    time.Time
	DoneCB     OpDoneCB
}
