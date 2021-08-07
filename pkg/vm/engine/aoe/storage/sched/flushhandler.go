package sched

import (
	iops "matrixone/pkg/vm/engine/aoe/storage/ops/base"
	ops "matrixone/pkg/vm/engine/aoe/storage/worker"
	"sync"

	"github.com/panjf2000/ants/v2"
	log "github.com/sirupsen/logrus"
)

var (
	flushHandlerName = "FlushHandler"
)

type flushHandler struct {
	BaseEventHandler
	opExec ops.OpExecFunc
	pool   *ants.Pool
	wg     *sync.WaitGroup
}

func NewFlushHandler(num int) *flushHandler {
	pool, err := ants.NewPool(num)
	if err != nil {
		panic(err)
	}
	h := &flushHandler{
		BaseEventHandler: *NewBaseEventHandler(flushHandlerName),
		pool:             pool,
		wg:               &sync.WaitGroup{},
	}
	h.opExec = h.ExecFunc
	h.ExecFunc = h.doHandle
	h.Start()
	return h
}

func (h *flushHandler) doHandle(op iops.IOp) {
	closure := func(o iops.IOp, wg *sync.WaitGroup) func() {
		return func() {
			e := op.(Event)
			log.Infof("Pool Handling flush event type %v, id %d", e.Type(), e.ID())
			h.opExec(o)
			wg.Done()
		}
	}
	h.wg.Add(1)
	h.pool.Submit(closure(op, h.wg))
}

func (h *flushHandler) Close() error {
	h.BaseEventHandler.Close()
	h.wg.Wait()
	return nil
}
