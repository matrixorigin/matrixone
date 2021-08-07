package sched

import (
	iops "matrixone/pkg/vm/engine/aoe/storage/ops/base"
	ops "matrixone/pkg/vm/engine/aoe/storage/worker"
	"sync"

	"github.com/panjf2000/ants/v2"
	log "github.com/sirupsen/logrus"
)

var (
	poolHandlerName = "FlushHandler"
)

type poolHandler struct {
	BaseEventHandler
	opExec ops.OpExecFunc
	pool   *ants.Pool
	wg     *sync.WaitGroup
}

func NewPoolHandler(num int) *poolHandler {
	pool, err := ants.NewPool(num)
	if err != nil {
		panic(err)
	}
	h := &poolHandler{
		BaseEventHandler: *NewBaseEventHandler(poolHandlerName),
		pool:             pool,
		wg:               &sync.WaitGroup{},
	}
	h.opExec = h.ExecFunc
	h.ExecFunc = h.doHandle
	h.Start()
	return h
}

func (h *poolHandler) doHandle(op iops.IOp) {
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

func (h *poolHandler) Close() error {
	h.BaseEventHandler.Close()
	h.wg.Wait()
	return nil
}
