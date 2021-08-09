package sched

import (
	"github.com/panjf2000/ants/v2"
	iops "matrixone/pkg/vm/engine/aoe/storage/ops/base"
	ops "matrixone/pkg/vm/engine/aoe/storage/worker"
	"sync"
	// log "github.com/sirupsen/logrus"
)

var (
	poolHandlerName = "PoolHandler"
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
	return h
}

func (h *poolHandler) doHandle(op iops.IOp) {
	closure := func(o iops.IOp, wg *sync.WaitGroup) func() {
		return func() {
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
