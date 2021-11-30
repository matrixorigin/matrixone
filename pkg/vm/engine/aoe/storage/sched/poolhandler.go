// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sched

import (
	"github.com/panjf2000/ants/v2"
	iops "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/ops/base"
	ops "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/worker"
	"sync"
	// log "github.com/sirupsen/logrus"
)

var (
	poolHandlerName = "PoolHandler"
)

type PreSubmitExec func(iops.IOp) bool

type poolHandler struct {
	BaseEventHandler
	opExec    ops.OpExecFunc
	pool      *ants.Pool
	wg        *sync.WaitGroup
	preSubmit PreSubmitExec
}

func NewPoolHandler(num int, preSubmit PreSubmitExec) *poolHandler {
	pool, err := ants.NewPool(num)
	if err != nil {
		panic(err)
	}
	h := &poolHandler{
		BaseEventHandler: *NewBaseEventHandler(poolHandlerName),
		pool:             pool,
		wg:               &sync.WaitGroup{},
	}
	h.preSubmit = preSubmit
	h.opExec = h.ExecFunc
	h.ExecFunc = h.doHandle
	return h
}

func (h *poolHandler) ExecuteEvent(e Event) {
	h.opExec(e)
}

func (h *poolHandler) doHandle(op iops.IOp) {
	if h.preSubmit != nil {
		ok := h.preSubmit(op)
		if !ok {
			return
		}
	}
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
