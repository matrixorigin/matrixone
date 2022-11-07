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

package tasks

import (
	"sync"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	iops "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks/ops/base"
	ops "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks/worker"
	"github.com/panjf2000/ants/v2"
)

var (
	poolHandlerName = "PoolHandler"
)

type poolHandler struct {
	BaseTaskHandler
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
		BaseTaskHandler: *NewBaseEventHandler(poolHandlerName),
		pool:            pool,
		wg:              &sync.WaitGroup{},
	}
	h.opExec = h.ExecFunc
	h.ExecFunc = h.doHandle
	return h
}

func (h *poolHandler) Execute(task Task) {
	h.opExec(task)
}

func (h *poolHandler) doHandle(op iops.IOp) {
	closure := func(o iops.IOp, wg *sync.WaitGroup) func() {
		return func() {
			h.opExec(o)
			wg.Done()
		}
	}
	h.wg.Add(1)
	err := h.pool.Submit(closure(op, h.wg))
	if err != nil {
		logutil.Warnf("%v", err)
		op.SetError(err)
		h.wg.Done()
	}
}

func (h *poolHandler) Close() error {
	h.pool.Release()
	h.BaseTaskHandler.Close()
	h.wg.Wait()
	return nil
}
