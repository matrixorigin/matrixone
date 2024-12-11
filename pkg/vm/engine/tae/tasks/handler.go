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
	"context"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/panjf2000/ants/v2"
)

var (
	ErrTaskHandleEnqueue = moerr.NewInternalErrorNoCtx("tae: task handle enqueue")
)

type baseTaskHandler struct {
	OpWorker
}

func NewBaseEventHandler(ctx context.Context, name string) *baseTaskHandler {
	h := &baseTaskHandler{
		OpWorker: *NewOpWorker(ctx, name),
	}
	return h
}

func (h *baseTaskHandler) Enqueue(task Task) {
	if !h.SendOp(task) {
		task.SetError(ErrTaskHandleEnqueue)
		err := task.Cancel()
		if err != nil {
			logutil.Warnf("%v", err)
		}
	}
}

func (h *baseTaskHandler) Execute(task Task) {
	h.execFunc(task)
}

var (
	poolHandlerName = "PoolHandler"
)

type poolHandler struct {
	baseTaskHandler
	opExec OpExecFunc
	pool   *ants.Pool
	wg     *sync.WaitGroup
}

func NewPoolHandler(ctx context.Context, num int) *poolHandler {
	pool, err := ants.NewPool(num)
	if err != nil {
		panic(err)
	}
	h := &poolHandler{
		baseTaskHandler: *NewBaseEventHandler(ctx, poolHandlerName),
		pool:            pool,
		wg:              &sync.WaitGroup{},
	}
	h.opExec = h.execFunc
	h.execFunc = h.doHandle
	return h
}

func (h *poolHandler) Execute(task Task) {
	h.opExec(task)
}

func (h *poolHandler) doHandle(op IOp) {
	closure := func(o IOp, wg *sync.WaitGroup) func() {
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

func (h *poolHandler) Stop() {
	h.pool.Release()
	h.baseTaskHandler.Stop()
	h.wg.Wait()
}
