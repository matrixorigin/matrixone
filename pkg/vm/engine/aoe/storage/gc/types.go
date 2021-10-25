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

package gc

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/gc/gci"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/ops"
	iops "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/ops/base"
	w "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/worker"
	iw "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/worker/base"
	"sync"
	"sync/atomic"
	"time"
)

const (
	ExecutorName = "GCExecutor"
)

type BaseRequest struct {
	ops.Op
	Next      gci.IRequest
	Iteration uint32
}

func (req *BaseRequest) GetIteration() uint32 {
	return req.Iteration
}

func (req *BaseRequest) IncIteration() {
	req.Iteration++
}

func (req *BaseRequest) GetNext() gci.IRequest {
	return req.Next
}

type Worker struct {
	w.OpWorker
	hb struct {
		interval time.Duration
		ctx      context.Context
		cancel   context.CancelFunc
		wg       sync.WaitGroup
	}
	exec struct {
		sync.RWMutex
		reqs   []gci.IRequest
		worker iw.IOpWorker
	}
}

func NewWorker(cfg *gci.WorkerCfg) gci.IWorker {
	wk := &Worker{
		OpWorker: *w.NewOpWorker("GCManager"),
	}
	wk.hb.interval = cfg.Interval
	wk.hb.ctx, wk.hb.cancel = context.WithCancel(context.Background())
	wk.exec.reqs = make([]gci.IRequest, 0)
	if cfg.Executor != nil {
		wk.exec.worker = cfg.Executor
	} else {
		wk.exec.worker = w.NewOpWorker(ExecutorName)
	}
	wk.ExecFunc = wk.onOp
	return wk
}

func (wk *Worker) onOp(op iops.IOp) {
	wk.exec.Lock()
	wk.exec.reqs = append(wk.exec.reqs, op.(gci.IRequest))
	wk.exec.Unlock()
}

func (wk *Worker) Accept(request gci.IRequest) {
	wk.SendOp(request)
}

func (wk *Worker) Start() {
	wk.hb.wg.Add(1)
	wk.OpWorker.Start()
	go wk.heartbeat()
	wk.exec.worker.Start()
}

func (wk *Worker) heartbeat() {
	ticker := time.NewTicker(wk.hb.interval)
	defer ticker.Stop()
	for {
		select {
		case <-wk.hb.ctx.Done():
			wk.hb.wg.Done()
			return
		case <-ticker.C:
			wk.process()
		}
	}
}

func (wk *Worker) process() {
	wk.exec.Lock()
	reqs := wk.exec.reqs
	wk.exec.reqs = make([]gci.IRequest, 0)
	wk.exec.Unlock()
	for _, req := range reqs {
		wk.exec.worker.SendOp(req)
		if req.GetIteration() >= 20 {
			panic("cannot execute gc req")
		}
		err := req.WaitDone()
		if err != nil {
			// TODO
			if req.GetIteration() > 3 {
				logutil.Warnf("handle req err: %s", err)
			}
			req.IncIteration()
			wk.Accept(req)
		} else {
			nextReq := req.GetNext()
			if nextReq != nil {
				wk.Accept(nextReq)
			}
		}
	}
	if atomic.LoadInt32(&wk.State) == w.STOPPED {
		wk.hb.cancel()
	}
}

func (wk *Worker) Stop() {
	wk.OpWorker.Stop()
	wk.hb.wg.Wait()
	wk.exec.worker.Stop()
}
