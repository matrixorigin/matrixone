// Copyright 2022 Matrix Origin
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

package iscp

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"go.uber.org/zap"
)

const (
	ISCPWorkerThread = 10
)

type Worker interface {
	Submit(iteration *IterationContext) error
	Stop()
}

type worker struct {
	cnUUID      string
	cnEngine    engine.Engine
	cnTxnClient client.TxnClient
	mp          *mpool.MPool
	taskChan    chan *IterationContext
	wg          sync.WaitGroup
	cancel      context.CancelFunc
	ctx         context.Context
	closed      atomic.Bool
}

func NewWorker(cnUUID string, cnEngine engine.Engine, cnTxnClient client.TxnClient, mp *mpool.MPool) Worker {
	worker := &worker{
		cnUUID:      cnUUID,
		cnEngine:    cnEngine,
		cnTxnClient: cnTxnClient,
		taskChan:    make(chan *IterationContext, 10000),
		mp:          mp,
	}
	worker.ctx, worker.cancel = context.WithCancel(context.Background())
	go worker.Run()
	return worker
}

func (w *worker) Run() {
	for i := 0; i < ISCPWorkerThread; i++ {
		w.wg.Add(1)
		go func() {
			defer w.wg.Done()
			for {
				select {
				case <-w.ctx.Done():
					return
				case task := <-w.taskChan:
					w.onItem(task)
				}
			}
		}()
	}
}

func (w *worker) Submit(iteration *IterationContext) error {
	status := make([]*JobStatus, len(iteration.jobNames))
	for i := range status {
		status[i] = &JobStatus{}
	}
	if w.closed.Load() {
		return moerr.NewInternalError(context.Background(), "ISCP-Worker is closed")
	}
	w.taskChan <- iteration
	return nil
}

func (w *worker) onItem(iterCtx *IterationContext) {
	err := retry(
		func() error {
			err := ExecuteIteration(
				w.ctx,
				w.cnUUID,
				w.cnEngine,
				w.cnTxnClient,
				iterCtx,
				w.mp,
			)
			if err != nil {
				logutil.Error(
					"ISCP-Task execute iteration failed",
					zap.Uint64("tableID", iterCtx.tableID),
					zap.Any("iterCtx", iterCtx.jobNames),
					zap.Error(err),
				)
			}
			return err
		},
		DefaultRetryTimes,
	)
	if err != nil {
		statuses := make([]*JobStatus, len(iterCtx.jobNames))
		for i := range statuses {
			statuses[i] = &JobStatus{
				ErrorMsg: err.Error(),
			}
		}
		for {
			select {
			case <-w.ctx.Done():
				return
			default:
			}
			err = FlushJobStatusOnIterationState(
				w.ctx,
				w.cnUUID,
				w.cnEngine,
				w.cnTxnClient,
				iterCtx.accountID,
				iterCtx.tableID,
				iterCtx.jobNames,
				iterCtx.jobIDs,
				statuses,
				iterCtx.fromTS,
				ISCPJobState_Completed,
			)
			if err == nil {
				break
			}
		}
	}
}

func (w *worker) Stop() {
	w.closed.Store(true)
	w.cancel()
	w.wg.Wait()
	close(w.taskChan)
}
