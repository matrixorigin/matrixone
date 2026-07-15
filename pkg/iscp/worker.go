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
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"go.uber.org/zap"
)

const (
	ISCPWorkerThread = 10

	SubmitRetryTimes    = 1000
	SubmitRetryDuration = time.Hour
)

type Worker interface {
	Submit(iteration *IterationContext) error
	// Stop cancels running work and discards work that has not started. The
	// executor repairs and reloads durable state before publishing a successor
	// generation, so abandoned optimistic state cannot cross that boundary.
	Stop()
}

type worker struct {
	exec        *ISCPTaskExecutor
	cnUUID      string
	cnEngine    engine.Engine
	cnTxnClient client.TxnClient
	mp          *mpool.MPool
	taskChan    chan *IterationContext
	wg          sync.WaitGroup
	cancel      context.CancelFunc
	ctx         context.Context
	closed      atomic.Bool
	stopOnce    sync.Once
	admissions  workerAdmissionGate
}

type workerAdmissionGate struct {
	mu sync.RWMutex
	wg sync.WaitGroup
}

// enter registers a sender before it can block. seal excludes new senders,
// cancels blocked ones, and waits until no sender can arrive after draining.
func (g *workerAdmissionGate) enter(closed *atomic.Bool) bool {
	if closed.Load() {
		return false
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	if closed.Load() {
		return false
	}
	g.wg.Add(1)
	return true
}

func (g *workerAdmissionGate) leave() {
	g.wg.Done()
}

func (g *workerAdmissionGate) seal(cancel context.CancelFunc) {
	g.mu.Lock()
	cancel()
	g.mu.Unlock()
	g.wg.Wait()
}

func NewWorker(exec *ISCPTaskExecutor, cnUUID string, cnEngine engine.Engine, cnTxnClient client.TxnClient, mp *mpool.MPool) Worker {
	worker := &worker{
		exec:        exec,
		cnUUID:      cnUUID,
		cnEngine:    cnEngine,
		cnTxnClient: cnTxnClient,
		taskChan:    make(chan *IterationContext, 10000),
		mp:          mp,
	}
	worker.ctx, worker.cancel = context.WithCancel(context.Background())
	worker.start()
	return worker
}

// start seals the worker generation before NewWorker publishes it. Stop must
// never observe a zero WaitGroup while worker goroutines can still be created.
func (w *worker) start() {
	w.wg.Add(ISCPWorkerThread)
	for i := 0; i < ISCPWorkerThread; i++ {
		go func() {
			defer w.wg.Done()
			for {
				select {
				case <-w.ctx.Done():
					return
				case task := <-w.taskChan:
					// Cancellation wins over queued work. This keeps Stop bounded
					// and prevents a stopped generation from starting new tasks.
					select {
					case <-w.ctx.Done():
						return
					default:
					}
					w.onItem(task)
				}
			}
		}()
	}
}

func (w *worker) Submit(iteration *IterationContext) error {
	if iteration == nil {
		return moerr.NewInternalErrorNoCtx("cannot submit a nil ISCP iteration")
	}
	if !w.admissions.enter(&w.closed) {
		return moerr.NewInternalErrorNoCtx("ISCP-Worker is closed")
	}
	defer w.admissions.leave()
	select {
	case w.taskChan <- iteration:
		return nil
	case <-w.ctx.Done():
		return moerr.NewInternalErrorNoCtx("ISCP-Worker is closed")
	}
}

func (w *worker) onItem(iterCtx *IterationContext) {
	iterCtx = w.exec.filterFencedIteration(iterCtx)
	if iterCtx == nil {
		return
	}
	err := retry(
		w.ctx,
		func() error {
			err := ExecuteIterationWithRuntime(
				w.ctx,
				w.exec,
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
		SubmitRetryTimes,
		DefaultRetryInterval,
		SubmitRetryDuration,
	)
	if err != nil {
		statuses := make([]*JobStatus, len(iterCtx.jobNames))
		for i := range statuses {
			statuses[i] = &JobStatus{
				ErrorMsg: err.Error(),
			}
		}
		preLSN := make([]uint64, len(iterCtx.lsn))
		for i := range iterCtx.lsn {
			preLSN[i] = iterCtx.lsn[i] - 1
		}
		err = retry(
			w.ctx,
			func() error {
				return FlushJobStatusOnIterationState(
					w.ctx,
					w.cnUUID,
					w.cnEngine,
					w.cnTxnClient,
					iterCtx.accountID,
					iterCtx.tableID,
					iterCtx.jobNames,
					iterCtx.jobIDs,
					iterCtx.lsn,
					statuses,
					iterCtx.fromTS,
					ISCPJobState_Completed,
					preLSN,
				)
			},
			SubmitRetryTimes,
			DefaultRetryInterval,
			SubmitRetryDuration,
		)
		if err != nil {
			logutil.Error(
				"ISCP-Task workerflush job status failed",
				zap.Error(err),
			)
		}
	}
}

func (w *worker) Stop() {
	w.stopOnce.Do(func() {
		w.closed.Store(true)
		w.admissions.seal(w.cancel)
		w.wg.Wait()
		for {
			select {
			case <-w.taskChan:
			default:
				return
			}
		}
	})
}
