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

package publication

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
	PublicationWorkerThread = 10

	SubmitRetryTimes    = 1000
	SubmitRetryDuration = time.Hour
)

type Worker interface {
	Submit(taskID uint64, lsn uint64, state int8) error
	Stop()
}

type worker struct {
	cnUUID                   string
	cnEngine                 engine.Engine
	cnTxnClient              client.TxnClient
	mp                       *mpool.MPool
	upstreamSQLHelperFactory UpstreamSQLHelperFactory
	taskChan                 chan *TaskContext
	wg                       sync.WaitGroup
	cancel                   context.CancelFunc
	ctx                      context.Context
	closed                   atomic.Bool
}

type TaskContext struct {
	TaskID uint64
	LSN    uint64
	State  int8
}

func NewWorker(
	cnUUID string,
	cnEngine engine.Engine,
	cnTxnClient client.TxnClient,
	mp *mpool.MPool,
	upstreamSQLHelperFactory UpstreamSQLHelperFactory,
) Worker {
	worker := &worker{
		cnUUID:                   cnUUID,
		cnEngine:                 cnEngine,
		cnTxnClient:              cnTxnClient,
		taskChan:                 make(chan *TaskContext, 10000),
		mp:                       mp,
		upstreamSQLHelperFactory: upstreamSQLHelperFactory,
	}
	worker.ctx, worker.cancel = context.WithCancel(context.Background())
	go worker.Run()
	return worker
}

func (w *worker) Run() {
	for i := 0; i < PublicationWorkerThread; i++ {
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

func (w *worker) Submit(taskID uint64, lsn uint64, state int8) error {
	if w.closed.Load() {
		return moerr.NewInternalError(context.Background(), "Publication-Worker is closed")
	}
	w.taskChan <- &TaskContext{
		TaskID: taskID,
		LSN:    lsn,
		State:  state,
	}
	return nil
}

func (w *worker) onItem(taskCtx *TaskContext) {
	err := retryPublication(
		w.ctx,
		func() error {
			err := ExecuteIteration(
				w.ctx,
				w.cnUUID,
				w.cnEngine,
				w.cnTxnClient,
				taskCtx.TaskID,
				taskCtx.LSN,
				taskCtx.State,
				w.upstreamSQLHelperFactory,
				w.mp,
				nil, // utHelper
			)
			if err != nil {
				logutil.Error(
					"Publication-Task execute iteration failed",
					zap.Uint64("taskID", taskCtx.TaskID),
					zap.Uint64("lsn", taskCtx.LSN),
					zap.Int8("state", taskCtx.State),
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
		logutil.Error(
			"Publication-Task worker execute iteration failed",
			zap.Uint64("taskID", taskCtx.TaskID),
			zap.Uint64("lsn", taskCtx.LSN),
			zap.Error(err),
		)
	}
}

func (w *worker) Stop() {
	w.closed.Store(true)
	w.cancel()
	w.wg.Wait()
	close(w.taskChan)
}
