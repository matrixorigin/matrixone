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

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/sm"
)

type Worker interface {
	Submit(iteration *IterationContext) error
	Stop()
}

type worker struct {
	queue       sm.Queue
	cnUUID      string
	cnEngine    engine.Engine
	cnTxnClient client.TxnClient
	mp          *mpool.MPool
}

func NewWorker(cnUUID string, cnEngine engine.Engine, cnTxnClient client.TxnClient, mp *mpool.MPool) Worker {
	worker := &worker{
		cnUUID:      cnUUID,
		cnEngine:    cnEngine,
		cnTxnClient: cnTxnClient,
		mp:          mp,
	}
	worker.queue = sm.NewSafeQueue(10000, 100, worker.onItem)
	worker.queue.Start()
	return worker
}

func (w *worker) Submit(iteration *IterationContext) error {
	status := make([]*JobStatus, len(iteration.jobNames))
	FlushJobStatusOnIterationState(
		context.Background(),
		w.cnUUID,
		w.cnEngine,
		w.cnTxnClient,
		iteration.accountID,
		iteration.tableID,
		iteration.jobNames,
		status,
		iteration.fromTS,
		ISCPJobState_Pending,
	)
	_, err := w.queue.Enqueue(iteration)
	return err
}

func (w *worker) onItem(items ...any) {
	for _, item := range items {
		iterCtx := item.(*IterationContext)
		for {
			err := ExecuteIteration(
				context.Background(),
				w.cnUUID,
				w.cnEngine,
				w.cnTxnClient,
				iterCtx,
				w.mp,
			)
			if err == nil {
				break
			}
		}
	}
}

func (w *worker) Stop() {
	w.queue.Stop()
}
