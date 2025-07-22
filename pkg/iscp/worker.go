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

package idxcdc

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/sm"
)

type Worker interface {
	Submit(iteration *Iteration) error
	Stop()
}

type worker struct {
	queue sm.Queue
}

func NewWorker() Worker {
	worker := &worker{}
	worker.queue = sm.NewSafeQueue(10000, 100, worker.onItem)
	worker.queue.Start()
	return worker
}

func (w *worker) Submit(iteration *Iteration) error {
	_, err := w.queue.Enqueue(iteration)
	return err
}

func (w *worker) onItem(items ...any) {
	for _, item := range items {
		item.(*Iteration).Run()
	}
}

func (w *worker) Stop() {
	w.queue.Stop()
}
