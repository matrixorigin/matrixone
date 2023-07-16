// Copyright 2023 Matrix Origin
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

package workerpool

import (
	"sync"
)

// Worker is responsible for executing the work. There could be many workers in a worker pool.
type Worker struct {
	wg               *sync.WaitGroup
	workerCh         chan Work
	freeWorkerPoolCh chan chan Work
	stopCh           chan bool
}

func NewWorker(freeWorkerPoolCh chan chan Work, wg *sync.WaitGroup) *Worker {
	return &Worker{
		wg:               wg,
		workerCh:         make(chan Work),
		freeWorkerPoolCh: freeWorkerPoolCh,
		stopCh:           make(chan bool),
	}
}

func (w *Worker) Start() {
	go func() {
		w.wg.Add(1)
		for {

			// Add workerCh back to freeWorkerPoolCh when it is not doing any work (ie completed the old work).
			w.freeWorkerPoolCh <- w.workerCh

			select {
			case work := <-w.workerCh:
				work()
			case <-w.stopCh:
				w.wg.Done()
				return
			}
		}
	}()
}

func (w *Worker) Stop() {
	w.stopCh <- true
}
