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
	"errors"
	"sync"
	"sync/atomic"
)

type Work func()

type WorkerPool struct {
	//Dispatcher : Responsible for listening to new Work request and distributing to the free Workers.
	dispatcherCh chan Work
	dispatcherWg *sync.WaitGroup

	// Worker Fields:
	workers []*Worker
	// if a worker completes the work, the workers listener channel is added to
	// freeWorkerPoolCh so that it can be used to serve new requests.
	freeWorkerPoolCh chan chan Work
	workersWg        *sync.WaitGroup

	//Worker Pool termination fields
	stopped atomic.Bool
	stopCh  chan bool
}

func NewWorkerPool(workerCount int) *WorkerPool {

	freeWorkerPoolCh := make(chan chan Work, workerCount)
	workersWg := &sync.WaitGroup{}
	workers := make([]*Worker, workerCount, workerCount)
	for i := 0; i < workerCount; i++ {
		workers[i] = NewWorker(freeWorkerPoolCh, workersWg)
	}

	return &WorkerPool{
		// In this worker pool pattern, dispatcherCh is unbounded.
		// Ref: https://github.com/dirkaholic/kyoo/blob/9ae445c9faa96238cb604edd4fe91b6d347586db/jobqueue.go#L28
		// In worker pool patterns, the dispatcher channel could be
		// bounded: https://github.com/godoylucase/workers-pool/blob/9ec8790cace339252642eed54d93c1f5dc46967f/wpool/exec.go#L39
		dispatcherCh: make(chan Work),
		dispatcherWg: &sync.WaitGroup{},

		workers:          workers,
		freeWorkerPoolCh: freeWorkerPoolCh,
		workersWg:        workersWg,

		stopped: atomic.Bool{},
		stopCh:  make(chan bool),
	}
}

func (q *WorkerPool) Start() {
	for i := 0; i < len(q.workers); i++ {
		q.workers[i].Start()
	}
	go q.startDispatcher()

	q.stopped.Store(false)
}

func (q *WorkerPool) startDispatcher() {
	q.dispatcherWg.Add(1)
	for {
		select {
		case work := <-q.dispatcherCh:
			workerChannel := <-q.freeWorkerPoolCh // wait for a free worker.
			workerChannel <- work                 // send this new work to that free worker.
		case <-q.stopCh:
			for i := 0; i < len(q.workers); i++ {
				q.workers[i].Stop()
			}
			q.workersWg.Wait()    // wait for all the works to complete
			q.dispatcherWg.Done() // close the dispatcher thread.
			return
		}
	}
}

func (q *WorkerPool) Stop() {
	q.stopped.Store(true)

	// Stopping queue
	q.stopCh <- true
	q.dispatcherWg.Wait()

	// Stopped queue
	close(q.dispatcherCh)
}

func (q *WorkerPool) Submit(work Work) error {
	if q.stopped.Load() {
		return errors.New("worker pool stopped")
	}

	q.dispatcherCh <- work
	return nil
}
