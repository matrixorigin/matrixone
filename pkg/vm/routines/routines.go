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

package routines

import (
	"matrixone/pkg/vm/process"
	"matrixone/pkg/vm/routines/task"
	"matrixone/pkg/vm/routines/worker"
	"sync"
	"sync/atomic"
)

func New(num int, procs []*process.Process) Routines {
	r := &routines{
		num: uint64(num),
		ch:  make(chan struct{}),
		ws:  make([]worker.Worker, num),
	}
	for i := 0; i < num; i++ {
		r.ws[i] = worker.New(procs[i])
	}
	return r
}

func (r *routines) Run() {
	var wg sync.WaitGroup

	for i, j := 0, len(r.ws); i < j; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			r.ws[idx].Run()
		}(i)
	}
	for {
		select {
		case <-r.ch:
			for _, w := range r.ws {
				w.Stop()
			}
			wg.Wait()
			r.ch <- struct{}{}
			return
		}
	}
}

func (r *routines) Stop() {
	r.ch <- struct{}{}
	<-r.ch
}

func (r *routines) AddTask(t task.Task) {
	r.ws[atomic.AddUint64(&r.cnt, 1)%r.num].AddTask(t)
}
