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

package worker

import (
	"matrixone/pkg/vm/process"
	"matrixone/pkg/vm/routines/task"
)

func New(proc *process.Process) Worker {
	return &worker{
		proc: proc,
		ch:   make(chan struct{}),
		ts:   make(chan task.Task),
	}
}

func (w *worker) Run() {
	for {
		select {
		case <-w.ch:
			w.ch <- struct{}{}
			return
		case t := <-w.ts:
			t.Stop(t.Execute(w.proc))
		}
	}
}

func (w *worker) Stop() {
	w.ch <- struct{}{}
	<-w.ch
	close(w.ch)
	close(w.ts)
}

func (w *worker) AddTask(t task.Task) {
	w.ts <- t
}
