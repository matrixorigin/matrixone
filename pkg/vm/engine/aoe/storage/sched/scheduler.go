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

package sched

import (
	"errors"
	logutil2 "matrixone/pkg/logutil"
	iops "matrixone/pkg/vm/engine/aoe/storage/ops/base"
	ops "matrixone/pkg/vm/engine/aoe/storage/worker"
	"strconv"
)

var (
	ErrDispatcherNotFound = errors.New("aoe sched: dispatcher not found")
	ErrSchedule           = errors.New("aoe sched: cannot schedule")
)

type BaseScheduler struct {
	ops.OpWorker
	idAlloc     IDAllocFunc
	dispatchers map[EventType]Dispatcher
}

func NewBaseScheduler(name string) *BaseScheduler {
	scheduler := &BaseScheduler{
		OpWorker:    *ops.NewOpWorker(name),
		idAlloc:     GetNextEventId,
		dispatchers: make(map[EventType]Dispatcher),
	}
	scheduler.ExecFunc = scheduler.doDispatch
	return scheduler
}

func (s *BaseScheduler) RegisterDispatcher(t EventType, dispatcher Dispatcher) {
	s.dispatchers[t] = dispatcher
}

func (s *BaseScheduler) Schedule(e Event) error {
	e.AttachID(s.idAlloc())
	if !s.SendOp(e) {
		return ErrSchedule
	}
	return nil
}

func (s *BaseScheduler) doDispatch(op iops.IOp) {
	e := op.(Event)
	dispatcher := s.dispatchers[e.Type()]
	if dispatcher == nil {
		logutil2.Error(strconv.Itoa(int(e.Type())))
		panic(ErrDispatcherNotFound)
	}
	dispatcher.Dispatch(e)
}

func (s *BaseScheduler) Stop() {
	s.OpWorker.Stop()
	for _, d := range s.dispatchers {
		d.Close()
	}
}
