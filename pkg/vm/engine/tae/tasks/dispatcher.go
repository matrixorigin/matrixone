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

package tasks

import (
	"io"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

var (
	ErrDispatchWrongTask = moerr.NewInternalErrorNoCtx("tae: wrong task type")
)

type Dispatcher interface {
	io.Closer
	Dispatch(Task)
}

type TaskHandler interface {
	io.Closer
	Start()
	Enqueue(Task)
	Execute(Task)
}

type BaseDispatcher struct {
	handlers map[TaskType]TaskHandler
}

func NewBaseDispatcher() *BaseDispatcher {
	d := &BaseDispatcher{
		handlers: make(map[TaskType]TaskHandler),
	}
	return d
}

func (d *BaseDispatcher) Dispatch(task Task) {
	handler, ok := d.handlers[task.Type()]
	if !ok {
		panic(ErrDispatchWrongTask)
	}
	handler.Enqueue(task)
}

func (d *BaseDispatcher) RegisterHandler(t TaskType, h TaskHandler) {
	d.handlers[t] = h
}

func (d *BaseDispatcher) Close() error {
	for _, h := range d.handlers {
		h.Close()
	}
	return nil
}

type ScopedTaskSharder = func(scope *common.ID) int
type BaseScopedDispatcher struct {
	handlers []TaskHandler
	sharder  ScopedTaskSharder
	curr     uint64
}

func NewBaseScopedDispatcher(sharder ScopedTaskSharder) *BaseScopedDispatcher {
	d := &BaseScopedDispatcher{
		handlers: make([]TaskHandler, 0),
		curr:     0,
	}
	if sharder == nil {
		d.sharder = d.roundRobinSharder
	} else {
		d.sharder = sharder
	}
	return d
}

func (d *BaseScopedDispatcher) AddHandle(h TaskHandler) {
	d.handlers = append(d.handlers, h)
}

func (d *BaseScopedDispatcher) roundRobinSharder(scope *common.ID) int {
	curr := atomic.AddUint64(&d.curr, uint64(1))
	return int(curr)
}

func (d *BaseScopedDispatcher) Dispatch(task Task) {
	scoped := task.(ScopedTask)
	val := d.sharder(scoped.Scope())

	shardIdx := val % len(d.handlers)
	d.handlers[shardIdx].Enqueue(task)
}

func (d *BaseScopedDispatcher) Close() error {
	for _, h := range d.handlers {
		h.Close()
	}
	return nil
}
