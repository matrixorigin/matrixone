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
	"github.com/matrixorigin/matrixone/pkg/logutil"
	iops "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/ops/base"
	ops "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/worker"
)

var (
	ErrEventHandleEnqueue = errors.New("aoe: event handle enqueue")
)

type mockEventHandler struct {
	BaseEventHandler
}

func newMockEventHandler(name string) *mockEventHandler {
	h := &mockEventHandler{
		BaseEventHandler: *NewBaseEventHandler(name),
	}
	h.ExecFunc = h.doHandle
	return h
}

func (h *mockEventHandler) doHandle(op iops.IOp) {
	e := op.(Event)
	logutil.Debugf("Handling event type %v, id %d", e.Type(), e.ID())
}

type BaseEventHandler struct {
	ops.OpWorker
}

func NewBaseEventHandler(name string) *BaseEventHandler {
	h := &BaseEventHandler{
		OpWorker: *ops.NewOpWorker(name),
	}
	return h
}

func (h *BaseEventHandler) Enqueue(e Event) {
	if !h.SendOp(e) {
		e.SetError(ErrEventHandleEnqueue)
		err := e.Cancel()
		if err != nil {
			logutil.Warnf("%v", err)
		}
	}
}

func (h *BaseEventHandler) ExecuteEvent(e Event) {
	h.ExecFunc(e)
}

func (h *BaseEventHandler) Close() error {
	h.Stop()
	return nil
}

type singleWorkerHandler struct {
	BaseEventHandler
}

func NewSingleWorkerHandler(name string) *singleWorkerHandler {
	h := &singleWorkerHandler{
		BaseEventHandler: *NewBaseEventHandler(name),
	}
	return h
}
