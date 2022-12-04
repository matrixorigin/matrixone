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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	ops "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks/worker"
)

var (
	ErrTaskHandleEnqueue = moerr.NewInternalErrorNoCtx("tae: task handle enqueue")
)

type BaseTaskHandler struct {
	ops.OpWorker
}

func NewBaseEventHandler(name string) *BaseTaskHandler {
	h := &BaseTaskHandler{
		OpWorker: *ops.NewOpWorker(name),
	}
	return h
}

func (h *BaseTaskHandler) Enqueue(task Task) {
	if !h.SendOp(task) {
		task.SetError(ErrTaskHandleEnqueue)
		err := task.Cancel()
		if err != nil {
			logutil.Warnf("%v", err)
		}
	}
}

func (h *BaseTaskHandler) Execute(task Task) {
	h.ExecFunc(task)
}

func (h *BaseTaskHandler) Close() error {
	h.Stop()
	return nil
}

type singleWorkerHandler struct {
	BaseTaskHandler
}

func NewSingleWorkerHandler(name string) *singleWorkerHandler {
	h := &singleWorkerHandler{
		BaseTaskHandler: *NewBaseEventHandler(name),
	}
	return h
}
