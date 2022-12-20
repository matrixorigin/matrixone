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

package ops

import (
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	iops "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks/ops/base"
	iworker "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks/worker/base"
)

func NewOp(impl iops.IOpInternal, w iworker.IOpWorker) *Op {
	op := &Op{
		Impl:       impl,
		Worker:     w,
		CreateTime: time.Now(),
	}
	return op
}

func (op *Op) Push() error {
	r := op.Worker.SendOp(op)
	if !r {
		return moerr.NewInternalErrorNoCtx("send op error")
	}
	return nil
}

func (op *Op) GetError() error {
	return op.Err
}

func (op *Op) SetError(err error) {
	op.EndTime = time.Now()
	op.Err = err
	if op.ErrorC != nil {
		op.ErrorC <- err
	} else if op.DoneCB != nil {
		op.DoneCB(op)
	} else {
		panic("logic error")
	}
	if op.Observers != nil {
		for _, observer := range op.Observers {
			observer.OnExecDone(op.Impl)
		}
	}
}

func (op *Op) Waitable() bool {
	return op.DoneCB == nil
}

func (op *Op) WaitDone() error {
	err := <-op.ErrorC
	return err
}

func (op *Op) PreExecute() error {
	return nil
}

func (op *Op) PostExecute() error {
	return nil
}

func (op *Op) Execute() error {
	return nil
}

func (op *Op) OnExec() error {
	op.StartTime = time.Now()
	err := op.Impl.PreExecute()
	if err != nil {
		return err
	}
	err = op.Impl.Execute()
	if err != nil {
		return err
	}
	err = op.Impl.PostExecute()
	return err
}

func (op *Op) GetCreateTime() time.Time {
	return op.CreateTime
}

func (op *Op) GetStartTime() time.Time {
	return op.StartTime
}

func (op *Op) GetEndTime() time.Time {
	return op.EndTime
}

func (op *Op) GetExecutTime() int64 {
	return op.EndTime.Sub(op.StartTime).Microseconds()
}

func (op *Op) AddObserver(o iops.Observer) {
	if op.Observers == nil {
		op.Observers = make([]iops.Observer, 0)
	}
	op.Observers = append(op.Observers, o)
}
