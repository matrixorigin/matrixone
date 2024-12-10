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
	"context"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

func (op *Op) GetError() error {
	return op.err
}

func (op *Op) SetError(err error) {
	op.endTime = time.Now()
	op.err = err
	if op.errorC != nil {
		op.errorC <- err
	} else if op.doneCB != nil {
		op.doneCB(op)
	} else {
		panic("logic error")
	}
	for _, observer := range op.observers {
		observer.OnExecDone(op.impl)
	}
}

func (op *Op) Waitable() bool {
	return op.doneCB == nil
}

func (op *Op) WaitDone(ctx context.Context) error {
	if op.waitedOnce.Load() {
		return moerr.NewTAEErrorNoCtx("wait done twice")
	}
	defer op.waitedOnce.Store(true)

	if op.errorC == nil {
		return moerr.NewTAEErrorNoCtx("wait done without error channel")
	}
	select {
	case err := <-op.errorC:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (op *Op) Execute() error {
	return nil
}

func (op *Op) OnExec(ctx context.Context) error {
	op.startTime = time.Now()
	return op.impl.Execute(ctx)
}

func (op *Op) GetCreateTime() time.Time {
	return op.createTime
}

func (op *Op) GetStartTime() time.Time {
	return op.startTime
}

func (op *Op) GetEndTime() time.Time {
	return op.endTime
}

func (op *Op) GetExecutTime() int64 {
	return op.endTime.Sub(op.startTime).Microseconds()
}

func (op *Op) AddObserver(o Observer) {
	if op.observers == nil {
		op.observers = make([]Observer, 0)
	}
	op.observers = append(op.observers, o)
}
