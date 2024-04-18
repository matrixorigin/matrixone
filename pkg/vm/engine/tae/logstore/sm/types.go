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

package sm

import (
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

var (
	ErrClose = moerr.NewInternalErrorNoCtx("closed")
	ErrFull  = moerr.NewInternalErrorNoCtx("nonblocking queue is full, dropped item")
)

type OnFinCB = func()
type EnqueueOp = func(any) any
type OnItemsCB = func(...any)

type Closable interface {
	IsClosed() bool
	TryClose() bool
}

type ClosedState struct {
	closed atomic.Int32
}

func (c *ClosedState) IsClosed() bool {
	return c.closed.Load() == int32(1)
}

func (c *ClosedState) TryClose() bool {
	return c.closed.CompareAndSwap(0, 1)
}

type Queue interface {
	Start()
	Stop()

	// Enqueue puts an item into this queue
	// it will return directly when if it is an unblocking queue and there has no more free space,
	// and ErrFull will be return to notify the producer.
	Enqueue(any) (any, error)
}

type StateMachine interface {
	Start()
	Stop()
	EnqueueRecevied(any) (any, error)
}
