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
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/ops"
	iops "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/ops/base"
)

var (
	eventId     uint64 = 0
	EventPrefix string = "Event"
)

type EventType uint16

const (
	EmptyEvent EventType = iota
	MockEvent
	CustomizedEvent
)

func GetNextEventId() uint64 {
	return atomic.AddUint64(&eventId, uint64(1))
}

type BaseEvent struct {
	ops.Op
	id   uint64
	t    EventType
	exec func(Event) error
}

func NewBaseEvent(impl iops.IOpInternal, t EventType, doneCB ops.OpDoneCB, waitable bool) *BaseEvent {
	e := &BaseEvent{t: t}
	if doneCB == nil && !waitable {
		doneCB = e.onDone
	}
	if impl == nil {
		impl = e
	}
	e.Op = ops.Op{
		Impl:   impl,
		DoneCB: doneCB,
	}
	if doneCB == nil {
		e.Op.ErrorC = make(chan error)
	}
	return e
}

func (e *BaseEvent) AttachID(id uint64) { e.id = id }
func (e *BaseEvent) ID() uint64         { return e.id }
func (e *BaseEvent) Type() EventType    { return e.t }
func (e *BaseEvent) Cancel() error      { return nil }
func (e *BaseEvent) Execute() error {
	if e.exec != nil {
		return e.exec(e)
	}
	logutil.Debugf("Execute Event Type=%d, ID=%d", e.t, e.id)
	return nil
}
func (e *BaseEvent) onDone(_ iops.IOp) {
	// log.Infof("Event %d is done: %v", e.id, e.Err)
}
