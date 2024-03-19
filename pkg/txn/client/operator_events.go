// Copyright 2023 Matrix Origin
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

package client

import (
	"time"

	"github.com/matrixorigin/matrixone/pkg/pb/txn"
)

type eventCallback struct {
	typ       EventType
	callbacks []func(TxnEvent)
}

func (ec eventCallback) call(event TxnEvent) {
	if ec.typ.Value != event.Event.Value {
		return
	}
	for _, cb := range ec.callbacks {
		cb(event)
	}
}

// TxnEvent txn events
type EventType struct {
	Value int
	Name  string
}

var (
	OpenEvent            = EventType{0, "open"}
	WaitActiveEvent      = EventType{1, "wait-active"}
	UpdateSnapshotEvent  = EventType{2, "update-snapshot"}
	LockEvent            = EventType{3, "lock"}
	UnlockEvent          = EventType{4, "unlock"}
	RangesEvent          = EventType{5, "ranges"}
	BuildPlanEvent       = EventType{6, "build-plan"}
	ExecuteSQLEvent      = EventType{7, "execute-sql"}
	CompileEvent         = EventType{8, "compile"}
	TableScanEvent       = EventType{9, "table-scan"}
	CommitEvent          = EventType{95, "commit"}
	CommitResponseEvent  = EventType{96, "commit-response"}
	CommitWaitApplyEvent = EventType{97, "wait-applied"}
	RollbackEvent        = EventType{98, "rollback"}
	ClosedEvent          = EventType{99, "closed"}
)

func (tc *txnOperator) AppendEventCallback(
	event EventType,
	callbacks ...func(TxnEvent)) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	if tc.mu.closed {
		panic("append callback on closed txn")
	}
	if len(tc.mu.callbacks) == 0 {
		tc.mu.callbacks = make([]eventCallback, 0, 32)
	}
	tc.mu.callbacks = append(tc.mu.callbacks, eventCallback{event, callbacks})
}

func (tc *txnOperator) triggerEvent(event TxnEvent) {
	tc.mu.RLock()
	defer tc.mu.RUnlock()
	tc.triggerEventLocked(event)
}

func (tc *txnOperator) triggerEventLocked(event TxnEvent) {
	if tc.mu.callbacks == nil {
		return
	}
	for _, cb := range tc.mu.callbacks {
		cb.call(event)
	}
}

func newCostEvent(
	event EventType,
	txn txn.TxnMeta,
	Sequence uint64,
	err error,
	cost time.Duration) TxnEvent {
	return TxnEvent{
		Event:     event,
		Txn:       txn,
		Sequence:  Sequence,
		Err:       err,
		Cost:      cost,
		CostEvent: true,
	}
}

func newEvent(
	event EventType,
	txn txn.TxnMeta,
	Sequence uint64,
	err error) TxnEvent {
	return TxnEvent{
		Event:     event,
		Txn:       txn,
		Sequence:  Sequence,
		Err:       err,
		CostEvent: false,
	}
}
