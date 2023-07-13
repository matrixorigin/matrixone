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
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
)

// TxnEvent txn events
type EventType int

const (
	// ClosedEvent txn closed event
	ClosedEvent = EventType(0)
)

func (tc *txnOperator) AppendEventCallback(
	event EventType,
	callbacks ...func(txn.TxnMeta)) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	if tc.mu.closed {
		panic("append callback on closed txn")
	}
	if tc.mu.callbacks == nil {
		tc.mu.callbacks = make(map[EventType][]func(txn.TxnMeta), 1)
	}
	tc.mu.callbacks[event] = append(tc.mu.callbacks[event], callbacks...)
}

func (tc *txnOperator) triggerEventLocked(event EventType) {
	if tc.mu.callbacks == nil {
		return
	}
	for _, cb := range tc.mu.callbacks[event] {
		cb(tc.mu.txn)
	}
}
