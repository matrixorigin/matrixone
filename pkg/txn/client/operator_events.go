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
	"context"
	"errors"

	"github.com/matrixorigin/matrixone/pkg/pb/txn"
)

// EventType identifies transaction lifecycle callbacks.
type EventType uint8

const (
	CommitEvent   EventType = 95
	RollbackEvent EventType = 98
	ClosedEvent   EventType = 99
)

// defaultTxnEventCallbacks is initialized once by txnClient and is immutable
// after the client is published.
type defaultTxnEventCallbacks struct {
	closed [2]TxnEventCallback
}

// txnEventCallbacks points at the client-owned defaults until a caller appends
// a custom callback. The custom callback map is then allocated per operator.
type txnEventCallbacks struct {
	defaults  *defaultTxnEventCallbacks
	callbacks map[EventType][]TxnEventCallback
}

func (tc *txnOperator) setDefaultEventCallbacks(callbacks *txnEventCallbacks) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	if tc.mu.closed {
		panic("set default callbacks on closed txn")
	}
	tc.mu.callbacks = callbacks
}

func (tc *txnOperator) AppendEventCallback(
	event EventType,
	callbacks ...TxnEventCallback) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	for _, callback := range callbacks {
		if callback.StatementScoped && event != ClosedEvent {
			panic("statement-scoped callback requires ClosedEvent")
		}
	}
	if tc.mu.closed {
		panic("append callback on closed txn")
	}
	if tc.mu.callbacks == nil || tc.mu.callbacks.callbacks == nil {
		var defaults *defaultTxnEventCallbacks
		if tc.mu.callbacks != nil {
			defaults = tc.mu.callbacks.defaults
		}
		tc.mu.callbacks = &txnEventCallbacks{
			defaults:  defaults,
			callbacks: make(map[EventType][]TxnEventCallback, 1),
		}
	}
	tc.mu.callbacks.callbacks[event] = append(tc.mu.callbacks.callbacks[event], callbacks...)
}

func (tc *txnOperator) BeginStatementCallbacks() {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	if tc.mu.callbacks == nil {
		tc.mu.statementCallbackStart = 0
		return
	}
	tc.mu.statementCallbackStart = len(tc.mu.callbacks.callbacks[ClosedEvent])
}

func (tc *txnOperator) RollbackStatementCallbacks(ctx context.Context) error {
	tc.mu.Lock()
	var detached []TxnEventCallback
	if tc.mu.callbacks != nil && tc.mu.callbacks.callbacks != nil {
		callbacks := tc.mu.callbacks.callbacks[ClosedEvent]
		start := min(tc.mu.statementCallbackStart, len(callbacks))
		kept := start
		for _, callback := range callbacks[start:] {
			if callback.StatementScoped {
				detached = append(detached, callback)
			} else {
				callbacks[kept] = callback
				kept++
			}
		}
		clear(callbacks[kept:])
		tc.mu.callbacks.callbacks[ClosedEvent] = callbacks[:kept]
		tc.mu.statementCallbackStart = kept
	}
	meta := tc.mu.txn
	meta.Status = txn.TxnStatus_Aborted // The detached actions, not the transaction, abort.
	tc.mu.Unlock()

	event := newEvent(ClosedEvent, meta, nil)
	var err error
	for _, callback := range detached {
		err = errors.Join(err, callback.Func(ctx, tc, event, callback.Value))
	}
	return err
}

func (tc *txnOperator) triggerEvent(ctx context.Context, event TxnEvent) error {
	tc.mu.RLock()
	defer tc.mu.RUnlock()
	return tc.triggerEventLocked(ctx, event)
}

func (tc *txnOperator) triggerEventLocked(ctx context.Context, event TxnEvent) (err error) {
	if tc.mu.callbacks == nil {
		return
	}
	if event.Event == ClosedEvent && tc.mu.callbacks.defaults != nil {
		for _, cb := range tc.mu.callbacks.defaults.closed {
			err = cb.Func(ctx, tc, event, cb.Value)
			if err != nil {
				return
			}
		}
	}
	for _, cb := range tc.mu.callbacks.callbacks[event.Event] {
		err = cb.Func(ctx, tc, event, cb.Value)
		if err != nil {
			return
		}
	}
	return
}

func newCompletionEvent(event EventType, meta txn.TxnMeta, err error) TxnEvent {
	return TxnEvent{Event: event, Txn: meta, Err: err, CostEvent: true}
}

func newEvent(event EventType, meta txn.TxnMeta, err error) TxnEvent {
	return TxnEvent{Event: event, Txn: meta, Err: err}
}
