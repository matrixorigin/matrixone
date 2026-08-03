// Copyright 2026 Matrix Origin
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
	"sync"
)

type activeTxnWaiterState uint8

const (
	activeTxnQueued activeTxnWaiterState = iota
	activeTxnPromoting
	activeTxnPromotionCanceled
	activeTxnAdmitted
	activeTxnCanceled
	activeTxnFailed
)

// activeTxnWaiter is the admission gate for one max-active queued transaction.
// Cancellation and promotion compete for queued ownership. Once promotion
// wins, a canceled creator waits until the client has published the operator in
// activeTxns; its ClosedEvent can then remove that ownership without falling
// into a dequeue-to-publication gap.
type activeTxnWaiter struct {
	doneC chan struct{}

	mu struct {
		sync.Mutex
		state activeTxnWaiterState
		err   error
	}
}

func newActiveTxnWaiter() *activeTxnWaiter {
	return &activeTxnWaiter{doneC: make(chan struct{})}
}

func (w *activeTxnWaiter) wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return w.abort(ctx.Err())
	case <-w.doneC:
		result, _ := w.result()
		return result
	}
}

// claimPromotion is the linearization point between queued cancellation and
// queued-to-active ownership transfer. It must be called while client.mu keeps
// the operator in waitActiveTxns.
func (w *activeTxnWaiter) claimPromotion() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.mu.state != activeTxnQueued {
		return false
	}
	w.mu.state = activeTxnPromoting
	return true
}

// abort makes every post-open creation failure participate in admission
// ownership transfer, including failures that happen before wait is called.
// If promotion already won, abort waits for active publication so ClosedEvent
// can remove the operator from activeTxns.
func (w *activeTxnWaiter) abort(err error) error {
	w.mu.Lock()
	switch w.mu.state {
	case activeTxnQueued:
		w.mu.state = activeTxnCanceled
		w.mu.err = err
		close(w.doneC)
	case activeTxnPromoting:
		w.mu.state = activeTxnPromotionCanceled
		w.mu.err = err
	}
	w.mu.Unlock()

	// doneC is already closed for every terminal state. Only a promotion that
	// owns publication can keep it open, and that path is local and bounded.
	<-w.doneC
	result, _ := w.result()
	return result
}

func (w *activeTxnWaiter) complete(err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	switch w.mu.state {
	case activeTxnQueued, activeTxnPromoting:
		if err == nil {
			w.mu.state = activeTxnAdmitted
		} else {
			w.mu.state = activeTxnFailed
		}
		w.mu.err = err
		close(w.doneC)
	case activeTxnPromotionCanceled:
		// Preserve the cancellation result recorded by the creator. Promotion
		// only supplies the ownership-publication barrier in this state.
		w.mu.state = activeTxnCanceled
		close(w.doneC)
	}
}

func (w *activeTxnWaiter) result() (error, bool) {
	select {
	case <-w.doneC:
		w.mu.Lock()
		defer w.mu.Unlock()
		return w.mu.err, true
	default:
		return nil, false
	}
}

func (w *activeTxnWaiter) canceled() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.mu.state == activeTxnPromotionCanceled ||
		w.mu.state == activeTxnCanceled
}
