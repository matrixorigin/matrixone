// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rscthrottler

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
)

// ContextThrottler is the optional blocking capability. Callers must retire
// their reclaimable data before waiting; spill workers must never use it.
type ContextThrottler interface {
	AcquireContext(context.Context, int64) error
	MaxAcquire() int64
	BatchBytes() int64
}

type RequestTooLargeError struct{ Requested, Maximum int64 }

func (e *RequestTooLargeError) Error() string {
	return fmt.Sprintf("memory request %d exceeds maximum %d", e.Requested, e.Maximum)
}

// WorkingSetCapacityError means that no candidate fits after the last active
// owner has retired and a complete process reclaim has finished.
type WorkingSetCapacityError struct{ Minimum, Available int64 }

func (e *WorkingSetCapacityError) Error() string {
	return fmt.Sprintf("import minimum working layout needs %d bytes, available %d", e.Minimum, e.Available)
}

// WorkingSetCandidatesThrottler is optional: existing single-spec callers and
// mocks retain their original interface and waiting contract.
type WorkingSetCandidatesThrottler interface {
	ReserveWorkingSetCandidates(context.Context, []WorkingSetSpec) (*WorkingSet, error)
}

var ErrWorkingSetClosed = errors.New("memory working set closed")

type InputSlotSpec struct {
	Name  string
	Count uint32
	Bytes int64
}

type WorkingSetSpec struct {
	UnitBytes int64
	Inputs    []InputSlotSpec
	IOBytes   int64
}

// WorkingSet is admitted before execution locks and the first input read.
// Input credits and the single process IO slot cannot borrow from each other.
type WorkingSet struct {
	coordinator   *memoryCoordinator
	spec          WorkingSetSpec
	inputBytes    int64
	inputUsed     []uint32
	inputWait     []chan struct{}
	closed        bool
	refs          int
	inputRefs     int
	retainedRefs  int
	ioRefs        int
	inputReleased bool
	ioReleased    bool
	closing       chan struct{}
}

type InputLease struct {
	ticket   *WorkingSet
	index    int
	released atomic.Bool
}

type WorkspaceLease struct {
	mu     sync.Mutex
	ticket *WorkingSet
	bytes  int64
	valid  bool
}

func MaxAcquire(t RSCThrottler) int64 {
	if c, ok := t.(ContextThrottler); ok {
		return c.MaxAcquire()
	}
	return math.MaxInt64
}

func BatchBytes(t RSCThrottler) int64 {
	if c, ok := t.(ContextThrottler); ok {
		return c.BatchBytes()
	}
	return 64 << 20
}

func AcquireContext(ctx context.Context, t RSCThrottler, ask int64) error {
	if c, ok := t.(ContextThrottler); ok {
		return c.AcquireContext(ctx, ask)
	}
	if err := context.Cause(ctx); err != nil {
		return err
	}
	if _, ok := t.Acquire(ask); ok {
		return nil
	}
	return errors.New("memory throttler does not support cancellable admission")
}

func ReserveWorkingSet(ctx context.Context, t RSCThrottler, spec WorkingSetSpec) (*WorkingSet, error) {
	if c, ok := t.(interface {
		ReserveWorkingSet(context.Context, WorkingSetSpec) (*WorkingSet, error)
	}); ok {
		return c.ReserveWorkingSet(ctx, spec)
	}
	return nil, errors.New("memory throttler does not support working sets")
}

func ReserveWorkingSetCandidates(ctx context.Context, t RSCThrottler, specs []WorkingSetSpec) (*WorkingSet, error) {
	if c, ok := t.(WorkingSetCandidatesThrottler); ok {
		return c.ReserveWorkingSetCandidates(ctx, specs)
	}
	return nil, errors.New("memory throttler does not support working-set candidates")
}

func (w *WorkingSet) UnitBytes() int64 { return w.spec.UnitBytes }

func (w *WorkingSet) IOBytes() int64 { return w.spec.IOBytes }

func (w *WorkingSet) InputIndex(name string) (int, bool) {
	for i, slot := range w.spec.Inputs {
		if slot.Name == name {
			return i, true
		}
	}
	return 0, false
}

func (w *WorkingSet) AcquireInput(ctx context.Context, index int) (*InputLease, error) {
	if index < 0 || index >= len(w.spec.Inputs) {
		return nil, errors.New("invalid memory input slot")
	}
	if w.spec.Inputs[index].Count == 0 {
		return nil, errors.New("memory input slot is disabled")
	}
	c := w.coordinator
	for {
		c.mu.Lock()
		if err := context.Cause(ctx); err != nil {
			c.mu.Unlock()
			return nil, err
		}
		if w.closed {
			c.mu.Unlock()
			return nil, ErrWorkingSetClosed
		}
		if w.inputUsed[index] < w.spec.Inputs[index].Count {
			w.inputUsed[index]++
			w.refs++
			w.inputRefs++
			c.mu.Unlock()
			return &InputLease{ticket: w, index: index}, nil
		}
		wake := w.inputWait[index]
		c.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, context.Cause(ctx)
		case <-w.closing:
			return nil, ErrWorkingSetClosed
		case <-wake:
		}
	}
}

func (l *InputLease) Capacity() int64 { return l.ticket.spec.Inputs[l.index].Bytes }

func (l *InputLease) Release() {
	if l == nil || !l.released.CompareAndSwap(false, true) {
		return
	}
	w := l.ticket
	c := w.coordinator
	c.mu.Lock()
	w.inputUsed[l.index]--
	w.inputRefs--
	close(w.inputWait[l.index])
	w.inputWait[l.index] = make(chan struct{})
	w.releaseRefLocked()
	c.mu.Unlock()
}

func (w *WorkingSet) AcquireIO(ctx context.Context, need int64) (*WorkspaceLease, error) {
	if need < 0 {
		return nil, errors.New("negative IO workspace size")
	}
	if need > w.spec.IOBytes {
		return nil, &RequestTooLargeError{need, w.spec.IOBytes}
	}
	c := w.coordinator
	n := &ioWaiter{ticket: w, wake: make(chan struct{}, 1)}
	c.mu.Lock()
	if w.closed {
		c.mu.Unlock()
		return nil, ErrWorkingSetClosed
	}
	w.refs++
	w.ioRefs++
	c.ioQueue = append(c.ioQueue, n)
	if c.queued != nil {
		c.queued("io")
	}
	c.notifyIOLocked()
	c.mu.Unlock()
	for {
		c.mu.Lock()
		err := context.Cause(ctx)
		if err == nil && w.closed {
			err = ErrWorkingSetClosed
		}
		if err != nil {
			c.removeIOLocked(n)
			w.ioRefs--
			w.releaseRefLocked()
			c.mu.Unlock()
			return nil, err
		}
		if !c.ioBusy && len(c.ioQueue) > 0 && c.ioQueue[0] == n {
			c.ioBusy = true
			c.ioQueue = c.ioQueue[1:]
			if len(c.ioQueue) == 0 {
				c.ioQueue = nil
			}
			c.mu.Unlock()
			return &WorkspaceLease{ticket: w, bytes: w.spec.IOBytes, valid: true}, nil
		}
		c.mu.Unlock()
		select {
		case <-ctx.Done():
		case <-w.closing:
		case <-n.wake:
		}
	}
}

func (l *WorkspaceLease) Capacity() int64 { return l.bytes }

// Transfer invalidates this handle. The receiving job becomes the sole owner.
func (l *WorkspaceLease) Transfer() *WorkspaceLease {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.valid {
		return nil
	}
	l.valid = false
	return &WorkspaceLease{ticket: l.ticket, bytes: l.bytes, valid: true}
}

func (l *WorkspaceLease) Release() {
	if l == nil {
		return
	}
	l.mu.Lock()
	if !l.valid {
		l.mu.Unlock()
		return
	}
	l.valid = false
	w := l.ticket
	l.mu.Unlock()
	c := w.coordinator
	c.mu.Lock()
	c.ioBusy = false
	w.ioRefs--
	w.releaseRefLocked()
	c.notifyIOLocked()
	c.mu.Unlock()
}

// Retain holds both input and IO promises for a scope attached to an admitted
// statement, including the interval before its operators acquire their leases.
// The returned release is idempotent and may run after Close.
func (w *WorkingSet) Retain() (func(), error) {
	if w == nil {
		return nil, ErrWorkingSetClosed
	}
	c := w.coordinator
	c.mu.Lock()
	if w.closed {
		c.mu.Unlock()
		return nil, ErrWorkingSetClosed
	}
	w.refs++
	w.retainedRefs++
	c.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			c.mu.Lock()
			w.retainedRefs--
			w.releaseRefLocked()
			c.mu.Unlock()
		})
	}, nil
}

func (w *WorkingSet) Close() {
	if w == nil {
		return
	}
	c := w.coordinator
	c.mu.Lock()
	if !w.closed {
		w.closed = true
		close(w.closing)
		w.releaseRefLocked()
	}
	c.mu.Unlock()
}

func (w *WorkingSet) releaseRefLocked() {
	w.refs--
	c := w.coordinator
	if w.closed && w.inputRefs == 0 && w.retainedRefs == 0 && !w.inputReleased {
		w.inputReleased = true
		c.inputPromise.Add(-w.inputBytes)
	}
	if w.closed && w.ioRefs == 0 && w.retainedRefs == 0 && !w.ioReleased {
		w.ioReleased = true
		var maximum int64
		for ticket := range c.tickets {
			if !ticket.ioReleased {
				maximum = max(maximum, ticket.spec.IOBytes)
			}
		}
		c.workspacePromise.Store(maximum)
	}
	if w.refs == 0 {
		delete(c.tickets, w)
	}
	c.notifyAdmissionLocked()
}
