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

package compile

import (
	"context"
	"sync"
)

const ViewMetadataEpochFenceRuntimeKey = "view-metadata-epoch-fence"

type viewMetadataEpochGeneration struct {
	epoch    uint64
	leases   uint64
	draining bool
	drained  chan struct{}
}

// ViewMetadataEpochFence prevents an epoch acknowledgement from overtaking a
// lifecycle-sensitive operation or transaction snapshot creation that entered
// under an older epoch.
type ViewMetadataEpochFence struct {
	mu                  sync.Mutex
	current             *viewMetadataEpochGeneration
	requestedEpoch      uint64
	catalogFencedEpoch  uint64
	refreshReadyEpoch   uint64
	refreshEnabledEpoch uint64
	stateChanged        chan struct{}
	advancing           bool
	advanceDone         chan struct{}
	closed              bool
	closedC             chan struct{}
}

// ViewMetadataEpochLease is an exactly-once read lease.
type ViewMetadataEpochLease struct {
	once       sync.Once
	fence      *ViewMetadataEpochFence
	generation *viewMetadataEpochGeneration
}

func NewViewMetadataEpochFence() *ViewMetadataEpochFence {
	return &ViewMetadataEpochFence{
		current:      &viewMetadataEpochGeneration{epoch: 0},
		stateChanged: make(chan struct{}),
		closedC:      make(chan struct{}),
	}
}

// Acquire enters the current epoch. New leases wait behind an in-progress
// transition and observe either its published epoch or its cancellation.
func (f *ViewMetadataEpochFence) Acquire(ctx context.Context) (*ViewMetadataEpochLease, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		f.mu.Lock()
		if f.closed {
			f.mu.Unlock()
			return nil, context.Canceled
		}
		if !f.advancing {
			generation := f.current
			generation.leases++
			f.mu.Unlock()
			return &ViewMetadataEpochLease{
				fence:      f,
				generation: generation,
			}, nil
		}
		done := f.advanceDone
		closed := f.closedC
		f.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-closed:
			return nil, context.Canceled
		case <-done:
		}
	}
}

// AcquireRefresh enters the current enabled refresh epoch. While an epoch is
// being fenced it waits for the durable catalog fence, closing the publication
// gap between Advance and RequireViewMetadataRevalidation. Once fenced but not
// enabled, callers still hold the current generation while durable catalog
// predicates fail closed.
func (f *ViewMetadataEpochFence) AcquireRefresh(
	ctx context.Context,
) (*ViewMetadataEpochLease, bool, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		f.mu.Lock()
		if f.closed {
			f.mu.Unlock()
			return nil, false, context.Canceled
		}
		var changed <-chan struct{}
		if f.advancing {
			changed = f.advanceDone
		} else if f.requestedEpoch > f.current.epoch {
			changed = f.stateChanged
		} else {
			epoch := f.current.epoch
			if epoch == 0 {
				f.mu.Unlock()
				return nil, false, nil
			}
			if f.catalogFencedEpoch >= epoch {
				generation := f.current
				generation.leases++
				enabled := f.refreshEnabledEpoch == epoch
				f.mu.Unlock()
				return &ViewMetadataEpochLease{fence: f, generation: generation}, enabled, nil
			}
			changed = f.stateChanged
		}
		closed := f.closedC
		f.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, false, ctx.Err()
		case <-closed:
			return nil, false, context.Canceled
		case <-changed:
		}
	}
}

func (l *ViewMetadataEpochLease) Epoch() uint64 {
	if l == nil || l.generation == nil {
		return 0
	}
	return l.generation.epoch
}

func (l *ViewMetadataEpochLease) Release() {
	if l == nil || l.fence == nil || l.generation == nil {
		return
	}
	l.once.Do(func() {
		l.fence.mu.Lock()
		defer l.fence.mu.Unlock()
		if l.generation.leases == 0 {
			return
		}
		l.generation.leases--
		if l.generation.draining && l.generation.leases == 0 {
			close(l.generation.drained)
		}
	})
}

// Advance drains the old generation before publishing targetEpoch. On
// cancellation it leaves ordinary work on the old generation but keeps refresh
// consumers sealed until a retry publishes the requested epoch.
func (f *ViewMetadataEpochFence) Advance(ctx context.Context, targetEpoch uint64) error {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		f.mu.Lock()
		if f.closed {
			f.mu.Unlock()
			return context.Canceled
		}
		if targetEpoch <= f.current.epoch {
			f.mu.Unlock()
			return nil
		}
		if targetEpoch > f.requestedEpoch {
			f.requestedEpoch = targetEpoch
			f.notifyStateChangedLocked()
		}
		if f.advancing {
			done := f.advanceDone
			closed := f.closedC
			f.mu.Unlock()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-closed:
				return context.Canceled
			case <-done:
				continue
			}
		}

		old := f.current
		old.draining = true
		old.drained = make(chan struct{})
		if old.leases == 0 {
			close(old.drained)
		}
		f.advancing = true
		f.advanceDone = make(chan struct{})
		f.notifyStateChangedLocked()
		done := f.advanceDone
		closed := f.closedC
		f.mu.Unlock()

		var advanceErr error
		select {
		case <-old.drained:
		case <-ctx.Done():
			advanceErr = ctx.Err()
		case <-closed:
			advanceErr = context.Canceled
		}

		f.mu.Lock()
		if advanceErr == nil && !f.closed {
			f.current = &viewMetadataEpochGeneration{epoch: targetEpoch}
			f.notifyStateChangedLocked()
		} else {
			old.draining = false
			if advanceErr == nil {
				advanceErr = context.Canceled
			}
		}
		f.advancing = false
		close(done)
		f.advanceDone = nil
		f.mu.Unlock()
		return advanceErr
	}
}

func (f *ViewMetadataEpochFence) Epoch() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.current.epoch
}

func (f *ViewMetadataEpochFence) MarkCatalogFenced(epoch uint64) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed || f.requestedEpoch > f.current.epoch ||
		epoch != f.current.epoch || epoch < f.catalogFencedEpoch {
		return false
	}
	if epoch > f.catalogFencedEpoch {
		f.catalogFencedEpoch = epoch
		f.notifyStateChangedLocked()
	}
	return true
}

func (f *ViewMetadataEpochFence) MarkRefreshReady(epoch uint64) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed || f.requestedEpoch > f.current.epoch ||
		epoch == 0 || epoch != f.current.epoch || f.catalogFencedEpoch < epoch {
		return false
	}
	if f.refreshReadyEpoch != epoch {
		f.refreshReadyEpoch = epoch
		f.notifyStateChangedLocked()
	}
	return true
}

func (f *ViewMetadataEpochFence) EnableRefresh(epoch uint64) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed || f.requestedEpoch > f.current.epoch ||
		epoch == 0 || epoch != f.current.epoch ||
		f.catalogFencedEpoch < epoch || f.refreshReadyEpoch != epoch {
		return false
	}
	if f.refreshEnabledEpoch != epoch {
		f.refreshEnabledEpoch = epoch
		f.notifyStateChangedLocked()
	}
	return true
}

func (f *ViewMetadataEpochFence) RefreshEnabled() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return !f.closed && f.requestedEpoch <= f.current.epoch &&
		f.current.epoch != 0 && f.refreshEnabledEpoch == f.current.epoch
}

func (f *ViewMetadataEpochFence) RecoveryAllowed() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return !f.closed && f.requestedEpoch <= f.current.epoch &&
		f.current.epoch != 0 && f.catalogFencedEpoch >= f.current.epoch &&
		f.refreshReadyEpoch == f.current.epoch
}

func (f *ViewMetadataEpochFence) notifyStateChangedLocked() {
	close(f.stateChanged)
	f.stateChanged = make(chan struct{})
}

// Close independently releases Acquire/Advance waiters. Existing lease owners
// retain their exactly-once Release responsibility.
func (f *ViewMetadataEpochFence) Close() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return
	}
	f.closed = true
	close(f.closedC)
	f.notifyStateChangedLocked()
}
