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
// lifecycle-sensitive operation that entered under an older epoch. The public
// SQL consumers remain disconnected until the activation layer; admission uses
// Advance to establish the same tested ordering contract now.
type ViewMetadataEpochFence struct {
	mu          sync.Mutex
	current     *viewMetadataEpochGeneration
	advancing   bool
	advanceDone chan struct{}
	closed      bool
	closedC     chan struct{}
}

// ViewMetadataEpochLease is an exactly-once read lease.
type ViewMetadataEpochLease struct {
	once       sync.Once
	fence      *ViewMetadataEpochFence
	generation *viewMetadataEpochGeneration
}

func NewViewMetadataEpochFence() *ViewMetadataEpochFence {
	return &ViewMetadataEpochFence{
		current: &viewMetadataEpochGeneration{epoch: 0},
		closedC: make(chan struct{}),
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
// cancellation it reopens the old generation and publishes no partial state.
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
}
