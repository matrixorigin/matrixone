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

package spillcleanup

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestQueueRetainsFailuresWithoutBlockingHealthyOwners(t *testing.T) {
	q := &queue{delay: time.Millisecond}
	var blocked atomic.Bool
	blocked.Store(true)
	var recovered, healthy atomic.Int32
	q.enqueue(func() error {
		if blocked.Load() {
			return errors.New("filesystem unavailable")
		}
		recovered.Add(1)
		return nil
	})
	var wg sync.WaitGroup
	for range 64 {
		wg.Add(1)
		go func() { defer wg.Done(); q.enqueue(func() error { healthy.Add(1); return nil }) }()
	}
	wg.Wait()
	require.Eventually(t, func() bool { return healthy.Load() == 64 }, time.Second, time.Millisecond)
	require.Zero(t, recovered.Load())
	blocked.Store(false)
	require.Eventually(t, func() bool {
		q.mu.Lock()
		defer q.mu.Unlock()
		return q.workers == 0 && len(q.pending) == 0
	}, time.Second, time.Millisecond)
	require.Equal(t, int32(1), recovered.Load())
	// A fully drained queue must restart for a later query.
	q.enqueue(func() error { healthy.Add(1); return nil })
	require.Eventually(t, func() bool { return healthy.Load() == 65 }, time.Second, time.Millisecond)
}

func TestQueueIsolatesBlockedCallback(t *testing.T) {
	q := &queue{delay: time.Millisecond}
	blocked := make(chan struct{})
	defer close(blocked)
	started := make(chan struct{})
	q.enqueue(func() error { close(started); <-blocked; return nil })
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("cleanup did not start")
	}
	healthy := make(chan struct{})
	q.enqueue(func() error { close(healthy); return nil })
	select {
	case <-healthy:
	case <-time.After(time.Second):
		t.Fatal("a blocked cleanup prevented an independent file from being removed")
	}
}

func TestReservationBoundsAndRestoresAdmission(t *testing.T) {
	var owners []*Reservation
	t.Cleanup(func() {
		for _, r := range owners {
			r.Release()
		}
	})
	for range Capacity {
		r, err := Reserve()
		require.NoError(t, err)
		owners = append(owners, r)
	}
	r, err := Reserve()
	require.Nil(t, r)
	require.ErrorIs(t, err, ErrCapacity)
	require.EqualValues(t, Capacity, Used())
	new(Reservation).Release() // a zero-value token cannot return another owner's slot
	require.EqualValues(t, Capacity, Used())
	var calls atomic.Int32
	var blocked atomic.Bool
	blocked.Store(true)
	owner := owners[0]
	cleanup := func() error {
		if blocked.Load() {
			return errors.New("unavailable")
		}
		calls.Add(1)
		owner.Release()
		return errors.New("close error after successful removal")
	}
	var wg sync.WaitGroup
	for range 64 {
		wg.Add(1)
		go func() { defer wg.Done(); owner.Enqueue(cleanup) }()
	}
	wg.Wait()
	require.EqualValues(t, Capacity, Used())
	blocked.Store(false)
	require.Eventually(t, func() bool { return Used() == Capacity-1 }, 3*time.Second, time.Millisecond)
	require.EqualValues(t, 1, calls.Load())
	require.Eventually(t, func() bool {
		terminal.mu.Lock()
		defer terminal.mu.Unlock()
		return terminal.workers == 0 && len(terminal.pending) == 0
	}, time.Second, time.Millisecond, "a completed owner must not retain an uncharged retry")
	owner.Release()
	owner.Enqueue(cleanup)
	require.EqualValues(t, Capacity-1, Used())
	r, err = Reserve()
	require.NoError(t, err)
	r.Release()
}

func TestQueueNeverReplacesBlockedWorkers(t *testing.T) {
	q := &queue{delay: time.Millisecond}
	blocked := make(chan struct{})
	var entered, done atomic.Int32
	for range maxWorkers + 32 {
		q.enqueue(func() error { entered.Add(1); <-blocked; done.Add(1); return nil })
	}
	require.Eventually(t, func() bool { return entered.Load() == maxWorkers }, time.Second, time.Millisecond)
	q.mu.Lock()
	workers, pending := q.workers, len(q.pending)
	q.mu.Unlock()
	require.Equal(t, maxWorkers, workers)
	require.Equal(t, 32, pending)
	close(blocked)
	require.Eventually(t, func() bool { return done.Load() == maxWorkers+32 }, time.Second, time.Millisecond)
}
