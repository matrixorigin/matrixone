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
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func admissionFixture(t *testing.T, total, limit, rss int64) (*memThrottler, *memoryCoordinator, <-chan string) {
	t.Helper()
	c := newMemoryCoordinator()
	c.ticks = make(chan time.Time)
	c.readSample = func() memorySample { return memorySample{rss: rss, total: total} }
	c.reclaim = func(*memThrottler) {}
	queued := make(chan string, 32)
	c.queued = func(kind string) { queued <- kind }
	m := &memThrottler{coordinator: c, limitRate: .9}
	m.fillDefaults()
	m.options.constLimit = limit
	m.options.acquirePolicy = AcquirePolicyForCNFlushS3
	m.options.maxAcquire = MaxAcquireForCNFlushS3
	m.limit.Store(limit)
	m.actualTotalMemory.Store(uint64(total))
	m.rss.Store(rss)
	m.lastRefresh.Store(time.Now().UnixNano())
	t.Cleanup(func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		require.Empty(t, c.queue)
		require.Empty(t, c.ioQueue)
		require.Empty(t, c.tickets)
		require.Zero(t, c.dataReserved.Load())
		require.Zero(t, c.inputPromise.Load())
		require.Zero(t, c.workspacePromise.Load())
		require.False(t, c.ioBusy)
	})
	return m, c, queued
}

func await[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(10 * time.Second):
		t.Fatal("memory ownership transition did not complete")
		var zero T
		return zero
	}
}

func TestMemoryAdmissionFIFOAndCancellation(t *testing.T) {
	m, c, queued := admissionFixture(t, 1000, 100, 0)
	_, ok := m.Acquire(80)
	require.True(t, ok)
	t.Cleanup(func() { m.Release(80) })
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	first := make(chan error, 1)
	go func() { first <- m.AcquireContext(ctx, 40) }()
	require.Equal(t, "data", await(t, queued))
	second := make(chan error, 1)
	secondCtx, secondCancel := context.WithCancel(context.Background())
	t.Cleanup(secondCancel)
	go func() { second <- m.AcquireContext(secondCtx, 20) }()
	require.Equal(t, "data", await(t, queued))
	m.Release(20)
	_, ok = m.Acquire(1)
	require.False(t, ok, "new grants cannot bypass queued requests")
	cancel()
	require.ErrorIs(t, await(t, first), context.Canceled)
	c.mu.Lock()
	done := c.workerDone
	c.mu.Unlock()
	require.NoError(t, await(t, second))
	m.Release(20)
	await(t, done)
	c.mu.Lock()
	require.Nil(t, c.workerStop, "the last waiter stops the fallback")
	c.mu.Unlock()
}

func TestMemoryAdmissionReclaimBelowSoftLimit(t *testing.T) {
	m, c, _ := admissionFixture(t, 2200, 1980, 1700)
	var calls atomic.Int32
	c.reclaim = func(*memThrottler) { calls.Add(1) }
	c.readSample = func() memorySample { return memorySample{rss: 1100, total: 2200} }
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	result := make(chan error, 1)
	go func() { result <- m.AcquireContext(ctx, 600) }()
	require.NoError(t, await(t, result))
	t.Cleanup(func() { m.Release(600) })
	require.Equal(t, int32(1), calls.Load(), "17+6>22 must recover despite RSS below 85 percent")
}

func TestMemoryAdmissionRequestLimits(t *testing.T) {
	m, c, _ := admissionFixture(t, 1000, 100, 0)
	require.Equal(t, int64(80), m.MaxAcquire())
	var large *RequestTooLargeError
	require.ErrorAs(t, m.AcquireContext(context.Background(), 81), &large)
	require.Equal(t, int64(80), large.Maximum)
	require.Error(t, m.AcquireContext(context.Background(), -1))
	require.NoError(t, m.AcquireContext(context.Background(), 0))
	_, ok := m.Acquire(-1)
	require.False(t, ok)
	m.Release(-1)
	require.Nil(t, c.workerStop, "invalid requests cannot start a worker")
	w, err := m.ReserveWorkingSet(context.Background(), WorkingSetSpec{UnitBytes: 1, Inputs: []InputSlotSpec{{Name: "disabled"}}, IOBytes: 1})
	require.NoError(t, err)
	t.Cleanup(w.Close)
	_, err = w.AcquireInput(context.Background(), 0)
	require.ErrorContains(t, err, "disabled")
	require.Zero(t, c.inputPromise.Load())
}

func TestWorkingSetDataFullIOProgressAndTransfer(t *testing.T) {
	m, c, queued := admissionFixture(t, 1000, 900, 100)
	w, err := m.ReserveWorkingSet(context.Background(), WorkingSetSpec{UnitBytes: 32, Inputs: []InputSlotSpec{{Name: "csv", Count: 1, Bytes: 100}}, IOBytes: 200})
	require.NoError(t, err)
	t.Cleanup(w.Close)
	require.Equal(t, "working-set", await(t, queued))
	_, ok := m.Acquire(600)
	require.True(t, ok)
	t.Cleanup(func() { m.Release(600) })
	_, ok = m.Acquire(1)
	require.False(t, ok, "ordinary data cannot consume promised input/IO bytes")
	input, err := w.AcquireInput(context.Background(), 0)
	require.NoError(t, err)
	t.Cleanup(input.Release)
	io, err := w.AcquireIO(context.Background(), 200)
	require.NoError(t, err, "IO progresses while normal data is full")
	t.Cleanup(io.Release)
	require.Equal(t, "io", await(t, queued))
	job := io.Transfer()
	require.NotNil(t, job)
	t.Cleanup(job.Release)
	require.Nil(t, io.Transfer(), "a transferred handle cannot transfer twice")
	io.Release()
	c.mu.Lock()
	require.True(t, c.ioBusy, "releasing the sender cannot release the job's slot")
	c.mu.Unlock()
	w.Close()
	c.mu.Lock()
	require.Contains(t, c.tickets, w, "root Close cannot invalidate transferred owners")
	c.mu.Unlock()
	job.Release()
	input.Release()
	c.mu.Lock()
	require.NotContains(t, c.tickets, w)
	c.mu.Unlock()
}

func TestWorkingSetQueueCancelAndMaxPromise(t *testing.T) {
	m, c, queued := admissionFixture(t, 2000, 1800, 0)
	one, err := m.ReserveWorkingSet(context.Background(), WorkingSetSpec{UnitBytes: 16, Inputs: []InputSlotSpec{{Count: 2, Bytes: 100}}, IOBytes: 100})
	require.NoError(t, err)
	t.Cleanup(one.Close)
	await(t, queued)
	two, err := m.ReserveWorkingSet(context.Background(), WorkingSetSpec{UnitBytes: 16, IOBytes: 200})
	require.NoError(t, err)
	t.Cleanup(two.Close)
	await(t, queued)
	require.Equal(t, int64(200), c.workspacePromise.Load(), "IO declarations share one maximum, not their sum")
	first, err := one.AcquireIO(context.Background(), 100)
	require.NoError(t, err)
	t.Cleanup(first.Release)
	await(t, queued)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	blocked := make(chan error, 1)
	go func() {
		lease, e := two.AcquireIO(ctx, 200)
		if lease != nil {
			lease.Release()
		}
		blocked <- e
	}()
	require.Equal(t, "io", await(t, queued))
	cancel()
	require.ErrorIs(t, await(t, blocked), context.Canceled)
	two.Close()
	require.Equal(t, int64(100), c.workspacePromise.Load())
	input1, err := one.AcquireInput(context.Background(), 0)
	require.NoError(t, err)
	t.Cleanup(input1.Release)
	input2, err := one.AcquireInput(context.Background(), 0)
	require.NoError(t, err)
	t.Cleanup(input2.Release)
	closeWait := make(chan error, 1)
	go func() {
		lease, e := one.AcquireInput(context.Background(), 0)
		if lease != nil {
			lease.Release()
		}
		closeWait <- e
	}()
	one.Close()
	require.ErrorIs(t, await(t, closeWait), ErrWorkingSetClosed)
	first.Release()
	input1.Release()
	input2.Release()
	require.Zero(t, c.inputPromise.Load())
	require.Zero(t, c.workspacePromise.Load())
}

func TestWorkingSetAtomicRejectionAndCgroupAccounting(t *testing.T) {
	m, c, queued := admissionFixture(t, 1000, 900, 100)
	m.cgroupUsage.Store(950)
	m.cgroupInactiveFile.Store(50)
	require.Equal(t, int64(900), m.physicalUsed())
	require.True(t, m.physicalFits(100))
	require.False(t, m.physicalFits(101))
	// Recovery is intentionally held at the same physical usage, so only
	// cancellation can complete this unsatisfied atomic reservation.
	c.readSample = func() memorySample { return memorySample{rss: 100, total: 1000, current: 950, inactiveFile: 50} }
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	result := make(chan error, 1)
	go func() {
		w, e := m.ReserveWorkingSet(ctx, WorkingSetSpec{UnitBytes: 10, Inputs: []InputSlotSpec{{Count: 1, Bytes: 60}}, IOBytes: 60})
		if w != nil {
			w.Close()
		}
		result <- e
	}()
	require.Equal(t, "working-set", await(t, queued))
	require.Zero(t, c.inputPromise.Load())
	require.Zero(t, c.workspacePromise.Load(), "partial I/W reservations cannot publish")
	cancel()
	require.ErrorIs(t, await(t, result), context.Canceled)
	_, err := workingSetBytes(WorkingSetSpec{UnitBytes: 1, Inputs: []InputSlotSpec{{Count: 2, Bytes: math.MaxInt64}}})
	require.Error(t, err)
	_, err = m.ReserveWorkingSet(context.Background(), WorkingSetSpec{UnitBytes: 1, IOBytes: 1001})
	var large *RequestTooLargeError
	require.True(t, errors.As(err, &large))
}

func TestWorkingSetConcurrentTransferClose(t *testing.T) {
	m, c, _ := admissionFixture(t, 1000, 900, 0)
	w, err := m.ReserveWorkingSet(context.Background(), WorkingSetSpec{UnitBytes: 16, Inputs: []InputSlotSpec{{Count: 1, Bytes: 100}}, IOBytes: 200})
	require.NoError(t, err)
	t.Cleanup(w.Close)
	input, err := w.AcquireInput(context.Background(), 0)
	require.NoError(t, err)
	t.Cleanup(input.Release)
	sender, err := w.AcquireIO(context.Background(), 200)
	require.NoError(t, err)
	t.Cleanup(sender.Release)
	start := make(chan struct{})
	transfers := make(chan *WorkspaceLease, 16)
	var workers sync.WaitGroup
	for i := 0; i < 16; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			if receiver := sender.Transfer(); receiver != nil {
				transfers <- receiver
			}
		}()
	}
	close(start)
	workers.Wait()
	close(transfers)
	var receiver *WorkspaceLease
	var count int
	for transfer := range transfers {
		receiver = transfer
		count++
	}
	require.Equal(t, 1, count, "a job has exactly one effective terminal owner")
	t.Cleanup(receiver.Release)
	start = make(chan struct{})
	for i := 0; i < 16; i++ {
		workers.Add(1)
		go func() { defer workers.Done(); <-start; w.Close(); sender.Release(); input.Release() }()
	}
	close(start)
	workers.Wait()
	require.Zero(t, c.inputPromise.Load(), "IO ownership must not retain closed input credits")
	require.Equal(t, int64(200), c.workspacePromise.Load(), "the transferred job retains its IO promise")
	start = make(chan struct{})
	for i := 0; i < 16; i++ {
		workers.Add(1)
		go func() { defer workers.Done(); <-start; receiver.Release() }()
	}
	close(start)
	workers.Wait()
	require.Zero(t, c.workspacePromise.Load())
}

func TestWorkingSetTryHeadroomAndRecoveryCancellation(t *testing.T) {
	m, c, _ := admissionFixture(t, 1000, 900, 100)
	first, ok, err := m.TryReserveWorkingSet(context.Background(), WorkingSetSpec{UnitBytes: 16, Inputs: []InputSlotSpec{{Count: 1, Bytes: 100}}, IOBytes: 200})
	require.NoError(t, err)
	require.True(t, ok)
	t.Cleanup(first.Close)
	headroom := m.WorkingSetHeadroom()
	require.Equal(t, int64(600), headroom.Available)
	require.True(t, headroom.HasOwners)
	second, ok, err := m.TryReserveWorkingSet(context.Background(), WorkingSetSpec{UnitBytes: 16, Inputs: []InputSlotSpec{{Count: 1, Bytes: 700}}, IOBytes: 200})
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, second)
	require.Equal(t, int64(100), c.inputPromise.Load(), "a failed try cannot publish part of its declaration")
	first.Close()
	require.False(t, m.WorkingSetHeadroom().HasOwners)
	started, finish := make(chan struct{}), make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-finish:
		default:
			close(finish)
		}
	})
	c.reclaim = func(*memThrottler) { close(started); <-finish }
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	result := make(chan error, 1)
	go func() { result <- m.ReclaimForWorkingSet(ctx) }()
	await(t, started)
	c.fullMu.Lock()
	done := c.fullDone
	c.fullMu.Unlock()
	cancel()
	require.ErrorIs(t, await(t, result), context.Canceled, "cancellation cannot wait for reclaim IO")
	close(finish)
	await(t, done)
	require.Zero(t, c.inputPromise.Load())
	require.Zero(t, c.workspacePromise.Load())
}

func TestMemoryAdmissionProcessSampleCoalescing(t *testing.T) {
	m, c, _ := admissionFixture(t, 1000, 900, 100)
	fixed := time.Unix(100, 0)
	c.now = func() time.Time { return fixed }
	var samples atomic.Int32
	c.readSample = func() memorySample { samples.Add(1); return memorySample{rss: 100, total: 1000} }
	var workers sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 16; i++ {
		workers.Add(1)
		go func() { defer workers.Done(); <-start; m.ForceRefresh() }()
	}
	close(start)
	workers.Wait()
	require.Equal(t, int32(1), samples.Load(), "concurrent refreshes share one process sample")
	c.invalidateSample()
	m.ForceRefresh()
	require.Equal(t, int32(2), samples.Load(), "reclaim invalidates the snapshot before admission rechecks")
}

func TestWorkingSetRetainedScopePreservesBothPromises(t *testing.T) {
	m, c, _ := admissionFixture(t, 1000, 900, 100)
	w, err := m.ReserveWorkingSet(context.Background(), WorkingSetSpec{UnitBytes: 32, Inputs: []InputSlotSpec{{Name: "csv", Count: 1, Bytes: 100}}, IOBytes: 200})
	require.NoError(t, err)
	t.Cleanup(w.Close)
	releaseA, err := w.Retain()
	require.NoError(t, err)
	t.Cleanup(releaseA)
	releaseB, err := w.Retain()
	require.NoError(t, err)
	t.Cleanup(releaseB)
	w.Close()
	require.Equal(t, int64(100), c.inputPromise.Load(), "an attached scope may not have prepared its source yet")
	require.Equal(t, int64(200), c.workspacePromise.Load())
	_, err = w.Retain()
	require.ErrorIs(t, err, ErrWorkingSetClosed)
	_, err = w.AcquireInput(context.Background(), 0)
	require.ErrorIs(t, err, ErrWorkingSetClosed)
	releaseA()
	require.Equal(t, int64(100), c.inputPromise.Load())
	var group sync.WaitGroup
	for i := 0; i < 16; i++ {
		group.Add(1)
		go func() { defer group.Done(); releaseB() }()
	}
	group.Wait()
	require.Zero(t, c.inputPromise.Load())
	require.Zero(t, c.workspacePromise.Load())
}

func TestMemorySampleFailureCannotEscapeContainerLimit(t *testing.T) {
	m, c, _ := admissionFixture(t, 51200, 40000, 0)
	sample := memorySample{rss: 100, current: 2100, limit: 2200, total: 51200}
	c.readSample = func() memorySample { return sample }
	refresh := func() { c.sampleMu.Lock(); c.lastSample = time.Time{}; c.sampleMu.Unlock(); m.ForceRefresh() }
	refresh()
	require.Equal(t, uint64(2200), m.actualTotalMemory.Load())
	sample = memorySample{rss: 100, total: 51200, cgroupErr: errors.New("memory.stat unavailable")}
	refresh()
	require.Equal(t, uint64(2200), m.actualTotalMemory.Load(), "host memory must not replace the last valid cgroup limit")
	require.Equal(t, int64(2100), m.cgroupUsage.Load(), "failed accounting must not imply all container pages were freed")
	_, granted, err := m.TryReserveWorkingSet(context.Background(), WorkingSetSpec{UnitBytes: 1, IOBytes: 1})
	require.NoError(t, err)
	require.False(t, granted, "even tiny new commitments wait for valid accounting")
	require.Zero(t, m.WorkingSetHeadroom().Available)
	sample = memorySample{rss: 100, current: 1000, limit: 2200, total: 51200}
	refresh()
	w, granted, err := m.TryReserveWorkingSet(context.Background(), WorkingSetSpec{UnitBytes: 1, IOBytes: 1})
	require.NoError(t, err)
	require.True(t, granted)
	t.Cleanup(w.Close)
	sample = memorySample{rss: -1, rssErr: errors.New("RSS unavailable"), current: 900, limit: 2200, total: 51200}
	refresh()
	require.Equal(t, int64(100), m.rss.Load())
	require.Equal(t, int64(900), m.cgroupUsage.Load(), "valid cgroup accounting still refreshes when RSS sampling fails")
	require.False(t, m.physicalFits(1))
}

func TestMemoryControllerAbsenceOnlyFallsBackBeforeFirstObservation(t *testing.T) {
	for _, limit := range []int64{0, 2200} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			m, c, _ := admissionFixture(t, 51200, 40000, 100)
			sample := memorySample{rss: 100, total: 51200, cgroupErr: errNoMemoryController}
			c.readSample = func() memorySample { return sample }
			refresh := func(throttler *memThrottler) {
				c.sampleMu.Lock()
				c.lastSample = time.Time{}
				c.sampleMu.Unlock()
				throttler.ForceRefresh()
			}
			refresh(m)
			require.False(t, m.sampleInvalid.Load(), "bare-metal Linux without a memory controller retains its RSS/host behavior")
			w, granted, err := m.TryReserveWorkingSet(context.Background(), WorkingSetSpec{UnitBytes: 1, IOBytes: 1})
			require.NoError(t, err)
			require.True(t, granted)
			w.Close()
			sample = memorySample{rss: 100, total: 51200, cgroupErr: os.ErrPermission}
			refresh(m)
			require.True(t, m.sampleInvalid.Load(), "unreadable accounting is not proof that the controller is absent")
			sample = memorySample{rss: 100, total: 51200, limit: limit, current: 2100, cgroupObserved: true}
			refresh(m)
			require.True(t, c.knownCgroup, "an unlimited controller still has current-memory semantics")
			sample = memorySample{rss: 100, total: 51200, cgroupErr: errNoMemoryController}
			refresh(m)
			require.True(t, m.sampleInvalid.Load())
			require.Equal(t, int64(2100), m.cgroupUsage.Load())
			require.False(t, m.physicalFits(1))
			next := &memThrottler{coordinator: c, limitRate: .9}
			next.fillDefaults()
			refresh(next)
			require.True(t, next.sampleInvalid.Load(), "a newly created pool cannot forget process-wide controller history")
			if limit > 0 {
				require.Equal(t, uint64(limit), next.actualTotalMemory.Load())
			}
		})
	}
}
