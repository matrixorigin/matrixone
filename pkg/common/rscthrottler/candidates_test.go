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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func candidateSpec(unit, input, workspace int64) WorkingSetSpec {
	return WorkingSetSpec{UnitBytes: unit, Inputs: []InputSlotSpec{{Name: "input", Count: 1, Bytes: input}}, IOBytes: workspace}
}

type candidateResult struct {
	working *WorkingSet
	err     error
}

func TestWorkingSetCandidatesFitSharedWorkspaceDelta(t *testing.T) {
	m, c, _ := admissionFixture(t, 1000, 900, 100)
	owner, err := m.ReserveWorkingSetCandidatesUntilFit(context.Background(), []WorkingSetSpec{{UnitBytes: 1, IOBytes: 700}})
	require.NoError(t, err)
	t.Cleanup(owner.Close)
	w, err := m.ReserveWorkingSetCandidates(context.Background(), []WorkingSetSpec{candidateSpec(64, 150, 700), candidateSpec(32, 200, 0)})
	require.NoError(t, err)
	t.Cleanup(w.Close)
	require.Equal(t, int64(64), w.UnitBytes(), "input150 fits remaining200 because W700 is already promised")
	require.Equal(t, int64(700), c.workspacePromise.Load())
	require.Equal(t, int64(150), c.inputPromise.Load())
}

func TestWorkingSetCandidatesStaticImpossibleWithActiveOwner(t *testing.T) {
	m, c, queued := admissionFixture(t, 1000, 900, 0)
	_, ok := m.Acquire(100)
	require.True(t, ok)
	t.Cleanup(func() { m.Release(100) })
	var reclaims atomic.Int32
	c.reclaim = func(*memThrottler) { reclaims.Add(1) }
	w, err := m.ReserveWorkingSetCandidates(context.Background(), []WorkingSetSpec{candidateSpec(64, 1000, 100), candidateSpec(32, 900, 200)})
	require.Nil(t, w)
	var capacity *WorkingSetCapacityError
	require.ErrorAs(t, err, &capacity)
	require.Equal(t, int64(1100), capacity.Minimum)
	require.Equal(t, int64(1000), capacity.Available)
	require.Empty(t, queued, "a layout larger than the whole limit cannot wait for active owners")
	require.Empty(t, c.queue)
	require.Nil(t, c.workerStop)
	require.Zero(t, reclaims.Load())
}

func TestWorkingSetCandidatesReleaseReselectsLargest(t *testing.T) {
	m, _, queued := admissionFixture(t, 1000, 900, 100)
	_, ok := m.Acquire(700)
	require.True(t, ok)
	t.Cleanup(func() { m.Release(700) })
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	result := make(chan candidateResult, 1)
	specs := []WorkingSetSpec{candidateSpec(64, 300, 100), candidateSpec(32, 200, 50)}
	go func() {
		w, err := m.ReserveWorkingSetCandidates(ctx, specs)
		result <- candidateResult{w, err}
	}()
	require.Equal(t, "working-set", await(t, queued))
	specs[0].Inputs[0].Bytes = 9999
	specs[0].IOBytes = 9999
	m.Release(700)
	actual := await(t, result)
	require.NoError(t, actual.err)
	t.Cleanup(actual.working.Close)
	require.Equal(t, int64(64), actual.working.UnitBytes(), "release wakes one node and chooses its now-affordable largest candidate")
	require.Equal(t, int64(100), actual.working.IOBytes(), "queued candidates own immutable input and workspace snapshots")
	input, err := actual.working.AcquireInput(ctx, 0)
	require.NoError(t, err)
	t.Cleanup(input.Release)
	require.Equal(t, int64(300), input.Capacity())
}

func TestWorkingSetCandidatesLastOwnerPressureUnblocksFIFO(t *testing.T) {
	m, _, queued := admissionFixture(t, 1000, 900, 800)
	_, ok := m.Acquire(100)
	require.True(t, ok)
	t.Cleanup(func() { m.Release(100) })
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	first, second := make(chan candidateResult, 1), make(chan candidateResult, 1)
	go func() {
		w, err := m.ReserveWorkingSetCandidates(ctx, []WorkingSetSpec{candidateSpec(64, 200, 100), candidateSpec(32, 150, 100)})
		first <- candidateResult{w, err}
	}()
	require.Equal(t, "working-set", await(t, queued))
	go func() {
		w, err := m.ReserveWorkingSetCandidates(ctx, []WorkingSetSpec{candidateSpec(64, 50, 50)})
		second <- candidateResult{w, err}
	}()
	require.Equal(t, "working-set", await(t, queued))
	m.Release(100)
	failed := await(t, first)
	require.Nil(t, failed.working)
	var capacity *WorkingSetCapacityError
	require.ErrorAs(t, failed.err, &capacity)
	require.Equal(t, int64(250), capacity.Minimum)
	require.Equal(t, int64(200), capacity.Available)
	succeeded := await(t, second)
	require.NoError(t, succeeded.err, "a retry-capable head retires its pressure response before a fitting successor")
	t.Cleanup(succeeded.working.Close)
}

func TestWorkingSetCandidatesCancellationKeepsMixedFIFO(t *testing.T) {
	m, _, queued := admissionFixture(t, 1000, 900, 100)
	_, ok := m.Acquire(700)
	require.True(t, ok)
	t.Cleanup(func() { m.Release(700) })
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	first := make(chan error, 1)
	go func() {
		w, err := m.ReserveWorkingSetCandidates(ctx, []WorkingSetSpec{candidateSpec(64, 200, 100)})
		if w != nil {
			w.Close()
		}
		first <- err
	}()
	require.Equal(t, "working-set", await(t, queued))
	nextCtx, nextCancel := context.WithCancel(context.Background())
	t.Cleanup(nextCancel)
	next := make(chan error, 1)
	go func() { next <- m.AcquireContext(nextCtx, 20) }()
	require.Equal(t, "data", await(t, queued))
	_, ok = m.Acquire(1)
	require.False(t, ok, "new ordinary requests cannot bypass the queued candidates")
	cancel()
	require.ErrorIs(t, await(t, first), context.Canceled)
	require.NoError(t, await(t, next))
	m.Release(20)
}

// Recovery reconsiders the largest feasible unit, even when a smaller one fits.
func TestWorkingSetCandidatesReclaimBeforeSmallerFit(t *testing.T) {
	for _, tc := range []struct {
		name                                                 string
		before, after, largeInput, smallInput, smallIO, unit int64
		reclaims                                             int32
	}{
		{"no_candidate_fits_before_reclaim", 900, 100, 200, 80, 50, 64, 1},
		{"reclaim_restores_largest_feasible", 600, 100, 400, 100, 100, 64, 1},
		{"unchanged_resident_keeps_smaller_fit_after_once", 600, 600, 400, 100, 100, 32, 1},
		{"statically_impossible_largest_needs_no_reclaim", 600, 100, 1000, 100, 100, 32, 0},
	} {
		for _, final := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/final=%t", tc.name, final), func(t *testing.T) {
				m, c, _ := admissionFixture(t, 1000, 900, tc.before)
				var rss atomic.Int64
				rss.Store(tc.before)
				var reclaims atomic.Int32
				c.readSample = func() memorySample { return memorySample{rss: rss.Load(), total: 1000} }
				c.reclaim = func(*memThrottler) { reclaims.Add(1); rss.Store(tc.after) }
				reserve := m.ReserveWorkingSetCandidates
				if final {
					reserve = m.ReserveWorkingSetCandidatesUntilFit
				}
				w, err := reserve(context.Background(), []WorkingSetSpec{candidateSpec(64, tc.largeInput, 100), candidateSpec(32, tc.smallInput, tc.smallIO)})
				require.NoError(t, err)
				t.Cleanup(w.Close)
				require.Equal(t, tc.unit, w.UnitBytes())
				require.Equal(t, tc.reclaims, reclaims.Load())
				require.Nil(t, c.workerStop, "a fitting request needs no fallback worker")
			})
		}
	}
}

func TestWorkingSetCandidatesActiveOwnerKeepsSmallerFitWithoutReclaim(t *testing.T) {
	m, c, _ := admissionFixture(t, 1000, 900, 100)
	owner, err := m.ReserveWorkingSetCandidatesUntilFit(context.Background(), []WorkingSetSpec{{UnitBytes: 1, IOBytes: 300}})
	require.NoError(t, err)
	t.Cleanup(owner.Close)
	var reclaims atomic.Int32
	c.reclaim = func(*memThrottler) { reclaims.Add(1) }
	w, err := m.ReserveWorkingSetCandidates(context.Background(), []WorkingSetSpec{candidateSpec(64, 700, 300), candidateSpec(32, 500, 300)})
	require.NoError(t, err)
	t.Cleanup(w.Close)
	require.Equal(t, int64(32), w.UnitBytes(), "an active owner's shared W still permits immediate smaller-unit progress")
	require.Zero(t, reclaims.Load())
	require.Equal(t, int64(500), c.inputPromise.Load())
	require.Equal(t, int64(300), c.workspacePromise.Load())
}

func TestWorkingSetCandidatesCancelDuringReclaimDespiteSmallerFit(t *testing.T) {
	for _, rss := range []int64{950, 600} {
		t.Run(fmt.Sprint(rss), func(t *testing.T) {
			m, c, queued := admissionFixture(t, 1000, 900, rss)
			entered, release := make(chan struct{}), make(chan struct{})
			unblock := sync.OnceFunc(func() { close(release) })
			t.Cleanup(func() {
				unblock()
				c.fullMu.Lock()
				done := c.fullDone
				c.fullMu.Unlock()
				if done != nil {
					await(t, done)
				}
			})
			c.reclaim = func(*memThrottler) { close(entered); <-release }
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			result := make(chan candidateResult, 1)
			go func() {
				w, err := m.ReserveWorkingSetCandidates(ctx, []WorkingSetSpec{candidateSpec(64, 400, 100), candidateSpec(32, 100, 100)})
				result <- candidateResult{w, err}
			}()
			require.Equal(t, "working-set", await(t, queued))
			await(t, entered)
			cancel()
			actual := await(t, result)
			if actual.working != nil {
				t.Cleanup(actual.working.Close)
			}
			require.Nil(t, actual.working)
			require.ErrorIs(t, actual.err, context.Canceled, "cancel retires the FIFO head before blocked recovery completes")
			c.mu.Lock()
			empty := len(c.queue) == 0
			c.mu.Unlock()
			require.True(t, empty)
			next, granted, err := m.TryReserveWorkingSet(context.Background(), candidateSpec(1, 10, 10))
			require.NoError(t, err)
			require.True(t, granted)
			t.Cleanup(next.Close)
		})
	}
}

func TestWorkingSetCandidatesReclaimRechecksAnotherPoolOwner(t *testing.T) {
	m, c, queued := admissionFixture(t, 1000, 900, 600)
	var rss atomic.Int64
	rss.Store(600)
	c.readSample = func() memorySample { return memorySample{rss: rss.Load(), total: 1000} }
	entered, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(func() {
		unblock()
		c.fullMu.Lock()
		done := c.fullDone
		c.fullMu.Unlock()
		if done != nil {
			await(t, done)
		}
	})
	var reclaims atomic.Int32
	c.reclaim = func(*memThrottler) {
		reclaims.Add(1)
		close(entered)
		<-release
		rss.Store(300)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	result := make(chan candidateResult, 1)
	go func() {
		w, err := m.ReserveWorkingSetCandidates(ctx, []WorkingSetSpec{candidateSpec(64, 400, 100), candidateSpec(32, 100, 100)})
		result <- candidateResult{w, err}
	}()
	require.Equal(t, "working-set", await(t, queued))
	await(t, entered)
	// Nonblocking Acquire in another pool has the same process coordinator,
	// but it is not behind this pool's FIFO node. Its live D must be re-read.
	other := &memThrottler{coordinator: c, limitRate: .9}
	other.fillDefaults()
	other.limit.Store(900)
	other.actualTotalMemory.Store(1000)
	other.rss.Store(600)
	other.lastRefresh.Store(m.lastRefresh.Load())
	_, granted := other.Acquire(300)
	require.True(t, granted)
	t.Cleanup(func() { other.Release(300) })
	unblock()
	actual := await(t, result)
	if actual.working != nil {
		t.Cleanup(actual.working.Close)
	}
	require.NoError(t, actual.err)
	require.Equal(t, int64(32), actual.working.UnitBytes(), "new D300 plus refreshed RSS300 makes I400/W100 unsafe")
	require.Equal(t, int32(1), reclaims.Load())
	require.Equal(t, int64(300), c.dataReserved.Load())
	require.Equal(t, int64(100), c.inputPromise.Load())
	require.Equal(t, int64(100), c.workspacePromise.Load())
	require.LessOrEqual(t, m.physicalUsed()+c.dataReserved.Load()+c.inputPromise.Load()+c.workspacePromise.Load(), int64(1000))
}

// The first full recovery cannot fit even the small candidate. The second
// callback is the existing worker, held before its fresh physical sample.
// Tests release physical memory without any controlled Release notification.
type finalCandidateWait struct {
	m             *memThrottler
	c             *memoryCoordinator
	rss           atomic.Int64
	queued        <-chan string
	fullEntered   chan struct{}
	workerEntered chan struct{}
	allowFull     func()
	allowWorker   func()
	cancel        context.CancelFunc
	result        chan candidateResult
	consumed      bool
	head          *admissionWaiter
}

func newFinalCandidateWait(t *testing.T) *finalCandidateWait {
	t.Helper()
	m, c, queued := admissionFixture(t, 1000, 900, 950)
	f := &finalCandidateWait{
		m: m, c: c, queued: queued, fullEntered: make(chan struct{}),
		workerEntered: make(chan struct{}), result: make(chan candidateResult, 1),
	}
	f.rss.Store(950)
	c.readSample = func() memorySample { return memorySample{rss: f.rss.Load(), total: 1000} }
	fullRelease, workerRelease := make(chan struct{}), make(chan struct{})
	f.allowFull = sync.OnceFunc(func() { close(fullRelease) })
	f.allowWorker = sync.OnceFunc(func() { close(workerRelease) })
	ctx, cancel := context.WithCancel(context.Background())
	f.cancel = cancel
	t.Cleanup(func() {
		cancel()
		f.allowFull()
		f.allowWorker()
		if !f.consumed {
			result := await(t, f.result)
			if result.working != nil {
				result.working.Close()
			}
		}
		c.fullMu.Lock()
		fullDone := c.fullDone
		c.fullMu.Unlock()
		if fullDone != nil {
			await(t, fullDone)
		}
		c.mu.Lock()
		workerDone := c.workerDone
		c.mu.Unlock()
		if workerDone != nil {
			await(t, workerDone)
		}
	})
	var recoveries atomic.Int32
	c.reclaim = func(*memThrottler) {
		switch recoveries.Add(1) {
		case 1:
			close(f.fullEntered)
			<-fullRelease
			// The callback already owns reclaimMu. Make the worker's initial
			// wake eligible without waiting for a wall-clock fallback tick.
			c.lastReclaim = time.Time{}
		case 2:
			close(f.workerEntered)
			<-workerRelease
		}
	}
	go func() {
		working, err := m.ReserveWorkingSetCandidatesUntilFit(ctx, []WorkingSetSpec{
			candidateSpec(64, 200, 100), candidateSpec(32, 100, 50),
		})
		f.result <- candidateResult{working, err}
	}()
	require.Equal(t, "working-set", await(t, queued))
	await(t, f.fullEntered)
	c.mu.Lock()
	f.head = c.queue[0]
	c.mu.Unlock()
	f.allowFull()
	await(t, f.workerEntered)
	return f
}

func (f *finalCandidateWait) receive(t *testing.T) candidateResult {
	t.Helper()
	result := await(t, f.result)
	f.consumed = true
	if result.working != nil {
		t.Cleanup(result.working.Close)
	}
	return result
}

func TestWorkingSetUntilFitWaitsForLatePhysicalReleaseAndReselectsLargest(t *testing.T) {
	f := newFinalCandidateWait(t)
	f.c.mu.Lock()
	sameHead := len(f.c.queue) == 1 && f.c.queue[0] == f.head
	f.c.mu.Unlock()
	require.True(t, sameHead, "the final graph must retain its original FIFO node after full recovery")
	require.Zero(t, f.c.inputPromise.Load())
	require.Zero(t, f.c.workspacePromise.Load())
	// No controlled owner or Release event accompanies this background free.
	f.rss.Store(100)
	f.allowWorker()
	result := f.receive(t)
	require.NoError(t, result.err)
	require.Equal(t, int64(64), result.working.UnitBytes(), "keep all candidates while waiting; a later sample can restore the large unit")
	require.Equal(t, int64(200), f.c.inputPromise.Load())
	require.Equal(t, int64(100), f.c.workspacePromise.Load())
}

func TestWorkingSetUntilFitCancellationRetiresNodeDuringRecovery(t *testing.T) {
	f := newFinalCandidateWait(t)
	f.cancel()
	result := f.receive(t)
	require.Nil(t, result.working)
	require.ErrorIs(t, result.err, context.Canceled)
	f.c.mu.Lock()
	empty := len(f.c.queue) == 0
	f.c.mu.Unlock()
	require.True(t, empty, "cancellation must not wait for the worker's blocked recovery callback")
	next, granted, err := f.m.TryReserveWorkingSet(context.Background(), candidateSpec(1, 10, 10))
	require.NoError(t, err)
	require.True(t, granted)
	t.Cleanup(next.Close)
}

func TestWorkingSetUntilFitKeepsMixedFIFO(t *testing.T) {
	f := newFinalCandidateWait(t)
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	consumed, granted := false, false
	t.Cleanup(func() {
		cancel()
		if !consumed {
			granted = await(t, result) == nil
		}
		if granted {
			f.m.Release(20)
		}
	})
	go func() { result <- f.m.AcquireContext(ctx, 20) }()
	require.Equal(t, "data", await(t, f.queued))
	_, bypass := f.m.Acquire(1)
	require.False(t, bypass, "a same-pool ordinary request cannot bypass the pending final graph")
	f.c.mu.Lock()
	ordered := len(f.c.queue) == 2 && f.c.queue[0] == f.head
	f.c.mu.Unlock()
	require.True(t, ordered)
	f.rss.Store(100)
	f.allowWorker()
	first := f.receive(t)
	require.NoError(t, first.err)
	require.Equal(t, int64(64), first.working.UnitBytes())
	err := await(t, result)
	consumed, granted = true, err == nil
	require.NoError(t, err)
	require.Equal(t, int64(200), f.c.inputPromise.Load(), "the successor runs after the first ticket was published")
}

func TestWorkingSetUntilFitRechecksOtherPoolAfterLateFree(t *testing.T) {
	f := newFinalCandidateWait(t)
	f.rss.Store(500)
	f.c.invalidateSample()
	other := &memThrottler{coordinator: f.c, limitRate: .9}
	other.fillDefaults()
	other.options.constLimit = 900
	other.limit.Store(900)
	other.ForceRefresh()
	_, granted := other.Acquire(300)
	require.True(t, granted)
	t.Cleanup(func() { other.Release(300) })
	f.allowWorker()
	result := f.receive(t)
	require.NoError(t, result.err)
	require.Equal(t, int64(32), result.working.UnitBytes(), "RSS500 + new D300 leaves room only for the smaller I100/W50")
	require.LessOrEqual(t, f.m.physicalUsed()+f.c.dataReserved.Load()+f.c.inputPromise.Load()+f.c.workspacePromise.Load(), int64(1000))
}

func TestWorkingSetUntilFitStaticCapacityBoundary(t *testing.T) {
	for _, test := range []struct {
		name  string
		input int64
		fail  bool
	}{
		{name: "minimum_larger_than_process", input: 901, fail: true},
		{name: "minimum_equals_process", input: 900},
	} {
		t.Run(test.name, func(t *testing.T) {
			m, c, queued := admissionFixture(t, 1000, 900, 0)
			var recoveries atomic.Int32
			c.reclaim = func(*memThrottler) { recoveries.Add(1) }
			working, err := m.ReserveWorkingSetCandidatesUntilFit(context.Background(), []WorkingSetSpec{candidateSpec(64, test.input, 100)})
			if working != nil {
				t.Cleanup(working.Close)
			}
			if test.fail {
				var capacity *WorkingSetCapacityError
				require.ErrorAs(t, err, &capacity)
				require.Equal(t, int64(1001), capacity.Minimum)
				require.Equal(t, int64(1000), capacity.Available)
				require.Empty(t, queued, "a proof of static impossibility needs no waiting node")
			} else {
				require.NoError(t, err)
				require.Equal(t, int64(64), working.UnitBytes())
			}
			require.Zero(t, recoveries.Load())
			require.Nil(t, c.workerStop)
		})
	}
}
