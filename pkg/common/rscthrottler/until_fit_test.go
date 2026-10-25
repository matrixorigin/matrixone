// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");

package rscthrottler

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

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

func TestWorkingSetUntilFitKeepsOneReclaimBeforeSmallerFit(t *testing.T) {
	m, c, _ := admissionFixture(t, 1000, 900, 600)
	var recoveries atomic.Int32
	c.reclaim = func(*memThrottler) { recoveries.Add(1) }
	working, err := m.ReserveWorkingSetCandidatesUntilFit(context.Background(), []WorkingSetSpec{
		candidateSpec(64, 400, 100), candidateSpec(32, 100, 100),
	})
	require.NoError(t, err)
	t.Cleanup(working.Close)
	require.Equal(t, int64(32), working.UnitBytes())
	require.Equal(t, int32(1), recoveries.Load())
	require.Nil(t, c.workerStop, "an already fitting smaller unit must not wait forever for a larger one")
}
