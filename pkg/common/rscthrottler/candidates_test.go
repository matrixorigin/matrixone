// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");

package rscthrottler

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func candidateSpec(unit, input, workspace int64) WorkingSetSpec {
	return WorkingSetSpec{UnitBytes: unit, Inputs: []InputSlotSpec{{Name: "input", Count: 1, Bytes: input}}, IOBytes: workspace}
}

type candidateResult struct {
	working *WorkingSet
	err     error
}

func TestWorkingSetCandidatesReclaimRestoresLargest(t *testing.T) {
	m, c, _ := admissionFixture(t, 1000, 900, 900)
	var rss atomic.Int64
	rss.Store(900)
	var reclaims atomic.Int32
	c.readSample = func() memorySample { return memorySample{rss: rss.Load(), total: 1000} }
	c.reclaim = func(*memThrottler) { reclaims.Add(1); rss.Store(100) }
	w, err := m.ReserveWorkingSetCandidates(context.Background(), []WorkingSetSpec{candidateSpec(64, 200, 100), candidateSpec(32, 80, 50)})
	require.NoError(t, err)
	t.Cleanup(w.Close)
	require.Equal(t, int64(64), w.UnitBytes(), "reclaim must reconsider the original large candidate")
	require.Equal(t, int32(1), reclaims.Load())
	require.Nil(t, c.workerStop, "ownerless synchronous recovery needs no fallback worker")
}

func TestWorkingSetCandidatesFitSharedWorkspaceDelta(t *testing.T) {
	m, c, _ := admissionFixture(t, 1000, 900, 100)
	owner, err := m.ReserveWorkingSet(context.Background(), WorkingSetSpec{UnitBytes: 1, IOBytes: 700})
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

func TestWorkingSetCandidatesCancelDuringFullReclaim(t *testing.T) {
	m, c, _ := admissionFixture(t, 1000, 900, 950)
	entered, release := make(chan struct{}), make(chan struct{})
	c.reclaim = func(*memThrottler) { close(entered); <-release }
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	result := make(chan error, 1)
	go func() {
		w, err := m.ReserveWorkingSetCandidates(ctx, []WorkingSetSpec{candidateSpec(64, 100, 100)})
		if w != nil {
			w.Close()
		}
		result <- err
	}()
	await(t, entered)
	c.fullMu.Lock()
	done := c.fullDone
	c.fullMu.Unlock()
	t.Cleanup(func() { close(release); await(t, done) })
	cancel()
	require.ErrorIs(t, await(t, result), context.Canceled, "cancel removes FIFO ownership without waiting for native/cache recovery")
	c.mu.Lock()
	require.Empty(t, c.queue)
	c.mu.Unlock()
}

func TestWorkingSetSingleSpecKeepsOwnerlessWaitContract(t *testing.T) {
	m, _, queued := admissionFixture(t, 1000, 900, 950)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	result := make(chan error, 1)
	go func() { _, err := m.ReserveWorkingSet(ctx, candidateSpec(64, 100, 100)); result <- err }()
	require.Equal(t, "working-set", await(t, queued))
	cancel()
	require.ErrorIs(t, await(t, result), context.Canceled, "legacy single-spec users retain their cancellable wait rather than new capacity errors")
}

// Unlike ReclaimRestoresLargest, a smaller unit already fits before recovery.
// These cells distinguish avoiding needless fragmentation from capacity failure.
func TestWorkingSetCandidatesReclaimBeforeSmallerFit(t *testing.T) {
	for _, test := range []struct {
		name        string
		specs       []WorkingSetSpec
		rssAfter    int64
		wantUnit    int64
		wantReclaim int32
	}{
		{
			name:        "reclaim_restores_largest_feasible",
			specs:       []WorkingSetSpec{candidateSpec(64, 400, 100), candidateSpec(32, 100, 100)},
			rssAfter:    100,
			wantUnit:    64,
			wantReclaim: 1,
		},
		{
			name:        "unchanged_resident_keeps_smaller_fit_after_once",
			specs:       []WorkingSetSpec{candidateSpec(64, 400, 100), candidateSpec(32, 100, 100)},
			rssAfter:    600,
			wantUnit:    32,
			wantReclaim: 1,
		},
		{
			name:        "statically_impossible_largest_needs_no_reclaim",
			specs:       []WorkingSetSpec{candidateSpec(64, 1000, 100), candidateSpec(32, 100, 100)},
			rssAfter:    100,
			wantUnit:    32,
			wantReclaim: 0,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			m, c, _ := admissionFixture(t, 1000, 900, 600)
			var rss atomic.Int64
			rss.Store(600)
			var reclaims atomic.Int32
			c.readSample = func() memorySample { return memorySample{rss: rss.Load(), total: 1000} }
			c.reclaim = func(*memThrottler) { reclaims.Add(1); rss.Store(test.rssAfter) }
			w, err := m.ReserveWorkingSetCandidates(context.Background(), test.specs)
			require.NoError(t, err)
			t.Cleanup(w.Close)
			require.Equal(t, test.wantUnit, w.UnitBytes())
			require.Equal(t, test.wantReclaim, reclaims.Load())
			require.Nil(t, c.workerStop, "a fitting ownerless request must not start periodic recovery")
		})
	}
}

func TestWorkingSetCandidatesActiveOwnerKeepsSmallerFitWithoutReclaim(t *testing.T) {
	m, c, _ := admissionFixture(t, 1000, 900, 100)
	owner, err := m.ReserveWorkingSet(context.Background(), WorkingSetSpec{UnitBytes: 1, IOBytes: 300})
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
	m, c, queued := admissionFixture(t, 1000, 900, 600)
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
	require.ErrorIs(t, actual.err, context.Canceled, "an already fitting small candidate cannot hide cancellation during recovery")
	c.mu.Lock()
	empty := len(c.queue) == 0
	c.mu.Unlock()
	require.True(t, empty, "cancelled recovery must relinquish its FIFO node before the recovery callback finishes")
	next, granted, err := m.TryReserveWorkingSet(context.Background(), candidateSpec(1, 10, 10))
	require.NoError(t, err)
	require.True(t, granted, "a successor may acquire after the cancelled head has retired")
	t.Cleanup(next.Close)
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
