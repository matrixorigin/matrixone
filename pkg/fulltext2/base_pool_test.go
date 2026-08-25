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

package fulltext2

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSegmentLeaseFreeIsIdempotent(t *testing.T) {
	template := NewSegment("base", 0)
	lease := newSegmentLease(template)
	view := lease.acquire(17)
	require.NotNil(t, view)
	require.Equal(t, int64(17), view.Recency)

	view.Free()
	view.Free()
	require.NotNil(t, lease.template, "the pool reference still owns the template")
	lease.retire()
	require.Nil(t, lease.template)
	lease.retire()
}

func TestSegmentLeaseAcquireAfterRetire(t *testing.T) {
	lease := newSegmentLease(NewSegment("base", 0))
	lease.retire()
	require.Nil(t, lease.acquire(0))
}

func TestImmutableBasePoolSingleflightAndRetire(t *testing.T) {
	p := &immutableBasePool{entries: make(map[baseKey]*baseEntry)}
	key := baseKey{index: "db.store", id: "base-0", checksum: "sum", filesize: 1}
	var loads atomic.Int32
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			view, err := p.acquire(context.Background(), key, func() (*Segment, error) {
				loads.Add(1)
				return NewSegment("base-0", 0), nil
			}, 3)
			if err != nil {
				errs <- err
				return
			}
			view.Free()
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.Equal(t, int32(1), loads.Load())

	p.commit(key.index, nil)
	require.Empty(t, p.entries)
}

func TestImmutableBasePoolWaiterCancellationDoesNotCancelLoader(t *testing.T) {
	p := &immutableBasePool{entries: make(map[baseKey]*baseEntry)}
	key := baseKey{index: "db.store", id: "base-0", checksum: "sum", filesize: 1}
	started := make(chan struct{})
	release := make(chan struct{})
	loaderDone := make(chan error, 1)
	go func() {
		view, err := p.acquire(context.Background(), key, func() (*Segment, error) {
			close(started)
			<-release
			return NewSegment("base-0", 0), nil
		}, 0)
		if view != nil {
			view.Free()
		}
		loaderDone <- err
	}()
	<-started

	ctx, cancel := context.WithCancel(context.Background())
	waiterDone := make(chan error, 1)
	go func() {
		_, err := p.acquire(ctx, key, func() (*Segment, error) {
			return nil, errors.New("canceled waiter became the loader")
		}, 0)
		waiterDone <- err
	}()
	select {
	case err := <-waiterDone:
		t.Fatalf("waiter returned before cancellation: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	cancel()
	select {
	case err := <-waiterDone:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("canceled waiter did not return")
	}

	close(release)
	require.NoError(t, <-loaderDone)
	p.clearAll()
}

type doneSignalContext struct {
	context.Context
	doneCalled chan struct{}
	once       sync.Once
}

func (c *doneSignalContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.doneCalled) })
	return c.Context.Done()
}

func TestImmutableBasePoolWaitingOwnerSurvivesObsoleteRollback(t *testing.T) {
	p := &immutableBasePool{entries: make(map[baseKey]*baseEntry)}
	key := baseKey{index: "db.store", id: "base-0", checksum: "sum", filesize: 1}
	loaderStarted := make(chan struct{})
	releaseLoader := make(chan struct{})
	loaderDone := make(chan error, 1)
	var loads atomic.Int32
	go func() {
		view, err := p.acquireOwned(context.Background(), key, func() (*Segment, error) {
			if loads.Add(1) == 1 {
				close(loaderStarted)
				<-releaseLoader
			}
			return NewSegment(key.id, 0), nil
		}, 0, 11)
		if view != nil {
			view.Free()
		}
		loaderDone <- err
	}()
	<-loaderStarted

	waiting := &doneSignalContext{Context: context.Background(), doneCalled: make(chan struct{})}
	replacementDone := make(chan error, 1)
	go func() {
		view, err := p.acquireOwned(waiting, key, func() (*Segment, error) {
			loads.Add(1)
			return NewSegment("replacement", 0), nil
		}, 0, 22)
		if view != nil {
			view.Free()
		}
		replacementDone <- err
	}()
	select {
	case <-waiting.doneCalled:
	case <-time.After(time.Second):
		t.Fatal("replacement did not reach the in-flight wait")
	}

	p.mu.Lock()
	_, claimed := p.entries[key].owners[22]
	p.mu.Unlock()
	require.True(t, claimed, "a waiting generation must claim the in-flight entry")

	p.rollback(11)
	close(releaseLoader)
	require.NoError(t, <-loaderDone)
	require.NoError(t, <-replacementDone)
	require.Equal(t, int32(1), loads.Load(), "the replacement must reuse the completed base")
	require.Contains(t, p.entries, key)

	p.rollback(22)
	p.clearAll()
}

func TestImmutableBasePoolCanceledWaiterReleasesProvisionalOwner(t *testing.T) {
	p := &immutableBasePool{entries: make(map[baseKey]*baseEntry)}
	key := baseKey{index: "db.store", id: "base-0", checksum: "sum", filesize: 1}
	loaderStarted := make(chan struct{})
	releaseLoader := make(chan struct{})
	loaderDone := make(chan error, 1)
	go func() {
		view, err := p.acquireOwned(context.Background(), key, func() (*Segment, error) {
			close(loaderStarted)
			<-releaseLoader
			return NewSegment(key.id, 0), nil
		}, 0, 11)
		if view != nil {
			view.Free()
		}
		loaderDone <- err
	}()
	<-loaderStarted

	ctx, cancel := context.WithCancel(context.Background())
	waiting := &doneSignalContext{Context: ctx, doneCalled: make(chan struct{})}
	waiterDone := make(chan error, 1)
	go func() {
		_, err := p.acquireOwned(waiting, key, func() (*Segment, error) {
			return nil, errors.New("canceled waiter became the loader")
		}, 0, 22)
		waiterDone <- err
	}()
	select {
	case <-waiting.doneCalled:
	case <-time.After(time.Second):
		t.Fatal("waiter did not reach the in-flight wait")
	}

	p.mu.Lock()
	_, claimed := p.entries[key].owners[22]
	p.mu.Unlock()
	require.True(t, claimed, "the canceled waiter must have a provisional claim to release")

	cancel()
	select {
	case err := <-waiterDone:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("canceled waiter did not return")
	}
	p.rollback(22)
	p.mu.Lock()
	_, claimed = p.entries[key].owners[22]
	_, loaderClaimed := p.entries[key].owners[11]
	p.mu.Unlock()
	require.False(t, claimed)
	require.True(t, loaderClaimed)

	p.rollback(11)
	close(releaseLoader)
	require.NoError(t, <-loaderDone)
	p.clearAll()
}

func TestImmutableBasePoolBoundsEntriesAndEvictsIdle(t *testing.T) {
	p := &immutableBasePool{
		entries:    make(map[baseKey]*baseEntry),
		maxEntries: 1,
		maxBytes:   1 << 30,
		idleTTL:    time.Hour,
	}
	for i := 0; i < 2; i++ {
		key := baseKey{index: "db" + string(rune('0'+i)), id: "base", checksum: "sum", filesize: 1}
		view, err := p.acquire(context.Background(), key, func() (*Segment, error) {
			return NewSegment(key.id, 0), nil
		}, 0)
		require.NoError(t, err)
		view.Free()
	}
	require.LessOrEqual(t, len(p.entries), 1)

	p.idleTTL = time.Nanosecond
	p.evict(time.Now().Add(time.Second))
	require.Empty(t, p.entries)
	p.clearAll()
}

func TestImmutableBasePoolBoundsBytes(t *testing.T) {
	p := &immutableBasePool{
		entries:    make(map[baseKey]*baseEntry),
		maxEntries: 8,
		maxBytes:   2,
		idleTTL:    time.Hour,
	}
	for i := 0; i < 2; i++ {
		key := baseKey{index: "db.store", id: string(rune('a' + i)), checksum: "sum", filesize: 2}
		view, err := p.acquire(context.Background(), key, func() (*Segment, error) {
			return NewSegment(key.id, 0), nil
		}, 0)
		require.NoError(t, err)
		view.Free()
	}
	require.LessOrEqual(t, p.totalBytes, int64(2))
	require.LessOrEqual(t, len(p.entries), 1)
	p.clearAll()
}

func TestImmutableBasePoolLoadFailureDoesNotRetainEntry(t *testing.T) {
	p := &immutableBasePool{entries: make(map[baseKey]*baseEntry)}
	key := baseKey{index: "db.store", id: "broken", checksum: "sum", filesize: 1}
	want := errors.New("load failed")
	_, err := p.acquire(context.Background(), key, func() (*Segment, error) {
		return nil, want
	}, 0)
	require.ErrorIs(t, err, want)
	require.Empty(t, p.entries)
	require.Zero(t, p.totalBytes)
}

func TestImmutableBasePoolInvalidationDuringLoadDoesNotRepoolStaleLease(t *testing.T) {
	p := &immutableBasePool{entries: make(map[baseKey]*baseEntry)}
	key := baseKey{index: "db.store", id: "base-0", checksum: "sum", filesize: 1}
	started := make(chan struct{})
	release := make(chan struct{})
	var loads atomic.Int32
	done := make(chan error, 1)
	go func() {
		view, err := p.acquire(context.Background(), key, func() (*Segment, error) {
			if loads.Add(1) == 1 {
				close(started)
				<-release
			}
			return NewSegment(key.id, 0), nil
		}, 0)
		if view != nil {
			view.Free()
		}
		done <- err
	}()
	<-started

	p.clearIndex(key.index)
	close(release)
	require.NoError(t, <-done)
	require.Equal(t, int32(1), loads.Load(), "the invalidated in-flight result must not be retried in place")
	require.Empty(t, p.entries, "the stale in-flight result is owned by the current query, not the pool")
	p.clearAll()
	require.Empty(t, p.entries)
}

func TestImmutableBasePoolCommittedReplacementSurvivesObsoleteRollback(t *testing.T) {
	p := &immutableBasePool{entries: make(map[baseKey]*baseEntry)}
	key := baseKey{index: "db.store", id: "base-0", checksum: "sum", filesize: 1}

	first, err := p.acquireOwned(context.Background(), key, func() (*Segment, error) {
		return NewSegment("base-0", 0), nil
	}, 0, 11)
	require.NoError(t, err)
	first.Free()

	// The replacement reuses the same immutable bytes and commits them before
	// the obsolete loader reports its later failure. Commit transfers ownership
	// away from the obsolete attempt, so its rollback must not retire this entry.
	replacement, err := p.acquireOwned(context.Background(), key, func() (*Segment, error) {
		return nil, errors.New("replacement unexpectedly became the loader")
	}, 0, 22)
	require.NoError(t, err)
	replacement.Free()
	p.commitOwned(key.index, map[baseKey]struct{}{key: {}}, 22)
	p.rollback(11)

	require.Contains(t, p.entries, key)
	p.clearAll()
}

func TestImmutableBasePoolReusedClaimSurvivesObsoleteRollback(t *testing.T) {
	p := &immutableBasePool{entries: make(map[baseKey]*baseEntry)}
	key := baseKey{index: "db.store", id: "base-0", checksum: "sum", filesize: 1}

	first, err := p.acquireOwned(context.Background(), key, func() (*Segment, error) {
		return NewSegment("base-0", 0), nil
	}, 0, 11)
	require.NoError(t, err)
	t.Cleanup(func() {
		first.Free()
		p.clearAll()
	})

	replacement, err := p.acquireOwned(context.Background(), key, func() (*Segment, error) {
		return nil, errors.New("replacement unexpectedly became the loader")
	}, 0, 22)
	require.NoError(t, err)
	t.Cleanup(replacement.Free)

	// The replacement has claimed the reused lease but has not published its
	// generation yet. The obsolete loader must not retire that lease first.
	p.rollback(11)
	p.commitOwned(key.index, map[baseKey]struct{}{key: {}}, 22)

	require.Contains(t, p.entries, key)
}

func TestImmutableBasePoolPublishedReuseFailureKeepsEntry(t *testing.T) {
	p := &immutableBasePool{entries: make(map[baseKey]*baseEntry)}
	key := baseKey{index: "db.store", id: "base-0", checksum: "sum", filesize: 1}

	first, err := p.acquireOwned(context.Background(), key, func() (*Segment, error) {
		return NewSegment("base-0", 0), nil
	}, 0, 11)
	require.NoError(t, err)
	first.Free()
	p.commitOwned(key.index, map[baseKey]struct{}{key: {}}, 11)

	replacement, err := p.acquireOwned(context.Background(), key, func() (*Segment, error) {
		return nil, errors.New("published entry unexpectedly became the loader")
	}, 0, 22)
	require.NoError(t, err)
	replacement.Free()
	p.rollback(22)

	require.Contains(t, p.entries, key, "a failed reuse must not retire a published base")
	p.clearAll()
}

func TestImmutableBasePoolRollbackOwnerIsGlobalAcrossIndexes(t *testing.T) {
	p := &immutableBasePool{entries: make(map[baseKey]*baseEntry)}
	firstGeneration := beginLoadGeneration("db.a")
	secondGeneration := beginLoadGeneration("db.b")
	require.NotEqual(t, firstGeneration.owner, secondGeneration.owner)

	firstKey := baseKey{index: "db.a", id: "base-0", checksum: "sum", filesize: 1}
	secondKey := baseKey{index: "db.b", id: "base-0", checksum: "sum", filesize: 1}
	first, err := p.acquireOwned(context.Background(), firstKey, func() (*Segment, error) {
		return NewSegment("base-a", 0), nil
	}, 0, firstGeneration.owner)
	require.NoError(t, err)
	first.Free()
	second, err := p.acquireOwned(context.Background(), secondKey, func() (*Segment, error) {
		return NewSegment("base-b", 0), nil
	}, 0, secondGeneration.owner)
	require.NoError(t, err)
	second.Free()

	p.rollback(firstGeneration.owner)
	require.NotContains(t, p.entries, firstKey)
	require.Contains(t, p.entries, secondKey)
	p.clearAll()
	endLoadGeneration(firstGeneration)
	endLoadGeneration(secondGeneration)
}

func TestLoadReasonRegistryIsDatabaseQualified(t *testing.T) {
	cleanup := setLoadObserver(func(LoadEvent) {})
	defer cleanup()
	key1 := loadReasonKey("db1", "store")
	key2 := loadReasonKey("db2", "store")
	rememberLoadReason(key1, LoadMissCDCFlush)
	rememberLoadReason(key2, LoadMissMerge)
	reason, generation := peekLoadReason(key1)
	require.Equal(t, LoadMissCDCFlush, reason)
	consumeLoadReason(key1, generation)
	reason, generation = peekLoadReason(key2)
	require.Equal(t, LoadMissMerge, reason)
	consumeLoadReason(key2, generation)
	reason, generation = peekLoadReason(key1)
	require.Empty(t, reason)
	require.Zero(t, generation)
}

func TestLoadReasonRegistryIsDisabledWithObserverOff(t *testing.T) {
	cleanup := setLoadObserver(nil)
	defer cleanup()
	key := loadReasonKey("db", "store")
	rememberLoadReason(key, LoadMissCDCFlush)
	reason, generation := peekLoadReason(key)
	require.Empty(t, reason)
	require.Zero(t, generation)
}
