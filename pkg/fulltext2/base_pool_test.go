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
	cancel()
	_, err := p.acquire(ctx, key, func() (*Segment, error) {
		t.Fatalf("canceled waiter became the loader")
		return nil, nil
	}, 0)
	require.ErrorIs(t, err, context.Canceled)

	close(release)
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

func TestImmutableBasePoolInvalidationDuringLoadRetriesWithoutRetainingStaleLease(t *testing.T) {
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
	require.Equal(t, int32(2), loads.Load(), "the invalidated in-flight result must not be reused")
	require.NotEmpty(t, p.entries, "the retry may install the current generation")
	p.clearAll()
	require.Empty(t, p.entries)
}

func TestLoadReasonRegistryIsDatabaseQualified(t *testing.T) {
	cleanup := setLoadObserver(func(LoadEvent) {})
	defer cleanup()
	key1 := loadReasonKey("db1", "store")
	key2 := loadReasonKey("db2", "store")
	rememberLoadReason(key1, LoadMissCDCFlush)
	rememberLoadReason(key2, LoadMissMerge)
	require.Equal(t, LoadMissCDCFlush, takeLoadReason(key1))
	require.Equal(t, LoadMissMerge, takeLoadReason(key2))
	require.Empty(t, takeLoadReason(key1))
}

func TestLoadReasonRegistryIsDisabledWithObserverOff(t *testing.T) {
	cleanup := setLoadObserver(nil)
	defer cleanup()
	key := loadReasonKey("db", "store")
	rememberLoadReason(key, LoadMissCDCFlush)
	require.Empty(t, takeLoadReason(key))
}
