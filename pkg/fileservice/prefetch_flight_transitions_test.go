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

package fileservice

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func prefetchBytes(fs *S3FS) int64 {
	f := &fs.diskCache.prefetchObjects
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.bytes
}

// A column read of a range a background file prefetch has not started, as it waits for a
// download connection, reads its own bytes.
func TestFlightTransitionForegroundReadNotHeldByWaitingFileConnection(t *testing.T) {
	fs, _, data := newPrefetchRangeFS(t, 0, 300<<10)
	fs.diskCache.maxDownloadConns = 1
	fs.diskCache.memoryPrefetchMax = 1
	ranges := columnRanges(2, 40<<10, 1<<10)
	got, offset, err := foregroundReadOfWaitingRange(t, fs, ranges)
	require.NoError(t, err)
	require.Equal(t, data[offset:offset+ranges[0].Size], got)
}

// A column read that copied its first entry from a flight that ended, and waits for the
// flight of its second, holds no budget for the first.
func TestFlightTransitionColumnReadReleasesCopiedFlight(t *testing.T) {
	fs, _, data := newPrefetchRangeFS(t, 0, 300<<10)
	ranges := columnRanges(3, 40<<10, 1<<10)
	unit := flightBufSize(ranges[0].Size)
	fs.diskCache.prefetchFlightBytes = 2 * unit
	gx, gy := make(chan struct{}), make(chan struct{})
	releaseX, releaseY := sync.OnceFunc(func() { close(gx) }), sync.OnceFunc(func() { close(gy) })
	defer releaseX()
	defer releaseY()
	g := newBlackBoxStorage(fs.storage, func(string, int64) bool { return false })
	g.gated = func(_ string, offset int64) chan struct{} {
		switch offset {
		case ranges[0].Offset:
			return gx
		case ranges[1].Offset:
			return gy
		}
		return nil
	}
	fs.storage = g
	ctx := context.Background()
	key := fs.pathToKey("object")
	require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "object", Size: 2 << 10, Ranges: ranges[:2], Scan: &PrefetchScan{}}))
	require.Eventually(t, func() bool { cur, _, _ := g.counts(key); return cur == 2 }, 5*time.Second, time.Millisecond)

	readCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		vec := &IOVector{FilePath: "object", Policy: ColumnPrefetch | SkipFullFilePreloads | SkipMemoryCache,
			Entries: []IOEntry{ranges[0].entry(), ranges[1].entry()}}
		err := fs.Read(readCtx, vec)
		if err == nil {
			vec.Release()
		}
		done <- err
	}()
	// The read holds both flights.
	require.Eventually(t, func() bool {
		f := &fs.diskCache.prefetchObjects
		f.mu.Lock()
		defer f.mu.Unlock()
		for _, o := range f.m[fs.diskCache.pathForFile("object")] {
			if fl, ok := o.(*memoryPrefetchObject); ok {
				fl.mu.Lock()
				refs := fl.refs
				fl.mu.Unlock()
				if refs < 2 {
					return false
				}
			}
		}
		return true
	}, 5*time.Second, time.Millisecond)
	releaseX()
	// X ended and was copied; only Y, which reads, holds budget.
	assertEventuallyBytes(t, fs, unit)

	// A read of another range, which needs one buffer, is not held by Y.
	otherCtx, cancelOther := context.WithTimeout(ctx, 2*time.Second)
	defer cancelOther()
	vec := &IOVector{FilePath: "object", Policy: SkipFullFilePreloads | SkipMemoryCache,
		Entries: []IOEntry{ranges[2].entry()}}
	if assert.NoError(t, fs.Read(otherCtx, vec), "a read of another range is not held by Y") {
		assert.Equal(t, data[ranges[2].Offset:ranges[2].Offset+ranges[2].Size], vec.Entries[0].Data)
		vec.Release()
	}
	releaseY()
	require.NoError(t, <-done)
}

// A range read that copied a block from a flight that ended, and waits for the flight of
// its next block, holds no budget for the first.
func TestFlightTransitionPrefetchReadReleasesCopiedFlight(t *testing.T) {
	const unit = 64 << 10
	fs, _, data := newPrefetchRangeFS(t, unit, 5*unit)
	fs.diskCache.prefetchFlightBytes = 2 * unit
	gx, gy := make(chan struct{}), make(chan struct{})
	releaseX, releaseY := sync.OnceFunc(func() { close(gx) }), sync.OnceFunc(func() { close(gy) })
	defer releaseX()
	defer releaseY()
	g := newBlackBoxStorage(fs.storage, func(string, int64) bool { return false })
	g.gated = func(_ string, offset int64) chan struct{} {
		switch offset {
		case 0:
			return gx
		case unit:
			return gy
		}
		return nil
	}
	fs.storage = g
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	key := fs.pathToKey("object")
	read := func(ctx context.Context, offset, size int64) ([]byte, error) {
		vec := &IOVector{FilePath: "object", Policy: SkipFullFilePreloads | SkipMemoryCache,
			Entries: []IOEntry{{Offset: offset, Size: size}}}
		if err := fs.Read(ctx, vec); err != nil {
			return nil, err
		}
		defer vec.Release()
		return append([]byte(nil), vec.Entries[0].Data...), nil
	}
	// B starts the flight Y of unit 1.
	doneB := make(chan error, 1)
	go func() { _, err := read(ctx, unit+5, 5); doneB <- err }()
	require.Eventually(t, func() bool { cur, _, _ := g.counts(key); return cur == 1 }, 5*time.Second, time.Millisecond)
	// A starts the flight X of unit 0 and joins Y.
	doneA := make(chan error, 1)
	go func() {
		got, err := read(ctx, 10, unit)
		if err == nil && string(got) != string(data[10:10+unit]) {
			err = context.Canceled
		}
		doneA <- err
	}()
	require.Eventually(t, func() bool { cur, _, _ := g.counts(key); return cur == 2 }, 5*time.Second, time.Millisecond)
	releaseX()
	// X ended and A copied it; only Y, which reads, holds budget.
	assertEventuallyBytes(t, fs, unit)
	otherCtx, cancelOther := context.WithTimeout(ctx, 2*time.Second)
	defer cancelOther()
	got, err := read(otherCtx, 3*unit+5, 5)
	if assert.NoError(t, err, "a read of another unit is not held by Y") {
		assert.Equal(t, data[3*unit+5:3*unit+10], got)
	}
	releaseY()
	require.NoError(t, <-doneA)
	require.NoError(t, <-doneB)
}

func (r PrefetchRange) entry() IOEntry { return IOEntry{Offset: r.Offset, Size: r.Size} }

// failOnceStorage fails the first read at offset fail once gate is closed.
type failOnceStorage struct {
	ObjectStorage
	fail   int64
	gate   chan struct{}
	failed atomic.Bool
}

func (s *failOnceStorage) Read(ctx context.Context, key string, from, to *int64) (io.ReadCloser, error) {
	if from != nil && *from == s.fail && s.failed.CompareAndSwap(false, true) {
		select {
		case <-s.gate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return nil, errors.New("probe: storage failed")
	}
	return s.ObjectStorage.Read(ctx, key, from, to)
}

func memoryWaiters(fs *S3FS, diskPath string) int {
	f := &fs.diskCache.prefetchObjects
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, o := range f.m[diskPath] {
		if fl, ok := o.(*memoryPrefetchObject); ok {
			fl.mu.Lock()
			n += len(fl.waiters)
			fl.mu.Unlock()
		}
	}
	return n
}

// A column read waiting on a background flight whose storage read fails fails.
func TestFlightTransitionColumnReadJoinedFailedFlightFails(t *testing.T) {
	fs, _, _ := newPrefetchRangeFS(t, 0, 300<<10)
	ranges := columnRanges(1, 40<<10, 1<<10)
	s := &failOnceStorage{ObjectStorage: fs.storage, fail: ranges[0].Offset, gate: make(chan struct{})}
	fs.storage = s
	ctx := context.Background()
	require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "object", Size: 1 << 10, Ranges: ranges, Scan: &PrefetchScan{}}))
	diskPath := fs.diskCache.pathForFile("object")
	require.Eventually(t, func() bool {
		_, ok := fs.diskCache.fastestPrefetch(diskPath, ranges[0].Offset, ranges[0].Offset+ranges[0].Size)
		return ok
	}, 5*time.Second, time.Millisecond)
	readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := readColumnRange(readCtx, fs, ranges[0]); done <- err }()
	require.Eventually(t, func() bool { return memoryWaiters(fs, diskPath) == 1 }, 5*time.Second, time.Millisecond,
		"the read waits on the flight")
	close(s.gate)
	require.Error(t, <-done, "a failed read of object storage fails the reads waiting on it")
}

// A range read joining a read-started flight whose storage read fails fails.
func TestFlightTransitionRangeReadJoinedFailedFlightFails(t *testing.T) {
	fs, _, _ := newPrefetchRangeFS(t, 0, 300<<10)
	s := &failOnceStorage{ObjectStorage: fs.storage, fail: 1000, gate: make(chan struct{})}
	fs.storage = s
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	read := func(offset, size int64) error {
		vec := &IOVector{FilePath: "object", Policy: SkipFullFilePreloads | SkipMemoryCache, Entries: []IOEntry{{Offset: offset, Size: size}}}
		err := fs.Read(ctx, vec)
		if err == nil {
			vec.Release()
		}
		return err
	}
	diskPath := fs.diskCache.pathForFile("object")
	a, b := make(chan error, 1), make(chan error, 1)
	go func() { a <- read(1000, 100) }()
	require.Eventually(t, func() bool { return memoryWaiters(fs, diskPath) == 1 }, 5*time.Second, time.Millisecond)
	go func() { b <- read(1010, 40) }()
	require.Eventually(t, func() bool { return memoryWaiters(fs, diskPath) == 2 }, 5*time.Second, time.Millisecond)
	close(s.gate)
	require.Error(t, <-a)
	require.Error(t, <-b)
	fs.diskCache.prefetchObjects.reads.Wait()
	require.Zero(t, prefetchBytes(fs))
}

// A read through prefetch ranges waiting on a flight whose storage read fails fails.
func TestFlightTransitionPrefetchReadJoinedFailedFlightFails(t *testing.T) {
	const unit = 64 << 10
	fs, _, _ := newPrefetchRangeFS(t, unit, 5*unit)
	s := &failOnceStorage{ObjectStorage: fs.storage, fail: unit, gate: make(chan struct{})}
	fs.storage = s
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	read := func(offset, size int64) error {
		vec := &IOVector{FilePath: "object", Policy: SkipFullFilePreloads | SkipMemoryCache, Entries: []IOEntry{{Offset: offset, Size: size}}}
		err := fs.Read(ctx, vec)
		if err == nil {
			vec.Release()
		}
		return err
	}
	diskPath := fs.diskCache.pathForFile("object")
	a, b := make(chan error, 1), make(chan error, 1)
	go func() { a <- read(unit+10, 100) }()
	require.Eventually(t, func() bool { return memoryWaiters(fs, diskPath) == 1 }, 5*time.Second, time.Millisecond)
	go func() { b <- read(unit+20, 40) }()
	require.Eventually(t, func() bool { return memoryWaiters(fs, diskPath) == 2 }, 5*time.Second, time.Millisecond)
	close(s.gate)
	errA, errB := <-a, <-b
	assert.Error(t, errA, "the read that started the flight")
	assert.Error(t, errB, "the read that joined the flight")
}

// Budget and download connections return to zero after a background column prefetch, and
// the connections never pass the bound.
func TestFlightTransitionColumnPrefetchReturnsBudgetAndConns(t *testing.T) {
	fs, _, _ := newPrefetchRangeFS(t, 0, 300<<10)
	fs.diskCache.maxDownloadConns = 2
	ranges := columnRanges(8, 20<<10, 1<<10)
	fs.diskCache.prefetchFlightBytes = 3 * flightBufSize(ranges[0].Size)
	g := newBlackBoxStorage(fs.storage, func(string, int64) bool { return true })
	fs.storage = g
	require.NoError(t, fs.PrefetchObject(context.Background(), ObjectPrefetch{FilePath: "object", Size: 8 << 10, Ranges: ranges, Scan: &PrefetchScan{}}))
	key := fs.pathToKey("object")
	require.Eventually(t, func() bool { cur, _, _ := g.counts(key); return cur == 2 }, 5*time.Second, time.Millisecond)
	require.LessOrEqual(t, fs.diskCache.downloadConns.Load(), int64(2))
	close(g.gate)
	require.Eventually(t, func() bool { return !fs.prefetcher.isRunning("object") }, 5*time.Second, time.Millisecond)
	fs.diskCache.prefetchObjects.reads.Wait()
	require.Zero(t, prefetchBytes(fs))
	require.Zero(t, fs.diskCache.downloadConns.Load())
	_, most, total := g.counts(key)
	require.Equal(t, len(ranges), total)
	require.LessOrEqual(t, most, 2)
}

// A background flight no read waits for, whose source stalls, stops after the idle timeout
// and frees its buffer and connection.
func TestFlightTransitionIdleFlightFreesBudgetAndConn(t *testing.T) {
	shortIdle(t, 50*time.Millisecond)
	fs, _, _ := newPrefetchRangeFS(t, 0, 300<<10)
	ranges := columnRanges(1, 20<<10, 1<<10)
	g := newBlackBoxStorage(fs.storage, func(string, int64) bool { return true })
	fs.storage = g
	defer close(g.gate)
	require.NoError(t, fs.PrefetchObject(context.Background(), ObjectPrefetch{FilePath: "object", Size: 1 << 10, Ranges: ranges, Scan: &PrefetchScan{}}))
	key := fs.pathToKey("object")
	require.Eventually(t, func() bool { cur, _, _ := g.counts(key); return cur == 1 }, 5*time.Second, time.Millisecond)
	require.Eventually(t, func() bool {
		return !fs.prefetcher.isRunning("object") && prefetchBytes(fs) == 0 && fs.diskCache.downloadConns.Load() == 0
	}, 5*time.Second, time.Millisecond)
	_, ok := fs.diskCache.ranges.covering(fs.diskCache.rangeObjectKey("object"), ranges[0].Offset, ranges[0].Size)
	require.False(t, ok)
}

// A delete of an object whose background flights wait on their source cancels them: no
// range is cached after the delete, and they free their buffers and connections.
func TestFlightTransitionDeleteStopsWaitingFlight(t *testing.T) {
	fs, _, _ := newPrefetchRangeFS(t, 0, 300<<10)
	ranges := columnRanges(2, 20<<10, 1<<10)
	g := newBlackBoxStorage(fs.storage, func(string, int64) bool { return true })
	fs.storage = g
	release := sync.OnceFunc(func() { close(g.gate) })
	defer release()
	ctx := context.Background()
	require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "object", Size: 2 << 10, Ranges: ranges, Scan: &PrefetchScan{}}))
	key := fs.pathToKey("object")
	require.Eventually(t, func() bool { cur, _, _ := g.counts(key); return cur == 2 }, 5*time.Second, time.Millisecond)
	require.NoError(t, fs.Delete(ctx, "object"))
	release()
	require.Eventually(t, func() bool { return !fs.prefetcher.isRunning("object") }, 5*time.Second, time.Millisecond)
	fs.diskCache.prefetchObjects.reads.Wait()
	require.Zero(t, prefetchBytes(fs))
	require.Zero(t, fs.diskCache.downloadConns.Load())
	rk := fs.diskCache.rangeObjectKey("object")
	for _, r := range ranges {
		_, ok := fs.diskCache.ranges.covering(rk, r.Offset, r.Size)
		require.False(t, ok, "no range cached after the delete")
	}
	_, _, total := g.counts(key)
	require.Equal(t, 2, total)
}

// A range read waiting for budget returns once the cache closes; nothing stays charged.
func TestFlightTransitionBudgetWaiterReturnsOnClose(t *testing.T) {
	fs, _, _ := newPrefetchRangeFS(t, 0, 300<<10)
	ranges := columnRanges(2, 40<<10, 1<<10)
	fs.diskCache.prefetchFlightBytes = flightBufSize(ranges[0].Size)
	g := newBlackBoxStorage(fs.storage, func(_ string, offset int64) bool { return offset == ranges[0].Offset })
	fs.storage = g
	defer close(g.gate)
	ctx := context.Background()
	require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "object", Size: 1 << 10, Ranges: ranges[:1], Scan: &PrefetchScan{}}))
	key := fs.pathToKey("object")
	require.Eventually(t, func() bool { cur, _, _ := g.counts(key); return cur == 1 }, 5*time.Second, time.Millisecond)
	readCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		vec := &IOVector{FilePath: "object", Policy: SkipFullFilePreloads | SkipMemoryCache, Entries: []IOEntry{ranges[1].entry()}}
		err := fs.Read(readCtx, vec)
		if err == nil {
			vec.Release()
		}
		done <- err
	}()
	require.Never(t, func() bool { return len(done) > 0 }, 100*time.Millisecond, time.Millisecond, "the read waits for budget")
	closeCtx, cancelClose := context.WithTimeout(ctx, 5*time.Second)
	defer cancelClose()
	fs.Close(closeCtx)
	require.NoError(t, closeCtx.Err(), "close does not wait for its context")
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the read waiting for budget did not return after close")
	}
	require.Zero(t, prefetchBytes(fs))
}

// A range read whose context ends while it waits for budget returns, holding nothing.
func TestFlightTransitionBudgetWaiterContextEnds(t *testing.T) {
	fs, _, _ := newPrefetchRangeFS(t, 0, 300<<10)
	ranges := columnRanges(2, 40<<10, 1<<10)
	fs.diskCache.prefetchFlightBytes = flightBufSize(ranges[0].Size)
	g := newBlackBoxStorage(fs.storage, func(_ string, offset int64) bool { return offset == ranges[0].Offset })
	fs.storage = g
	release := sync.OnceFunc(func() { close(g.gate) })
	defer release()
	ctx := context.Background()
	require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "object", Size: 1 << 10, Ranges: ranges[:1], Scan: &PrefetchScan{}}))
	key := fs.pathToKey("object")
	require.Eventually(t, func() bool { cur, _, _ := g.counts(key); return cur == 1 }, 5*time.Second, time.Millisecond)
	readCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	vec := &IOVector{FilePath: "object", Policy: SkipFullFilePreloads | SkipMemoryCache, Entries: []IOEntry{ranges[1].entry()}}
	require.ErrorIs(t, fs.Read(readCtx, vec), context.DeadlineExceeded)
	_, _, total := g.counts(key)
	require.Equal(t, 1, total)
	release()
	require.Eventually(t, func() bool { return !fs.prefetcher.isRunning("object") }, 5*time.Second, time.Millisecond)
	fs.diskCache.prefetchObjects.reads.Wait()
	require.Zero(t, prefetchBytes(fs))
	require.Zero(t, fs.diskCache.downloadConns.Load())
}

func assertEventuallyBytes(t *testing.T, fs *S3FS, want int64) bool {
	return assert.Eventually(t, func() bool { return prefetchBytes(fs) == want }, 2*time.Second, time.Millisecond,
		"the buffer of the copied, ended flight is still charged: %d", prefetchBytes(fs))
}
