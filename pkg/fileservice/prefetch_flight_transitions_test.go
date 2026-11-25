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

// A column read past disk-prefetch-memory-max reads through a FilePrefetchObject of its
// own, which waits for a download connection as a prefetch does (decision 2026-11-23): it
// waits, without failing, while a stalled prefetch holds the only connection, and gets its
// bytes once the connection frees.
func TestFlightTransitionForegroundFileReadWaitsForConnection(t *testing.T) {
	fs, _, data := newPrefetchRangeFS(t, 0, 300<<10)
	fs.diskCache.conns.max = 1
	fs.diskCache.memoryPrefetchMax = 1
	ranges := columnRanges(2, 40<<10, 1<<10)
	held := make(chan struct{})
	release := sync.OnceFunc(func() { close(held) })
	t.Cleanup(release)
	var heldOffset atomic.Int64
	heldOffset.Store(-1)
	g := newBlackBoxStorage(fs.storage, func(string, int64) bool { return false })
	g.gated = func(_ string, offset int64) chan struct{} {
		if heldOffset.CompareAndSwap(-1, offset) {
			return held
		}
		return nil
	}
	fs.storage = g
	ctx := context.Background()
	key := fs.pathToKey("object")
	require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "object", Size: 2 << 10, Ranges: ranges, Scan: &PrefetchScan{}}))
	require.Eventually(t, func() bool { cur, _, _ := g.counts(key); return cur == 1 }, 5*time.Second, time.Millisecond)
	r := ranges[0]
	if heldOffset.Load() == r.Offset {
		r = ranges[1]
	}
	type result struct {
		got []byte
		err error
	}
	done := make(chan result, 1)
	go func() {
		got, err := readColumnRange(ctx, fs, r)
		done <- result{got, err}
	}()
	select {
	case res := <-done:
		t.Fatalf("the read did not wait for the held connection: %v", res.err)
	case <-time.After(300 * time.Millisecond):
	}
	release()
	select {
	case res := <-done:
		require.NoError(t, res.err)
		require.Equal(t, data[r.Offset:r.Offset+r.Size], res.got)
	case <-time.After(10 * time.Second):
		t.Fatal("the read did not get its bytes once the connection freed")
	}
}

// A column read of two ranges two flights read holds only the flight of the range it waits
// for: once either flight ends, the read holds no budget for it, a read of another range is
// not held by the read, and the range of a flight that ended before the read reached it is
// read from the disk cache, not object storage.
func TestFlightTransitionColumnReadReleasesCopiedFlight(t *testing.T) {
	for _, tc := range []struct {
		name  string
		first int
	}{
		{"the first range ends first", 0},
		{"the second range ends first", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs, _, data := newPrefetchRangeFS(t, 0, 300<<10)
			ranges := columnRanges(3, 40<<10, 1<<10)
			unit := flightBufSize(ranges[0].Size)
			fs.diskCache.prefetchFlightBytes = 2 * unit
			gates := []chan struct{}{make(chan struct{}), make(chan struct{})}
			release := []func(){sync.OnceFunc(func() { close(gates[0]) }), sync.OnceFunc(func() { close(gates[1]) })}
			defer release[0]()
			defer release[1]()
			g := newBlackBoxStorage(fs.storage, func(string, int64) bool { return false })
			g.gated = func(_ string, offset int64) chan struct{} {
				for i := range gates {
					if offset == ranges[i].Offset {
						return gates[i]
					}
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
			type result struct {
				data [2][]byte
				err  error
			}
			done := make(chan result, 1)
			cache := &publicationVectorCache{}
			go func() {
				vec := &IOVector{FilePath: "object", Policy: ColumnPrefetch | SkipFullFilePreloads | SkipMemoryCache,
					Caches: []IOVectorCache{cache}, Entries: []IOEntry{ranges[0].entry(), ranges[1].entry()}}
				var r result
				if r.err = fs.Read(readCtx, vec); r.err == nil {
					for i := range r.data {
						r.data[i] = append([]byte(nil), vec.Entries[i].Data...)
					}
					vec.Release()
				}
				done <- r
			}()
			// The read waits for the first range.
			require.Eventually(t, func() bool { return flightRefs(fs, ranges[0].Offset) >= 2 }, 5*time.Second, time.Millisecond)
			release[tc.first]()
			// Only the flight still reading holds budget.
			assertEventuallyBytes(t, fs, unit)

			otherCtx, cancelOther := context.WithTimeout(ctx, 2*time.Second)
			defer cancelOther()
			vec := &IOVector{FilePath: "object", Policy: SkipFullFilePreloads | SkipMemoryCache,
				Entries: []IOEntry{ranges[2].entry()}}
			if assert.NoError(t, fs.Read(otherCtx, vec), "a read of another range is not held by the read") {
				assert.Equal(t, data[ranges[2].Offset:ranges[2].Offset+ranges[2].Size], vec.Entries[0].Data)
				vec.Release()
			}
			release[1-tc.first]()
			r := <-done
			require.NoError(t, r.err)
			for i := range r.data {
				require.Equal(t, data[ranges[i].Offset:ranges[i].Offset+ranges[i].Size], r.data[i])
			}
			_, _, total := g.counts(key)
			require.Equal(t, 3, total, "each range is read from object storage once")
			require.Equal(t, int64(1), cache.updates.Load(), "the read updates the caches it missed")
		})
	}
}

// flightRefs returns the holders of the flight of the object that starts at offset.
func flightRefs(fs *S3FS, offset int64) int {
	f := &fs.diskCache.prefetchObjects
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, o := range f.m[fs.diskCache.pathForFile("object")] {
		if fl, ok := o.(*memoryPrefetchObject); ok && fl.start == offset {
			fl.mu.Lock()
			defer fl.mu.Unlock()
			return fl.refs
		}
	}
	return 0
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

// A range read whose runs end out of order holds no budget for a run that ended and
// cached its bytes while it waits for an earlier one.
func TestFlightTransitionRangeReadReleasesEndedRun(t *testing.T) {
	const unit = 64 << 10
	fs, _, data := newPrefetchRangeFS(t, unit, 5*unit)
	fs.diskCache.prefetchFlightBytes = 2 * unit
	// Unit 1 is cached, so the read runs units 0 and 2 apart.
	readRangeOf(t, fs, 0, unit+5, 5)
	gate := make(chan struct{})
	release := sync.OnceFunc(func() { close(gate) })
	defer release()
	g := newBlackBoxStorage(fs.storage, func(string, int64) bool { return false })
	g.gated = func(_ string, offset int64) chan struct{} {
		if offset == 0 {
			return gate
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
	done := make(chan error, 1)
	go func() {
		got, err := read(ctx, 10, 2*unit)
		if err == nil && string(got) != string(data[10:10+2*unit]) {
			err = context.Canceled
		}
		done <- err
	}()
	require.Eventually(t, func() bool { _, _, total := g.counts(key); return total == 2 }, 5*time.Second, time.Millisecond)
	// Unit 2 ended; only unit 0, which reads, holds budget.
	assertEventuallyBytes(t, fs, unit)
	otherCtx, cancelOther := context.WithTimeout(ctx, 2*time.Second)
	defer cancelOther()
	got, err := read(otherCtx, 3*unit+5, 5)
	if assert.NoError(t, err, "a read of another unit is not held by the ended run") {
		assert.Equal(t, data[3*unit+5:3*unit+10], got)
	}
	release()
	require.NoError(t, <-done)
	_, _, total := g.counts(key)
	require.Equal(t, 3, total, "each unit is read from object storage once")
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
	fs.diskCache.conns.max = 16
	holdConns(t, fs.diskCache, 14)
	ranges := columnRanges(8, 20<<10, 1<<10)
	fs.diskCache.prefetchFlightBytes = 3 * flightBufSize(ranges[0].Size)
	g := newBlackBoxStorage(fs.storage, func(string, int64) bool { return true })
	fs.storage = g
	require.NoError(t, fs.PrefetchObject(context.Background(), ObjectPrefetch{FilePath: "object", Size: 8 << 10, Ranges: ranges, Scan: &PrefetchScan{}}))
	key := fs.pathToKey("object")
	require.Eventually(t, func() bool { cur, _, _ := g.counts(key); return cur == 2 }, 5*time.Second, time.Millisecond)
	require.LessOrEqual(t, fs.diskCache.conns.held.Load(), int64(16))
	close(g.gate)
	require.Eventually(t, func() bool { return !fs.prefetcher.isRunning("object") }, 5*time.Second, time.Millisecond)
	fs.diskCache.prefetchObjects.reads.Wait()
	require.Zero(t, prefetchBytes(fs))
	require.Equal(t, int64(14), fs.diskCache.conns.held.Load())
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
		return !fs.prefetcher.isRunning("object") && prefetchBytes(fs) == 0 && fs.diskCache.conns.held.Load() == 0
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
	require.Zero(t, fs.diskCache.conns.held.Load())
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
	require.Zero(t, fs.diskCache.conns.held.Load())
}

func assertEventuallyBytes(t *testing.T, fs *S3FS, want int64) bool {
	return assert.Eventually(t, func() bool { return prefetchBytes(fs) == want }, 2*time.Second, time.Millisecond,
		"the buffer of the copied, ended flight is still charged: %d", prefetchBytes(fs))
}
