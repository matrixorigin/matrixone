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
	"bytes"
	"context"
	"errors"
	"io"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// gateStorage holds every read until release is closed or the read's context ends.
type gateStorage struct {
	ObjectStorage
	opens   atomic.Int64
	release chan struct{}
}

func (h *gateStorage) Read(ctx context.Context, key string, min, max *int64) (io.ReadCloser, error) {
	h.opens.Add(1)
	select {
	case <-h.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return h.ObjectStorage.Read(ctx, key, min, max)
}

// heldMemory returns the memory the server of d holds.
func heldMemory(d *DiskCache) int64 {
	used := d.server.memoryInUse()
	return used[foregroundRequest] + used[backgroundRequest]
}

// fillRoom holds all the free room of g until the returned function frees it.
func fillRoom(t *testing.T, g *objectStorageServer) func() {
	t.Helper()
	var frees []func()
	for _, c := range []requestClass{backgroundRequest, foregroundRequest} {
		for {
			free, ok := admitted(g, c)
			if !ok {
				break
			}
			occupiedRoom(g).Add(1)
			frees = append(frees, func() {
				free()
				occupiedRoom(g).Add(-1)
			})
		}
	}
	return sync.OnceFunc(func() {
		for _, free := range frees {
			free()
		}
	})
}

// Concurrent reads of different prefetch ranges run MemoryPrefetchObjects only up to the
// bytes the memory budget holds; the others read their ranges through file fills at once,
// never waiting for memory. A read whose context ends returns, and once storage answers
// every read gets its bytes and no buffer stays counted.
func TestPrefetchMemoryBoundUnderConcurrentReads(t *testing.T) {
	const (
		unit    = 64 << 10
		units   = 16
		budget  = 4 * unit
		waiting = 4
	)
	fs, _, data := newPrefetchRangeFS(t, unit, (units+waiting)*unit)
	fs.server.memoryLimit = budget
	storage := &gateStorage{ObjectStorage: fs.server.raw, release: make(chan struct{})}
	fs.server.raw = storage
	f := &fs.diskCache.prefetchObjects

	var peak atomic.Int64
	stopSampling := make(chan struct{})
	sampled := make(chan struct{})
	go func() {
		defer close(sampled)
		for {
			if n := heldMemory(fs.diskCache); n > peak.Load() {
				peak.Store(n)
			}
			select {
			case <-stopSampling:
				return
			default:
				runtime.Gosched()
			}
		}
	}()

	type result struct {
		i    int
		data []byte
		err  error
	}
	read := func(ctx context.Context, i int, out chan<- result) {
		vec := &IOVector{FilePath: "object", Policy: SkipFullFilePreloads | SkipMemoryCache,
			Entries: []IOEntry{{Offset: int64(i)*unit + 100, Size: 10}}}
		err := fs.Read(ctx, vec)
		r := result{i: i, err: err}
		if err == nil {
			r.data = slices.Clone(vec.Entries[0].Data)
		}
		vec.Release()
		out <- r
	}

	done := make(chan result, units)
	for i := range units {
		go read(context.Background(), i, done)
	}
	require.Eventually(t, func() bool { return storage.opens.Load() == units }, 10*time.Second, time.Millisecond,
		"the reads past the bound read at once")

	ctx, cancel := context.WithCancel(context.Background())
	canceled := make(chan result, waiting)
	var started sync.WaitGroup
	for i := range waiting {
		started.Add(1)
		go func() {
			started.Done()
			read(ctx, units+i, canceled)
		}()
	}
	started.Wait()
	require.Eventually(t, func() bool { return storage.opens.Load() == units+waiting }, 10*time.Second, time.Millisecond)
	cancel()
	for range waiting {
		r := <-canceled
		require.ErrorIs(t, r.err, context.Canceled)
	}

	close(storage.release)
	for range units {
		select {
		case r := <-done:
			require.NoError(t, r.err)
			off := r.i*unit + 100
			require.Equal(t, data[off:off+10], r.data)
		case <-time.After(diskCacheLifecycleTestTimeout):
			t.Fatal("a read did not return")
		}
	}
	f.reads.Wait()
	close(stopSampling)
	<-sampled

	require.LessOrEqual(t, peak.Load(), int64(budget))
	require.Zero(t, heldMemory(fs.diskCache))
	require.Empty(t, f.m)
}

// suffixGateStorage holds the reads from offset from until release is closed.
type suffixGateStorage struct {
	ObjectStorage
	from    int64
	held    atomic.Int64
	release chan struct{}
}

func (h *suffixGateStorage) Read(ctx context.Context, key string, min, max *int64) (io.ReadCloser, error) {
	if min != nil && *min >= h.from {
		h.held.Add(1)
		select {
		case <-h.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return h.ObjectStorage.Read(ctx, key, min, max)
}

// A MemoryPrefetchObject whose read ended stops being counted once the read copying from
// it copied its bytes, while that read waits for another of its ranges.
func TestPrefetchBufferCountedUntilLastRelease(t *testing.T) {
	const unit = 64 << 10
	fs, _, data := newPrefetchRangeFS(t, unit, 4*unit)
	storage := &suffixGateStorage{ObjectStorage: fs.server.raw, from: 2 * unit, release: make(chan struct{})}
	fs.server.raw = storage
	f := &fs.diskCache.prefetchObjects
	bytes := func() int64 { return heldMemory(fs.diskCache) }
	memoryObjects := func() int {
		f.mu.Lock()
		defer f.mu.Unlock()
		n := 0
		for _, objects := range f.m {
			for _, o := range objects {
				if _, ok := o.(*memoryPrefetchObject); ok {
					n++
				}
			}
		}
		return n
	}

	vec := &IOVector{FilePath: "object", Policy: SkipFullFilePreloads | SkipMemoryCache,
		Entries: []IOEntry{{Offset: 100, Size: 10}, {Offset: 2*unit + 100, Size: 10}}}
	done := make(chan error, 1)
	go func() { done <- fs.Read(context.Background(), vec) }()
	require.Eventually(t, func() bool { return storage.held.Load() == 1 && memoryObjects() == 1 },
		10*time.Second, time.Millisecond, "the first range's object ended; the second waits")
	// The read releases the ended object's buffer once it has copied from it, while its
	// second range still waits.
	require.Eventually(t, func() bool { return bytes() == flightBufSize(unit) }, 10*time.Second, time.Millisecond,
		"the read copied from the ended object and released its buffer")

	close(storage.release)
	require.NoError(t, <-done)
	require.Equal(t, data[100:110], vec.Entries[0].Data)
	require.Equal(t, data[2*unit+100:2*unit+110], vec.Entries[1].Data)
	vec.Release()
	f.reads.Wait()
	require.Zero(t, bytes())
}

// A cold read whose entries make more runs than the memory budget holds reads every run at
// once, those past the budget through file fills; the buffers never exceed the bound, each
// run is read once, and the read gets its bytes.
func TestPrefetchMemoryBoundForColdMultiRunRead(t *testing.T) {
	const (
		unit   = 64 << 10
		runs   = 8
		budget = 2 * unit
	)
	fs, _, data := newPrefetchRangeFS(t, unit, 2*runs*unit)
	fs.server.memoryLimit = budget
	storage := &gateStorage{ObjectStorage: fs.server.raw, release: make(chan struct{})}
	fs.server.raw = storage
	f := &fs.diskCache.prefetchObjects

	entries := make([]IOEntry, runs)
	for i := range entries {
		entries[i] = IOEntry{Offset: int64(2*i)*unit + 100, Size: 10}
	}
	vec := &IOVector{FilePath: "object", Policy: SkipFullFilePreloads | SkipMemoryCache, Entries: entries}
	done := make(chan error, 1)
	go func() { done <- fs.Read(context.Background(), vec) }()
	require.Eventually(t, func() bool { return storage.opens.Load() == runs }, 10*time.Second, time.Millisecond,
		"every run is read at once")
	require.LessOrEqual(t, heldMemory(fs.diskCache), int64(budget))
	_ = f

	close(storage.release)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(diskCacheLifecycleTestTimeout):
		t.Fatal("the read did not return")
	}
	for i := range vec.Entries {
		off := 2*i*unit + 100
		require.Equal(t, data[off:off+10], vec.Entries[i].Data)
	}
	vec.Release()
	require.Equal(t, int64(runs), storage.opens.Load(), "no run is read twice")
}

// A cold column prefetch of more ranges than the memory budget holds reads every range at
// once, background buffers within half the budget and the others through file fills, and
// once storage answers every range is read once.
func TestColumnPrefetchMemoryBoundForColdRanges(t *testing.T) {
	const (
		unit   = 64 << 10
		ranges = 8
		size   = 16 << 10
	)
	fs, _, data := newPrefetchRangeFS(t, unit, 2*ranges*unit)
	budget := 4 * flightBufSize(size)
	fs.server.memoryLimit = budget
	f := &fs.diskCache.prefetchObjects
	var opens atomic.Int64
	release := make(chan struct{})
	open := func(ctx context.Context, start, end int64) (io.ReadCloser, error) {
		opens.Add(1)
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return io.NopCloser(bytes.NewReader(data[start:end])), nil
	}
	rs := make([]PrefetchRange, ranges)
	for i := range rs {
		rs[i] = PrefetchRange{Offset: int64(2*i) * unit, Size: size}
	}
	done := make(chan error, 1)
	go func() {
		prefetchRanges(fs, rs, open)
		done <- nil
	}()
	require.Eventually(t, func() bool { return opens.Load() == ranges }, 10*time.Second, time.Millisecond)
	require.LessOrEqual(t, heldMemory(fs.diskCache), budget/2, "background buffers take at most half the budget")
	_ = f

	close(release)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(diskCacheLifecycleTestTimeout):
		t.Fatal("the prefetch did not return")
	}
	require.Equal(t, int64(ranges), opens.Load(), "no range is read twice")
}

// Concurrent cold reads, each of more runs than the memory budget holds, all return with
// their bytes: no run waits for memory. The buffers never exceed the bound, and none stays
// counted.
func TestPrefetchConcurrentMultiRunReadsUnderTightBudget(t *testing.T) {
	const (
		unit    = 64 << 10
		runs    = 6
		readers = 16
		budget  = 2 * unit
	)
	fs, _, data := newPrefetchRangeFS(t, unit, 2*runs*unit*2)
	fs.server.memoryLimit = budget
	f := &fs.diskCache.prefetchObjects
	var peak atomic.Int64
	stop := make(chan struct{})
	sampled := make(chan struct{})
	go func() {
		defer close(sampled)
		for {
			if n := heldMemory(fs.diskCache); n > peak.Load() {
				peak.Store(n)
			}
			select {
			case <-stop:
				return
			default:
				runtime.Gosched()
			}
		}
	}()
	errs := make(chan error, readers)
	for r := range readers {
		go func() {
			entries := make([]IOEntry, runs)
			for i := range entries {
				// Readers overlap: each reads every other unit, shifted by r%2.
				entries[i] = IOEntry{Offset: int64(2*i+r%2)*unit + int64(r), Size: 10}
			}
			vec := &IOVector{FilePath: "object", Policy: SkipFullFilePreloads | SkipMemoryCache, Entries: entries}
			err := fs.Read(context.Background(), vec)
			for i := range vec.Entries {
				off := vec.Entries[i].Offset
				if err == nil && !bytes.Equal(data[off:off+10], vec.Entries[i].Data) {
					err = errors.New("wrong bytes")
				}
			}
			vec.Release()
			errs <- err
		}()
	}
	for range readers {
		select {
		case err := <-errs:
			require.NoError(t, err)
		case <-time.After(diskCacheLifecycleTestTimeout):
			t.Fatal("a read did not return")
		}
	}
	f.reads.Wait()
	close(stop)
	<-sampled
	require.LessOrEqual(t, peak.Load(), int64(budget))
	require.Zero(t, heldMemory(fs.diskCache))
}

// A read whose entry spans a block another read's run is reading and a block no run reads
// takes the first block from that run and reads the other; once both reads return, no
// buffer stays counted.
func TestPrefetchReadOfJoinedBlockReleasesIt(t *testing.T) {
	const unit = 64 << 10
	fs, _, data := newPrefetchRangeFS(t, unit, 4*unit)
	storage := &suffixGateStorage{ObjectStorage: fs.server.raw, from: unit, release: make(chan struct{})}
	fs.server.raw = storage
	f := &fs.diskCache.prefetchObjects
	read := func(off, size int64, done chan<- error) {
		vec := &IOVector{FilePath: "object", Policy: SkipFullFilePreloads | SkipMemoryCache,
			Entries: []IOEntry{{Offset: off, Size: size}}}
		err := fs.Read(context.Background(), vec)
		if err == nil && !bytes.Equal(data[off:off+size], vec.Entries[0].Data) {
			err = errors.New("wrong bytes")
		}
		vec.Release()
		done <- err
	}
	first := make(chan error, 1)
	go read(unit+100, 10, first)
	require.Eventually(t, func() bool { return storage.held.Load() == 1 }, 10*time.Second, time.Millisecond,
		"the first read's run waits for storage")
	second := make(chan error, 1)
	go read(2*unit-5, 10, second)
	require.Eventually(t, func() bool { return storage.held.Load() == 2 }, 10*time.Second, time.Millisecond,
		"the second read reads the block no run reads")
	close(storage.release)
	require.NoError(t, <-first)
	require.NoError(t, <-second)
	f.reads.Wait()
	require.Zero(t, heldMemory(fs.diskCache), "no buffer stays counted")
}

// A read of units of which it takes some blocks, with memory for one run, returns.
func TestPrefetchReadHoldsNoUnneededRun(t *testing.T) {
	const (
		block = 64 << 10
		unit  = 4 * block
	)
	fs, _, data := newPrefetchRangeFS(t, unit, 4*unit)
	fs.server.memoryLimit = flightBufSize(2 * block)
	f := &fs.diskCache.prefetchObjects
	// Each entry is in the third block of a unit, the units apart: the read takes the
	// third and fourth blocks of each as one run, and reads the first two as another.
	entries := []IOEntry{{Offset: 2*block + 100, Size: 10}, {Offset: 2*unit + 2*block + 100, Size: 10}}
	vec := &IOVector{FilePath: "object", Policy: SkipFullFilePreloads | SkipMemoryCache, Entries: entries}
	done := make(chan error, 1)
	go func() { done <- fs.Read(context.Background(), vec) }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(diskCacheLifecycleTestTimeout):
		t.Fatal("the read did not return")
	}
	for _, e := range vec.Entries {
		require.Equal(t, data[e.Offset:e.Offset+e.Size], e.Data)
	}
	vec.Release()
	f.reads.Wait()
	require.Zero(t, heldMemory(fs.diskCache))
}

// keepOnDeleteStorage serves an object after it is deleted, as object storage may for a
// while.
type keepOnDeleteStorage struct{ ObjectStorage }

func (keepOnDeleteStorage) Delete(context.Context, ...string) error { return nil }

// A read whose run waits for room while its object is deleted leaves no range of the
// deleted object cached, even while storage still serves it.
func TestPrefetchReadRunWaitingAcrossDeleteCachesNothing(t *testing.T) {
	const unit = 64 << 10
	fs, _, data := newPrefetchRangeFS(t, unit, 4*unit)
	ctx := context.Background()
	require.NoError(t, fs.Write(ctx, IOVector{FilePath: "other", Entries: []IOEntry{{Size: int64(len(data)), Data: data}},
		Policy: SkipDiskCache | SkipMemoryCache}))
	fs.server.memoryLimit = flightBufSize(unit)
	otherKey := fs.pathToKey("other")
	g := newBlackBoxStorage(keepOnDeleteStorage{fs.server.raw}, func(key string, _ int64) bool { return key == otherKey })
	fs.server.raw = g
	release := sync.OnceFunc(func() { close(g.gate) })
	defer release()
	read := func(name string, done chan<- error) {
		vec := &IOVector{FilePath: name, Policy: SkipFullFilePreloads | SkipMemoryCache, Entries: []IOEntry{{Offset: 100, Size: 10}}}
		done <- fs.Read(ctx, vec)
		vec.Release()
	}
	other := make(chan error, 1)
	go read("other", other)
	require.Eventually(t, func() bool { cur, _, _ := g.counts(otherKey); return cur == 1 }, 5*time.Second, time.Millisecond,
		"the read of other holds its room")
	freeRoom := fillRoom(t, fs.server)
	object := make(chan error, 1)
	go read("object", object)
	f := &fs.diskCache.prefetchObjects
	require.Eventually(t, waitingFor(fs.server, foregroundRequest, 1), 5*time.Second, time.Millisecond,
		"the read of object waits for room")
	// The delete's own request waits for room behind the run.
	deleted := make(chan error, 1)
	go func() { deleted <- fs.Delete(ctx, "object") }()
	require.Eventually(t, waitingFor(fs.server, foregroundRequest, 2), 5*time.Second, time.Millisecond)
	freeRoom()
	require.NoError(t, <-deleted)
	release()
	require.NoError(t, <-other)
	<-object
	f.reads.Wait()
	_, cached := fs.diskCache.ranges.covering(fs.diskCache.objectKey("object"), 0, unit)
	require.False(t, cached, "no range of the deleted object is cached")
}

// A read of a run past the bytes of a MemoryPrefetchObject whose part, cut at a stale
// object size, holds none of its blocks returns.
func TestPrefetchReadOfRunCutAtStaleSizeReturns(t *testing.T) {
	const unit = 64 << 10
	fs, _, data := newPrefetchRangeFS(t, unit, 4*unit)
	fs.diskCache.memoryPrefetchMax = unit
	fs.diskCache.prefetched.store(fs.diskCache.objectKey("object"), 30<<10)
	vec := &IOVector{FilePath: "object", Policy: SkipFullFilePreloads | SkipMemoryCache, Entries: []IOEntry{{Offset: 60 << 10, Size: 10 << 10}}}
	done := make(chan error, 1)
	go func() { done <- fs.Read(context.Background(), vec) }()
	select {
	case err := <-done:
		require.NoError(t, err)
		require.Equal(t, data[60<<10:70<<10], vec.Entries[0].Data)
	case <-time.After(diskCacheLifecycleTestTimeout):
		t.Fatal("the read did not return")
	}
	vec.Release()
}

// A read of an object deleted while it reads returns none of the bytes it assembled: they
// may be of two objects at the path, so the caller reads them again.
func TestPrefetchReadAcrossDeleteReturnsNoBytes(t *testing.T) {
	const unit = 64 << 10
	fs, _, data := newPrefetchRangeFS(t, unit, 4*unit)
	d := fs.diskCache
	diskPath := d.objectKey("object")
	var opens atomic.Int64
	open := func(ctx context.Context, start, end int64) (io.ReadCloser, error) {
		// The second run opens once the object is deleted.
		if opens.Add(1) == 2 {
			d.deletes.deleted(diskPath)
		}
		return io.NopCloser(bytes.NewReader(data[start:end])), nil
	}
	entries := []IOEntry{{Offset: 100, Size: 10}, {Offset: 2*unit + 100, Size: 10}}
	_, _, ok, err := d.prefetchRead(context.Background(), "object", SkipFullFilePreloads|SkipMemoryCache, entries, 100, 2*unit+110, unit, openerSource(open))
	require.NoError(t, err)
	require.Equal(t, int64(2), opens.Load())
	require.False(t, ok, "no bytes read across a delete are returned")
}

// With no memory available, concurrent reads hold no buffer: each reads its range through a
// file fill, and all of them return their bytes.
func TestPrefetchMemoryExhaustedHoldsNoBuffer(t *testing.T) {
	const (
		unit  = 64 << 10
		units = 8
	)
	prev := memoryAvailable
	t.Cleanup(func() { memoryAvailable = prev })
	memoryAvailable = func() (uint64, bool) { return 0, true }
	fs, _, data := newPrefetchRangeFS(t, unit, units*unit)
	fs.server.memoryLimit = 0
	fs.server.measuredAt.Store(0)
	storage := &gateStorage{ObjectStorage: fs.server.raw, release: make(chan struct{})}
	fs.server.raw = storage
	release := sync.OnceFunc(func() { close(storage.release) })
	t.Cleanup(release)
	f := &fs.diskCache.prefetchObjects

	type result struct {
		i    int
		data []byte
		err  error
	}
	done := make(chan result, units)
	for i := range units {
		go func() {
			vec := &IOVector{FilePath: "object", Policy: SkipFullFilePreloads | SkipMemoryCache,
				Entries: []IOEntry{{Offset: int64(i)*unit + 100, Size: 10}}}
			err := fs.Read(context.Background(), vec)
			r := result{i: i, err: err}
			if err == nil {
				r.data = slices.Clone(vec.Entries[0].Data)
			}
			vec.Release()
			done <- r
		}()
	}
	require.Eventually(t, func() bool { return storage.opens.Load() == units }, 10*time.Second, time.Millisecond,
		"every read reads at once")
	require.Zero(t, heldMemory(fs.diskCache), "no read holds a buffer")

	release()
	for range units {
		r := <-done
		require.NoError(t, r.err)
		off := r.i*unit + 100
		require.Equal(t, data[off:off+10], r.data)
	}
	f.reads.Wait()
	require.Zero(t, heldMemory(fs.diskCache))
}
