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

// Concurrent reads of different prefetch ranges run MemoryPrefetchObjects only up to the
// bytes the buffers may hold; the others wait without reading object storage, a waiting
// read whose context ends returns, and once storage answers every read gets its bytes and
// no buffer stays counted.
func TestPrefetchMemoryBoundUnderConcurrentReads(t *testing.T) {
	const (
		unit    = 64 << 10
		units   = 16
		budget  = 4 * unit
		waiting = 4
	)
	fs, _, data := newPrefetchRangeFS(t, unit, (units+waiting)*unit)
	fs.diskCache.prefetchFlightBytes = budget
	storage := &gateStorage{ObjectStorage: fs.storage, release: make(chan struct{})}
	fs.storage = storage
	f := &fs.diskCache.prefetchObjects

	var peak atomic.Int64
	stopSampling := make(chan struct{})
	sampled := make(chan struct{})
	go func() {
		defer close(sampled)
		for {
			f.mu.Lock()
			if f.bytes > peak.Load() {
				peak.Store(f.bytes)
			}
			f.mu.Unlock()
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
	require.Eventually(t, func() bool {
		f.mu.Lock()
		defer f.mu.Unlock()
		return storage.opens.Load() == budget/unit && f.freed != nil
	}, 10*time.Second, time.Millisecond, "the reads past the bound wait")

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
	require.Never(t, func() bool { return storage.opens.Load() > budget/unit }, 200*time.Millisecond, time.Millisecond,
		"no read past the bound reads object storage")
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
	f.mu.Lock()
	defer f.mu.Unlock()
	require.Zero(t, f.bytes)
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
	storage := &suffixGateStorage{ObjectStorage: fs.storage, from: 2 * unit, release: make(chan struct{})}
	fs.storage = storage
	f := &fs.diskCache.prefetchObjects
	bytes := func() int64 {
		f.mu.Lock()
		defer f.mu.Unlock()
		return f.bytes
	}
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
	require.Equal(t, flightBufSize(unit), bytes(), "the read copied from the ended object and released its buffer")

	close(storage.release)
	require.NoError(t, <-done)
	require.Equal(t, data[100:110], vec.Entries[0].Data)
	require.Equal(t, data[2*unit+100:2*unit+110], vec.Entries[1].Data)
	vec.Release()
	f.reads.Wait()
	require.Zero(t, bytes())
}

// A cold read whose entries make more runs than the buffers may hold reads the runs that
// fit, the others waiting, in order, for buffers; the buffers never exceed the bound, each
// run is read once, and the read gets its bytes.
func TestPrefetchMemoryBoundForColdMultiRunRead(t *testing.T) {
	const (
		unit   = 64 << 10
		runs   = 8
		budget = 2 * unit
	)
	fs, _, data := newPrefetchRangeFS(t, unit, 2*runs*unit)
	fs.diskCache.prefetchFlightBytes = budget
	storage := &gateStorage{ObjectStorage: fs.storage, release: make(chan struct{})}
	fs.storage = storage
	f := &fs.diskCache.prefetchObjects

	entries := make([]IOEntry, runs)
	for i := range entries {
		entries[i] = IOEntry{Offset: int64(2*i)*unit + 100, Size: 10}
	}
	vec := &IOVector{FilePath: "object", Policy: SkipFullFilePreloads | SkipMemoryCache, Entries: entries}
	done := make(chan error, 1)
	go func() { done <- fs.Read(context.Background(), vec) }()
	require.Eventually(t, func() bool { return storage.opens.Load() == budget/unit }, 10*time.Second, time.Millisecond,
		"the runs that fit are read")
	require.Never(t, func() bool { return storage.opens.Load() > budget/unit }, 100*time.Millisecond, time.Millisecond,
		"the others wait for buffers")
	f.mu.Lock()
	inMemory := f.bytes
	f.mu.Unlock()
	require.LessOrEqual(t, inMemory, int64(budget))

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

// A cold column prefetch of more ranges than the flight bytes hold reads only the ranges
// that fit, the first one at least; the others wait, and once storage answers every range
// is read once. The download counts as started, once, when its first ranges are under way.
func TestColumnPrefetchMemoryBoundForColdRanges(t *testing.T) {
	const (
		unit   = 64 << 10
		ranges = 8
		size   = 16 << 10
	)
	fs, _, data := newPrefetchRangeFS(t, unit, 2*ranges*unit)
	budget := 2 * flightBufSize(size)
	fs.diskCache.prefetchFlightBytes = budget
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
	var started atomic.Int64
	ctx := context.WithValue(context.Background(), downloadStartedKey{}, func() { started.Add(1) })
	done := make(chan error, 1)
	go func() {
		ok, err := fs.diskCache.readRanges(ctx, "object", fs.diskCache.deletes.now(), SkipMemoryCacheWrites|SkipFullFilePreloads|ColumnPrefetch, rs, open)
		if err == nil && !ok {
			err = errors.New("the ranges were not read")
		}
		done <- err
	}()
	require.Eventually(t, func() bool { return opens.Load() == 2 }, 10*time.Second, time.Millisecond)
	f.mu.Lock()
	inMemory := f.bytes
	f.mu.Unlock()
	require.LessOrEqual(t, inMemory, budget)
	require.Equal(t, int64(1), started.Load(), "started while ranges wait")

	close(release)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(diskCacheLifecycleTestTimeout):
		t.Fatal("the prefetch did not return")
	}
	require.Equal(t, int64(ranges), opens.Load(), "no range is read twice")
	require.Equal(t, int64(1), started.Load())
}

// Concurrent cold reads, each of more runs than the buffers may hold, all return with
// their bytes: a read holds only runs under way while its later runs wait for buffers, so
// no read waits for buffers another read holds while it waits. The buffers never exceed
// the bound, and none stays counted.
func TestPrefetchConcurrentMultiRunReadsUnderTightBudget(t *testing.T) {
	const (
		unit    = 64 << 10
		runs    = 6
		readers = 16
		budget  = 2 * unit
	)
	fs, _, data := newPrefetchRangeFS(t, unit, 2*runs*unit*2)
	fs.diskCache.prefetchFlightBytes = budget
	f := &fs.diskCache.prefetchObjects
	var peak atomic.Int64
	stop := make(chan struct{})
	sampled := make(chan struct{})
	go func() {
		defer close(sampled)
		for {
			f.mu.Lock()
			if f.bytes > peak.Load() {
				peak.Store(f.bytes)
			}
			f.mu.Unlock()
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
	f.mu.Lock()
	defer f.mu.Unlock()
	require.Zero(t, f.bytes)
}

// A read whose entry spans a block another read's run is reading and a block no run reads
// takes the first block from that run and reads the other; once both reads return, no
// buffer stays counted.
func TestPrefetchReadOfJoinedBlockReleasesIt(t *testing.T) {
	const unit = 64 << 10
	fs, _, data := newPrefetchRangeFS(t, unit, 4*unit)
	storage := &suffixGateStorage{ObjectStorage: fs.storage, from: unit, release: make(chan struct{})}
	fs.storage = storage
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
	f.mu.Lock()
	defer f.mu.Unlock()
	require.Zero(t, f.bytes, "no buffer stays counted")
}

// A read of units of which it takes some blocks, with buffers for one run, returns: it
// holds no run of blocks it takes no bytes of while its next run waits for buffers.
func TestPrefetchReadHoldsNoUnneededRun(t *testing.T) {
	const (
		block = 64 << 10
		unit  = 4 * block
	)
	fs, _, data := newPrefetchRangeFS(t, unit, 4*unit)
	fs.diskCache.prefetchFlightBytes = flightBufSize(2 * block)
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
	f.mu.Lock()
	defer f.mu.Unlock()
	require.Zero(t, f.bytes)
}
