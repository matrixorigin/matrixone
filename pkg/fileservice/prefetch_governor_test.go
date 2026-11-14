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

// A MemoryPrefetchObject whose read ended stays counted while a read that copies from it
// waits for another of its ranges, and stops being counted once that read returns.
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
	require.Equal(t, 2*flightBufSize(unit), bytes(), "the ended object's buffer is still held by the read")

	close(storage.release)
	require.NoError(t, <-done)
	require.Equal(t, data[100:110], vec.Entries[0].Data)
	require.Equal(t, data[2*unit+100:2*unit+110], vec.Entries[1].Data)
	vec.Release()
	f.reads.Wait()
	require.Zero(t, bytes())
}
