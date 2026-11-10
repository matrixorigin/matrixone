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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type prefetchRecorder struct {
	mu      sync.Mutex
	ran     []string
	release chan struct{}
	started chan string
}

func newPrefetchRecorder() *prefetchRecorder {
	return &prefetchRecorder{release: make(chan struct{}), started: make(chan string, 64)}
}

func (r *prefetchRecorder) run(req ObjectPrefetch) {
	r.started <- req.FilePath
	<-r.release
	r.mu.Lock()
	r.ran = append(r.ran, req.FilePath)
	r.mu.Unlock()
}

// The prefetcher downloads the objects in the order they are queued, queues an object
// once, and never queues one it is downloading or has.
func TestPrefetcherDownloadsInQueueOrder(t *testing.T) {
	r := newPrefetchRecorder()
	have := map[string]bool{"cached": true}
	p := newPrefetcher(1, func() int64 { return 1 << 40 }, func(path string) bool { return have[path] }, r.run)
	p.add(ObjectPrefetch{FilePath: "first", Size: 100})
	require.Equal(t, "first", <-r.started)

	for _, path := range []string{"a", "b", "a", "first", "cached", "c"} {
		p.add(ObjectPrefetch{FilePath: path, Size: 100})
	}
	require.True(t, p.isQueued("a"))
	require.False(t, p.isQueued("first"), "a running download is not queued again")
	require.False(t, p.isQueued("cached"))
	require.Equal(t, int64(3), p.queued.Load())

	close(r.release)
	for range 3 {
		<-r.started
	}
	p.close(context.Background())
	r.mu.Lock()
	defer r.mu.Unlock()
	require.Equal(t, []string{"first", "a", "b", "c"}, r.ran)
}

// A queue that holds its most bytes drops the newest requests: the queued objects are
// those reads need first.
func TestPrefetcherFullQueueDropsNewest(t *testing.T) {
	r := newPrefetchRecorder()
	p := newPrefetcher(0, func() int64 { return 250 }, func(string) bool { return false }, r.run)
	for _, path := range []string{"a", "b", "c"} {
		p.add(ObjectPrefetch{FilePath: path, Size: 100})
	}
	require.True(t, p.isQueued("a"))
	require.True(t, p.isQueued("b"))
	require.False(t, p.isQueued("c"), "300 bytes would pass the bound")
	p.add(ObjectPrefetch{FilePath: "d", Size: 50})
	require.True(t, p.isQueued("d"), "it fits")
	p.close(context.Background())
	require.False(t, p.isQueued("a"))
	p.add(ObjectPrefetch{FilePath: "late", Size: 1})
	require.Zero(t, p.queued.Load(), "a closed prefetcher queues nothing")
}

// The prefetch queue holds no more bytes than half the disk cache keeps, nor than the
// downloads write within the horizon.
func TestPrefetchQueueLimit(t *testing.T) {
	fs, _, _ := newObjectSplitFS(t, objectSplitConfig{parts: 1}, 1)
	rate := downloadRates.rate()
	byRate := int64(rate * 4 * prefetchHorizon.Seconds())
	require.Equal(t, min(fs.diskCache.capacityFunc()/2, byRate), fs.prefetchQueueLimit(4)())
	require.LessOrEqual(t, fs.prefetchQueueLimit(1<<30)(), fs.diskCache.capacityFunc()/2)
}

// A read of a queued object moves its download to the front of the queue.
func TestPrefetcherPromote(t *testing.T) {
	r := newPrefetchRecorder()
	p := newPrefetcher(1, func() int64 { return 1 << 40 }, func(string) bool { return false }, r.run)
	p.add(ObjectPrefetch{FilePath: "first", Size: 100})
	require.Equal(t, "first", <-r.started)
	later := time.Now().Add(time.Hour).UnixNano()
	for _, path := range []string{"a", "b", "c"} {
		p.add(ObjectPrefetch{FilePath: path, Size: 100, Due: later})
	}
	require.True(t, p.promote("c"))
	require.True(t, p.promote("c"), "already in front")
	require.False(t, p.promote("first"), "a running download is not queued")
	require.False(t, p.promote("missing"))
	close(r.release)
	for range 3 {
		<-r.started
	}
	p.close(context.Background())
	r.mu.Lock()
	defer r.mu.Unlock()
	require.Equal(t, []string{"first", "c", "a", "b"}, r.ran)
}

// The prefetcher downloads first the objects the reads need soonest, whatever order they
// are asked for in, and a request that needs a queued object sooner moves it up.
func TestPrefetcherDownloadsSoonestFirst(t *testing.T) {
	r := newPrefetchRecorder()
	p := newPrefetcher(1, func() int64 { return 1 << 40 }, func(string) bool { return false }, r.run)
	p.add(ObjectPrefetch{FilePath: "first", Size: 100})
	require.Equal(t, "first", <-r.started)
	p.add(ObjectPrefetch{FilePath: "far", Size: 100, Due: 900})
	p.add(ObjectPrefetch{FilePath: "near", Size: 100, Due: 10})
	p.add(ObjectPrefetch{FilePath: "mid", Size: 100, Due: 400})
	p.add(ObjectPrefetch{FilePath: "far", Size: 100, Due: 5})
	close(r.release)
	for range 3 {
		<-r.started
	}
	p.close(context.Background())
	r.mu.Lock()
	defer r.mu.Unlock()
	require.Equal(t, []string{"first", "far", "near", "mid"}, r.ran, "far was asked for again, due sooner")
}

// A queue at its most bytes makes room for an object needed sooner by dropping the one
// needed last.
func TestPrefetcherFullQueueDropsFarthest(t *testing.T) {
	r := newPrefetchRecorder()
	p := newPrefetcher(0, func() int64 { return 200 }, func(string) bool { return false }, r.run)
	p.add(ObjectPrefetch{FilePath: "a", Size: 100, Due: 100})
	p.add(ObjectPrefetch{FilePath: "b", Size: 100, Due: 300})
	p.add(ObjectPrefetch{FilePath: "c", Size: 100, Due: 200})
	require.True(t, p.isQueued("a"))
	require.False(t, p.isQueued("b"), "the farthest makes room")
	require.True(t, p.isQueued("c"))
	p.add(ObjectPrefetch{FilePath: "d", Size: 100, Due: 500})
	require.False(t, p.isQueued("d"), "the farthest request itself is dropped")
	p.close(context.Background())
}
