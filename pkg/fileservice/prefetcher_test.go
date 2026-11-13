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
	"slices"
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

func (r *prefetchRecorder) run(_ context.Context, req ObjectPrefetch) {
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
	p := newPrefetcher(1, func() int64 { return 1 << 40 }, func(req ObjectPrefetch) bool { return have[req.FilePath] }, r.run)
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
	p := newPrefetcher(0, func() int64 { return 250 }, func(ObjectPrefetch) bool { return false }, r.run)
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

// A read that reaches a queued object drops its download.
func TestPrefetcherRemove(t *testing.T) {
	r := newPrefetchRecorder()
	p := newPrefetcher(1, func() int64 { return 1 << 40 }, func(ObjectPrefetch) bool { return false }, r.run)
	p.add(ObjectPrefetch{FilePath: "first", Size: 100})
	require.Equal(t, "first", <-r.started)
	for _, path := range []string{"a", "b", "c"} {
		p.add(ObjectPrefetch{FilePath: path, Size: 100})
	}
	require.True(t, p.remove("b"))
	require.False(t, p.remove("b"), "already dropped")
	require.False(t, p.remove("first"), "a running download is not queued")
	require.False(t, p.remove("missing"))
	require.Equal(t, int64(200), p.prefetch.bytes)
	close(r.release)
	for range 2 {
		<-r.started
	}
	p.close(context.Background())
	r.mu.Lock()
	defer r.mu.Unlock()
	require.Equal(t, []string{"first", "a", "c"}, r.ran)
}

// The prefetcher downloads first the objects the reads need soonest, whatever order they
// are asked for in, and a request that needs a queued object sooner moves it up.
func TestPrefetcherDownloadsSoonestFirst(t *testing.T) {
	r := newPrefetchRecorder()
	p := newPrefetcher(1, func() int64 { return 1 << 40 }, func(ObjectPrefetch) bool { return false }, r.run)
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

// The prefetcher downloads first the objects of which the reads read a larger share, then
// those needed sooner.
func TestPrefetcherDownloadsLargerShareFirst(t *testing.T) {
	r := newPrefetchRecorder()
	p := newPrefetcher(1, func() int64 { return 1 << 40 }, func(ObjectPrefetch) bool { return false }, r.run)
	p.add(ObjectPrefetch{FilePath: "first", Size: 100})
	require.Equal(t, "first", <-r.started)
	p.add(ObjectPrefetch{FilePath: "tenth", Size: 100, Due: 10, Reads: 6, Blocks: 57})
	p.add(ObjectPrefetch{FilePath: "half", Size: 100, Due: 20, Reads: 5, Blocks: 10})
	p.add(ObjectPrefetch{FilePath: "whole", Size: 100, Due: 900, Reads: 65, Blocks: 65})
	p.add(ObjectPrefetch{FilePath: "unknown", Size: 100, Due: 30})
	p.add(ObjectPrefetch{FilePath: "tenth", Size: 100, Due: 40, Reads: 57, Blocks: 57})
	close(r.release)
	for range 4 {
		<-r.started
	}
	p.close(context.Background())
	r.mu.Lock()
	defer r.mu.Unlock()
	require.Equal(t, []string{"first", "tenth", "unknown", "whole", "half"}, r.ran,
		"tenth was asked for again, read whole")
}

// A download not started is dropped once every read it is for ended.
func TestPrefetcherDropsEndedScans(t *testing.T) {
	r := newPrefetchRecorder()
	p := newPrefetcher(1, func() int64 { return 1 << 40 }, func(ObjectPrefetch) bool { return false }, r.run)
	p.add(ObjectPrefetch{FilePath: "first", Size: 100})
	require.Equal(t, "first", <-r.started)
	a, b := new(PrefetchScan), new(PrefetchScan)
	p.add(ObjectPrefetch{FilePath: "ended", Size: 100, Scan: a})
	p.add(ObjectPrefetch{FilePath: "shared", Size: 100, Scan: a})
	p.add(ObjectPrefetch{FilePath: "shared", Size: 100, Scan: b})
	p.add(ObjectPrefetch{FilePath: "live", Size: 100, Scan: b})
	a.End()
	p.add(ObjectPrefetch{FilePath: "late", Size: 100, Scan: a})
	require.False(t, p.isQueued("late"), "a read that ended asks for nothing")
	close(r.release)
	for range 2 {
		<-r.started
	}
	p.close(context.Background())
	r.mu.Lock()
	defer r.mu.Unlock()
	require.Equal(t, []string{"first", "shared", "live"}, r.ran)
}

func TestObjectPrefetchMerge(t *testing.T) {
	got := ObjectPrefetch{Size: 7, Due: 20, Reads: 1, Blocks: 10}.merge(ObjectPrefetch{Size: 7, Due: 30, Reads: 9, Blocks: 10})
	require.Equal(t, ObjectPrefetch{Size: 7, Due: 20, Reads: 9, Blocks: 10}, got)

	id, cat, name := PrefetchRange{Offset: 0, Size: 10}, PrefetchRange{Offset: 100, Size: 20}, PrefetchRange{Offset: 50, Size: 5}
	columns := ObjectPrefetch{Size: 30, Ranges: []PrefetchRange{id, cat}}
	got = columns.merge(ObjectPrefetch{Size: 15, Ranges: []PrefetchRange{id, name}})
	require.Equal(t, []PrefetchRange{id, cat, name}, got.Ranges, "the ranges add up")
	require.Equal(t, int64(35), got.Size)
	require.Equal(t, []PrefetchRange{id, cat}, columns.Ranges)

	got = columns.merge(ObjectPrefetch{Size: 1000})
	require.Nil(t, got.Ranges, "the whole object takes over")
	require.Equal(t, int64(1000), got.Size)
	got = ObjectPrefetch{Size: 1000}.merge(columns)
	require.Nil(t, got.Ranges, "the whole object stays whole")
	require.Equal(t, int64(1000), got.Size)
}

// A request merged into a queued one does not widen it when the cache already satisfies
// the request.
func TestPrefetcherMergeOfCachedRequestKeepsQueuedDownload(t *testing.T) {
	r := newPrefetchRecorder()
	cached := PrefetchRange{Offset: 500, Size: 50}
	p := newPrefetcher(0, func() int64 { return 1 << 40 }, func(req ObjectPrefetch) bool {
		return slices.Equal(req.Ranges, []PrefetchRange{cached})
	}, r.run)
	columns := ObjectPrefetch{FilePath: "a", Size: 10, Ranges: []PrefetchRange{{Offset: 0, Size: 10}}}
	p.add(columns)
	p.add(ObjectPrefetch{FilePath: "a", Size: 50, Ranges: []PrefetchRange{cached}})
	require.Equal(t, columns.Ranges, p.entries["a"].req.Ranges, "the cache satisfies the request")
	require.Equal(t, int64(10), p.prefetch.bytes)
	p.add(ObjectPrefetch{FilePath: "a", Size: 1000})
	require.Nil(t, p.entries["a"].req.Ranges, "the whole object takes over")
	require.Equal(t, int64(1000), p.prefetch.bytes)
	p.close(context.Background())
}

// A request merged into a queued one does not widen it past the queue's bytes.
func TestPrefetcherMergeKeepsQueueBytes(t *testing.T) {
	r := newPrefetchRecorder()
	p := newPrefetcher(0, func() int64 { return 200 }, func(ObjectPrefetch) bool { return false }, r.run)
	columns := ObjectPrefetch{FilePath: "a", Size: 10, Ranges: []PrefetchRange{{Offset: 0, Size: 10}}}
	p.add(columns)
	p.add(ObjectPrefetch{FilePath: "a", Size: 1000, Due: -1})
	got := p.entries["a"].req
	require.Equal(t, columns.Ranges, got.Ranges, "the whole object is past the queue's bytes")
	require.Equal(t, int64(10), got.Size)
	require.Equal(t, int64(-1), got.Due, "it is still needed sooner")
	require.Equal(t, int64(10), p.prefetch.bytes)
	p.add(ObjectPrefetch{FilePath: "a", Size: 100})
	require.Nil(t, p.entries["a"].req.Ranges, "within the queue's bytes the whole object takes over")
	require.Equal(t, int64(100), p.prefetch.bytes)
	p.close(context.Background())
}

// A queue at its most bytes makes room for an object needed sooner by dropping the one
// needed last.
func TestPrefetcherFullQueueDropsFarthest(t *testing.T) {
	r := newPrefetchRecorder()
	p := newPrefetcher(0, func() int64 { return 200 }, func(ObjectPrefetch) bool { return false }, r.run)
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

// Close cancels the running downloads instead of waiting for them to end.
func TestPrefetcherCloseCancelsRunning(t *testing.T) {
	started := make(chan struct{})
	p := newPrefetcher(1, func() int64 { return 1 << 40 }, func(ObjectPrefetch) bool { return false }, func(ctx context.Context, _ ObjectPrefetch) {
		close(started)
		<-ctx.Done()
	})
	p.add(ObjectPrefetch{FilePath: "a", Size: 100})
	<-started
	p.close(context.Background())
}

// A full queue drops a download whose reads all ended before it drops a live one.
func TestPrefetcherFullQueueDropsEndedFirst(t *testing.T) {
	r := newPrefetchRecorder()
	p := newPrefetcher(0, func() int64 { return 200 }, func(ObjectPrefetch) bool { return false }, r.run)
	ended := new(PrefetchScan)
	p.add(ObjectPrefetch{FilePath: "ended", Size: 100, Due: 1, Scan: ended})
	p.add(ObjectPrefetch{FilePath: "live", Size: 100, Due: 2})
	ended.End()
	p.add(ObjectPrefetch{FilePath: "late", Size: 100, Due: 3})
	require.False(t, p.isQueued("ended"))
	require.True(t, p.isQueued("live"))
	require.True(t, p.isQueued("late"))
	p.close(context.Background())
}

// A request larger than the queue holds drops nothing queued.
func TestPrefetcherOversizedRequestDropsNothing(t *testing.T) {
	r := newPrefetchRecorder()
	p := newPrefetcher(0, func() int64 { return 200 }, func(ObjectPrefetch) bool { return false }, r.run)
	p.add(ObjectPrefetch{FilePath: "a", Size: 100, Due: 100})
	p.add(ObjectPrefetch{FilePath: "b", Size: 100, Due: 200})
	p.add(ObjectPrefetch{FilePath: "big", Size: 300, Due: 1})
	require.False(t, p.isQueued("big"))
	require.True(t, p.isQueued("a"))
	require.True(t, p.isQueued("b"))
	p.close(context.Background())
}

// A read joins a download only when it wants its request. One still in the prefetch
// queue moves to the live queue, whose worker starts it while the prefetch worker is
// busy, and the read waits until reads can join it.
func TestPrefetcherJoin(t *testing.T) {
	r := newPrefetchRecorder()
	joinable := make(chan struct{})
	run := func(ctx context.Context, req ObjectPrefetch) {
		r.started <- req.FilePath
		if req.FilePath == "a" {
			<-joinable
			downloadStarted(ctx)
		}
		<-r.release
	}
	p := newPrefetcher(1, func() int64 { return 1 << 40 }, func(ObjectPrefetch) bool { return false }, run)
	p.add(ObjectPrefetch{FilePath: "first", Size: 10})
	require.Equal(t, "first", <-r.started)
	p.add(ObjectPrefetch{FilePath: "a", Size: 10, Ranges: []PrefetchRange{{Offset: 0, Size: 10}}})
	p.add(ObjectPrefetch{FilePath: "b", Size: 20})
	columns := func(req ObjectPrefetch) bool { return len(req.Ranges) > 0 }

	_, _, ok := p.join("a", func(req ObjectPrefetch) bool { return len(req.Ranges) == 0 })
	require.False(t, ok)
	require.True(t, p.isQueued("a"))
	started, _, ok := p.join("a", columns)
	require.True(t, ok)
	require.Equal(t, "a", <-r.started, "a live worker starts it")
	require.False(t, p.isQueued("a"))
	require.Equal(t, int64(20), p.prefetch.bytes)
	select {
	case <-started:
		t.Fatal("joinable before its download says so")
	default:
	}
	close(joinable)
	<-started
	again, _, ok := p.join("a", columns)
	require.True(t, ok, "a running download is joined")
	<-again

	close(r.release)
	require.Equal(t, "b", <-r.started)
	p.close(context.Background())
	_, _, ok = p.join("a", columns)
	require.False(t, ok, "an ended download is not joined")
}

// The prefetch queue starts nothing while a download waits in the live queue.
func TestPrefetcherLiveQueueGoesFirst(t *testing.T) {
	started := make(chan string, 8)
	release := map[string]chan struct{}{}
	for _, path := range []string{"first", "a", "b", "c"} {
		release[path] = make(chan struct{})
	}
	run := func(_ context.Context, req ObjectPrefetch) {
		started <- req.FilePath
		<-release[req.FilePath]
	}
	p := newPrefetcher(1, func() int64 { return 1 << 40 }, func(ObjectPrefetch) bool { return false }, run)
	p.add(ObjectPrefetch{FilePath: "first", Size: 10})
	require.Equal(t, "first", <-started)
	for _, path := range []string{"a", "b", "c"} {
		p.add(ObjectPrefetch{FilePath: path, Size: 10})
	}
	all := func(ObjectPrefetch) bool { return true }
	_, _, ok := p.join("b", all)
	require.True(t, ok)
	require.Equal(t, "b", <-started)
	waiting, _, ok := p.join("c", all)
	require.True(t, ok)

	close(release["first"])
	require.Eventually(t, func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.prefetch.running == 0
	}, 10*time.Second, time.Millisecond)
	require.True(t, p.isQueued("a"), "the free prefetch worker waits for the live queue")

	close(release["b"])
	require.ElementsMatch(t, []string{"c", "a"}, []string{<-started, <-started},
		"the prefetch queue starts once the live queue is empty")
	close(release["c"])
	<-waiting
	close(release["a"])
	p.close(context.Background())
}

// A download in the live queue starts though every read it was asked for ended: a read
// waits for it.
func TestPrefetcherLiveQueueKeepsEndedReads(t *testing.T) {
	started := make(chan string, 8)
	release := make(chan struct{})
	run := func(_ context.Context, req ObjectPrefetch) {
		started <- req.FilePath
		<-release
	}
	p := newPrefetcher(1, func() int64 { return 1 << 40 }, func(ObjectPrefetch) bool { return false }, run)
	scan := new(PrefetchScan)
	p.add(ObjectPrefetch{FilePath: "first", Size: 10})
	require.Equal(t, "first", <-started)
	p.add(ObjectPrefetch{FilePath: "b", Size: 10})
	p.add(ObjectPrefetch{FilePath: "a", Size: 10, Scan: scan})
	all := func(ObjectPrefetch) bool { return true }
	_, _, ok := p.join("b", all)
	require.True(t, ok)
	require.Equal(t, "b", <-started)
	waiting, _, ok := p.join("a", all)
	require.True(t, ok)
	scan.End()

	close(release)
	require.Equal(t, "a", <-started, "the live download starts")
	<-waiting
	p.close(context.Background())
}

// A download in the live queue whose reads all left and whose scans ended is dropped
// before it starts, and the prefetch queue goes on.
func TestPrefetcherLiveQueueDropsLeftReads(t *testing.T) {
	started := make(chan string, 8)
	release := make(chan struct{})
	run := func(_ context.Context, req ObjectPrefetch) {
		started <- req.FilePath
		<-release
	}
	p := newPrefetcher(1, func() int64 { return 1 << 40 }, func(ObjectPrefetch) bool { return false }, run)
	scan := new(PrefetchScan)
	p.add(ObjectPrefetch{FilePath: "first", Size: 10})
	require.Equal(t, "first", <-started)
	p.add(ObjectPrefetch{FilePath: "b", Size: 10})
	p.add(ObjectPrefetch{FilePath: "a", Size: 10, Scan: scan})
	p.add(ObjectPrefetch{FilePath: "c", Size: 10})
	all := func(ObjectPrefetch) bool { return true }
	_, leaveB, ok := p.join("b", all)
	require.True(t, ok)
	require.Equal(t, "b", <-started)
	leaveB()
	waiting, leave, ok := p.join("a", all)
	require.True(t, ok)
	scan.End()
	require.True(t, p.isQueued("a"), "a read still waits for it")
	leave()
	leave()
	select {
	case <-waiting:
	case <-time.After(10 * time.Second):
		t.Fatal("a download no read waits for is still queued")
	}
	require.False(t, p.isQueued("a"), "dropped once its last read left")

	close(release)
	require.Equal(t, "c", <-started, "the prefetch queue goes on")
	p.close(context.Background())
	select {
	case path := <-started:
		t.Fatalf("%s started", path)
	default:
	}
}

// Closing the prefetcher wakes the reads waiting for downloads it drops.
func TestPrefetcherCloseWakesJoins(t *testing.T) {
	r := newPrefetchRecorder()
	p := newPrefetcher(1, func() int64 { return 1 << 40 }, func(ObjectPrefetch) bool { return false }, r.run)
	p.add(ObjectPrefetch{FilePath: "first", Size: 10})
	require.Equal(t, "first", <-r.started)
	p.add(ObjectPrefetch{FilePath: "a", Size: 10})
	p.add(ObjectPrefetch{FilePath: "b", Size: 10})
	all := func(ObjectPrefetch) bool { return true }
	_, _, ok := p.join("a", all)
	require.True(t, ok)
	require.Equal(t, "a", <-r.started)
	waiting, _, ok := p.join("b", all)
	require.True(t, ok)
	closed := make(chan struct{})
	go func() {
		p.close(context.Background())
		close(closed)
	}()
	<-waiting
	close(r.release)
	<-closed
}

// isQueued reports whether the download of filePath is queued and not started.
func (p *prefetcher) isQueued(filePath string) bool {
	if p.queued.Load() == 0 {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	e, ok := p.entries[filePath]
	return ok && e.queue != nil
}
