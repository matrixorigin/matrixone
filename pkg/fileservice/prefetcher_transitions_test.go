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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type gatedRun struct {
	started chan string
	release map[string]chan struct{}
}

func newGatedRun(paths ...string) *gatedRun {
	g := &gatedRun{started: make(chan string, 64), release: map[string]chan struct{}{}}
	for _, p := range paths {
		g.release[p] = make(chan struct{})
	}
	return g
}

func (g *gatedRun) run(ctx context.Context, req ObjectPrefetch) {
	g.started <- req.FilePath
	if c, ok := g.release[req.FilePath]; ok {
		select {
		case <-c:
		case <-ctx.Done():
		}
	}
}

func (g *gatedRun) releaseAll() {
	for _, c := range g.release {
		select {
		case <-c:
		default:
			close(c)
		}
	}
}

func (p *prefetcher) checkCharges(t *testing.T) {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, q := range []*fetchQueue{&p.prefetch, &p.live} {
		var sum int64
		for _, e := range q.queue {
			sum += e.req.Size
		}
		require.Equal(t, sum, q.bytes)
		require.GreaterOrEqual(t, q.bytes, int64(0))
	}
	var d int64
	for e := range p.deferredOf {
		require.NotNil(t, e.next)
		d += e.next.Size
	}
	require.Equal(t, d, p.deferred)
	require.Equal(t, int64(len(p.entries)), p.known.Load())
}

// wait: a read's join → leave; event: read leaves while scans live.
// A download whose last read left stays in the live queue, ahead of a download a read
// waits for: the waiting read is blocked by a download no read needs.
func TestPrefetcherTransitionLiveQueueOrphanBlocksWaitingRead(t *testing.T) {
	g := newGatedRun("p0", "z", "x", "y")
	p := newPrefetcher(1, func() int64 { return 1 << 40 }, func(ObjectPrefetch) bool { return false }, g.run)
	defer p.close(context.Background())
	defer g.releaseAll()
	p.add(ObjectPrefetch{FilePath: "p0", Size: 1})
	require.Equal(t, "p0", <-g.started)
	scan := &PrefetchScan{}
	for _, path := range []string{"z", "x", "y"} {
		p.add(ObjectPrefetch{FilePath: path, Size: 10, Scan: scan})
	}
	all := func(ObjectPrefetch) bool { return true }
	_, leaveZ, ok := p.join("z", all)
	require.True(t, ok)
	require.Equal(t, "z", <-g.started)
	defer leaveZ()
	_, leaveX, ok := p.join("x", all)
	require.True(t, ok)
	leaveX() // the read of x gives up (its context ended)
	waitingY, leaveY, ok := p.join("y", all)
	require.True(t, ok)
	defer leaveY()

	close(g.release["z"])
	first := <-g.started
	p.checkCharges(t)
	require.Equal(t, "y", first, "the live worker starts x, which no read waits for, while the read of y waits")
	close(g.release["y"])
	<-waitingY
}

// wait: a live-queue entry with no read; event: its scans end.
// A download in the live queue whose reads left and whose scans ended afterwards holds
// queue bytes that a live request needs: the live request is dropped instead.
func TestPrefetcherTransitionDeadLiveQueueEntryHoldsBytes(t *testing.T) {
	g := newGatedRun("p0", "z")
	p := newPrefetcher(1, func() int64 { return 30 }, func(ObjectPrefetch) bool { return false }, g.run)
	defer p.close(context.Background())
	defer g.releaseAll()
	p.add(ObjectPrefetch{FilePath: "p0", Size: 1})
	require.Equal(t, "p0", <-g.started)
	p.add(ObjectPrefetch{FilePath: "z", Size: 1})
	all := func(ObjectPrefetch) bool { return true }
	_, leaveZ, ok := p.join("z", all)
	require.True(t, ok)
	require.Equal(t, "z", <-g.started)
	defer leaveZ()

	sx := &PrefetchScan{}
	p.add(ObjectPrefetch{FilePath: "x", Size: 20, Scan: sx})
	_, leaveX, ok := p.join("x", all)
	require.True(t, ok)
	leaveX() // read gives up, scan still live: x stays in the live queue
	sx.End() // then the scan ends

	p.add(ObjectPrefetch{FilePath: "r", Size: 20, Scan: &PrefetchScan{}})
	p.checkCharges(t)
	require.True(t, p.isQueued("r"), "a live request is dropped while an entry whose scans all ended holds 20 bytes in the live queue")
}

// wait: roomLocked → p.limit(); event: the queued entry's scans end.
// The merge with the entry's request was chosen while its scans were live; when they end
// during admission, the live request is dropped although, replacing the ended request, it
// fits.
func TestPrefetcherTransitionQueuedMergeScanEndsDuringAdmission(t *testing.T) {
	g := newGatedRun("p0")
	var onLimit atomic.Pointer[func()]
	limit := func() int64 {
		if f := onLimit.Swap(nil); f != nil {
			(*f)()
		}
		return 100
	}
	p := newPrefetcher(1, limit, func(ObjectPrefetch) bool { return false }, g.run)
	defer p.close(context.Background())
	defer g.releaseAll()
	p.add(ObjectPrefetch{FilePath: "p0", Size: 1})
	require.Equal(t, "p0", <-g.started)
	sa := &PrefetchScan{}
	p.add(ObjectPrefetch{FilePath: "a", Size: 40, Ranges: []PrefetchRange{{Offset: 0, Size: 40}}, Scan: sa})
	p.add(ObjectPrefetch{FilePath: "b", Size: 50, Scan: &PrefetchScan{}})
	end := sa.End
	onLimit.Store(&end)
	live := &PrefetchScan{}
	p.add(ObjectPrefetch{FilePath: "a", Size: 30, Ranges: []PrefetchRange{{Offset: 100, Size: 30}}, Scan: live})
	p.checkCharges(t)
	p.mu.Lock()
	defer p.mu.Unlock()
	e := p.entries["a"]
	require.NotNil(t, e)
	require.True(t, e.live(), "the queued entry of a holds only ended scans; the live request was dropped")
	require.Equal(t, []PrefetchRange{{Offset: 100, Size: 30}}, e.req.Ranges)
}

// wait: prefetch worker paused for the live queue; event: a delete removes the live-queue entry.
func TestPrefetcherTransitionRemoveLiveEntryResumesPrefetchQueue(t *testing.T) {
	g := newGatedRun("p0", "z", "x")
	p := newPrefetcher(1, func() int64 { return 1 << 40 }, func(ObjectPrefetch) bool { return false }, g.run)
	defer p.close(context.Background())
	defer g.releaseAll()
	p.add(ObjectPrefetch{FilePath: "p0", Size: 1})
	require.Equal(t, "p0", <-g.started)
	for _, path := range []string{"z", "x", "q"} {
		p.add(ObjectPrefetch{FilePath: path, Size: 10})
	}
	all := func(ObjectPrefetch) bool { return true }
	_, leaveZ, ok := p.join("z", all)
	require.True(t, ok)
	require.Equal(t, "z", <-g.started)
	defer leaveZ()
	waitingX, leaveX, ok := p.join("x", all)
	require.True(t, ok)
	defer leaveX()
	close(g.release["p0"])
	require.Eventually(t, func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.prefetch.running == 0
	}, 10*time.Second, time.Millisecond)
	require.True(t, p.isQueued("q"))

	require.True(t, p.remove("x"))
	<-waitingX
	p.checkCharges(t)
	select {
	case path := <-g.started:
		require.Equal(t, "q", path)
	case <-time.After(2 * time.Second):
		t.Fatal("the prefetch queue stays paused after the live queue emptied by a delete")
	}
}

// wait: running download's end → re-add of the deferred request; event: close.
// Nothing is left after close: no entry, no deferred charge.
func TestPrefetcherTransitionCloseWithDeferredLeavesNothing(t *testing.T) {
	g := newGatedRun("a")
	p := newPrefetcher(1, func() int64 { return 1 << 40 }, func(ObjectPrefetch) bool { return false }, g.run)
	p.add(ObjectPrefetch{FilePath: "a", Size: 10, Ranges: []PrefetchRange{{Offset: 0, Size: 10}}})
	require.Equal(t, "a", <-g.started)
	p.add(ObjectPrefetch{FilePath: "a", Size: 10, Ranges: []PrefetchRange{{Offset: 100, Size: 10}}, Scan: &PrefetchScan{}})
	p.add(ObjectPrefetch{FilePath: "b", Size: 10, Scan: &PrefetchScan{}})
	all := func(ObjectPrefetch) bool { return true }
	waiting, leave, ok := p.join("b", all)
	require.True(t, ok)
	defer leave()
	p.close(context.Background())
	<-waiting
	p.checkCharges(t)
	p.mu.Lock()
	defer p.mu.Unlock()
	require.Empty(t, p.entries)
	require.Empty(t, p.deferredOf)
	require.Zero(t, p.deferred)
	require.Zero(t, p.queued.Load())
	require.Zero(t, p.prefetch.running+p.live.running)
}

// Probe (wait: join's leave; event: delete of the joined queued entry). The leave after a
// delete changes no charge and drops nothing else.
func TestPrefetcherTransitionLeaveAfterRemove(t *testing.T) {
	g := newGatedRun("p0", "z")
	p := newPrefetcher(1, func() int64 { return 1 << 40 }, func(ObjectPrefetch) bool { return false }, g.run)
	defer p.close(context.Background())
	defer g.releaseAll()
	p.add(ObjectPrefetch{FilePath: "p0", Size: 1})
	<-g.started
	p.add(ObjectPrefetch{FilePath: "z", Size: 1})
	all := func(ObjectPrefetch) bool { return true }
	_, leaveZ, _ := p.join("z", all)
	<-g.started
	defer leaveZ()
	sx := &PrefetchScan{}
	p.add(ObjectPrefetch{FilePath: "x", Size: 10, Scan: sx})
	waiting, leave, ok := p.join("x", all)
	require.True(t, ok)
	require.True(t, p.remove("x"))
	<-waiting
	sx.End()
	p.add(ObjectPrefetch{FilePath: "x", Size: 10, Scan: &PrefetchScan{}})
	leave()
	p.checkCharges(t)
	require.True(t, p.isQueued("x"), "the old read's leave does not touch the new entry")
}

// wait: have() before the lock; event: queued entry's scans end and the read joins.
// Charges stay consistent across a merge, a join, a leave and a drop.
func TestPrefetcherTransitionChargesAcrossTransitions(t *testing.T) {
	g := newGatedRun("p0")
	var onHave atomic.Pointer[func()]
	have := func(ObjectPrefetch) bool {
		if f := onHave.Swap(nil); f != nil {
			(*f)()
		}
		return false
	}
	p := newPrefetcher(1, func() int64 { return 100 }, have, g.run)
	defer p.close(context.Background())
	defer g.releaseAll()
	p.add(ObjectPrefetch{FilePath: "p0", Size: 1})
	<-g.started
	sa := &PrefetchScan{}
	p.add(ObjectPrefetch{FilePath: "a", Size: 30, Ranges: []PrefetchRange{{Offset: 0, Size: 30}}, Scan: sa})
	f := func() { sa.End() }
	onHave.Store(&f)
	p.add(ObjectPrefetch{FilePath: "a", Size: 20, Ranges: []PrefetchRange{{Offset: 50, Size: 20}}, Scan: &PrefetchScan{}})
	p.checkCharges(t)
	p.mu.Lock()
	require.Equal(t, int64(20), p.entries["a"].req.Size)
	p.mu.Unlock()
	_, leave, ok := p.join("a", func(ObjectPrefetch) bool { return true })
	require.True(t, ok)
	p.checkCharges(t)
	leave()
	p.checkCharges(t)
}

// A prefetch request made before a delete of its object, queued again after it, downloads
// nothing: whole or by ranges.
func TestPrefetchObjectAfterDeleteDownloadsNothing(t *testing.T) {
	for _, ranges := range [][]PrefetchRange{nil, {{Offset: 0, Size: 1 << 10}}} {
		fs, storage, _ := newPrefetchRangeFS(t, 0, 64<<10)
		ctx := context.Background()
		req := ObjectPrefetch{FilePath: "object", Size: 64 << 10, Ranges: ranges, Scan: &PrefetchScan{},
			deletes: fs.diskCache.deletes.now()}
		require.NoError(t, fs.Delete(ctx, "object"))
		reads := storage.reads.Load()
		fs.prefetchObject(ctx, req)
		require.Equal(t, reads, storage.reads.Load(), "ranges %v", ranges)
	}
}
