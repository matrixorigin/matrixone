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
	"cmp"
	"container/heap"
	"context"
	"io"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	metric "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"go.uber.org/zap"
)

// ObjectPrefetch asks to download, ahead of the read, the object at FilePath, of Size
// bytes, which the read reaches at Due, in Unix nanoseconds, and of whose Blocks blocks
// it reads Reads, 0 when unknown. The download is dropped if Scan ends before it starts.
// With Ranges, only those ranges of the object, of Size bytes in all, are downloaded.
type ObjectPrefetch struct {
	FilePath string
	Size     int64
	Due      int64
	Reads    int
	Blocks   int
	Ranges   []PrefetchRange
	Scan     *PrefetchScan
	// deletes is the disk cache's count of deleted objects when the request was made.
	deletes uint64
	// indexObject publishes the download of the whole object into the index-file cache.
	indexObject bool
}

// PrefetchRange is a range of an object.
type PrefetchRange struct {
	Offset int64
	Size   int64
}

// PrefetchScan is the read prefetches are for. A nil PrefetchScan never ends.
type PrefetchScan struct {
	ended atomic.Bool
}

// End ends the read: its downloads not started are dropped.
func (s *PrefetchScan) End() {
	if s != nil {
		s.ended.Store(true)
	}
}

func (s *PrefetchScan) live() bool {
	return s == nil || !s.ended.Load()
}

// prefetchKey identifies a prefetch: its object, and whether it fills the index-file cache
// rather than the disk cache. The prefetches of an object into the two caches are apart:
// one never merges with, covers or takes the place of the other.
type prefetchKey struct {
	path  string
	index bool
}

func (r ObjectPrefetch) key() prefetchKey { return prefetchKey{path: r.FilePath, index: r.indexObject} }

// merge returns the prefetch of the object for both r and o, of one key: the sooner Due
// and the larger share.
// A request of the whole object takes over one of ranges, and ranges add up.
func (r ObjectPrefetch) merge(o ObjectPrefetch) ObjectPrefetch {
	r.Due = min(r.Due, o.Due)
	r.deletes = min(r.deletes, o.deletes)
	if o.share() > r.share() {
		r.Reads, r.Blocks = o.Reads, o.Blocks
	}
	if len(r.Ranges) > 0 && len(o.Ranges) > 0 {
		r.Ranges, r.Size = unionRanges(r.Ranges, o.Ranges)
		return r
	}
	if len(r.Ranges) > 0 {
		r.Size = o.Size
	}
	r.Ranges = nil
	return r
}

// need returns what of o, of the same key, the download of r does not download, and
// false when it downloads all of it: o whole when o asks for the whole object and r for
// ranges, otherwise the bytes of the ranges of o that no range of r holds, of their size.
func (r ObjectPrefetch) need(o ObjectPrefetch) (ObjectPrefetch, bool) {
	if len(r.Ranges) == 0 {
		return ObjectPrefetch{}, false
	}
	if len(o.Ranges) == 0 {
		return o, true
	}
	held, _ := unionRanges(r.Ranges, nil)
	var left []PrefetchRange
	var size int64
	for _, x := range o.Ranges {
		pos, end := x.Offset, x.Offset+x.Size
		for _, y := range held {
			if y.Offset+y.Size <= pos || y.Offset >= end {
				continue
			}
			if y.Offset > pos {
				left = append(left, PrefetchRange{Offset: pos, Size: y.Offset - pos})
				size += y.Offset - pos
			}
			pos = max(pos, y.Offset+y.Size)
		}
		if pos < end {
			left = append(left, PrefetchRange{Offset: pos, Size: end - pos})
			size += end - pos
		}
	}
	if len(left) == 0 {
		return ObjectPrefetch{}, false
	}
	o.Ranges, o.Size = left, size
	return o, true
}

// covers reports whether the download of r downloads every byte o, of the same key, asks
// for.
func (r ObjectPrefetch) covers(o ObjectPrefetch) bool {
	_, needed := r.need(o)
	return !needed
}

// unionRanges returns the ranges holding the bytes of a and of b, in order, those that
// overlap made one, and their bytes, each counted once.
func unionRanges(a, b []PrefetchRange) ([]PrefetchRange, int64) {
	all := slices.Concat(a, b)
	slices.SortFunc(all, func(x, y PrefetchRange) int { return cmp.Compare(x.Offset, y.Offset) })
	out := all[:0]
	var size int64
	for _, r := range all {
		if n := len(out); n > 0 && r.Offset < out[n-1].Offset+out[n-1].Size {
			last := &out[n-1]
			end := max(last.Offset+last.Size, r.Offset+r.Size)
			size += end - (last.Offset + last.Size)
			last.Size = end - last.Offset
			continue
		}
		out = append(out, r)
		size += r.Size
	}
	return out, size
}

// share returns, in tenths, the share of the object's blocks the read reads.
func (r ObjectPrefetch) share() int {
	if r.Blocks <= 0 || r.Reads <= 0 || r.Reads >= r.Blocks {
		return 10
	}
	return r.Reads * 10 / r.Blocks
}

// ObjectPrefetcher is a file service that downloads objects ahead of the reads that need
// them.
type ObjectPrefetcher interface {
	// PrefetchObject queues req and returns without waiting for it.
	PrefetchObject(ctx context.Context, req ObjectPrefetch) error
}

var _ ObjectPrefetcher = new(S3FS)

// prefetchEntry is an object queued, or being downloaded, by a prefetcher.
type prefetchEntry struct {
	req   ObjectPrefetch
	scans []*PrefetchScan
	seq   uint64
	queue *fetchQueue // the queue it waits in, nil once its download started
	index int         // in queue
	// started is closed once reads can join the download, or it ended.
	started chan struct{}
	once    sync.Once
	// joiners counts the reads that wait for the download to start.
	joiners int
	// next is what requests for the object asked for that its running download does not
	// download, with their scans: it is queued once the download ends.
	next      *ObjectPrefetch
	nextScans []*PrefetchScan
}

// start marks the download joinable.
func (e *prefetchEntry) start() {
	e.once.Do(func() { close(e.started) })
}

// live reports whether a read the download is for has not ended.
func (e *prefetchEntry) live() bool {
	if e.joiners > 0 {
		return true
	}
	for _, s := range e.scans {
		if s.live() {
			return true
		}
	}
	return false
}

func (e *prefetchEntry) addScan(s *PrefetchScan) {
	e.scans = slices.DeleteFunc(e.scans, func(s *PrefetchScan) bool { return !s.live() })
	if !slices.Contains(e.scans, s) {
		e.scans = append(e.scans, s)
	}
}

// before reports whether e is downloaded before o: the read reads a larger share of it,
// or a read reaches it sooner, or as soon and it was asked for first.
func (e *prefetchEntry) before(o *prefetchEntry) bool {
	if a, b := e.req.share(), o.req.share(); a != b {
		return a > b
	}
	if e.req.Due != o.req.Due {
		return e.req.Due < o.req.Due
	}
	return e.seq < o.seq
}

type prefetchQueue []*prefetchEntry

func (q prefetchQueue) Len() int           { return len(q) }
func (q prefetchQueue) Less(i, j int) bool { return q[i].before(q[j]) }
func (q prefetchQueue) Swap(i, j int) {
	q[i], q[j] = q[j], q[i]
	q[i].index, q[j].index = i, j
}
func (q *prefetchQueue) Push(x any) {
	e := x.(*prefetchEntry)
	e.index = len(*q)
	*q = append(*q, e)
}
func (q *prefetchQueue) Pop() any {
	old := *q
	e := old[len(old)-1]
	old[len(old)-1] = nil
	*q = old[:len(old)-1]
	e.index = -1
	return e
}

// fetchQueue is a queue of downloads and its workers, which run at most max of them at
// once. A download whose reads all ended is dropped when it would start.
type fetchQueue struct {
	queue   prefetchQueue
	bytes   int64
	max     int
	running int
}

func (q *fetchQueue) push(e *prefetchEntry) {
	heap.Push(&q.queue, e)
	e.queue = q
	q.bytes += e.req.Size
}

func (q *fetchQueue) remove(e *prefetchEntry) {
	heap.Remove(&q.queue, e.index)
	e.queue = nil
	q.bytes -= e.req.Size
}

// prefetcher downloads the objects queued for it, those the reads reach soonest first,
// whatever order they are asked for in. A read that reaches an object whose download
// waits moves it to the live queue, whose workers run before the prefetch queue's start
// more. An object is queued once, and never while it is being downloaded or cached.
type prefetcher struct {
	run    func(context.Context, ObjectPrefetch)
	ctx    context.Context
	cancel context.CancelFunc
	have   func(ObjectPrefetch) bool // what the request downloads is cached or being downloaded
	limit  func() int64              // the most bytes queued
	queued atomic.Int64
	known  atomic.Int64 // entries

	mu       sync.Mutex
	prefetch fetchQueue // downloads ahead of the reads
	live     fetchQueue // downloads a read waits for
	entries  map[prefetchKey]*prefetchEntry
	deferred int64 // bytes of the requests deferred behind running downloads
	// deferredOf holds the running downloads with a request deferred behind them.
	deferredOf map[*prefetchEntry]struct{}
	seq        uint64
	closed     bool
	wg         sync.WaitGroup
}

func newPrefetcher(workers int, limit func() int64, have func(ObjectPrefetch) bool, run func(context.Context, ObjectPrefetch)) *prefetcher {
	ctx, cancel := context.WithCancel(context.Background())
	return &prefetcher{
		run: run, ctx: ctx, cancel: cancel, have: have, limit: limit,
		prefetch:   fetchQueue{max: workers},
		live:       fetchQueue{max: workers},
		entries:    make(map[prefetchKey]*prefetchEntry),
		deferredOf: make(map[*prefetchEntry]struct{}),
	}
}

// add queues req, or moves the queued object nearer the front when req needs it sooner.
func (p *prefetcher) add(req ObjectPrefetch) {
	if !req.Scan.live() {
		return
	}
	have := p.have(req)
	p.mu.Lock()
	defer p.mu.Unlock()
	p.addLocked(req, have)
}

// addLocked is add of req, of which have reports whether it is cached or downloading.
func (p *prefetcher) addLocked(req ObjectPrefetch, have bool) {
	if p.closed {
		return
	}
	if e, ok := p.entries[req.key()]; ok {
		if e.queue != nil && have {
			metric.FSPrefetchCachedCounter.Inc()
			return
		}
		metric.FSPrefetchMergedCounter.Inc()
		if needed, ok := e.req.need(req); e.queue == nil && ok {
			// The running download does not download what req asks for: what it does not is
			// queued once the download ends. Past the queues' bytes, it is not kept. What
			// scans that all ended asked for is dropped, not merged into req.
			req := needed
			var next ObjectPrefetch
			var grow int64
			for {
				if e.next != nil && !slices.ContainsFunc(e.nextScans, (*PrefetchScan).live) {
					p.takeDeferredLocked(e)
					metric.FSPrefetchEndedCounter.Inc()
				}
				before := e.next
				next, grow = req, req.Size
				if before != nil {
					next = before.merge(req)
					grow = next.Size - before.Size
				}
				if grow <= 0 {
					break
				}
				if !p.roomLocked(grow, nil) {
					metric.FSPrefetchDroppedCounter.Inc()
					return
				}
				// Making room may have dropped what was deferred behind e.
				if e.next == before {
					break
				}
			}
			p.deferred += grow
			p.deferredOf[e] = struct{}{}
			e.next = &next
			e.nextScans = slices.DeleteFunc(e.nextScans, func(s *PrefetchScan) bool { return !s.live() })
			if !slices.Contains(e.nextScans, req.Scan) {
				e.nextScans = append(e.nextScans, req.Scan)
			}
			return
		}
		if q := e.queue; q != nil {
			// What scans that all ended asked for is replaced by req, not merged into it.
			var merged ObjectPrefetch
			for {
				live := e.live()
				merged = req
				if live {
					merged = e.req.merge(req)
				}
				if grow := merged.Size - e.req.Size; grow <= 0 || p.roomLocked(grow, e) {
					break
				}
				// Making room may have ended the scans of e.
				if e.live() != live {
					continue
				}
				// Past the queues' bytes, a queued download in use keeps what it downloads,
				// needed as soon as req; one whose reads all ended is not kept by req.
				metric.FSPrefetchDroppedCounter.Inc()
				if !live {
					return
				}
				merged.Size, merged.Ranges = e.req.Size, e.req.Ranges
				break
			}
			e.addScan(req.Scan)
			q.bytes += merged.Size - e.req.Size
			e.req = merged
			heap.Fix(&q.queue, e.index)
			return
		}
		e.addScan(req.Scan)
		return
	}
	if have {
		metric.FSPrefetchCachedCounter.Inc()
		return
	}
	p.seq++
	e := &prefetchEntry{req: req, scans: []*PrefetchScan{req.Scan}, seq: p.seq, started: make(chan struct{})}
	limit := p.limit()
	if req.Size > limit {
		metric.FSPrefetchDroppedCounter.Inc()
		return
	}
	// Room is made from the requests of ended scans deferred behind running downloads, then
	// in the prefetch queue: a read waits for each download of the live queue.
	if p.queuedBytesLocked()+req.Size > limit {
		p.reclaimEndedDeferredLocked()
	}
	for p.queuedBytesLocked()+req.Size > limit {
		q := p.prefetch.queue
		// An entry whose scans ended goes first, then the last in download order.
		last := -1
		for i := range q {
			if !q[i].live() {
				last = i
				break
			}
			if last < 0 || q[last].before(q[i]) {
				last = i
			}
		}
		if last < 0 || q[last].live() && !e.before(q[last]) {
			if p.reclaimEndedDeferredLocked() {
				continue
			}
			metric.FSPrefetchDroppedCounter.Inc()
			return
		}
		p.dropLocked(q[last])
	}
	p.prefetch.push(e)
	metric.FSPrefetchQueuedCounter.Inc()
	p.entries[req.key()] = e
	p.known.Add(1)
	p.queued.Add(1)
	p.startLocked()
}

// queuedBytesLocked returns the bytes of the downloads both queues hold and of the
// requests deferred behind running downloads.
func (p *prefetcher) queuedBytesLocked() int64 {
	return p.prefetch.bytes + p.live.bytes + p.deferred
}

// roomLocked reports whether need more bytes fit in the queues, after dropping as many as
// it takes of the requests deferred behind running downloads whose scans all ended, then
// of the downloads in the prefetch queue whose reads all ended, other than keep.
func (p *prefetcher) roomLocked(need int64, keep *prefetchEntry) bool {
	limit := p.limit()
	if p.queuedBytesLocked()+need <= limit {
		return true
	}
	// Nothing is dropped unless dropping makes room.
	free := int64(0)
	for e := range p.deferredOf {
		if e.next != nil && !slices.ContainsFunc(e.nextScans, (*PrefetchScan).live) {
			free += e.next.Size
		}
	}
	for _, o := range p.prefetch.queue {
		if o != keep && !o.live() {
			free += o.req.Size
		}
	}
	if p.queuedBytesLocked()-free+need > limit {
		return false
	}
	p.reclaimEndedDeferredLocked()
	for p.queuedBytesLocked()+need > limit {
		i := slices.IndexFunc(p.prefetch.queue, func(o *prefetchEntry) bool { return o != keep && !o.live() })
		if i < 0 {
			return false
		}
		p.dropLocked(p.prefetch.queue[i])
	}
	return true
}

// reclaimEndedDeferredLocked drops the requests deferred behind running downloads whose
// scans all ended, and reports whether it dropped any.
func (p *prefetcher) reclaimEndedDeferredLocked() bool {
	if p.deferred == 0 {
		return false
	}
	reclaimed := false
	for e := range p.deferredOf {
		if slices.ContainsFunc(e.nextScans, (*PrefetchScan).live) {
			continue
		}
		p.takeDeferredLocked(e)
		metric.FSPrefetchEndedCounter.Inc()
		reclaimed = true
	}
	return reclaimed
}

// takeDeferredLocked returns the request deferred behind e and its scans, and clears them.
func (p *prefetcher) takeDeferredLocked(e *prefetchEntry) (*ObjectPrefetch, []*PrefetchScan) {
	next, scans := e.next, e.nextScans
	if next != nil {
		p.deferred -= next.Size
	}
	delete(p.deferredOf, e)
	e.next, e.nextScans = nil, nil
	return next, scans
}

func (p *prefetcher) forgetLocked(e *prefetchEntry) {
	delete(p.entries, e.req.key())
	p.known.Add(-1)
}

// removeLocked removes the queued download e, which no read waits for then.
func (p *prefetcher) removeLocked(e *prefetchEntry) {
	e.queue.remove(e)
	p.forgetLocked(e)
	p.queued.Add(-1)
	e.start()
}

// dropLocked drops the queued download e.
func (p *prefetcher) dropLocked(e *prefetchEntry) {
	live := e.live()
	p.removeLocked(e)
	if live {
		metric.FSPrefetchDroppedCounter.Inc()
	} else {
		metric.FSPrefetchEndedCounter.Inc()
	}
}

// startLocked starts the downloads the workers have room for, those of the live queue
// first; the prefetch queue starts none while the live queue waits.
func (p *prefetcher) startLocked() {
	p.startQueueLocked(&p.live)
	if len(p.live.queue) == 0 {
		p.startQueueLocked(&p.prefetch)
	}
}

func (p *prefetcher) startQueueLocked(q *fetchQueue) {
	for q.running < q.max && len(q.queue) > 0 {
		e := q.queue[0]
		q.remove(e)
		p.queued.Add(-1)
		if !e.live() {
			p.forgetLocked(e)
			e.start()
			metric.FSPrefetchEndedCounter.Inc()
			continue
		}
		q.running++
		p.wg.Add(1)
		go func() {
			defer func() {
				// The deferred request is queued, joinable, as the download ends, and dropped
				// once found cached, before the worker takes its next download.
				p.mu.Lock()
				p.forgetLocked(e)
				e.start()
				next, scans := p.takeDeferredLocked(e)
				var deferred *prefetchEntry
				for _, s := range scans {
					req := *next
					req.Scan = s
					p.addLocked(req, false)
				}
				if next != nil {
					deferred = p.entries[next.key()]
				}
				p.mu.Unlock()
				if deferred != nil && p.have(*next) {
					p.mu.Lock()
					// Unless it started meanwhile, or asks for bytes the check did not find.
					if p.entries[next.key()] == deferred && deferred.queue != nil && next.covers(deferred.req) {
						metric.FSPrefetchCachedCounter.Inc()
						p.removeLocked(deferred)
					}
					p.mu.Unlock()
				}
				p.mu.Lock()
				q.running--
				p.startLocked()
				p.mu.Unlock()
				p.wg.Done()
			}()
			p.run(withDownloadStarted(p.ctx, e.start), e.req)
		}()
	}
}

// entriesLocked returns the prefetches of filePath, into the disk cache and the index-file
// cache.
func (p *prefetcher) entriesLocked(filePath string) []*prefetchEntry {
	var out []*prefetchEntry
	for _, index := range []bool{false, true} {
		if e, ok := p.entries[prefetchKey{path: filePath, index: index}]; ok {
			out = append(out, e)
		}
	}
	return out
}

// isRunning reports whether a worker took a download of filePath.
func (p *prefetcher) isRunning(filePath string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.ContainsFunc(p.entriesLocked(filePath), func(e *prefetchEntry) bool { return e.queue == nil })
}

// remove drops the queued downloads of filePath, and the requests deferred behind its
// running downloads, and reports whether a download was queued.
func (p *prefetcher) remove(filePath string) bool {
	if p.known.Load() == 0 {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	queued := false
	for _, e := range p.entriesLocked(filePath) {
		if e.queue == nil {
			// The running download's deferred request is not queued after it ends.
			p.takeDeferredLocked(e)
			continue
		}
		p.removeLocked(e)
		queued = true
	}
	if queued {
		p.startLocked()
	}
	return queued
}

// join returns, when want accepts the request of a download of filePath, a channel
// closed once reads can join the download, or it ended, and leave, to call once when the
// read stops waiting. A download still waiting in the prefetch queue moves to the live
// queue. When its last read leaves, a queued download is dropped if its scans have ended,
// and otherwise goes back to the prefetch queue.
func (p *prefetcher) join(filePath string, want func(ObjectPrefetch) bool) (started <-chan struct{}, leave func(), ok bool) {
	if p.known.Load() == 0 {
		return nil, nil, false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	i := slices.IndexFunc(p.entriesLocked(filePath), func(e *prefetchEntry) bool { return want(e.req) })
	if i < 0 {
		return nil, nil, false
	}
	e := p.entriesLocked(filePath)[i]
	if e.queue == &p.prefetch {
		metric.FSPrefetchMissedCounter.Inc()
		p.prefetch.remove(e)
		p.live.push(e)
	}
	e.joiners++
	p.startLocked()
	var once sync.Once
	return e.started, func() {
		once.Do(func() {
			p.mu.Lock()
			defer p.mu.Unlock()
			e.joiners--
			if e.queue == nil || e.joiners > 0 {
				return
			}
			// A download no read waits for leaves the live queue.
			if !e.live() {
				p.dropLocked(e)
			} else if e.queue == &p.live {
				p.live.remove(e)
				p.prefetch.push(e)
			}
			p.startLocked()
		})
	}, true
}

// close drops the queued downloads, cancels the running ones and waits for them, or ctx.
func (p *prefetcher) close(ctx context.Context) {
	p.mu.Lock()
	p.closed = true
	p.cancel()
	for _, q := range []*fetchQueue{&p.prefetch, &p.live} {
		for len(q.queue) > 0 {
			p.removeLocked(q.queue[0])
		}
	}
	p.mu.Unlock()
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
	}
}

type downloadStartedKey struct{}

// withDownloadStarted returns ctx, through which a download calls started once reads
// can join it.
func withDownloadStarted(ctx context.Context, started func()) context.Context {
	return context.WithValue(ctx, downloadStartedKey{}, started)
}

// isDownload reports whether ctx is that of a prefetcher's download.
func isDownload(ctx context.Context) bool {
	_, ok := ctx.Value(downloadStartedKey{}).(func())
	return ok
}

// downloadStarted tells the prefetcher whose download ctx is for that reads can join it.
func downloadStarted(ctx context.Context) {
	if started, ok := ctx.Value(downloadStartedKey{}).(func()); ok {
		started()
	}
}

func (s *S3FS) PrefetchObject(ctx context.Context, req ObjectPrefetch) error {
	// The prefetcher keys an object by its path at the service, whatever its spelling.
	path, err := parseFilePathAtService(req.FilePath, s.name)
	if err != nil {
		return nil
	}
	req.FilePath = path.File
	// An object the index-file cache holds is not downloaded into the disk cache, nor one
	// that cannot fill it.
	if policy := GetFileServicePolicy(ctx); s.indexObject(policy) {
		if s.indexObjectReadable(policy) && s.indexObjectHeld(req.FilePath) {
			return nil
		}
		// No cache holds the ranges of an index object.
		if len(req.Ranges) > 0 || !s.indexObjectFillable(policy) {
			return nil
		}
		req.indexObject = true
	}
	if s.prefetcher != nil && req.Size > 0 {
		req.deletes = s.diskCache.deletes.now()
		s.prefetcher.add(req)
	}
	return nil
}

// prefetchQueueLimit is the most bytes queued for download: half the disk cache keeps.
func (s *S3FS) prefetchQueueLimit() func() int64 {
	return func() int64 { return s.diskCache.capacityFunc() / 2 }
}

// haveObject reports whether the whole object of req is cached or being downloaded, or
// whether cached ranges cover the ranges req needs. A request of the whole object marks a
// cached one used.
func (s *S3FS) haveObject(req ObjectPrefetch) bool {
	path, err := parseFilePathAtService(req.FilePath, s.name)
	if err != nil {
		return true
	}
	// A download into the index-file cache is had only once that cache holds the object.
	if req.indexObject && s.cachesIndexObjects() {
		return s.indexObjectHeld(req.FilePath)
	}
	// A fill of the index-file cache does not fill the disk cache.
	diskPath := s.diskCache.pathForFile(path.File)
	if s.diskCache.wholeFill(diskPath, false) != nil {
		return true
	}
	ctx := context.Background()
	if len(req.Ranges) == 0 {
		_, ok := s.diskCache.cache.Get(ctx, diskPath)
		return ok && s.diskCache.cachedFileExists(ctx, diskPath)
	}
	if s.diskCache.cache.Contains(diskPath) && s.diskCache.cachedFileExists(ctx, diskPath) {
		return true
	}
	// Every range is checked, so the record of a missing file is dropped before the
	// download decides which ranges to fetch.
	key := s.diskCache.rangeObjectKey(path.File)
	size, sized := s.diskCache.prefetched.size(diskPath)
	checked := make(map[string]struct{})
	have := true
	for _, r := range req.Ranges {
		if sized && r.Offset < size && size < r.Offset+r.Size {
			// A range ends at the object's known end, not past it.
			r.Size = size - r.Offset
		}
		rs, ok := s.diskCache.ranges.covering(key, r.Offset, r.Size)
		if !ok {
			have = false
			continue
		}
		for _, c := range rs {
			if _, ok := checked[c.diskPath]; ok {
				continue
			}
			checked[c.diskPath] = struct{}{}
			if !s.diskCache.cachedFileExists(ctx, c.diskPath) {
				have = false
			}
		}
	}
	return have
}

// prefetchObject downloads the whole object of req at its known size, or its ranges.
func (s *S3FS) prefetchObject(ctx context.Context, req ObjectPrefetch) {
	path, err := parseFilePathAtService(req.FilePath, s.name)
	if err != nil {
		return
	}
	// A request queued again after its object was deleted downloads nothing.
	objectPath := s.diskCache.pathForFile(path.File)
	if s.diskCache.deletes.since(objectPath, req.deletes) {
		metric.FSPrefetchEndedCounter.Inc()
		return
	}
	if closing := s.diskCache.async.ctx; closing != nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithCancel(ctx)
		defer cancel()
		defer context.AfterFunc(closing, cancel)()
	}
	if len(req.Ranges) > 0 {
		s.prefetchRanges(ctx, req)
		return
	}
	// A size told before an object was deleted may be another object's at that path.
	s.diskCache.storePrefetchedSize(objectPath, req.Size, req.deletes)
	ctx = withDeletesMark(ctx, req.deletes)
	if req.indexObject {
		ctx = WithFileServicePolicy(ctx, GetFileServicePolicy(ctx)|IndexFileObject)
	}
	s.countPrefetch(req, s.PrefetchFile(ctx, req.FilePath))
}

// countPrefetch counts the outcome of the prefetch of req that ended with err: a prefetch
// of an object deleted since it was asked for ended.
func (s *S3FS) countPrefetch(req ObjectPrefetch, err error) {
	switch {
	case err == nil:
		metric.FSPrefetchDownloadedCounter.Inc()
	case s.diskCache.deletes.since(s.diskCache.pathForFile(req.FilePath), req.deletes):
		metric.FSPrefetchEndedCounter.Inc()
	default:
		metric.FSPrefetchFailedCounter.Inc()
		logutil.Debug("fileservice: prefetch object", zap.String("path", req.FilePath), zap.Error(err))
	}
}

// prefetchRanges reads into the disk cache the ranges of req not cached, those that are
// adjacent in one read each, so the bytes between them are not read.
func (s *S3FS) prefetchRanges(ctx context.Context, req ObjectPrefetch) {
	ranges := s.rangesToDownload(req)
	if path, err := parseFilePathAtService(req.FilePath, s.name); err == nil && len(ranges) > 0 {
		key := s.pathToKey(path.File)
		open := func(ctx context.Context, start, end int64) (io.ReadCloser, error) {
			return s.storage.Read(ctx, key, &start, &end)
		}
		// Reads of the ranges join their flights instead of reading their bytes again.
		if ok, err := s.diskCache.readRanges(ctx, path.File, req.deletes, SkipMemoryCacheWrites|SkipFullFilePreloads|ColumnPrefetch, ranges, open); ok {
			s.countPrefetch(req, err)
			return
		}
	}
	for _, r := range ranges {
		vector := &IOVector{
			FilePath: req.FilePath,
			Entries:  []IOEntry{{Offset: r.Offset, Size: r.Size}},
			Policy:   SkipMemoryCacheWrites | SkipFullFilePreloads | ColumnPrefetch,
		}
		err := s.Read(ctx, vector)
		vector.Release()
		if err != nil {
			s.countPrefetch(req, err)
			return
		}
	}
	s.countPrefetch(req, nil)
}

// rangesToDownload returns the ranges of req not covered by cached ranges, in order, with
// adjacent ones joined.
func (s *S3FS) rangesToDownload(req ObjectPrefetch) []PrefetchRange {
	var key string
	if path, err := parseFilePathAtService(req.FilePath, s.name); err == nil {
		key = s.diskCache.rangeObjectKey(path.File)
	}
	size, sized := int64(0), false
	if key != "" {
		size, sized = s.diskCache.prefetched.size(s.diskCache.pathForFile(req.FilePath))
	}
	out := make([]PrefetchRange, 0, len(req.Ranges))
	for _, r := range req.Ranges {
		if sized && r.Offset < size && size < r.Offset+r.Size {
			// A range ends at the object's known end, not past it.
			r.Size = size - r.Offset
		}
		if key != "" {
			if _, ok := s.diskCache.ranges.covering(key, r.Offset, r.Size); ok {
				continue
			}
		}
		out = append(out, r)
	}
	slices.SortFunc(out, func(a, b PrefetchRange) int { return cmp.Compare(a.Offset, b.Offset) })
	joined := out[:0]
	for _, r := range out {
		if n := len(joined); n > 0 && joined[n-1].Offset+joined[n-1].Size == r.Offset {
			joined[n-1].Size += r.Size
			continue
		}
		joined = append(joined, r)
	}
	return joined
}
