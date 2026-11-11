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
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	metric "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"go.uber.org/zap"
)

// ObjectPrefetch asks to download, ahead of the read, the object at FilePath, of Size
// bytes, which the read reaches at Due, in Unix nanoseconds, and of whose Blocks blocks
// it reads Reads, 0 when unknown. The download is dropped if Scan ends before it starts.
// With Ranges, only those ranges of the object, of Size bytes in all, are downloaded.
// A request of the whole object with Needs is not downloaded while cached ranges cover
// Needs.
type ObjectPrefetch struct {
	FilePath string
	Size     int64
	Due      int64
	Reads    int
	Blocks   int
	Ranges   []PrefetchRange
	Needs    []PrefetchRange
	Scan     *PrefetchScan
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

// merge returns the prefetch of the object for both r and o: the sooner Due and the
// larger share.
// A request of the whole object takes over one of ranges, and ranges add up. The whole
// object has Needs only when both requests are satisfied by cached ranges.
func (r ObjectPrefetch) merge(o ObjectPrefetch) ObjectPrefetch {
	r.Due = min(r.Due, o.Due)
	if o.share() > r.share() {
		r.Reads, r.Blocks = o.Reads, o.Blocks
	}
	if len(r.Ranges) > 0 && len(o.Ranges) > 0 {
		r.Ranges, r.Size = unionRanges(r.Ranges, o.Ranges, r.Size)
		return r
	}
	var needs []PrefetchRange
	if rn, on := r.needed(), o.needed(); len(rn) > 0 && len(on) > 0 {
		needs, _ = unionRanges(rn, on, 0)
	}
	if len(r.Ranges) > 0 {
		r.Size = o.Size
	}
	r.Ranges, r.Needs = nil, needs
	return r
}

// unionRanges returns a copy of a with the ranges of b not in it, and size plus their bytes.
func unionRanges(a, b []PrefetchRange, size int64) ([]PrefetchRange, int64) {
	a = slices.Clone(a)
	for _, x := range b {
		if !slices.Contains(a, x) {
			a = append(a, x)
			size += x.Size
		}
	}
	return a, size
}

// needed returns the ranges that, cached, satisfy r.
func (r ObjectPrefetch) needed() []PrefetchRange {
	if len(r.Ranges) > 0 {
		return r.Ranges
	}
	return r.Needs
}

// share returns, in tenths, the share of the object's blocks the read reads.
func (r ObjectPrefetch) share() int {
	if r.Blocks <= 0 || r.Reads >= r.Blocks {
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

// prefetchHorizon is how far ahead of the reads prefetches are queued: no more bytes
// than the downloads write in that time.
const prefetchHorizon = 30 * time.Second

// prefetchEntry is an object queued, or being downloaded, by a prefetcher.
type prefetchEntry struct {
	req   ObjectPrefetch
	scans []*PrefetchScan
	seq   uint64
	index int // in the queue, or -1 once its download started
}

// live reports whether a read the download is for has not ended.
func (e *prefetchEntry) live() bool {
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

// prefetcher downloads the objects queued for it, a bounded number at once, those the
// reads reach soonest first, whatever order they are asked for in. An object is queued
// once, and never while it is being downloaded or cached.
type prefetcher struct {
	run    func(context.Context, ObjectPrefetch)
	ctx    context.Context
	cancel context.CancelFunc
	have   func(ObjectPrefetch) bool // what the request downloads is cached or being downloaded
	limit  func() int64              // the most bytes queued
	max    int
	queued atomic.Int64

	mu          sync.Mutex
	queue       prefetchQueue
	queuedBytes int64
	entries     map[string]*prefetchEntry
	seq         uint64
	running     int
	closed      bool
	wg          sync.WaitGroup
}

func newPrefetcher(max int, limit func() int64, have func(ObjectPrefetch) bool, run func(context.Context, ObjectPrefetch)) *prefetcher {
	ctx, cancel := context.WithCancel(context.Background())
	return &prefetcher{run: run, ctx: ctx, cancel: cancel, have: have, limit: limit, max: max, entries: make(map[string]*prefetchEntry)}
}

// add queues req, or moves the queued object nearer the front when req needs it sooner.
func (p *prefetcher) add(req ObjectPrefetch) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	if !req.Scan.live() {
		return
	}
	if e, ok := p.entries[req.FilePath]; ok {
		if e.index >= 0 && p.have(req) {
			metric.FSPrefetchCachedCounter.Inc()
			return
		}
		e.addScan(req.Scan)
		metric.FSPrefetchMergedCounter.Inc()
		if e.index >= 0 {
			merged := e.req.merge(req)
			if grow := merged.Size - e.req.Size; grow > 0 && p.queuedBytes+grow > p.limit() {
				// Past the queue's bytes, the queued download keeps what it downloads.
				merged.Size, merged.Ranges, merged.Needs = e.req.Size, e.req.Ranges, e.req.Needs
				metric.FSPrefetchDroppedCounter.Inc()
			}
			p.queuedBytes += merged.Size - e.req.Size
			e.req = merged
			heap.Fix(&p.queue, e.index)
		}
		return
	}
	if p.have(req) {
		metric.FSPrefetchCachedCounter.Inc()
		return
	}
	p.seq++
	e := &prefetchEntry{req: req, scans: []*PrefetchScan{req.Scan}, seq: p.seq}
	limit := p.limit()
	if req.Size > limit {
		metric.FSPrefetchDroppedCounter.Inc()
		return
	}
	for p.queuedBytes+req.Size > limit {
		last := -1
		for i := range p.queue {
			if !p.queue[i].live() {
				last = i
				break
			}
			if last < 0 || p.queue[last].before(p.queue[i]) {
				last = i
			}
		}
		if last < 0 || p.queue[last].live() && !e.before(p.queue[last]) {
			metric.FSPrefetchDroppedCounter.Inc()
			return
		}
		p.dropLocked(last)
	}
	heap.Push(&p.queue, e)
	metric.FSPrefetchQueuedCounter.Inc()
	p.entries[req.FilePath] = e
	p.queuedBytes += req.Size
	p.queued.Add(1)
	p.startLocked()
}

// dropLocked drops the queued download at index i of the queue.
func (p *prefetcher) dropLocked(i int) {
	e := heap.Remove(&p.queue, i).(*prefetchEntry)
	delete(p.entries, e.req.FilePath)
	p.queuedBytes -= e.req.Size
	p.queued.Add(-1)
	if e.live() {
		metric.FSPrefetchDroppedCounter.Inc()
	} else {
		metric.FSPrefetchEndedCounter.Inc()
	}
}

func (p *prefetcher) startLocked() {
	for p.running < p.max && len(p.queue) > 0 {
		e := heap.Pop(&p.queue).(*prefetchEntry)
		p.queuedBytes -= e.req.Size
		p.queued.Add(-1)
		if !e.live() {
			delete(p.entries, e.req.FilePath)
			metric.FSPrefetchEndedCounter.Inc()
			continue
		}
		p.running++
		p.wg.Add(1)
		go func() {
			defer func() {
				p.mu.Lock()
				delete(p.entries, e.req.FilePath)
				p.running--
				p.startLocked()
				p.mu.Unlock()
				p.wg.Done()
			}()
			p.run(p.ctx, e.req)
		}()
	}
}

// isRunning reports whether a worker took the download of filePath.
func (p *prefetcher) isRunning(filePath string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	e, ok := p.entries[filePath]
	return ok && e.index < 0
}

// isQueued reports whether the download of filePath is queued and not started.
func (p *prefetcher) isQueued(filePath string) bool {
	if p.queued.Load() == 0 {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	e, ok := p.entries[filePath]
	return ok && e.index >= 0
}

// remove drops the queued download of filePath, as a read reached the object before it,
// and reports whether it was queued.
func (p *prefetcher) remove(filePath string) bool {
	if p.queued.Load() == 0 {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	e, ok := p.entries[filePath]
	if !ok || e.index < 0 {
		return false
	}
	heap.Remove(&p.queue, e.index)
	delete(p.entries, filePath)
	p.queuedBytes -= e.req.Size
	p.queued.Add(-1)
	return true
}

// close drops the queued downloads, cancels the running ones and waits for them, or ctx.
func (p *prefetcher) close(ctx context.Context) {
	p.mu.Lock()
	p.closed = true
	p.cancel()
	for _, e := range p.queue {
		delete(p.entries, e.req.FilePath)
	}
	p.queued.Add(-int64(len(p.queue)))
	p.queue = nil
	p.queuedBytes = 0
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

func (s *S3FS) PrefetchObject(ctx context.Context, req ObjectPrefetch) error {
	// An object the index-file cache keeps whole is downloaded there, not into the disk
	// cache.
	if GetFileServicePolicy(ctx).Any(IndexFileObject) && s.pinsObjects() {
		s.pinObjects(req.FilePath)
		return nil
	}
	if s.prefetcher != nil && req.Size > 0 {
		s.prefetcher.add(req)
	}
	return nil
}

// prefetchQueueLimit is the most bytes queued for download: no more than half the disk
// cache keeps, nor than the downloads write within the prefetch horizon.
func (s *S3FS) prefetchQueueLimit(workers int) func() int64 {
	return func() int64 {
		byRate := int64(downloadRates.rate() * float64(workers) * prefetchHorizon.Seconds())
		return min(s.diskCache.capacityFunc()/2, byRate)
	}
}

// haveObject reports whether the whole object of req is cached or being downloaded, or
// whether cached ranges cover the ranges req needs. A request of the whole object marks a
// cached one used.
func (s *S3FS) haveObject(req ObjectPrefetch) bool {
	path, err := parseFilePathAtService(req.FilePath, s.name)
	if err != nil {
		return true
	}
	diskPath := s.diskCache.pathForFile(path.File)
	if s.diskCache.download(diskPath) != nil {
		return true
	}
	if len(req.Ranges) > 0 {
		if s.diskCache.cache.Contains(diskPath) {
			return true
		}
	} else if _, ok := s.diskCache.cache.Get(context.Background(), diskPath); ok {
		return true
	}
	needs := req.needed()
	if len(needs) == 0 {
		return false
	}
	key := s.diskCache.rangeObjectKey(path.File)
	for _, r := range needs {
		if _, ok := s.diskCache.ranges.covering(key, r.Offset, r.Size); !ok {
			return false
		}
	}
	return true
}

// prefetchObject downloads the whole object of req at its known size, or its ranges.
func (s *S3FS) prefetchObject(ctx context.Context, req ObjectPrefetch) {
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
	path, err := parseFilePathAtService(req.FilePath, s.name)
	if err != nil {
		return
	}
	s.diskCache.prefetched.store(s.diskCache.pathForFile(path.File), req.Size)
	if err := s.PrefetchFile(ctx, req.FilePath); err != nil {
		metric.FSPrefetchFailedCounter.Inc()
		logutil.Debug("fileservice: prefetch object", zap.String("path", req.FilePath), zap.Error(err))
		return
	}
	metric.FSPrefetchDownloadedCounter.Inc()
}

// prefetchRanges reads into the disk cache the ranges of req not cached, those that are
// adjacent in one read each, so the bytes between them are not read.
func (s *S3FS) prefetchRanges(ctx context.Context, req ObjectPrefetch) {
	for _, r := range s.rangesToDownload(req) {
		vector := &IOVector{
			FilePath: req.FilePath,
			Entries:  []IOEntry{{Offset: r.Offset, Size: r.Size}},
			Policy:   SkipMemoryCacheWrites | SkipFullFilePreloads | ColumnPrefetch,
		}
		err := s.Read(ctx, vector)
		vector.Release()
		if err != nil {
			metric.FSPrefetchFailedCounter.Inc()
			logutil.Debug("fileservice: prefetch ranges", zap.String("path", req.FilePath), zap.Error(err))
			return
		}
	}
	metric.FSPrefetchDownloadedCounter.Inc()
}

// rangesToDownload returns the ranges of req not covered by cached ranges, in order, with
// adjacent ones joined.
func (s *S3FS) rangesToDownload(req ObjectPrefetch) []PrefetchRange {
	var key string
	if path, err := parseFilePathAtService(req.FilePath, s.name); err == nil {
		key = s.diskCache.rangeObjectKey(path.File)
	}
	out := make([]PrefetchRange, 0, len(req.Ranges))
	for _, r := range req.Ranges {
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
