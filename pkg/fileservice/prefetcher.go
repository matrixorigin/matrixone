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
	"time"

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
	// deletes is the file service's count of deleted objects when the request was made.
	deletes uint64
	// live is set once a read waits for the download.
	live bool
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
// A request of the whole object takes over one of ranges, and ranges add up.
func (r ObjectPrefetch) merge(o ObjectPrefetch) ObjectPrefetch {
	r.Due = min(r.Due, o.Due)
	r.deletes = min(r.deletes, o.deletes)
	if o.share() > r.share() {
		r.Reads, r.Blocks = o.Reads, o.Blocks
	}
	if len(r.Ranges) > 0 && len(o.Ranges) > 0 {
		r.Ranges, r.Size = unionRanges(r.Ranges, o.Ranges, r.Size)
		return r
	}
	if len(r.Ranges) > 0 {
		r.Size = o.Size
	}
	r.Ranges = nil
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
	queue *fetchQueue // the queue it waits in, nil once its download started
	index int         // in queue
	// started is closed once reads can join the download, or it ended.
	started chan struct{}
	once    sync.Once
}

// start marks the download joinable.
func (e *prefetchEntry) start() {
	e.once.Do(func() { close(e.started) })
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

// fetchQueue is a queue of downloads and its workers, which run at most max of them at
// once.
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
	entries  map[string]*prefetchEntry
	seq      uint64
	closed   bool
	wg       sync.WaitGroup
}

func newPrefetcher(workers int, limit func() int64, have func(ObjectPrefetch) bool, run func(context.Context, ObjectPrefetch)) *prefetcher {
	ctx, cancel := context.WithCancel(context.Background())
	return &prefetcher{
		run: run, ctx: ctx, cancel: cancel, have: have, limit: limit,
		prefetch: fetchQueue{max: workers},
		live:     fetchQueue{max: max(workers, 1)},
		entries:  make(map[string]*prefetchEntry),
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
	if p.closed {
		return
	}
	if e, ok := p.entries[req.FilePath]; ok {
		if e.queue == &p.prefetch && have {
			metric.FSPrefetchCachedCounter.Inc()
			return
		}
		e.addScan(req.Scan)
		metric.FSPrefetchMergedCounter.Inc()
		if e.queue == &p.prefetch {
			merged := e.req.merge(req)
			if grow := merged.Size - e.req.Size; grow > 0 && p.prefetch.bytes+grow > p.limit() {
				// Past the queue's bytes, the queued download keeps what it downloads.
				merged.Size, merged.Ranges = e.req.Size, e.req.Ranges
				metric.FSPrefetchDroppedCounter.Inc()
			}
			p.prefetch.bytes += merged.Size - e.req.Size
			e.req = merged
			heap.Fix(&p.prefetch.queue, e.index)
		}
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
	for q := p.prefetch.queue; p.prefetch.bytes+req.Size > limit; q = p.prefetch.queue {
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
			metric.FSPrefetchDroppedCounter.Inc()
			return
		}
		p.dropLocked(q[last])
	}
	p.prefetch.push(e)
	metric.FSPrefetchQueuedCounter.Inc()
	p.entries[req.FilePath] = e
	p.known.Add(1)
	p.queued.Add(1)
	p.startLocked()
}

func (p *prefetcher) forgetLocked(e *prefetchEntry) {
	delete(p.entries, e.req.FilePath)
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
		// A read waits for a live download, whatever the scans it was asked for.
		if q == &p.prefetch && !e.live() {
			p.forgetLocked(e)
			e.start()
			metric.FSPrefetchEndedCounter.Inc()
			continue
		}
		q.running++
		p.wg.Add(1)
		go func() {
			defer func() {
				p.mu.Lock()
				p.forgetLocked(e)
				q.running--
				e.start()
				p.startLocked()
				p.mu.Unlock()
				p.wg.Done()
			}()
			p.run(withDownloadStarted(p.ctx, e.start), e.req)
		}()
	}
}

// isRunning reports whether a worker took the download of filePath.
func (p *prefetcher) isRunning(filePath string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	e, ok := p.entries[filePath]
	return ok && e.queue == nil
}

// remove drops the queued download of filePath, and reports whether it was queued.
func (p *prefetcher) remove(filePath string) bool {
	if p.queued.Load() == 0 {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	e, ok := p.entries[filePath]
	if !ok || e.queue == nil {
		return false
	}
	p.removeLocked(e)
	return true
}

// join returns, when want accepts the request of the download of filePath, a channel
// closed once reads can join the download, or it ended. A download still waiting in the
// prefetch queue moves to the live queue.
func (p *prefetcher) join(filePath string, want func(ObjectPrefetch) bool) (<-chan struct{}, bool) {
	if p.known.Load() == 0 {
		return nil, false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	e, ok := p.entries[filePath]
	if !ok || !want(e.req) {
		return nil, false
	}
	if e.queue == &p.prefetch {
		metric.FSPrefetchMissedCounter.Inc()
		p.prefetch.remove(e)
		e.req.live = true
		p.live.push(e)
		p.startLocked()
	}
	return e.started, true
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
	// An object the index-file cache holds is not downloaded into the disk cache.
	if GetFileServicePolicy(ctx).Any(IndexFileObject) && s.cachesIndexObjects() {
		if _, ok := s.indexObjectCached(req.FilePath); ok {
			return nil
		}
	}
	if s.prefetcher != nil && req.Size > 0 {
		req.deletes = s.diskCache.prefetched.deletes.Load()
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
	if len(req.Ranges) == 0 {
		_, ok := s.diskCache.cache.Get(context.Background(), diskPath)
		return ok
	}
	if s.diskCache.cache.Contains(diskPath) {
		return true
	}
	key := s.diskCache.rangeObjectKey(path.File)
	for _, r := range req.Ranges {
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
	// A size told before an object was deleted may be another object's at that path.
	s.diskCache.prefetched.storeCurrent(s.diskCache.pathForFile(path.File), req.Size, req.deletes)
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
	ranges := s.rangesToDownload(req)
	if path, err := parseFilePathAtService(req.FilePath, s.name); err == nil && len(ranges) > 0 {
		key := s.pathToKey(path.File)
		open := func(ctx context.Context, start, end int64) (io.ReadCloser, error) {
			return s.storage.Read(ctx, key, &start, &end)
		}
		// Reads of the ranges join their flights instead of reading their bytes again.
		if ok, err := s.diskCache.readRanges(ctx, path.File, SkipMemoryCacheWrites|SkipFullFilePreloads|ColumnPrefetch, ranges, open, req.live); ok {
			if err != nil {
				metric.FSPrefetchFailedCounter.Inc()
				logutil.Debug("fileservice: prefetch ranges", zap.String("path", req.FilePath), zap.Error(err))
				return
			}
			metric.FSPrefetchDownloadedCounter.Inc()
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
