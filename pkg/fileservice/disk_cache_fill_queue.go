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
	"errors"
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

// share returns, in tenths, the share of the object's blocks the read reads.
func (r ObjectPrefetch) share() int {
	if r.Blocks <= 0 || r.Reads <= 0 || r.Reads >= r.Blocks {
		return 10
	}
	return r.Reads * 10 / r.Blocks
}

// unionRanges returns the ranges holding the bytes of a, in order, those that overlap or
// touch made one.
func unionRanges(a []PrefetchRange) []PrefetchRange {
	all := slices.Clone(a)
	slices.SortFunc(all, func(x, y PrefetchRange) int { return cmp.Compare(x.Offset, y.Offset) })
	out := all[:0]
	for _, r := range all {
		if r.Size <= 0 {
			continue
		}
		if n := len(out); n > 0 && r.Offset <= out[n-1].Offset+out[n-1].Size {
			last := &out[n-1]
			last.Size = max(last.Offset+last.Size, r.Offset+r.Size) - last.Offset
			continue
		}
		out = append(out, r)
	}
	return out
}

// subtractRange returns the parts of ranges outside [start, end).
func subtractRange(ranges []PrefetchRange, start, end int64) []PrefetchRange {
	var out []PrefetchRange
	for _, r := range ranges {
		lo, hi := r.Offset, r.Offset+r.Size
		if hi <= start || lo >= end {
			out = append(out, r)
			continue
		}
		if lo < start {
			out = append(out, PrefetchRange{Offset: lo, Size: start - lo})
		}
		if hi > end {
			out = append(out, PrefetchRange{Offset: end, Size: hi - end})
		}
	}
	return out
}

// ObjectPrefetcher is a file service that downloads objects ahead of the reads that need
// them.
type ObjectPrefetcher interface {
	// PrefetchObject queues req and returns without waiting for it.
	PrefetchObject(ctx context.Context, req ObjectPrefetch) error
}

var _ ObjectPrefetcher = new(S3FS)

// fillKey identifies the fills of an object into one cache: the disk cache, or the
// index-file cache when index is set.
type fillKey struct {
	diskPath string
	index    bool
}

// queuedFill is a fill a prefetch made: the bytes [start, end) of an object, or the whole
// object, into one cache. It waits in the registry holding nothing, then runs: a fill of
// ranges becomes the prefetch objects it registers as it starts, a fill of the whole
// object stays registered until reads can join its download.
type queuedFill struct {
	req        ObjectPrefetch
	key        fillKey
	src        objectSource
	start, end int64
	whole      bool
	scans      []*PrefetchScan
	seq        uint64
	pos        int // in the waiting fills; -1 when not waiting
	joiners    int
	// splitAt is where the first read that joined a waiting fill of the whole object
	// needs its bytes from.
	splitAt int64
	running bool
	// background is set while it runs counted among the background fills.
	background bool
	promoted   chan struct{}
	// cancel, when set, ends the wait of its memory flight for room.
	cancel context.CancelFunc
	// started is closed once reads can join what it downloads, or it ended.
	started     chan struct{}
	startedOnce sync.Once
}

func (q *queuedFill) size() int64 { return q.end - q.start }

func (q *queuedFill) isPromoted() bool {
	select {
	case <-q.promoted:
		return true
	default:
		return false
	}
}

func (q *queuedFill) markStarted() { q.startedOnce.Do(func() { close(q.started) }) }

// live reports whether a read joined q or a scan q is for has not ended.
func (q *queuedFill) live() bool {
	return q.joiners > 0 || slices.ContainsFunc(q.scans, (*PrefetchScan).live)
}

// before reports whether q starts before o: it is foreground and o is not, the read reads
// a larger share of it, a read reaches it sooner, or as soon and it was asked for first.
func (q *queuedFill) before(o *queuedFill) bool {
	if a, b := q.isPromoted(), o.isPromoted(); a != b {
		return a
	}
	if a, b := q.req.share(), o.req.share(); a != b {
		return a > b
	}
	if q.req.Due != o.req.Due {
		return q.req.Due < o.req.Due
	}
	return q.seq < o.seq
}

// merge merges into q, waiting, the request req for bytes it holds: the sooner due time,
// the larger share and its scan.
func (q *queuedFill) merge(req ObjectPrefetch) {
	q.req.Due = min(q.req.Due, req.Due)
	q.req.deletes = min(q.req.deletes, req.deletes)
	if req.share() > q.req.share() {
		q.req.Reads, q.req.Blocks = req.Reads, req.Blocks
	}
	q.scans = slices.DeleteFunc(q.scans, func(s *PrefetchScan) bool { return !s.live() })
	if !slices.Contains(q.scans, req.Scan) {
		q.scans = append(q.scans, req.Scan)
	}
}

// holdsNeeded reports whether q holds bytes of an entry of vector not done.
func (q *queuedFill) holdsNeeded(vector *IOVector) bool {
	for i := range vector.Entries {
		e := &vector.Entries[i]
		if !e.done && (q.whole || (e.Size < 0 || q.start < e.Offset+e.Size) && e.Offset < q.end) {
			return true
		}
	}
	return false
}

type fillHeap []*queuedFill

func (h fillHeap) Len() int           { return len(h) }
func (h fillHeap) Less(i, j int) bool { return h[i].before(h[j]) }
func (h fillHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].pos, h[j].pos = i, j
}
func (h *fillHeap) Push(x any) {
	q := x.(*queuedFill)
	q.pos = len(*h)
	*h = append(*h, q)
}
func (h *fillHeap) Pop() any {
	old := *h
	q := old[len(old)-1]
	old[len(old)-1] = nil
	*h = old[:len(old)-1]
	q.pos = -1
	return q
}

type queuedFillKey struct{}

// withQueuedFill returns ctx of the run of q: its requests are background until q is
// promoted, and reads under it join no fill.
func withQueuedFill(ctx context.Context, q *queuedFill) context.Context {
	ctx = withPromotion(ctx, q.promoted)
	ctx = context.WithValue(ctx, queuedFillKey{}, q)
	return withDownloadStarted(ctx, q.markStarted)
}

func queuedFillOf(ctx context.Context) *queuedFill {
	q, _ := ctx.Value(queuedFillKey{}).(*queuedFill)
	return q
}

type downloadStartedKey struct{}

// withDownloadStarted returns ctx, through which a download calls started once reads
// can join it.
func withDownloadStarted(ctx context.Context, started func()) context.Context {
	return context.WithValue(ctx, downloadStartedKey{}, started)
}

// isDownload reports whether ctx is that of a fill's download.
func isDownload(ctx context.Context) bool {
	_, ok := ctx.Value(downloadStartedKey{}).(func())
	return ok
}

// downloadStarted tells the fill whose download ctx is for that reads can join it.
func downloadStarted(ctx context.Context) {
	if started, ok := ctx.Value(downloadStartedKey{}).(func()); ok {
		started()
	}
}

// rangeFillPolicy is the policy a fill of ranges caches them under.
const rangeFillPolicy = SkipMemoryCacheWrites | SkipFullFilePreloads | ColumnPrefetch

// addFills makes waiting background fills of the bytes req asks for, have reporting
// whether they are cached. Bytes a waiting fill of the same cache holds merge into it,
// bytes a running fill holds need nothing more, and the other bytes get fills of their
// own; a fill of the whole object takes over the waiting fills of ranges of it no read
// joined.
func (d *DiskCache) addFills(req ObjectPrefetch, have bool, src objectSource) {
	if !req.Scan.live() {
		return
	}
	key := fillKey{diskPath: d.objectKey(req.FilePath), index: req.indexObject}
	var pieces []PrefetchRange
	if len(req.Ranges) > 0 {
		size, sized := d.prefetched.size(key.diskPath)
		for _, r := range req.Ranges {
			if sized && r.Offset < size && size < r.Offset+r.Size {
				// A range ends at the object's known end, not past it.
				r.Size = size - r.Offset
			}
			if _, cached := d.ranges.covering(key.diskPath, r.Offset, r.Size); !cached {
				pieces = append(pieces, r)
			}
		}
		if pieces = unionRanges(pieces); len(pieces) == 0 {
			return
		}
	}
	f := &d.prefetchObjects
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return
	}
	defer d.startFillsLocked()
	if have {
		metric.FSPrefetchCachedCounter.Inc()
		return
	}
	if pieces == nil {
		d.addWholeLocked(req, key, src)
		return
	}
	for _, o := range f.m[key.diskPath] {
		if o.target().index != key.index || !o.live() || !o.endsCached() {
			continue
		}
		start, end := o.span()
		if held := subtractRange(pieces, start, end); !slices.Equal(held, pieces) {
			if p, ok := o.(*filePrefetchObject); ok {
				p.kept = true
			}
			pieces = held
		}
	}
	for _, q := range f.fills[key] {
		if len(pieces) == 0 {
			break
		}
		held := subtractRange(pieces, q.start, q.end)
		if slices.Equal(held, pieces) {
			continue
		}
		metric.FSPrefetchMergedCounter.Inc()
		if q.pos >= 0 {
			q.merge(req)
			heap.Fix(&f.waitingFills, q.pos)
		}
		pieces = held
	}
	for _, r := range pieces {
		d.pushFillLocked(&queuedFill{req: req, key: key, src: src, start: r.Offset, end: r.Offset + r.Size})
	}
}

// addWholeLocked is addFills of req, a request of the whole object of key.
func (d *DiskCache) addWholeLocked(req ObjectPrefetch, key fillKey, src objectSource) {
	f := &d.prefetchObjects
	for _, q := range f.fills[key] {
		if q.whole {
			metric.FSPrefetchMergedCounter.Inc()
			if q.pos >= 0 {
				q.merge(req)
				heap.Fix(&f.waitingFills, q.pos)
			}
			return
		}
	}
	for _, o := range f.m[key.diskPath] {
		if p, ok := o.(*filePrefetchObject); ok && p.whole && p.index == key.index && p.live() {
			metric.FSPrefetchMergedCounter.Inc()
			return
		}
	}
	whole := &queuedFill{req: req, key: key, src: src, end: req.Size, whole: true}
	var taken []*queuedFill
	for _, q := range f.fills[key] {
		if q.pos >= 0 && q.joiners == 0 && !q.isPromoted() {
			taken = append(taken, q)
		}
	}
	for _, q := range taken {
		whole.req.Due = min(whole.req.Due, q.req.Due)
		whole.scans = append(whole.scans, q.scans...)
		heap.Remove(&f.waitingFills, q.pos)
		f.queuedBytes -= q.size()
		d.forgetFillLocked(q)
	}
	if d.pushFillLocked(whole) {
		for _, q := range taken {
			q.markStarted()
		}
		return
	}
	// The fills of ranges stay queued when the whole object does not fit.
	for _, q := range taken {
		heap.Push(&f.waitingFills, q)
		f.queuedBytes += q.size()
		f.fills[key] = append(f.fills[key], q)
	}
}

// pushFillLocked queues q, a new background fill, unless it does not fit the bytes of the
// waiting background fills, and reports whether it did.
func (d *DiskCache) pushFillLocked(q *queuedFill) bool {
	f := &d.prefetchObjects
	if !d.roomLocked(q) {
		metric.FSPrefetchDroppedCounter.Inc()
		return false
	}
	f.fillSeq++
	q.seq, q.pos = f.fillSeq, -1
	q.promoted, q.started = make(chan struct{}), make(chan struct{})
	if !slices.Contains(q.scans, q.req.Scan) {
		q.scans = append(q.scans, q.req.Scan)
	}
	heap.Push(&f.waitingFills, q)
	f.queuedBytes += q.size()
	if f.fills == nil {
		f.fills = make(map[fillKey][]*queuedFill)
	}
	f.fills[q.key] = append(f.fills[q.key], q)
	metric.FSPrefetchQueuedCounter.Inc()
	return true
}

// fillQueueLimit is the most bytes of waiting background fills: half the disk cache keeps.
func (d *DiskCache) fillQueueLimit() int64 { return d.capacityFunc() / 2 }

// roomLocked reports whether q fits the bytes of the waiting background fills, after
// dropping those whose scans all ended and no read joined, then the last in order while q
// starts before it.
func (d *DiskCache) roomLocked(q *queuedFill) bool {
	f := &d.prefetchObjects
	limit := d.fillQueueLimit()
	if q.size() > limit {
		return false
	}
	for f.queuedBytes+q.size() > limit {
		var last *queuedFill
		for _, o := range f.waitingFills {
			if o.isPromoted() {
				continue
			}
			if !o.live() {
				last = o
				break
			}
			if last == nil || last.before(o) {
				last = o
			}
		}
		if last == nil || last.live() && !q.before(last) {
			return false
		}
		d.dropLocked(last)
	}
	return true
}

// removeWaitingLocked removes q, waiting, from the registry.
func (d *DiskCache) removeWaitingLocked(q *queuedFill) {
	f := &d.prefetchObjects
	if q.pos >= 0 {
		heap.Remove(&f.waitingFills, q.pos)
		if !q.isPromoted() {
			f.queuedBytes -= q.size()
		}
	}
	d.forgetFillLocked(q)
	q.markStarted()
}

func (d *DiskCache) forgetFillLocked(q *queuedFill) {
	f := &d.prefetchObjects
	fills := slices.DeleteFunc(f.fills[q.key], func(o *queuedFill) bool { return o == q })
	if len(fills) == 0 {
		delete(f.fills, q.key)
	} else {
		f.fills[q.key] = fills
	}
}

// dropLocked drops q, waiting.
func (d *DiskCache) dropLocked(q *queuedFill) {
	if q.live() {
		metric.FSPrefetchDroppedCounter.Inc()
	} else {
		metric.FSPrefetchEndedCounter.Inc()
	}
	d.removeWaitingLocked(q)
}

// fillsMaxLocked returns the most background fills that run at once.
func (d *DiskCache) fillsMaxLocked() int {
	if n := d.prefetchObjects.fillsMax; n > 0 {
		return n
	}
	return d.backgroundRequests()
}

// memoryBacked reports whether a fill of n bytes is made to be held in memory: it is at
// most memoryPrefetchMax bytes and its buffer fits the memory of the server for its class
// now. Its buffer is taken only once its request is admitted, if it still fits.
func (d *DiskCache) memoryBacked(n int64, background bool) bool {
	c := foregroundRequest
	if background {
		c = backgroundRequest
	}
	return n <= d.memoryPrefetchMax && d.server.memoryFits(flightBufSize(n), c)
}

// startFillsLocked starts the waiting fills in order while the first can start: it is
// foreground, or fewer than fillsMax background fills run. A fill whose scans all ended
// and no read joined is dropped instead.
func (d *DiskCache) startFillsLocked() {
	f := &d.prefetchObjects
	for len(f.waitingFills) > 0 && !f.closed {
		q := f.waitingFills[0]
		if !q.live() {
			d.dropLocked(q)
			continue
		}
		background := !q.isPromoted()
		if background && f.backgroundFills >= d.fillsMaxLocked() {
			return
		}
		heap.Pop(&f.waitingFills)
		if background {
			f.queuedBytes -= q.size()
		}
		d.startFillLocked(q, background, !q.whole && d.memoryBacked(q.size(), background))
	}
}

// startFillLocked starts q, taken from the waiting fills. A fill of the whole object runs
// on its own goroutine, registered until reads can join its download. A fill of a range
// that finds its bytes cached, held by a prefetch object or its object deleted ends;
// otherwise it runs one prefetch object of its range on its own goroutine: when memory is
// set, a memory flight admitted with the room of its request and its buffer, or a file
// fill once its buffer does not fit; a file fill otherwise. The requests of q wait in its
// order: its share, then its due time.
func (d *DiskCache) startFillLocked(q *queuedFill, background bool, memory bool) {
	f := &d.prefetchObjects
	closing, _ := f.openLocked()
	q.running, q.background = true, background
	if background {
		f.backgroundFills++
	}
	ctx := withRequestOrder(withQueuedFill(context.Background(), q), requestOrder{share: q.req.share(), due: q.req.Due})
	if q.whole {
		f.reads.Add(1)
		run := f.runWhole
		go func() {
			defer f.reads.Done()
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			defer context.AfterFunc(closing, cancel)()
			run(ctx, q)
			d.endWhole(q)
		}()
		return
	}
	diskPath, filePath := q.key.diskPath, q.req.FilePath
	end := func() {
		d.forgetFillLocked(q)
		q.markStarted()
		d.endFillLocked(q)
	}
	if d.deletes.since(diskPath, q.req.deletes) {
		metric.FSPrefetchEndedCounter.Inc()
		end()
		return
	}
	if _, cached := d.ranges.covering(diskPath, q.start, q.size()); cached {
		metric.FSPrefetchCachedCounter.Inc()
		end()
		return
	}
	if o := f.holdingLocked(diskPath, false, q.start, q.end); o != nil {
		if p, ok := o.(*filePrefetchObject); ok {
			p.kept = true
		}
		metric.FSPrefetchMergedCounter.Inc()
		end()
		return
	}
	deletes := q.req.deletes
	needs := flightNeeds{caching: true, deletes: &deletes}
	if memory {
		// The fill stays in the registry while its flight waits for room.
		ctx, q.cancel = context.WithCancel(ctx)
		f.reads.Add(1)
		go d.runRangeFill(ctx, q, needs)
		return
	}
	d.forgetFillLocked(q)
	q.markStarted()
	p := d.newFileObject(ctx, filePath, q.start, q.end, q.src, needs)
	p.fill = q
	setStateLocked(p, objectRegistered)
	p.registerLocked(ctx, closing)
	go p.run()
}

// runRangeFill runs q, a fill of a range that may be held in memory: its memory flight,
// once admitted with its buffer, otherwise a file fill of its range. A fill that finds its
// bytes cached, held by a prefetch object, its object deleted or the cache closed ends.
func (d *DiskCache) runRangeFill(ctx context.Context, q *queuedFill, needs flightNeeds) {
	f := &d.prefetchObjects
	defer f.reads.Done()
	fl := d.newFlight(q.req.FilePath, q.start, q.end, rangeFillPolicy, q.src, needs)
	fl.fill = q
	var o waitingObject = fl
	got, err := admit(ctx, fl)
	f.mu.Lock()
	cancel := q.cancel
	q.cancel = nil
	d.forgetFillLocked(q)
	f.mu.Unlock()
	defer cancel()
	q.markStarted()
	if errors.Is(err, errNoBuffer) {
		p := d.newFileObject(ctx, q.req.FilePath, q.start, q.end, q.src, needs)
		p.fill = q
		o = p
		got, err = admit(ctx, p)
	}
	if got == prefetchObject(o) {
		got.release()
		o.run()
		return
	}
	o.abandon()
	if got != nil {
		defer got.release()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	switch {
	case got != nil:
		if p, ok := got.(*filePrefetchObject); ok {
			p.kept = true
		}
		metric.FSPrefetchMergedCounter.Inc()
	case err == nil:
		metric.FSPrefetchCachedCounter.Inc()
	case errors.Is(err, errPrefetchDeleted):
		metric.FSPrefetchEndedCounter.Inc()
	}
	d.endFillLocked(q)
	d.startFillsLocked()
}

// rangeEndedLocked ends q, whose prefetch object ended with err, why it cached nothing.
func (d *DiskCache) rangeEndedLocked(q *queuedFill, err error) {
	d.countPrefetch(q.req, err)
	d.endFillLocked(q)
	d.startFillsLocked()
}

// endWhole ends q, a fill of the whole object, whose run returned.
func (d *DiskCache) endWhole(q *queuedFill) {
	f := &d.prefetchObjects
	f.mu.Lock()
	defer f.mu.Unlock()
	d.forgetFillLocked(q)
	q.markStarted()
	d.endFillLocked(q)
	d.startFillsLocked()
}

func (d *DiskCache) endFillLocked(q *queuedFill) {
	if q.background {
		d.prefetchObjects.backgroundFills--
		q.background = false
	}
}

// promoteLocked makes q foreground: it starts before every background fill, its requests
// are foreground, and it leaves the bytes of the waiting background fills.
func (d *DiskCache) promoteLocked(q *queuedFill) {
	if q == nil || q.isPromoted() {
		return
	}
	f := &d.prefetchObjects
	close(q.promoted)
	if q.pos >= 0 {
		metric.FSPrefetchMissedCounter.Inc()
		f.queuedBytes -= q.size()
		heap.Fix(&f.waitingFills, q.pos)
	}
}

// promoteObjectLocked promotes the fill of o, which a read joins, and moves the buffer of
// a background flight to the foreground charge.
func (d *DiskCache) promoteObjectLocked(o prefetchObject) {
	t := o.target()
	d.promoteLocked(t.fill)
	if fl, ok := o.(*memoryPrefetchObject); ok && fl.background && fl.charge > 0 {
		d.server.promoteMemory(fl.charge)
		fl.background = false
	}
}

// joinFills joins the fills of the cache of vector waiting, or running before reads can
// join their downloads, that hold bytes it needs: each is promoted, and the read waits
// until reads can join what it downloads, it ends, or ctx ends. A waiting fill of the
// whole object starts at the first block the read needs. It reports whether it joined
// one.
func (s *S3FS) joinFills(ctx context.Context, vector *IOVector, src readSources) bool {
	if !src.fetch {
		return false
	}
	path, err := parseFilePathAtService(vector.FilePath, s.name)
	if err != nil {
		return false
	}
	d := s.diskCache
	key := fillKey{diskPath: d.objectKey(path.File), index: s.indexObject(vector.Policy)}
	f := &d.prefetchObjects
	f.mu.Lock()
	var joined []*queuedFill
	for _, q := range f.fills[key] {
		if !q.holdsNeeded(vector) {
			continue
		}
		q.joiners++
		if q.whole && q.pos >= 0 && !q.isPromoted() {
			q.splitAt = firstNeeded(vector)
		}
		d.promoteLocked(q)
		joined = append(joined, q)
	}
	if len(joined) == 0 {
		f.mu.Unlock()
		return false
	}
	d.startFillsLocked()
	f.mu.Unlock()
	for _, q := range joined {
		select {
		case <-q.started:
		case <-ctx.Done():
		}
	}
	f.mu.Lock()
	for _, q := range joined {
		if q.joiners--; q.pos >= 0 && !q.live() {
			d.dropLocked(q)
		}
	}
	f.mu.Unlock()
	return true
}

// dropFills drops the waiting fills of the object at filePath into both caches.
func (d *DiskCache) dropFills(filePath string) {
	f := &d.prefetchObjects
	f.mu.Lock()
	defer f.mu.Unlock()
	d.dropFillsLocked(d.objectKey(filePath))
}

func (d *DiskCache) dropFillsLocked(diskPath string) {
	f := &d.prefetchObjects
	for _, index := range []bool{false, true} {
		for _, q := range slices.Clone(f.fills[fillKey{diskPath: diskPath, index: index}]) {
			if q.pos >= 0 {
				d.removeWaitingLocked(q)
			} else if q.cancel != nil {
				q.cancel()
			}
		}
	}
	d.startFillsLocked()
}

// fillsUnderWay reports whether a fill of the object at filePath runs.
func (d *DiskCache) fillsUnderWay(filePath string) bool {
	f := &d.prefetchObjects
	f.mu.Lock()
	defer f.mu.Unlock()
	diskPath := d.objectKey(filePath)
	if len(f.m[diskPath]) > 0 {
		return true
	}
	for _, index := range []bool{false, true} {
		if slices.ContainsFunc(f.fills[fillKey{diskPath: diskPath, index: index}], func(q *queuedFill) bool { return q.running }) {
			return true
		}
	}
	return false
}

func (s *S3FS) PrefetchObject(ctx context.Context, req ObjectPrefetch) error {
	// A fill keys an object by its path at the service, whatever its spelling.
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
	if s.diskCache != nil && req.Size > 0 && req.Scan.live() {
		req.deletes = s.diskCache.deletes.now()
		s.diskCache.addFills(req, s.haveObject(req), s.source(req.FilePath))
	}
	return nil
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
	key := s.diskCache.objectKey(path.File)
	if s.diskCache.wholeFill(key, false) != nil {
		return true
	}
	ctx := context.Background()
	// The whole object is the range [0, size).
	ranges, whole := req.Ranges, len(req.Ranges) == 0
	if whole {
		ranges = []PrefetchRange{{Offset: 0, Size: req.Size}}
	}
	// Every range is checked, so the record of a missing file is dropped before the
	// download decides which ranges to fetch.
	size, sized := s.diskCache.prefetched.size(key)
	checked := make(map[string]struct{})
	have := true
	for _, r := range ranges {
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
			} else if whole {
				s.diskCache.cache.Get(ctx, c.diskPath)
			}
		}
	}
	return have
}

// runWholeFill downloads the whole object of q at its known size, from the block of its
// split.
func (s *S3FS) runWholeFill(ctx context.Context, q *queuedFill) {
	req := q.req
	ctx = withBackgroundRequests(ctx)
	// A fill started after its object was deleted downloads nothing.
	objectPath := s.diskCache.objectKey(req.FilePath)
	if s.diskCache.deletes.since(objectPath, req.deletes) {
		metric.FSPrefetchEndedCounter.Inc()
		return
	}
	// A size told before an object was deleted may be another object's at that path.
	s.diskCache.storePrefetchedSize(objectPath, req.Size, req.deletes)
	ctx = withDeletesMark(ctx, req.deletes)
	if req.indexObject {
		ctx = WithFileServicePolicy(ctx, GetFileServicePolicy(ctx)|IndexFileObject)
	}
	s.diskCache.countPrefetch(req, s.prefetchFile(ctx, req.FilePath, q.splitAt))
}

// countPrefetch counts the outcome of the prefetch of req that ended with err: a prefetch
// of an object deleted since it was asked for ended.
func (d *DiskCache) countPrefetch(req ObjectPrefetch, err error) {
	switch {
	case err == nil:
		metric.FSPrefetchDownloadedCounter.Inc()
	case d.deletes.since(d.objectKey(req.FilePath), req.deletes):
		metric.FSPrefetchEndedCounter.Inc()
	default:
		metric.FSPrefetchFailedCounter.Inc()
		logutil.Debug("fileservice: prefetch object", zap.String("path", req.FilePath), zap.Error(err))
	}
}
