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
	"errors"
	"io"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	metric "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
)

// objectDownload is a whole-object file being written to the disk cache. Readers of the
// object read the bytes already written through the download's own handle on the file,
// which keeps them readable after the file is published or removed.
type objectDownload struct {
	started  time.Time
	measured bool // the download's rate counts toward downloadRates

	mu          sync.Mutex
	parts       []downloadPart // the ranges of the object written at once, in order
	ended       bool           // no more bytes will be written
	complete    bool           // every byte of the object was written
	waiters     byteWaiters
	file        *os.File // closed once the download is retired and no reader holds it
	readers     int
	onReadsDone []func() // eviction accounting deferred until active readers finish
	retired     bool
	served      int64         // bytes readers took from the download before credited
	credited    bool          // the cached file counts the bytes readers take
	credit      func(n int64) // counts n bytes read toward the published file
}

// downloadPart is a range of the object a download writes from its start.
type downloadPart struct {
	start, end int64 // end is -1 for the end of the object
	written    int64
	began      time.Time // when the read of the part began, zero before
	first      time.Time // when its first byte was written, zero before
}

// serve records n bytes a reader took from the download, which count toward the cached
// file once it is published.
func (o *objectDownload) serve(n int64) {
	o.mu.Lock()
	if !o.credited {
		o.served += n
		o.mu.Unlock()
		return
	}
	o.mu.Unlock()
	if o.credit != nil {
		o.credit(n)
	}
}

// takeServed returns the bytes readers took so far; readers count later bytes
// themselves.
func (o *objectDownload) takeServed() int64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.credited = true
	n := o.served
	o.served = 0
	return n
}

// newObjectDownload returns a download that writes the ranges between bounds at once, or
// the object as one range when bounds is nil.
func newObjectDownload(tempPath string, measured bool, bounds []int64) *objectDownload {
	o := &objectDownload{
		started:  time.Now(),
		measured: measured,
		parts:    []downloadPart{{end: -1}},
	}
	if bounds != nil {
		o.parts = o.parts[:0]
		for i := 0; i+1 < len(bounds); i++ {
			o.parts = append(o.parts, downloadPart{start: bounds[i], end: bounds[i+1]})
		}
	}
	if f, err := os.Open(tempPath); err == nil {
		o.file = f
	}
	return o
}

// acquire returns the download's file for reading, which the reader gives back with
// release, or false once the file is closed.
func (o *objectDownload) acquire() (*os.File, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.file == nil || o.retired {
		return nil, false
	}
	o.readers++
	return o.file, true
}

func (o *objectDownload) release() {
	var onReadsDone []func()
	o.mu.Lock()
	o.readers--
	if o.readers == 0 {
		onReadsDone = o.onReadsDone
		o.onReadsDone = nil
	}
	o.closeIfUnusedLocked()
	o.mu.Unlock()
	for _, fn := range onReadsDone {
		fn()
	}
}

// afterReads seals the download against new readers and runs fn after the
// readers currently holding it release it. It runs fn immediately when idle.
func (o *objectDownload) afterReads(fn func()) {
	o.mu.Lock()
	o.retired = true
	if o.readers > 0 {
		o.onReadsDone = append(o.onReadsDone, fn)
		o.mu.Unlock()
		return
	}
	o.closeIfUnusedLocked()
	o.mu.Unlock()
	fn()
}

// retire closes the file once no reader holds it; no new reader finds the download.
func (o *objectDownload) retire() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.retired = true
	o.closeIfUnusedLocked()
}

func (o *objectDownload) closeIfUnusedLocked() {
	if o.retired && o.readers == 0 && o.file != nil {
		_ = o.file.Close()
		o.file = nil
	}
}

func (o *objectDownload) isComplete() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.complete
}

// byteWaiter waits for the bytes [off, end) of a download, or for its whole object when
// end is negative.
type byteWaiter struct {
	off, end int64
	ch       chan struct{}
}

// byteWaiters are the readers waiting for bytes; each is woken once, by closing its ch.
type byteWaiters []byteWaiter

func (w *byteWaiters) add(off, end int64) chan struct{} {
	ch := make(chan struct{})
	*w = append(*w, byteWaiter{off: off, end: end, ch: ch})
	return ch
}

func (w *byteWaiters) remove(ch chan struct{}) {
	for i := range *w {
		if (*w)[i].ch == ch {
			*w = append((*w)[:i], (*w)[i+1:]...)
			return
		}
	}
}

func (w *byteWaiters) wakeAll() {
	for _, x := range *w {
		close(x.ch)
	}
	*w = nil
}

// wakeLocked wakes the readers whose bytes are written.
func (o *objectDownload) wakeLocked() {
	keep := o.waiters[:0]
	for _, w := range o.waiters {
		if w.end >= 0 && o.availableLocked(w.off) >= w.end {
			close(w.ch)
		} else {
			keep = append(keep, w)
		}
	}
	clear(o.waiters[len(keep):])
	o.waiters = keep
}

// begin records that the read of the part began.
func (o *objectDownload) begin(part int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.parts[part].began.IsZero() {
		o.parts[part].began = time.Now()
	}
}

func (o *objectDownload) advance(part int, n int64) {
	o.mu.Lock()
	p := &o.parts[part]
	if p.first.IsZero() {
		p.first = time.Now()
	}
	sample := o.measured && p.written < downloadRateMinBytes && p.written+n >= downloadRateMinBytes
	p.written += n
	written, first := p.written, p.first
	if len(o.waiters) > 0 {
		o.wakeLocked()
	}
	o.mu.Unlock()
	if sample {
		downloadRates.observe(written, time.Since(first))
	}
}

// writtenLocked returns the bytes written of all parts.
func (o *objectDownload) writtenLocked() int64 {
	var n int64
	for _, p := range o.parts {
		n += p.written
	}
	return n
}

// availableLocked returns the end of the written bytes from off, or off when the byte at
// off is not written.
func (o *objectDownload) availableLocked(off int64) int64 {
	pos := off
	for _, p := range o.parts {
		if p.end >= 0 && p.end <= pos {
			continue
		}
		if p.start > pos {
			break
		}
		pos = max(pos, p.start+p.written)
		if p.end < 0 || pos < p.end {
			break
		}
	}
	return pos
}

func (o *objectDownload) end(complete bool) {
	o.mu.Lock()
	if !o.ended {
		o.ended, o.complete = true, complete
		o.waiters.wakeAll()
		if complete && o.measured && !o.parts[0].first.IsZero() {
			downloadRates.observe(o.writtenLocked()/int64(len(o.parts)), time.Since(o.parts[0].first))
		}
	}
	o.mu.Unlock()
}

// estimate returns the bytes a part of the download still writes before the bytes in
// [off, end) are written, the most of the parts holding them, and the time until they
// are: the time to the first byte of a part not yet writing, plus its bytes left at its
// rate so far, or at the recent downloads' rate before it wrote enough to measure its own.
func (o *objectDownload) estimate(off, end int64) (int64, time.Duration) {
	now := time.Now()
	latency := rangeReadCosts.firstByte()
	recent := downloadRates.rate()
	o.mu.Lock()
	defer o.mu.Unlock()
	var behind int64
	var wait time.Duration
	for _, p := range o.parts {
		if p.end >= 0 && p.end <= off || p.start >= end {
			continue
		}
		stop := end
		if p.end >= 0 {
			stop = min(stop, p.end)
		}
		left := stop - (p.start + p.written)
		if left <= 0 {
			continue
		}
		behind = max(behind, left)
		var ttfb time.Duration
		if p.first.IsZero() {
			ttfb = latency
			if !p.began.IsZero() {
				ttfb = max(latency-now.Sub(p.began), 0)
			}
		}
		rate := recent
		if elapsed := now.Sub(p.first); !p.first.IsZero() && p.written >= downloadRateMinBytes && elapsed > 0 {
			rate = float64(p.written) / elapsed.Seconds()
		}
		wait = max(wait, ttfb+time.Duration(float64(left)/rate*float64(time.Second)))
	}
	return behind, wait
}

// waitFor waits until the bytes in [off, end) are written, or until the whole object is
// when end is negative. It returns the end of the written bytes from off, and false when
// they will never cover end.
func (o *objectDownload) waitFor(ctx context.Context, off, end int64, timeout time.Duration) (int64, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	covered := func() bool { return end >= 0 && o.availableLocked(off) >= end || end < 0 && o.complete }
	if !covered() && !o.ended {
		ch := o.waiters.add(off, end)
		o.mu.Unlock()
		timer := time.NewTimer(timeout)
		select {
		case <-ch:
		case <-ctx.Done():
		case <-timer.C:
		}
		timer.Stop()
		o.mu.Lock()
		o.waiters.remove(ch)
	}
	return o.availableLocked(off), covered()
}

// sourceIdleTimeout is how long a read of a cache fill's source may wait for bytes before
// the fill fails.
var sourceIdleTimeout = readWriteTimeout

var errSourceIdle = moerr.NewInternalErrorNoCtx("fileservice: no bytes from the source within the idle timeout")

// idleReader reads r, which reads through ctx, and cancels ctx when a Read waits for
// bytes past sourceIdleTimeout.
type idleReader struct {
	r      io.Reader
	ctx    context.Context
	cancel context.CancelCauseFunc
	timer  *time.Timer
}

// newIdleReader returns a context to open the source through and the reader that guards
// it once its r is set. Close releases the context.
func newIdleReader(ctx context.Context) (context.Context, *idleReader) {
	ctx, cancel := context.WithCancelCause(ctx)
	return ctx, &idleReader{ctx: ctx, cancel: cancel}
}

func (r *idleReader) Read(b []byte) (int, error) {
	if r.timer == nil {
		r.timer = time.AfterFunc(sourceIdleTimeout, func() { r.cancel(errSourceIdle) })
	} else {
		r.timer.Reset(sourceIdleTimeout)
	}
	n, err := r.r.Read(b)
	r.timer.Stop()
	if err != nil && err != io.EOF && errors.Is(context.Cause(r.ctx), errSourceIdle) {
		err = errSourceIdle
	}
	return n, err
}

func (r *idleReader) Close() {
	if r.timer != nil {
		r.timer.Stop()
	}
	r.cancel(nil)
}

type downloadProgressWriter struct {
	w        io.Writer
	download *objectDownload
	part     int
}

func (p downloadProgressWriter) Write(b []byte) (int, error) {
	n, err := p.w.Write(b)
	if n > 0 {
		p.download.advance(p.part, int64(n))
	}
	return n, err
}

// objectSplitConfig sets the parts a whole-object download reads at once.
type objectSplitConfig struct {
	// parts is how many ranges the download of an object of at least min bytes reads.
	parts int
	min   int64
}

// objectLayout returns the size of an object to download, and the offset, or 0, a read
// needs its bytes from first.
type objectLayout func(context.Context) (size, splitAt int64, err error)

// bounds returns the 64KiB-aligned bounds of the parts the download of an object of size
// bytes reads at once, or nil to read it as one range. With splitAt inside the object, a
// part starts there, so the bytes a read needs from there come first.
func (c objectSplitConfig) bounds(size, splitAt int64) []int64 {
	if c.parts <= 1 || size < c.min {
		return nil
	}
	if at := splitAt / prefetchBlockMax * prefetchBlockMax; at > 0 && at < size {
		return []int64{0, at, size}
	}
	part := (size + int64(c.parts) - 1) / int64(c.parts)
	part = (part + prefetchBlockMax - 1) / prefetchBlockMax * prefetchBlockMax
	bounds := []int64{0}
	for pos := part; pos < size; pos += part {
		bounds = append(bounds, pos)
	}
	return append(bounds, size)
}

// rangeOpener opens the bytes [start, end) of an object, or the bytes from start on when
// end is negative.
type rangeOpener func(ctx context.Context, start, end int64) (io.ReadCloser, error)

// lazyObjectReader reads a whole object through read from its first Read on, and opens
// its ranges.
type lazyObjectReader struct {
	ctx  context.Context
	read func(ctx context.Context, min, max *int64) (io.ReadCloser, error)
	r    io.ReadCloser
}

func (l *lazyObjectReader) Read(b []byte) (int, error) {
	if l.r == nil {
		r, err := l.read(l.ctx, ptrTo[int64](0), nil)
		if err != nil {
			return 0, err
		}
		l.r = r
	}
	return l.r.Read(b)
}

func (l *lazyObjectReader) Close() error {
	if l.r == nil {
		return nil
	}
	return l.r.Close()
}

// openRange opens [start, end), or the bytes from start on when end is negative.
func (l *lazyObjectReader) openRange(ctx context.Context, start, end int64) (io.ReadCloser, error) {
	if end < 0 {
		return l.read(ctx, &start, nil)
	}
	return l.read(ctx, &start, &end)
}

// copyParts writes the parts of download between bounds into f at once, each read through
// open, and returns the bytes written. A failed part fails the copy.
func copyParts(ctx context.Context, f *os.File, download *objectDownload, open rangeOpener, bounds []int64) (int64, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	errs := make([]error, len(bounds)-1)
	var wg sync.WaitGroup
	for i := range errs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if errs[i] = copyPart(ctx, f, download, open, i, bounds[i], bounds[i+1], i == len(bounds)-2); errs[i] != nil {
				cancel()
			}
		}()
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil && !errors.Is(err, context.Canceled) {
			return 0, err
		}
	}
	for _, err := range errs {
		if err != nil {
			return 0, err
		}
	}
	return bounds[len(bounds)-1], nil
}

func copyPart(ctx context.Context, f *os.File, download *objectDownload, open rangeOpener, part int, start, end int64, last bool) error {
	download.begin(part)
	// The last part reads to the object's end, so an object of another size fails the copy.
	openEnd := end
	if last {
		openEnd = -1
	}
	ctx, idle := newIdleReader(ctx)
	defer idle.Close()
	r, err := open(ctx, start, openEnd)
	if err != nil {
		return &diskCacheSourceError{err: err}
	}
	defer r.Close()
	idle.r = r
	source := &diskCacheSourceReader{Reader: idle}
	var buf []byte
	put := ioBufferPool.Get(&buf)
	defer put.Put()
	dst := downloadProgressWriter{w: io.NewOffsetWriter(f, start), download: download, part: part}
	n, err := io.CopyBuffer(dst, source, buf)
	if err != nil {
		if source.err != nil {
			return &diskCacheSourceError{err: source.err}
		}
		return err
	}
	if n != end-start {
		return &diskCacheSourceError{err: io.ErrUnexpectedEOF}
	}
	return nil
}

// diskCacheDownloads indexes the whole-object downloads in flight by disk path.
type diskCacheDownloads struct {
	m sync.Map // diskPath -> *objectDownload
	n atomic.Int64
}

func (s *diskCacheDownloads) store(diskPath string, o *objectDownload) {
	if _, loaded := s.m.Swap(diskPath, o); !loaded {
		s.n.Add(1)
	}
}

func (s *diskCacheDownloads) remove(diskPath string, o *objectDownload) {
	if s.m.CompareAndDelete(diskPath, o) {
		s.n.Add(-1)
	}
}

func (s *diskCacheDownloads) load(diskPath string) *objectDownload {
	if s.n.Load() == 0 {
		return nil
	}
	if o, ok := s.m.Load(diskPath); ok {
		return o.(*objectDownload)
	}
	return nil
}

func (d *DiskCache) startDownload(diskPath, tempPath string, measured bool, bounds []int64) *objectDownload {
	o := newObjectDownload(tempPath, measured, bounds)
	o.credit = func(n int64) { d.preloads.readDownload(diskPath, o, n) }
	d.downloads.store(diskPath, o)
	return o
}

func (d *DiskCache) finishDownload(diskPath string, o *objectDownload) {
	o.end(false)
	d.downloads.remove(diskPath, o)
	o.retire()
}

// downloading returns the number of whole-object downloads in flight.
func (d *DiskCache) downloading() int {
	return int(d.downloads.n.Load())
}

// prefetchWorkers is the most prefetches run at once, a quarter of the connections
// downloads may hold: past it more streams share the bandwidth without adding any. It is
// at least 4, or each connection downloads may hold when fewer, and at least 1.
func (d *DiskCache) prefetchWorkers() int {
	limit := d.maxDownloadConns
	return int(max(limit/4, min(limit, 4), 1))
}

// reserveDownloadConns reserves n of the connections whole-object downloads may hold,
// half the connections to the object storage host, or reports that they are taken.
func (d *DiskCache) reserveDownloadConns(n int64) bool {
	limit := d.maxDownloadConns
	for {
		cur := d.downloadConns.Load()
		if cur+n > limit {
			return false
		}
		if d.downloadConns.CompareAndSwap(cur, cur+n) {
			return true
		}
	}
}

func (d *DiskCache) download(diskPath string) *objectDownload {
	return d.downloads.load(diskPath)
}

// readFromDownload fills entry from the download of the object at diskPath when its bytes
// are written, or will be no more than the max extra wait later than a range read could
// fetch them, or are more than an eighth of what the download writes before them. It
// reports whether a download covers the object, and whether it filled entry.
func (d *DiskCache) readFromDownload(ctx context.Context, diskPath string, entry *IOEntry, vector *IOVector) (downloading, filled bool, err error) {
	o := d.download(diskPath)
	if o == nil {
		return false, false, nil
	}
	end := entry.Offset + entry.Size
	behind, wait := o.estimate(entry.Offset, end)
	if wait == 0 {
		metric.FSDiskCacheDownloadReadWrittenCounter.Inc()
	} else {
		if eta, ok := d.fastestPrefetch(diskPath, entry.Offset, end); ok && eta < wait {
			if _, ok := d.prefetchUnit(vector); ok {
				// A prefetch range being read delivers the bytes sooner.
				return true, false, nil
			}
		}
		budget := rangeReadCosts.cost(entry.Size) + d.downloadMaxExtraWait
		if wait > budget {
			// A range read fetches these bytes again; it pays only when they are a small
			// part of what the download still writes before them.
			if entry.Size*downloadDuplicateRatio <= behind {
				metric.FSDiskCacheDownloadReadDeclinedCounter.Inc()
				metric.FSDiskCacheDownloadDeclinedBytesCounter.Add(float64(entry.Size))
				return true, false, nil
			}
		}
		// The read waits for the download it chose until its bytes are written or it ends.
		if _, ok := o.waitFor(ctx, entry.Offset, end, maxIOWaitDuration); !ok {
			metric.FSDiskCacheDownloadReadTimedOutCounter.Inc()
			return true, false, nil
		}
		metric.FSDiskCacheDownloadReadWaitedCounter.Inc()
	}
	f, ok := o.acquire()
	if !ok {
		return false, false, nil
	}
	defer o.release()
	if err := entry.readFrom(ctx, io.NewSectionReader(f, entry.Offset, entry.Size), d.cacheDataAllocator); err != nil {
		return true, false, err
	}
	// Only bytes delivered successfully count toward read share. Eviction accounting
	// waits for active readers, so publication and eviction cannot lose this read.
	o.serve(entry.Size)
	return true, true, nil
}

const (
	// downloadDuplicateRatio is how many times its own size a read must be behind a
	// download before it reads its bytes again rather than wait for them.
	downloadDuplicateRatio = 8
	// downloadRateMinBytes is written by a part before its own rate is used, and each
	// part's rate at that point is a sample of the recent downloads' rate.
	downloadRateMinBytes = 256 << 10
	// objectSplitMaxDownloads is the most whole-object downloads in flight with which
	// another download is split: beyond them the link is shared and a split adds no
	// bandwidth.
	objectSplitMaxDownloads = 4
	// rangeReadSmall is the largest range read whose time counts as latency.
	rangeReadSmall = 256 << 10
	costWeight     = 0.1
)

// downloadRateModel averages the rate of completed whole-object downloads.
type downloadRateModel struct {
	bytesPerS atomic.Uint64 // float64 bits
}

func newDownloadRateModel(bytesPerS float64) *downloadRateModel {
	m := &downloadRateModel{}
	m.set(bytesPerS)
	return m
}

var downloadRates = newDownloadRateModel(100 << 20)

func (m *downloadRateModel) observe(n int64, d time.Duration) {
	if n < downloadRateMinBytes || d <= 0 {
		return
	}
	sample := float64(n) / d.Seconds()
	for {
		old := m.bytesPerS.Load()
		rate := (1-costWeight)*math.Float64frombits(old) + costWeight*sample
		if m.bytesPerS.CompareAndSwap(old, math.Float64bits(rate)) {
			return
		}
	}
}

func (m *downloadRateModel) rate() float64 {
	return math.Float64frombits(m.bytesPerS.Load())
}

func (m *downloadRateModel) set(bytesPerS float64) {
	m.bytesPerS.Store(math.Float64bits(bytesPerS))
}

// rangeReadCost is a latency and a bandwidth of range reads.
type rangeReadCost struct {
	latency   float64 // seconds
	bytesPerS float64
}

// rangeReadCostModel estimates the time of a range read as a latency plus its size over a
// bandwidth, averaged from the range reads observed.
type rangeReadCostModel struct {
	p atomic.Pointer[rangeReadCost]
}

func newRangeReadCostModel(latency, bytesPerS float64) *rangeReadCostModel {
	m := &rangeReadCostModel{}
	m.set(latency, bytesPerS)
	return m
}

var rangeReadCosts = newRangeReadCostModel(0.02, 100<<20)

func (m *rangeReadCostModel) get() rangeReadCost {
	return *m.p.Load()
}

func (m *rangeReadCostModel) set(latency, bytesPerS float64) {
	m.p.Store(&rangeReadCost{latency: latency, bytesPerS: bytesPerS})
}

// update replaces the cost with what next returns for it, unless next reports no change.
func (m *rangeReadCostModel) update(next func(c rangeReadCost) (rangeReadCost, bool)) {
	for {
		old := m.p.Load()
		c, ok := next(*old)
		if !ok || m.p.CompareAndSwap(old, &c) {
			return
		}
	}
}

func (m *rangeReadCostModel) observe(n int64, d time.Duration) {
	if n <= 0 || d <= 0 {
		return
	}
	m.update(func(c rangeReadCost) (rangeReadCost, bool) {
		if n <= rangeReadSmall {
			c.latency = (1-costWeight)*c.latency + costWeight*d.Seconds()
			return c, true
		}
		transfer := d.Seconds() - c.latency
		if transfer <= 0 {
			return c, false
		}
		c.bytesPerS = (1-costWeight)*c.bytesPerS + costWeight*float64(n)/transfer
		return c, true
	})
}

// observeLatency adds a sample of the latency of range reads: the time until a range
// read starts to return bytes.
func (m *rangeReadCostModel) observeLatency(d time.Duration) {
	if d <= 0 {
		return
	}
	m.update(func(c rangeReadCost) (rangeReadCost, bool) {
		c.latency = (1-costWeight)*c.latency + costWeight*d.Seconds()
		return c, true
	})
}

// firstByte returns the latency of range reads.
func (m *rangeReadCostModel) firstByte() time.Duration {
	return time.Duration(m.get().latency * float64(time.Second))
}

// bandwidthDelay returns the bytes a range read could take in the time of its latency.
func (m *rangeReadCostModel) bandwidthDelay() int64 {
	c := m.get()
	return int64(c.latency * c.bytesPerS)
}

func (m *rangeReadCostModel) cost(n int64) time.Duration {
	return m.get().cost(n)
}

func (c rangeReadCost) cost(n int64) time.Duration {
	return time.Duration((c.latency + float64(n)/c.bytesPerS) * float64(time.Second))
}
