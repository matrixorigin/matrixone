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
	"os"
	"sync"
	"time"

	metric "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
)

// objectDownload is a whole-object file being written to the disk cache. Readers of the
// object read the bytes already written through the download's own handle on the file,
// which keeps them readable after the file is published or removed.
type objectDownload struct {
	started  time.Time
	measured bool // the download's rate counts toward downloadRates

	mu       sync.Mutex
	parts    []downloadPart // the ranges of the object written at once, in order
	ended    bool           // no more bytes will be written
	complete bool           // every byte of the object was written
	changed  chan struct{}
	file     *os.File // closed once the download is retired and no reader holds it
	readers  int
	retired  bool
	served   int64         // bytes readers took from the download before credited
	credited bool          // the cached file counts the bytes readers take
	credit   func(n int64) // counts n bytes read toward the published file
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
		changed:  make(chan struct{}),
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
	if o.file == nil {
		return nil, false
	}
	o.readers++
	return o.file, true
}

func (o *objectDownload) release() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.readers--
	o.closeIfUnusedLocked()
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

func (o *objectDownload) notifyLocked() {
	close(o.changed)
	o.changed = make(chan struct{})
}

// begin records that the read of the part began.
func (o *objectDownload) begin(part int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.parts[part].began.IsZero() {
		o.parts[part].began = time.Now()
	}
}

// split reports whether the download writes more than one part.
func (o *objectDownload) split() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.parts) > 1
}

// bounds returns the bounds of the parts.
func (o *objectDownload) bounds() []int64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	bounds := make([]int64, 0, len(o.parts)+1)
	for _, p := range o.parts {
		bounds = append(bounds, p.start)
	}
	return append(bounds, o.parts[len(o.parts)-1].end)
}

func (o *objectDownload) advance(part int, n int64) {
	o.mu.Lock()
	p := &o.parts[part]
	if p.first.IsZero() {
		p.first = time.Now()
	}
	sample := o.measured && p.written < downloadRateMinBytes && p.written+n >= downloadRateMinBytes
	p.written += n
	written, elapsed := p.written, time.Since(p.first)
	o.notifyLocked()
	o.mu.Unlock()
	if sample {
		downloadRates.observe(written, elapsed)
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

// progressLocked returns the bytes written of the parts holding [off, end), or of the
// parts from off on when end is negative.
func (o *objectDownload) progressLocked(off, end int64) int64 {
	var n int64
	for _, p := range o.parts {
		if p.end >= 0 && p.end <= off || end >= 0 && p.start >= end {
			continue
		}
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
		o.notifyLocked()
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
	return o.waitForProgress(ctx, off, end, timeout, 0)
}

// waitForProgress is waitFor that also gives up when the parts holding [off, end) write
// nothing for stall, unless stall is 0.
func (o *objectDownload) waitForProgress(ctx context.Context, off, end int64, timeout, stall time.Duration) (int64, bool) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	var stalled <-chan time.Time
	var stallTimer *time.Timer
	if stall > 0 {
		stallTimer = time.NewTimer(stall)
		defer stallTimer.Stop()
		stalled = stallTimer.C
	}
	last := int64(-1)
	for {
		o.mu.Lock()
		written, ended, complete, changed := o.availableLocked(off), o.ended, o.complete, o.changed
		progress := o.progressLocked(off, end)
		o.mu.Unlock()
		if end >= 0 && written >= end || end < 0 && complete {
			return written, true
		}
		if ended {
			return written, false
		}
		if stallTimer != nil && last >= 0 && progress > last {
			stallTimer.Reset(stall)
		}
		last = progress
		select {
		case <-changed:
		case <-stalled:
			return written, false
		case <-ctx.Done():
			return written, false
		case <-timer.C:
			return written, false
		}
	}
}

// downloadStall is how long a read waits for the bytes of the download it chose to
// grow before it reads its range.
func downloadStall() time.Duration {
	return max(downloadStallMin, 4*rangeReadCosts.firstByte())
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

// objectRangeOpener opens a range [start, end) of the object it reads.
type objectRangeOpener interface {
	openRange(ctx context.Context, start, end int64) (io.ReadCloser, error)
}

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

func (l *lazyObjectReader) openRange(ctx context.Context, start, end int64) (io.ReadCloser, error) {
	return l.read(ctx, &start, &end)
}

// copyParts writes the parts of download into f at once, each read through open, and
// returns the bytes written. A failed part fails the copy.
func copyParts(ctx context.Context, f *os.File, download *objectDownload, open objectRangeOpener) (int64, error) {
	bounds := download.bounds()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	errs := make([]error, len(bounds)-1)
	var wg sync.WaitGroup
	for i := range errs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if errs[i] = copyPart(ctx, f, download, open, i, bounds[i], bounds[i+1]); errs[i] != nil {
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

func copyPart(ctx context.Context, f *os.File, download *objectDownload, open objectRangeOpener, part int, start, end int64) error {
	download.begin(part)
	r, err := open.openRange(ctx, start, end)
	if err != nil {
		return &diskCacheSourceError{err: err}
	}
	defer r.Close()
	source := &diskCacheSourceReader{Reader: r}
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

type diskCacheDownloads struct {
	mu sync.Mutex
	m  map[string]*objectDownload
}

func (d *DiskCache) startDownload(diskPath, tempPath string, measured bool, bounds []int64) *objectDownload {
	o := newObjectDownload(tempPath, measured, bounds)
	o.credit = func(n int64) { d.preloads.read(diskPath, n) }
	d.downloads.mu.Lock()
	if d.downloads.m == nil {
		d.downloads.m = make(map[string]*objectDownload)
	}
	d.downloads.m[diskPath] = o
	d.downloads.mu.Unlock()
	return o
}

func (d *DiskCache) finishDownload(diskPath string, o *objectDownload) {
	o.end(false)
	d.downloads.mu.Lock()
	if d.downloads.m[diskPath] == o {
		delete(d.downloads.m, diskPath)
	}
	d.downloads.mu.Unlock()
	o.retire()
}

// downloadConnLimit is the most object-storage connections whole-object downloads hold.
func (d *DiskCache) downloadConnLimit() int64 {
	if d.maxDownloadConns > 0 {
		return d.maxDownloadConns
	}
	return int64(maxConnsPerHost / 2)
}

// prefetchWorkers is the most prefetches run at once, a quarter of the connections
// downloads may hold: past it more streams share the bandwidth without adding any.
func (d *DiskCache) prefetchWorkers() int {
	return int(max(d.downloadConnLimit()/4, 4))
}

// reserveDownloadConns reserves n of the connections whole-object downloads may hold,
// half the connections to the object storage host, or reports that they are taken.
func (d *DiskCache) reserveDownloadConns(n int64) bool {
	limit := d.downloadConnLimit()
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
	d.downloads.mu.Lock()
	defer d.downloads.mu.Unlock()
	return d.downloads.m[diskPath]
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
		if eta, ok := d.fastestPrefetch(diskPath, entry.Offset, end); ok && eta < wait && d.canPrefetch(vector) {
			// A prefetch range being read delivers the bytes sooner.
			return true, false, nil
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
		// The read waits for the download it chose while its bytes keep coming.
		if _, ok := o.waitForProgress(ctx, entry.Offset, end, maxIOWaitDuration, downloadStall()); !ok {
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
	// The bytes count before they are read, so a file published and evicted while the
	// read runs still has them.
	o.serve(entry.Size)
	if err := entry.readFrom(ctx, io.NewSectionReader(f, entry.Offset, entry.Size), d.cacheDataAllocator); err != nil {
		return true, false, err
	}
	return true, true, nil
}

const (
	// downloadDuplicateRatio is how many times its own size a read must be behind a
	// download before it reads its bytes again rather than wait for them.
	downloadDuplicateRatio = 8
	// downloadRateMinBytes is written by a part before its own rate is used, and each
	// part's rate at that point is a sample of the recent downloads' rate.
	downloadRateMinBytes = 256 << 10
	// downloadStallMin is the least time a read waits for the bytes of its download to
	// grow.
	downloadStallMin = 100 * time.Millisecond
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
	mu        sync.Mutex
	bytesPerS float64
}

var downloadRates = &downloadRateModel{bytesPerS: 100 << 20}

func (m *downloadRateModel) observe(n int64, d time.Duration) {
	if n < downloadRateMinBytes || d <= 0 {
		return
	}
	m.mu.Lock()
	m.bytesPerS = (1-costWeight)*m.bytesPerS + costWeight*float64(n)/d.Seconds()
	m.mu.Unlock()
}

func (m *downloadRateModel) rate() float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.bytesPerS
}

// rangeReadCostModel estimates the time of a range read as a latency plus its size over a
// bandwidth, averaged from the range reads observed.
type rangeReadCostModel struct {
	mu        sync.Mutex
	latency   float64 // seconds
	bytesPerS float64
}

var rangeReadCosts = &rangeReadCostModel{latency: 0.02, bytesPerS: 100 << 20}

func (m *rangeReadCostModel) observe(n int64, d time.Duration) {
	if n <= 0 || d <= 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if n <= rangeReadSmall {
		m.latency = (1-costWeight)*m.latency + costWeight*d.Seconds()
		return
	}
	if transfer := d.Seconds() - m.latency; transfer > 0 {
		m.bytesPerS = (1-costWeight)*m.bytesPerS + costWeight*float64(n)/transfer
	}
}

// observeLatency adds a sample of the latency of range reads: the time until a range
// read starts to return bytes.
func (m *rangeReadCostModel) observeLatency(d time.Duration) {
	if d <= 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.latency = (1-costWeight)*m.latency + costWeight*d.Seconds()
}

// firstByte returns the latency of range reads.
func (m *rangeReadCostModel) firstByte() time.Duration {
	m.mu.Lock()
	defer m.mu.Unlock()
	return time.Duration(m.latency * float64(time.Second))
}

// bandwidthDelay returns the bytes a range read could take in the time of its latency.
func (m *rangeReadCostModel) bandwidthDelay() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return int64(m.latency * m.bytesPerS)
}

func (m *rangeReadCostModel) cost(n int64) time.Duration {
	m.mu.Lock()
	defer m.mu.Unlock()
	return time.Duration((m.latency + float64(n)/m.bytesPerS) * float64(time.Second))
}
