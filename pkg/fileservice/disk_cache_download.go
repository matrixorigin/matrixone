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
	written  int64
	ended    bool // no more bytes will be written
	complete bool // every byte of the object was written
	changed  chan struct{}
	file     *os.File // closed once the download is retired and no reader holds it
	readers  int
	retired  bool
	served   int64         // bytes readers took from the download before credited
	credited bool          // the cached file counts the bytes readers take
	credit   func(n int64) // counts n bytes read toward the published file
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

func newObjectDownload(tempPath string, measured bool) *objectDownload {
	o := &objectDownload{started: time.Now(), measured: measured, changed: make(chan struct{})}
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

func (o *objectDownload) advance(n int64) {
	o.mu.Lock()
	o.written += n
	o.notifyLocked()
	o.mu.Unlock()
}

func (o *objectDownload) end(complete bool) {
	o.mu.Lock()
	if !o.ended {
		o.ended, o.complete = true, complete
		o.notifyLocked()
		if complete && o.measured {
			downloadRates.observe(o.written, time.Since(o.started))
		}
	}
	o.mu.Unlock()
}

// estimate returns the bytes written and the time the download needs to write the bytes
// before end, at its rate so far, or at the recent downloads' rate before it wrote enough
// to measure its own.
func (o *objectDownload) estimate(end int64) (int64, time.Duration) {
	o.mu.Lock()
	written := o.written
	o.mu.Unlock()
	if written >= end {
		return written, 0
	}
	rate := downloadRates.rate()
	if elapsed := time.Since(o.started); written >= downloadRateMinBytes && elapsed > 0 {
		rate = float64(written) / elapsed.Seconds()
	}
	return written, time.Duration(float64(end-written) / rate * float64(time.Second))
}

// waitFor waits until the bytes before end are written, or until the whole object is when
// end is negative. It returns the bytes written, and false when they will never cover end.
func (o *objectDownload) waitFor(ctx context.Context, end int64, timeout time.Duration) (int64, bool) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		o.mu.Lock()
		written, ended, complete, changed := o.written, o.ended, o.complete, o.changed
		o.mu.Unlock()
		if end >= 0 && written >= end || end < 0 && complete {
			return written, true
		}
		if ended {
			return written, false
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return written, false
		case <-timer.C:
			return written, false
		}
	}
}

type downloadProgressWriter struct {
	w        io.Writer
	download *objectDownload
}

func (p downloadProgressWriter) Write(b []byte) (int, error) {
	n, err := p.w.Write(b)
	if n > 0 {
		p.download.advance(int64(n))
	}
	return n, err
}

type diskCacheDownloads struct {
	mu sync.Mutex
	m  map[string]*objectDownload
}

func (d *DiskCache) startDownload(diskPath, tempPath string, measured bool) *objectDownload {
	o := newObjectDownload(tempPath, measured)
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

func (d *DiskCache) download(diskPath string) *objectDownload {
	d.downloads.mu.Lock()
	defer d.downloads.mu.Unlock()
	return d.downloads.m[diskPath]
}

// readFromDownload fills entry from the download of the object at diskPath when its bytes
// are written, or will be no more than the max extra wait later than a range read could
// fetch them, or are more than an eighth of what the download writes before them. It reports whether
// a download covers the object, and whether it filled entry.
func (d *DiskCache) readFromDownload(ctx context.Context, diskPath string, entry *IOEntry) (downloading, filled bool, err error) {
	o := d.download(diskPath)
	if o == nil {
		return false, false, nil
	}
	end := entry.Offset + entry.Size
	written, wait := o.estimate(end)
	if wait == 0 {
		metric.FSDiskCacheDownloadReadWrittenCounter.Inc()
	} else {
		budget := rangeReadCosts.cost(entry.Size) + d.downloadMaxExtraWait
		if wait > budget {
			// A range read fetches these bytes again; it pays only when they are a small
			// part of what the download still writes before them.
			if entry.Size*downloadDuplicateRatio <= end-written {
				metric.FSDiskCacheDownloadReadDeclinedCounter.Inc()
				metric.FSDiskCacheDownloadDeclinedBytesCounter.Add(float64(entry.Size))
				return true, false, nil
			}
			budget += wait
		}
		if _, ok := o.waitFor(ctx, end, budget); !ok {
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
	// downloadRateMinBytes is written before a download's own rate is used.
	downloadRateMinBytes = 1 << 20
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

func (m *rangeReadCostModel) cost(n int64) time.Duration {
	m.mu.Lock()
	defer m.mu.Unlock()
	return time.Duration((m.latency + float64(n)/m.bytesPerS) * float64(time.Second))
}
