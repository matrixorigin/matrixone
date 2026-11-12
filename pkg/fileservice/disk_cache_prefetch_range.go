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
	"bytes"
	"context"
	"io"
	"math/bits"
	"os"
	pathpkg "path"
	"slices"
	"strings"
	"sync"
	"time"
)

const (
	// The automatic prefetch unit is the bandwidth-delay product of range reads,
	// rounded up to a power of two between prefetchRangeAutoMin and prefetchRangeAutoMax.
	prefetchRangeAutoMin = 64 << 10
	prefetchRangeAutoMax = 8 << 20
)

// prefetchRangeConfig sets the prefetch ranges of range reads.
type prefetchRangeConfig struct {
	// on enables prefetch ranges; only tests turn them off.
	on bool
	// fixed, when positive, is the unit instead of the bandwidth-delay product of range
	// reads.
	fixed int64
}

// unit returns the size of prefetch ranges, 0 when they are off.
func (c prefetchRangeConfig) unit() int64 {
	if !c.on {
		return 0
	}
	if c.fixed > 0 {
		return c.fixed
	}
	return autoPrefetchUnit(rangeReadCosts.bandwidthDelay())
}

func autoPrefetchUnit(bdp int64) int64 {
	if bdp <= prefetchRangeAutoMin {
		return prefetchRangeAutoMin
	}
	if bdp >= prefetchRangeAutoMax {
		return prefetchRangeAutoMax
	}
	return 1 << bits.Len64(uint64(bdp-1))
}

// prefetchUnit returns the prefetch unit for a read of the vector, or false to read
// its ranges alone. Only objects whose range files the range index holds, those with
// no separator in their path, read through prefetch ranges.
func (d *DiskCache) prefetchUnit(vector *IOVector) (int64, bool) {
	unit := d.prefetchRange.unit()
	if unit <= 0 || vector.Policy.Any(SkipDiskCacheReads, SkipDiskCacheWrites, ColumnPrefetch, ExactRead) || !vector.Policy.CacheIOEntry() {
		return 0, false
	}
	if path, err := ParsePath(vector.FilePath); err != nil || strings.Contains(pathpkg.Clean(path.File), "/") {
		return 0, false
	}
	for i := range vector.Entries {
		e := &vector.Entries[i]
		if !e.done && (e.Size <= 0 || e.WriterForRead != nil || e.ReadCloserForRead != nil) {
			return 0, false
		}
	}
	return unit, true
}

// canPrefetch reports whether a read of the vector reads through prefetch ranges.
func (d *DiskCache) canPrefetch(vector *IOVector) bool {
	_, ok := d.prefetchUnit(vector)
	return ok
}

const (
	// prefetchBlockMax is the largest block of the grid prefetch ranges are made of.
	prefetchBlockMax = 64 << 10
	// prefetchWindowBlocks is the most blocks a read through prefetch ranges spans.
	prefetchWindowBlocks = 4096
	// prefetchChunk is the most bytes of a prefetch range read at a time.
	prefetchChunk = 64 << 10
	// prefetchFlightBytesMax is the most bytes of prefetch range runs under way.
	prefetchFlightBytesMax = 256 << 20
)

func (d *DiskCache) prefetchFlightBytesLimit() int64 {
	if d.prefetchFlightBytes > 0 {
		return d.prefetchFlightBytes
	}
	return prefetchFlightBytesMax
}

type blockState int

const (
	blockMissing blockState = iota
	blockCached
	blockInFlight
)

// prefetchBlock is a block of a prefetch window and where its bytes come from.
type prefetchBlock struct {
	start, end int64 // the block's bytes the read takes, or all of them
	needed     bool  // the read takes bytes of the block
	wanted     bool  // the block is in a unit the read takes bytes of
	state      blockState
	cached     cachedRange
	flight     *prefetchFlight
}

// prefetchRead returns a buffer of the bytes [lo, hi) of filePath that holds the bytes
// of the entries not done, and the number of range reads it started. It splits the
// unit-aligned window around them into blocks and takes each block an entry needs from
// a cached range, from the read of a prefetch range under way whose bytes reach the
// block first, or from a new read: each run of blocks in units the entries take bytes
// of that are neither cached nor being read and holds a needed block is read from
// object storage through open as one range, served to other reads as its bytes arrive,
// and cached. It returns false when the bytes are not all read.
func (d *DiskCache) prefetchRead(
	ctx context.Context,
	filePath string,
	policy Policy,
	entries []IOEntry,
	lo, hi, unit int64,
	open func(ctx context.Context, start, end int64) (io.ReadCloser, error),
) ([]byte, int, bool) {
	if lo < 0 || hi <= lo || ctx.Err() != nil {
		return nil, 0, false
	}
	block := int64(prefetchBlockMax)
	if unit < block || unit%block != 0 {
		block = unit
	}
	windowStart := lo / unit * unit
	windowEnd := (hi + unit - 1) / unit * unit
	if (windowEnd-windowStart)/block > prefetchWindowBlocks {
		return nil, 0, false
	}
	key := d.rangeObjectKey(filePath)
	diskPath := d.pathForFile(filePath)
	ranges := d.ranges.of(key)

	n := int((windowEnd - windowStart) / block)
	needed := make([]bool, n)
	for i := range entries {
		e := &entries[i]
		if e.done || e.Size <= 0 {
			continue
		}
		for j := (e.Offset - windowStart) / block; j <= (e.Offset+e.Size-1-windowStart)/block; j++ {
			needed[j] = true
		}
	}
	perUnit := int(unit / block)
	wanted := make([]bool, (n+perUnit-1)/perUnit)
	for j := range needed {
		wanted[j/perUnit] = wanted[j/perUnit] || needed[j]
	}

	blocks := make([]prefetchBlock, 0, n)
	for j, start := 0, windowStart; start < windowEnd; j, start = j+1, start+block {
		b := prefetchBlock{start: start, end: start + block, wanted: wanted[j/perUnit]}
		if needed[j] {
			b.needed = true
			b.start, b.end = max(b.start, lo), min(b.end, hi)
		}
		if r, ok := smallestContaining(ranges, b.start, b.end-b.start, nil); ok && d.cache.Contains(r.diskPath) {
			b.state, b.cached = blockCached, r
		}
		blocks = append(blocks, b)
	}

	// Find the blocks being read and register the runs to read under one lock, so
	// concurrent reads never read a block twice.
	type run struct {
		first, last int // block indexes
		flight      *prefetchFlight
	}
	var runs []run
	f := &d.prefetchFlights
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return nil, 0, false
	}
	if f.closing == nil {
		f.closing, f.cancelReads = context.WithCancel(context.Background())
	}
	closing := f.closing
	addRun := func(first, last int, now time.Time) {
		start := windowStart + int64(first)*block
		end := windowStart + int64(last+1)*block
		f.bytes += end - start
		fl := newPrefetchFlight(start, end, now)
		fl.readDeletes = d.readDeletes()
		// The read outlives the read that starts it, not the cache or a delete of the
		// object.
		fl.ctx, fl.cancel = context.WithCancel(context.WithoutCancel(ctx))
		if f.m == nil {
			f.m = make(map[string][]*prefetchFlight)
		}
		f.m[diskPath] = append(f.m[diskPath], fl)
		runs = append(runs, run{first: first, last: last, flight: fl})
		f.reads.Add(1)
	}
	now := time.Now()
	for i := range blocks {
		b := &blocks[i]
		if b.state != blockMissing {
			continue
		}
		if fl, _ := f.fastestLocked(diskPath, b.start, b.end, now); fl != nil {
			b.state, b.flight = blockInFlight, fl
		}
	}
	var pending [][2]int
	for i := 0; i < len(blocks); {
		if blocks[i].state != blockMissing || !blocks[i].wanted {
			i++
			continue
		}
		j, first := i, -1
		for j < len(blocks) && blocks[j].state == blockMissing && blocks[j].wanted {
			if first < 0 && blocks[j].needed {
				first = j
			}
			j++
		}
		if first >= 0 {
			pending = append(pending, [2]int{first, j - 1})
			if first > i {
				pending = append(pending, [2]int{i, first - 1})
			}
		}
		i = j
	}
	var need int64
	for _, p := range pending {
		need += int64(p[1]-p[0]+1) * block
	}
	if need > 0 && f.bytes+need > d.prefetchFlightBytesLimit() {
		// Past the bytes of the runs under way, the read reads its own entries.
		f.mu.Unlock()
		return nil, 0, false
	}
	for _, p := range pending {
		addRun(p[0], p[1], now)
	}
	f.mu.Unlock()

	// Read the runs. A run serves its bytes to waiting reads as they arrive and stays
	// registered until its range file is cached, so no read reads it again. A run
	// outlives the read that starts it, which may return before its last bytes arrive.
	for i, r := range runs {
		go d.runFlight(ctx, filePath, diskPath, policy, r.flight, open, closing)
		for j := r.first; j <= r.last; j++ {
			if blocks[j].needed {
				blocks[j].state, blocks[j].flight = blockInFlight, runs[i].flight
			}
		}
	}

	// Assemble the bytes.
	out := make([]byte, hi-lo)
	var deadline time.Time
	for i := 0; i < len(blocks); i++ {
		b := &blocks[i]
		if !b.needed {
			continue
		}
		dst := out[b.start-lo : b.end-lo]
		switch b.state {
		case blockInFlight:
			content, ok := b.flight.waitFor(ctx, b.end)
			if !ok {
				return nil, len(runs), false
			}
			copy(dst, content[b.start-b.flight.start:b.end-b.flight.start])
		case blockCached:
			j := i
			for j+1 < len(blocks) && blocks[j+1].needed && blocks[j+1].state == blockCached &&
				blocks[j+1].cached.diskPath == b.cached.diskPath {
				j++
			}
			dst = out[b.start-lo : blocks[j].end-lo]
			if !d.readCachedRange(ctx, b.cached, b.start, dst, &deadline) {
				return nil, len(runs), false
			}
			i = j
		default:
			return nil, len(runs), false
		}
	}
	return out, len(runs), true
}

// runFlight reads the flight fl of the object at filePath through open, caches the range
// it read, and forgets the flight. The flight outlives the read that registered it, and is
// canceled by a delete of the object or the closing of the cache.
func (d *DiskCache) runFlight(
	ctx context.Context,
	filePath, diskPath string,
	policy Policy,
	fl *prefetchFlight,
	open func(ctx context.Context, start, end int64) (io.ReadCloser, error),
	closing context.Context,
) {
	defer d.prefetchFlights.reads.Done()
	defer fl.cancel()
	defer context.AfterFunc(closing, fl.cancel)()
	err := fl.read(fl.ctx, open)
	content := fl.content()
	if err == nil && len(content) == 0 {
		err = io.ErrUnexpectedEOF
	}
	fl.finish()
	if err == nil {
		_ = d.Update(context.WithoutCancel(ctx), &IOVector{
			FilePath: filePath,
			Policy:   policy,
			Entries:  []IOEntry{{Offset: fl.start, Size: int64(len(content)), Data: content, readDeletes: fl.readDeletes}},
		}, false)
	}
	d.endPrefetch(diskPath, fl)
}

// readRanges reads the ranges of the object at filePath through open as flights, which
// reads of their bytes join, caches each, and returns once they end or ctx is done. A range
// a flight already reads is waited for, not read again. It reports false, reading nothing,
// when the cache is closed or the ranges would pass the bytes of the flights under way.
func (d *DiskCache) readRanges(
	ctx context.Context,
	filePath string,
	policy Policy,
	ranges []PrefetchRange,
	open func(ctx context.Context, start, end int64) (io.ReadCloser, error),
) bool {
	diskPath := d.pathForFile(filePath)
	f := &d.prefetchFlights
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return false
	}
	if f.closing == nil {
		f.closing, f.cancelReads = context.WithCancel(context.Background())
	}
	closing := f.closing
	now := time.Now()
	var missing []PrefetchRange
	var waits []*prefetchFlight
	var need int64
	for _, r := range ranges {
		if fl, _ := f.fastestLocked(diskPath, r.Offset, r.Offset+r.Size, now); fl != nil {
			waits = append(waits, fl)
			continue
		}
		missing = append(missing, r)
		need += r.Size
	}
	if need > 0 && f.bytes+need > d.prefetchFlightBytesLimit() {
		f.mu.Unlock()
		return false
	}
	started := make([]*prefetchFlight, 0, len(missing))
	for _, r := range missing {
		f.bytes += r.Size
		fl := newPrefetchFlight(r.Offset, r.Offset+r.Size, now)
		fl.readDeletes = d.readDeletes()
		fl.ctx, fl.cancel = context.WithCancel(context.WithoutCancel(ctx))
		if f.m == nil {
			f.m = make(map[string][]*prefetchFlight)
		}
		f.m[diskPath] = append(f.m[diskPath], fl)
		f.reads.Add(1)
		started = append(started, fl)
	}
	f.mu.Unlock()
	for _, fl := range started {
		go d.runFlight(ctx, filePath, diskPath, policy, fl, open, closing)
	}
	for _, fl := range append(waits, started...) {
		select {
		case <-fl.ended:
		case <-ctx.Done():
			return true
		}
	}
	return true
}

// readFromFlights fills the entries of vector not done, of the object at filePath, from the
// flights reading their bytes, waiting for the bytes to arrive. An entry no flight holds,
// or whose flight ends without its bytes, is left to the caller.
func (d *DiskCache) readFromFlights(ctx context.Context, filePath string, vector *IOVector, allocator CacheDataAllocator) error {
	diskPath := d.pathForFile(filePath)
	f := &d.prefetchFlights
	for i := range vector.Entries {
		e := &vector.Entries[i]
		if e.done || e.Size <= 0 {
			continue
		}
		end := e.Offset + e.Size
		f.mu.Lock()
		fl, _ := f.fastestLocked(diskPath, e.Offset, end, time.Now())
		f.mu.Unlock()
		if fl == nil {
			continue
		}
		content, ok := fl.waitFor(ctx, end)
		if !ok {
			if err := ctx.Err(); err != nil {
				return err
			}
			continue
		}
		if err := e.readFrom(ctx, bytes.NewReader(content[e.Offset-fl.start:end-fl.start]), allocator); err != nil {
			return err
		}
		e.done = true
		e.fromCache = d
	}
	return nil
}

// readCachedRange reads len(dst) bytes at offset of the object from the cached range r.
func (d *DiskCache) readCachedRange(ctx context.Context, r cachedRange, offset int64, dst []byte, deadline *time.Time) bool {
	generation, ok := d.startPathReadWithin(ctx, r.diskPath, deadline, shortIOWaitDuration)
	if !ok {
		return false
	}
	defer d.finishPathRead(r.diskPath, generation)
	file, err := os.Open(r.diskPath)
	if err != nil {
		d.ranges.remove(r.diskPath)
		return false
	}
	defer file.Close()
	if _, err := file.ReadAt(dst, offset-r.offset); err != nil {
		return false
	}
	d.cache.Get(ctx, r.diskPath)
	return true
}

// prefetchFlight is a prefetch range being read from object storage into buf.
type prefetchFlight struct {
	start, end int64
	began      time.Time
	buf        []byte

	// ctx is the read's; cancel ends it. ended is closed when the flight, and the cache
	// write of its range, are done.
	ctx    context.Context
	cancel context.CancelFunc
	ended  chan struct{}

	mu       sync.Mutex
	n        int64         // bytes of buf read
	finished bool          // no more bytes arrive
	progress chan struct{} // closed when n grows or the flight finishes
	// readDeletes marks the range's cache entry; see IOEntry.readDeletes.
	readDeletes uint64
}

func newPrefetchFlight(start, end int64, now time.Time) *prefetchFlight {
	return &prefetchFlight{start: start, end: end, began: now, buf: make([]byte, end-start),
		progress: make(chan struct{}), ended: make(chan struct{})}
}

// dropPrefetches cancels the reads of prefetch ranges of the object at diskPath and
// waits for them and their cache writes to end, or ctx.
func (d *DiskCache) dropPrefetches(ctx context.Context, diskPath string) error {
	f := &d.prefetchFlights
	f.mu.Lock()
	flights := slices.Clone(f.m[diskPath])
	for _, fl := range flights {
		fl.cancel()
	}
	f.mu.Unlock()
	for _, fl := range flights {
		select {
		case <-fl.ended:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// read reads the range through open into buf, chunk by chunk, publishing each chunk,
// and reads the reader to its end so its connection is reused. The time open takes is
// a sample of the latency of range reads.
func (f *prefetchFlight) read(ctx context.Context, open func(ctx context.Context, start, end int64) (io.ReadCloser, error)) error {
	t0 := time.Now()
	r, err := open(ctx, f.start, f.end)
	if err != nil {
		return err
	}
	defer r.Close()
	rangeReadCosts.observeLatency(time.Since(t0))
	for pos := 0; pos < len(f.buf); {
		n, err := r.Read(f.buf[pos:min(pos+prefetchChunk, len(f.buf))])
		if n > 0 {
			pos += n
			f.wrote(n)
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
	_, err = io.Copy(io.Discard, r)
	return err
}

// wrote publishes n more bytes of buf.
func (f *prefetchFlight) wrote(n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.n += int64(n)
	close(f.progress)
	f.progress = make(chan struct{})
}

// finish ends the flight; bytes not read by then never arrive.
func (f *prefetchFlight) finish() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.finished {
		f.finished = true
		close(f.progress)
	}
}

// content returns the bytes read.
func (f *prefetchFlight) content() []byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.buf[:f.n]
}

// eta estimates how long after now the bytes of the flight up to end arrive.
func (f *prefetchFlight) eta(now time.Time, end int64) time.Duration {
	return max(rangeReadCosts.cost(end-f.start)-now.Sub(f.began), 0)
}

// waitFor returns the bytes read once they reach end, or false if the flight finished
// short of it or ctx ended first.
func (f *prefetchFlight) waitFor(ctx context.Context, end int64) ([]byte, bool) {
	for {
		f.mu.Lock()
		if f.start+f.n >= end {
			content := f.buf[:f.n]
			f.mu.Unlock()
			return content, true
		}
		if f.finished {
			f.mu.Unlock()
			return nil, false
		}
		progress := f.progress
		f.mu.Unlock()
		select {
		case <-progress:
		case <-ctx.Done():
			return nil, false
		}
	}
}

// prefetchFlights indexes the prefetch ranges being read by the disk path of their
// object.
type prefetchFlights struct {
	mu sync.Mutex
	m  map[string][]*prefetchFlight
	// bytes is the size of the runs under way.
	bytes  int64
	closed bool
	reads  sync.WaitGroup // reads of runs and their cache writes
	// closing is canceled when the cache closes; it cancels the reads of runs.
	closing     context.Context
	cancelReads context.CancelFunc
}

// closePrefetches stops new prefetch reads, cancels those under way and waits for them,
// or ctx.
func (d *DiskCache) closePrefetches(ctx context.Context) {
	f := &d.prefetchFlights
	f.mu.Lock()
	f.closed = true
	cancelReads := f.cancelReads
	f.mu.Unlock()
	if cancelReads != nil {
		cancelReads()
	}
	done := make(chan struct{})
	go func() {
		f.reads.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
	}
}

// fastestLocked returns the flight of diskPath containing [lo, hi) whose bytes reach hi
// first.
func (f *prefetchFlights) fastestLocked(diskPath string, lo, hi int64, now time.Time) (*prefetchFlight, time.Duration) {
	var best *prefetchFlight
	var bestETA time.Duration
	for _, fl := range f.m[diskPath] {
		if fl.start > lo || fl.end < hi {
			continue
		}
		if eta := fl.eta(now, hi); best == nil || eta < bestETA {
			best, bestETA = fl, eta
		}
	}
	return best, bestETA
}

// fastestPrefetch returns the time until the bytes [lo, hi) of diskPath arrive from the
// fastest flight containing them, or false if none does.
func (d *DiskCache) fastestPrefetch(diskPath string, lo, hi int64) (time.Duration, bool) {
	f := &d.prefetchFlights
	f.mu.Lock()
	defer f.mu.Unlock()
	fl, eta := f.fastestLocked(diskPath, lo, hi, time.Now())
	return eta, fl != nil
}

// endPrefetch finishes a flight the caller started and forgets it.
func (d *DiskCache) endPrefetch(diskPath string, fl *prefetchFlight) {
	f := &d.prefetchFlights
	f.mu.Lock()
	flights := f.m[diskPath]
	for i := range flights {
		if flights[i] == fl {
			flights = append(flights[:i], flights[i+1:]...)
			break
		}
	}
	if len(flights) == 0 {
		delete(f.m, diskPath)
	} else {
		f.m[diskPath] = flights
	}
	f.bytes -= fl.end - fl.start
	f.mu.Unlock()
	fl.finish()
	close(fl.ended)
}
