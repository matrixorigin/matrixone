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
	"errors"
	"io"
	"math"
	"math/bits"
	"os"
	pathpkg "path"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/system"
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

const (
	// prefetchBlockMax is the largest block of the grid prefetch ranges are made of.
	prefetchBlockMax = 64 << 10
	// prefetchWindowBlocks is the most blocks a read through prefetch ranges spans.
	prefetchWindowBlocks = 4096
	// prefetchChunk is the most bytes of a prefetch range read at a time.
	prefetchChunk = 64 << 10
	// prefetchFlightBytesMax is the most bytes of prefetch range runs under way when the
	// memory available on the node cannot be measured.
	prefetchFlightBytesMax = 256 << 20
	// flightMemoryPercent is the share, in percent, of the memory available on the node
	// that the prefetch range runs under way may hold.
	flightMemoryPercent = 10
	// flightBudgetRefresh is how often the memory available on the node is measured.
	flightBudgetRefresh = time.Second
)

// memoryAvailable measures the memory available on the node.
var memoryAvailable = system.MemoryAvailableIncludingCache

// flightBudget is the most bytes of the prefetch range runs under way, as last measured.
type flightBudget struct {
	bytes atomic.Int64
	at    atomic.Int64 // when measured, in Unix nanoseconds
}

// flightBytes returns the most bytes of prefetch range runs under way: prefetchFlightBytes
// when set, otherwise a share of the memory available on the node, at least one
// MemoryPrefetchObject's bytes.
func (d *DiskCache) flightBytes() int64 {
	if d.prefetchFlightBytes > 0 {
		return d.prefetchFlightBytes
	}
	b := &d.flightBudget
	now := time.Now().UnixNano()
	at := b.at.Load()
	bytes := b.bytes.Load()
	// One caller measures once the last measurement is stale; the others keep it meanwhile.
	if bytes > 0 && (now-at < int64(flightBudgetRefresh) || !b.at.CompareAndSwap(at, now)) {
		return bytes
	}
	bytes = int64(prefetchFlightBytesMax)
	if avail, ok := memoryAvailable(); ok {
		bytes = int64(min(avail/100*flightMemoryPercent, math.MaxInt64))
	}
	bytes = max(bytes, d.memoryPrefetchMax)
	b.bytes.Store(bytes)
	b.at.Store(now)
	return bytes
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
	// object is the prefetch object holding the block's bytes.
	object prefetchObject
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
	open rangeOpener,
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
		flight      *memoryPrefetchObject
	}
	var runs []run
	// The flights the bytes are copied from keep their buffers until the copy ends.
	var held []prefetchObject
	budget := d.flightBytes()
	defer func() {
		for _, fl := range held {
			fl.release()
		}
	}()
	f := &d.prefetchObjects
	f.mu.Lock()
	var (
		closing context.Context
		now     time.Time
		pending [][2]int
		files   [][2]int
	)
	for {
		var ok bool
		if closing, ok = f.openLocked(); !ok {
			f.mu.Unlock()
			return nil, 0, false
		}
		now = time.Now()
		for i := range blocks {
			b := &blocks[i]
			if b.state != blockMissing {
				continue
			}
			if o := f.fastestLocked(diskPath, b.start, b.end, now, d.downloadMaxExtraWait); o != nil {
				b.state, b.object = blockInFlight, o
				o.hold()
				held = append(held, o)
			}
		}
		pending, files = pending[:0], files[:0]
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
		// A run past the bytes of a MemoryPrefetchObject is a FilePrefetchObject.
		var need int64
		memory := pending[:0]
		for _, p := range pending {
			if size := int64(p[1]-p[0]+1) * block; size > d.memoryPrefetchMax {
				files = append(files, p)
			} else {
				memory = append(memory, p)
				need += flightBufSize(size)
			}
		}
		pending = memory
		if need == 0 || f.bytes == 0 || f.bytes+need <= budget {
			break
		}
		// Past the bytes of the runs under way, the read waits for runs to end, then
		// looks again at what is cached or being read.
		for _, fl := range held {
			fl.release()
		}
		held = held[:0]
		for i := range blocks {
			if blocks[i].state == blockInFlight {
				blocks[i].state, blocks[i].object = blockMissing, nil
			}
		}
		if f.freed == nil {
			f.freed = make(chan struct{})
		}
		freed := f.freed
		f.mu.Unlock()
		select {
		case <-freed:
		case <-ctx.Done():
			return nil, 0, false
		}
		budget = d.flightBytes()
		f.mu.Lock()
	}
	for _, p := range pending {
		start, end := windowStart+int64(p[0])*block, windowStart+int64(p[1]+1)*block
		fl := d.startFlightLocked(ctx, diskPath, start, end, now)
		fl.hold()
		held = append(held, fl)
		runs = append(runs, run{first: p[0], last: p[1], flight: fl})
	}
	f.mu.Unlock()

	// Read the runs. A run serves its bytes to waiting reads as they arrive and stays
	// registered until its range file is cached, so no read reads it again. A run
	// outlives the read that starts it, which may return before its last bytes arrive.
	for i, r := range runs {
		go d.runFlight(ctx, filePath, diskPath, policy, r.flight, open, closing)
		for j := r.first; j <= r.last; j++ {
			if blocks[j].needed {
				blocks[j].state, blocks[j].object = blockInFlight, runs[i].flight
			}
		}
	}
	for _, p := range files {
		start, end := windowStart+int64(p[0])*block, windowStart+int64(p[1]+1)*block
		o := d.startFileRun(ctx, closing, filePath, start, end, open)
		if o == nil {
			return nil, len(runs), false
		}
		for j := p[0]; j <= p[1]; j++ {
			if blocks[j].needed {
				blocks[j].state, blocks[j].object = blockInFlight, o
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
			if err := b.object.use(ctx, b.start, int64(len(dst)), func(bs []byte) error {
				copy(dst, bs)
				return nil
			}); err != nil {
				return nil, len(runs), false
			}
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

// startFileRun starts, as a FilePrefetchObject that outlives the read and ends when the
// cache closes, the download of the bytes [start, end) of the object at filePath, and
// returns it once reads can join it, or nil if it downloads nothing.
func (d *DiskCache) startFileRun(ctx, closing context.Context, filePath string, start, end int64, open rangeOpener) prefetchObject {
	joinable := make(chan struct{})
	mark := sync.OnceFunc(func() { close(joinable) })
	runCtx, cancel := context.WithCancel(withDownloadStarted(context.WithoutCancel(ctx), mark))
	stop := context.AfterFunc(closing, cancel)
	d.prefetchObjects.reads.Add(1)
	go func() {
		defer d.prefetchObjects.reads.Done()
		defer stop()
		defer cancel()
		defer mark()
		_, _ = d.prefetchFile(runCtx, filePath, start, end, open)
	}()
	select {
	case <-joinable:
	case <-ctx.Done():
		return nil
	}
	f := &d.prefetchObjects
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, o := range f.m[d.pathForFile(filePath)] {
		if p, ok := o.(*filePrefetchObject); ok && p.start == start && p.end == end && p.joinable() {
			return p
		}
	}
	return nil
}

// readDownload reads the bytes of download o from off into dst, once they are written.
func readDownload(ctx context.Context, o *objectDownload, off int64, dst []byte) bool {
	if _, ok := o.waitFor(ctx, off, off+int64(len(dst)), maxIOWaitDuration); !ok {
		return false
	}
	f, ok := o.acquire()
	if !ok {
		return false
	}
	defer o.release()
	_, err := f.ReadAt(dst, off)
	return err == nil
}

// fetchRange returns the bytes [start, end) of the object at filePath from a prefetch
// object holding them: one under way, or a MemoryPrefetchObject of at most
// disk-prefetch-memory-max bytes it starts and runs itself, which caches nothing. It
// reports whether it returned them, and whether the prefetch object caches them.
func (d *DiskCache) fetchRange(
	ctx context.Context,
	filePath string,
	policy Policy,
	start, end int64,
	open rangeOpener,
) (content []byte, ok, cached bool, err error) {
	if start < 0 || end <= start {
		return nil, false, false, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, false, false, err
	}
	diskPath := d.pathForFile(filePath)
	out := make([]byte, end-start)
	f := &d.prefetchObjects
	budget := d.flightBytes()
	f.mu.Lock()
	var (
		closing context.Context
		o       prefetchObject
		started *memoryPrefetchObject
	)
	for {
		var isOpen bool
		if closing, isOpen = f.openLocked(); !isOpen {
			f.mu.Unlock()
			return nil, false, false, nil
		}
		now := time.Now()
		if o = f.fastestLocked(diskPath, start, end, now, d.downloadMaxExtraWait); o != nil {
			o.hold()
			break
		}
		if end-start > d.memoryPrefetchMax {
			break
		}
		if need := flightBufSize(end - start); f.bytes == 0 || f.bytes+need <= budget {
			started = d.startFlightLocked(ctx, diskPath, start, end, now)
			started.skipCache = true
			started.hold()
			o = started
			break
		}
		// Past the bytes of the runs under way, the read waits for runs to end.
		if f.freed == nil {
			f.freed = make(chan struct{})
		}
		freed := f.freed
		f.mu.Unlock()
		select {
		case <-freed:
		case <-ctx.Done():
			return nil, false, false, ctx.Err()
		}
		budget = d.flightBytes()
		f.mu.Lock()
	}
	f.mu.Unlock()

	if o == nil {
		return nil, false, false, nil
	}
	defer o.release()
	if started != nil {
		go d.runFlight(ctx, filePath, diskPath, policy, started, open, closing)
	}
	// A prefetch object that failed to read the bytes fails the read; one that ended
	// without them leaves the read to the caller.
	if err := o.use(ctx, start, end-start, func(bs []byte) error {
		copy(out, bs)
		return nil
	}); err != nil {
		if errors.Is(err, errPrefetchMissed) {
			return nil, false, false, nil
		}
		return nil, false, false, err
	}
	cached = true
	if m, isMemory := o.(*memoryPrefetchObject); isMemory {
		cached = !m.skipCache
	}
	return out, true, cached, nil
}

// runFlight reads the flight fl of the object at filePath through open, caches the range
// it read, and forgets the flight. The flight outlives the read that registered it, and is
// canceled by a delete of the object or the closing of the cache.
func (d *DiskCache) runFlight(
	ctx context.Context,
	filePath, diskPath string,
	policy Policy,
	fl *memoryPrefetchObject,
	open rangeOpener,
	closing context.Context,
) {
	defer d.prefetchObjects.reads.Done()
	defer fl.cancelRead()
	defer context.AfterFunc(closing, fl.cancelRead)()
	err := fl.read(fl.ctx, open)
	content := fl.content()
	if err == nil && len(content) == 0 {
		err = io.ErrUnexpectedEOF
	}
	fl.fail(err)
	if err == nil && !fl.skipCache {
		err = d.Update(context.WithoutCancel(ctx), &IOVector{
			FilePath: filePath,
			Policy:   policy,
			Entries:  []IOEntry{{Offset: fl.start, Size: int64(len(content)), Data: content, readDeletes: fl.readDeletes}},
		}, false)
	}
	fl.cacheErr = err
	d.endPrefetch(diskPath, fl)
	fl.release()
}

// readRanges reads the ranges of the object at filePath through open as flights, which
// reads of their bytes join, caches each, and returns once they end or ctx is done, with
// the error of a flight that did not cache its range. A range a flight already reads is
// waited for, not read again. Ranges that would pass the bytes of the flights under way
// wait until they fit, or no flight is under way. It reports false, reading
// nothing, when the cache is closed.
func (d *DiskCache) readRanges(
	ctx context.Context,
	filePath string,
	policy Policy,
	ranges []PrefetchRange,
	open rangeOpener,
) (bool, error) {
	diskPath := d.pathForFile(filePath)
	f := &d.prefetchObjects
	budget := d.flightBytes()
	f.mu.Lock()
	var (
		closing context.Context
		now     time.Time
		missing []PrefetchRange
		waits   []prefetchObject
	)
	for {
		var ok bool
		if closing, ok = f.openLocked(); !ok {
			f.mu.Unlock()
			return false, nil
		}
		now = time.Now()
		missing, waits = missing[:0], waits[:0]
		var need int64
		for _, r := range ranges {
			if fl := f.fastestLocked(diskPath, r.Offset, r.Offset+r.Size, now, d.downloadMaxExtraWait); fl != nil {
				waits = append(waits, fl)
				continue
			}
			missing = append(missing, r)
			need += flightBufSize(r.Size)
		}
		// Past the bytes of the flights under way, the ranges wait for flights to end.
		if need == 0 || f.bytes == 0 || f.bytes+need <= budget {
			break
		}
		if f.freed == nil {
			f.freed = make(chan struct{})
		}
		freed := f.freed
		f.mu.Unlock()
		select {
		case <-freed:
		case <-ctx.Done():
			return true, ctx.Err()
		}
		f.mu.Lock()
	}
	objects := waits
	for _, r := range missing {
		fl := d.startFlightLocked(ctx, diskPath, r.Offset, r.Offset+r.Size, now)
		objects = append(objects, fl)
	}
	f.mu.Unlock()
	for _, o := range objects[len(waits):] {
		go d.runFlight(ctx, filePath, diskPath, policy, o.(*memoryPrefetchObject), open, closing)
	}
	downloadStarted(ctx)
	if err := waitFlights(ctx, objects); err != nil {
		return true, err
	}
	for _, o := range objects {
		if err := o.err(); err != nil {
			return true, err
		}
	}
	return true, nil
}

// readFromFlights fills the entries of vector not done, of the object at filePath, from the
// flights reading their bytes, waiting for the bytes to arrive. An entry no flight holds,
// or whose flight ends without its bytes, is left to the caller.
func (d *DiskCache) readFromFlights(ctx context.Context, filePath string, vector *IOVector) error {
	diskPath := d.pathForFile(filePath)
	f := &d.prefetchObjects
	f.mu.Lock()
	if len(f.m[diskPath]) == 0 {
		f.mu.Unlock()
		return nil
	}
	flights := make([]prefetchObject, len(vector.Entries))
	found := false
	now := time.Now()
	for i := range vector.Entries {
		if e := &vector.Entries[i]; !e.done && e.Size > 0 {
			if fl := f.fastestLocked(diskPath, e.Offset, e.Offset+e.Size, now, d.downloadMaxExtraWait); fl != nil {
				fl.hold()
				flights[i], found = fl, true
			}
		}
	}
	f.mu.Unlock()
	if !found {
		return nil
	}
	defer func() {
		for _, fl := range flights {
			if fl != nil {
				fl.release()
			}
		}
	}()
	for i, fl := range flights {
		if fl == nil {
			continue
		}
		e := &vector.Entries[i]
		var decodeErr error
		err := fl.use(ctx, e.Offset, e.Size, func(bs []byte) error {
			decodeErr = e.readFrom(ctx, bytes.NewReader(bs), d.cacheDataAllocator)
			return decodeErr
		})
		if decodeErr != nil {
			return decodeErr
		}
		if err != nil {
			if err := ctx.Err(); err != nil {
				return err
			}
			continue
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

// memoryPrefetchObject is a prefetch range being read from object storage into buf.
type memoryPrefetchObject struct {
	start, end int64
	began      time.Time
	buf        []byte

	// ctx is the read's; cancelRead ends it. endedCh is closed when the read, and the
	// cache write of its range, are done; cacheErr is then why its range was not cached.
	ctx        context.Context
	cancelRead context.CancelFunc
	endedCh    chan struct{}
	cacheErr   error

	mu       sync.Mutex
	n        int64 // bytes of buf read
	finished bool  // no more bytes arrive
	waiters  byteWaiters
	// readDeletes marks the range's cache entry; see IOEntry.readDeletes.
	readDeletes uint64
	// refs counts the holders of buf: the flight until it is forgotten, and each read
	// copying from it. The last release keeps buf for reuse.
	refs int
	// skipCache leaves the range read out of the disk cache.
	skipCache bool
	// readErr is the error the flight failed to read its range with.
	readErr error
}

func newMemoryPrefetchObject(start, end int64, now time.Time) *memoryPrefetchObject {
	return &memoryPrefetchObject{start: start, end: end, began: now, endedCh: make(chan struct{}), refs: 1}
}

// hold keeps buf for a read that copies from it, until release. Holds are taken while the
// flight is registered, so they come before the flight's own release.
func (f *memoryPrefetchObject) hold() {
	f.mu.Lock()
	f.refs++
	f.mu.Unlock()
}

func (f *memoryPrefetchObject) release() {
	f.mu.Lock()
	f.refs--
	var buf []byte
	if f.refs == 0 {
		buf, f.buf = f.buf, nil
	}
	f.mu.Unlock()
	putFlightBuf(buf)
}

const (
	// flightBufMinShift and flightBufMaxShift bound the power-of-two sizes of the flight
	// buffers kept for reuse; a larger flight allocates its own.
	flightBufMinShift = 16
	flightBufMaxShift = 25
)

// flightBufs keeps released flight buffers for reuse, by power-of-two size.
var flightBufs [flightBufMaxShift - flightBufMinShift + 1]sync.Pool

// flightBufShift returns the power of two of the buffer of a flight of n bytes.
func flightBufShift(n int64) int {
	return max(bits.Len64(uint64(max(n, 1)-1)), flightBufMinShift)
}

// flightBufSize returns the bytes of the buffer of a flight of n bytes.
func flightBufSize(n int64) int64 {
	if shift := flightBufShift(n); shift <= flightBufMaxShift {
		return 1 << shift
	}
	return n
}

// getFlightBuf returns a buffer of n bytes, of capacity flightBufSize(n).
func getFlightBuf(n int64) []byte {
	shift := flightBufShift(n)
	if shift > flightBufMaxShift {
		return make([]byte, n)
	}
	if b, ok := flightBufs[shift-flightBufMinShift].Get().(*[]byte); ok {
		return (*b)[:n]
	}
	return make([]byte, n, 1<<shift)
}

// putFlightBuf keeps b, from getFlightBuf, for reuse.
func putFlightBuf(b []byte) {
	c := int64(cap(b))
	if c == 0 || flightBufSize(c) != c || c > 1<<flightBufMaxShift {
		return
	}
	b = b[:c]
	flightBufs[flightBufShift(c)-flightBufMinShift].Put(&b)
}

// openLocked returns the context the closing of the cache cancels, or false once the cache
// is closed.
func (f *prefetchObjects) openLocked() (context.Context, bool) {
	if f.closed {
		return nil, false
	}
	if f.closing == nil {
		f.closing, f.cancelReads = context.WithCancel(context.Background())
	}
	return f.closing, true
}

// startFlightLocked registers a flight of the bytes [start, end) of the object at diskPath.
// The flight outlives the read that starts it, not the cache or a delete of the object.
func (d *DiskCache) startFlightLocked(ctx context.Context, diskPath string, start, end int64, now time.Time) *memoryPrefetchObject {
	f := &d.prefetchObjects
	f.bytes += flightBufSize(end - start)
	fl := newMemoryPrefetchObject(start, end, now)
	fl.readDeletes = d.readDeletes()
	fl.ctx, fl.cancelRead = context.WithCancel(context.WithoutCancel(ctx))
	f.registerLocked(diskPath, fl)
	f.reads.Add(1)
	return fl
}

// waitFlights waits for the prefetch objects to end, or ctx.
func waitFlights(ctx context.Context, objects []prefetchObject) error {
	for _, o := range objects {
		select {
		case <-o.ended():
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// dropPrefetches cancels the prefetch objects of the object at diskPath and waits for
// them and their cache writes to end, or ctx.
func (d *DiskCache) dropPrefetches(ctx context.Context, diskPath string) error {
	f := &d.prefetchObjects
	f.mu.Lock()
	objects := slices.Clone(f.m[diskPath])
	f.mu.Unlock()
	for _, o := range objects {
		o.stop()
	}
	return waitFlights(ctx, objects)
}

// read reads the range through open into buf, chunk by chunk, publishing each chunk,
// and reads the reader to its end so its connection is reused. The time open takes is
// a sample of the latency of range reads.
func (f *memoryPrefetchObject) read(ctx context.Context, open rangeOpener) error {
	buf := getFlightBuf(f.end - f.start)
	f.mu.Lock()
	f.buf = buf
	f.mu.Unlock()
	ctx, idle := newIdleReader(ctx)
	defer idle.Close()
	t0 := time.Now()
	r, err := open(ctx, f.start, f.end)
	if err != nil {
		return err
	}
	defer r.Close()
	idle.r = r
	rangeReadCosts.observeLatency(time.Since(t0))
	for pos := 0; pos < len(buf); {
		n, err := idle.Read(buf[pos:min(pos+prefetchChunk, len(buf))])
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
	_, err = io.Copy(io.Discard, idle)
	return err
}

// wrote publishes n more bytes of buf.
func (f *memoryPrefetchObject) wrote(n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.n += int64(n)
	keep := f.waiters[:0]
	for _, w := range f.waiters {
		if f.start+f.n >= w.end {
			close(w.ch)
		} else {
			keep = append(keep, w)
		}
	}
	clear(f.waiters[len(keep):])
	f.waiters = keep
}

// finish ends the flight; bytes not read by then never arrive.
func (f *memoryPrefetchObject) finish() {
	f.fail(nil)
}

// fail finishes the flight, which failed to read its range with err when set.
func (f *memoryPrefetchObject) fail(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.finished {
		f.finished = true
		f.readErr = err
		f.waiters.wakeAll()
	}
}

// failure returns the error the flight failed to read its range with.
func (f *memoryPrefetchObject) failure() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.readErr
}

// content returns the bytes read.
func (f *memoryPrefetchObject) content() []byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.buf[:f.n]
}

// eta estimates how long after now the bytes of the flight up to end arrive.
func (f *memoryPrefetchObject) eta(now time.Time, end int64) time.Duration {
	return max(rangeReadCosts.cost(end-f.start)-now.Sub(f.began), 0)
}

// waitFor returns the bytes read once they reach end, or false if the flight finished
// short of it or ctx ended first.
func (f *memoryPrefetchObject) waitFor(ctx context.Context, end int64) ([]byte, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.start+f.n < end && !f.finished {
		ch := f.waiters.add(f.start, end)
		f.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
		}
		f.mu.Lock()
		f.waiters.remove(ch)
	}
	if f.start+f.n >= end {
		return f.buf[:f.n], true
	}
	return nil, false
}

// prefetchObjects indexes the prefetch ranges being read by the disk path of their
// object.
type prefetchObjects struct {
	mu sync.Mutex
	m  map[string][]prefetchObject
	// bytes is the size of the buffers of the runs under way; freed, when set, is closed
	// once it shrinks.
	bytes  int64
	freed  chan struct{}
	closed bool
	reads  sync.WaitGroup // reads of runs and their cache writes
	// closing is canceled when the cache closes; it cancels the reads of runs.
	closing     context.Context
	cancelReads context.CancelFunc
}

// closePrefetches stops new prefetch reads, cancels those under way and waits for them,
// or ctx.
func (d *DiskCache) closePrefetches(ctx context.Context) {
	f := &d.prefetchObjects
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

// fastestLocked returns the prefetch object of diskPath holding [lo, hi) whose bytes reach
// hi first, of the ones reads can join. A FilePrefetchObject is joined only when its bytes
// arrive no more than extraWait later than a range read could fetch them.
func (f *prefetchObjects) fastestLocked(diskPath string, lo, hi int64, now time.Time, extraWait time.Duration) prefetchObject {
	var best prefetchObject
	var bestETA time.Duration
	for _, o := range f.m[diskPath] {
		if start, end := o.span(); start > lo || end < hi || !o.joinable() {
			continue
		}
		eta := o.eta(now, hi)
		if _, ok := o.(*filePrefetchObject); ok && eta > rangeReadCosts.cost(hi-lo)+extraWait {
			continue
		}
		if best == nil || eta < bestETA {
			best, bestETA = o, eta
		}
	}
	return best
}

// fastestPrefetch returns the time until the bytes [lo, hi) of diskPath arrive from the
// fastest flight containing them, or false if none does.
func (d *DiskCache) fastestPrefetch(diskPath string, lo, hi int64) (time.Duration, bool) {
	f := &d.prefetchObjects
	f.mu.Lock()
	defer f.mu.Unlock()
	now := time.Now()
	if fl := f.fastestLocked(diskPath, lo, hi, now, d.downloadMaxExtraWait); fl != nil {
		return fl.eta(now, hi), true
	}
	return 0, false
}

// endPrefetch finishes a flight the caller started and forgets it.
func (d *DiskCache) endPrefetch(diskPath string, fl *memoryPrefetchObject) {
	f := &d.prefetchObjects
	f.mu.Lock()
	f.forgetLocked(diskPath, fl)
	f.bytes -= flightBufSize(fl.end - fl.start)
	if f.freed != nil {
		close(f.freed)
		f.freed = nil
	}
	f.mu.Unlock()
	fl.finish()
	close(fl.endedCh)
}
