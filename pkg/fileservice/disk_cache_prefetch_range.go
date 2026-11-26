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
	"cmp"
	"context"
	"errors"
	"math"
	"math/bits"
	"os"
	pathpkg "path"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/system"
	metric "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/prometheus/client_golang/prometheus"
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
// its ranges alone. Objects with a separator in their path read their ranges alone.
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
		if !e.done && (e.Size <= 0 || !e.materializes(d.memoryPrefetchMax)) {
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
	flightMemoryPercent = 5
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
// when set, otherwise a share of the memory available on the node, at most the buffer of
// one MemoryPrefetchObject for each background request.
func (d *DiskCache) flightBytes() int64 {
	if d.prefetchFlightBytes > 0 {
		return d.prefetchFlightBytes
	}
	b := &d.flightBudget
	now := time.Now().UnixNano()
	at := b.at.Load()
	bytes := b.bytes.Load()
	// One caller measures once the last measurement is stale; the others keep it meanwhile.
	if at != 0 && (now-at < int64(flightBudgetRefresh) || !b.at.CompareAndSwap(at, now)) {
		return bytes
	}
	bytes = int64(prefetchFlightBytesMax)
	if avail, ok := memoryAvailable(); ok {
		bytes = int64(min(avail/100*flightMemoryPercent, math.MaxInt64))
	}
	if n := int64(d.backgroundRequests()); n > 0 {
		if per := flightBufSize(d.memoryPrefetchMax); per <= bytes/n {
			bytes = min(bytes, per*n)
		}
	}
	b.bytes.Store(bytes)
	b.at.Store(now)
	return bytes
}

type blockState int

const (
	blockMissing blockState = iota
	blockCached
	blockInFlight
	// blockJoined is a block another prefetch object was reading when the read began.
	blockJoined
)

// prefetchBlock is a block of a prefetch window and where its bytes come from.
type prefetchBlock struct {
	start, end int64 // the block's bytes the read takes, or all of them
	needed     bool  // the read takes bytes of the block
	wanted     bool  // the block is in a unit the read takes bytes of
	state      blockState
	cached     cachedRange
	// run is the run of the read reading the block's bytes.
	run *readRun
}

// prefetchRead returns a buffer of the bytes [lo, hi) of filePath that holds the bytes
// of the entries not done, and the number of range reads it started. It splits the
// unit-aligned window around them into blocks and takes each block an entry needs from
// a cached range, from the read of a prefetch range under way whose bytes reach the
// block first, or from a new read: each run of blocks in units the entries take bytes
// of that are neither cached nor being read and holds a needed block is read from
// object storage through open as one range, served to other reads as its bytes arrive,
// and cached. It returns false when the bytes are not all read, or the object was deleted
// since it began, and the error of a failed read of object storage it waited on.
func (d *DiskCache) prefetchRead(
	ctx context.Context,
	filePath string,
	policy Policy,
	entries []IOEntry,
	lo, hi, unit int64,
	src objectSource,
) ([]byte, int, bool, error) {
	if lo < 0 || hi <= lo || ctx.Err() != nil {
		return nil, 0, false, nil
	}
	block := int64(prefetchBlockMax)
	if unit < block || unit%block != 0 {
		block = unit
	}
	windowStart := lo / unit * unit
	windowEnd := (hi + unit - 1) / unit * unit
	if (windowEnd-windowStart)/block > prefetchWindowBlocks {
		return nil, 0, false, nil
	}
	// No run of the read starts after a delete of the object counted since it began, and
	// no bytes it read across one are returned.
	deletes := d.deletes.now()
	diskPath := d.objectKey(filePath)
	ranges := d.ranges.of(d.objectKey(filePath))

	n := int((windowEnd - windowStart) / block)
	needed := make([]bool, n)
	for i := range entries {
		e := &entries[i]
		if !e.ranged() {
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

	// Find the blocks being read under one lock, and make a prefetch object of each run of
	// the others: a MemoryPrefetchObject of a run of at most disk-prefetch-memory-max
	// bytes, otherwise the parts of the run, each a FilePrefetchObject the reads of its
	// bytes share. The read holds no prefetch object while it waits for one of its own to
	// be admitted: it holds the one reading a block it joined only to copy the block.
	f := &d.prefetchObjects
	f.mu.Lock()
	if _, ok := f.openLocked(); !ok {
		f.mu.Unlock()
		return nil, 0, false, nil
	}
	now := time.Now()
	for i := range blocks {
		b := &blocks[i]
		if b.state != blockMissing {
			continue
		}
		if d.joinableLocked(diskPath, false, b.start, b.end, now, true) != nil {
			b.state = blockJoined
		}
	}
	f.mu.Unlock()
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
	var runs []*readRun
	memoryRuns := 0
	background := requestClassOf(ctx) == backgroundRequest
	for _, p := range pending {
		start, end := windowStart+int64(p[0])*block, windowStart+int64(p[1]+1)*block
		if d.memoryBacked(end-start, background) {
			r := newReadRun(d.newFlight(filePath, start, end, policy, src, flightNeeds{caching: true, deletes: &deletes}))
			r.first, r.last = p[0], p[1]
			r.copyHolds = true
			runs = append(runs, r)
			memoryRuns++
			continue
		}
		if size, ok := d.prefetched.size(diskPath); ok && start < size && size < end {
			// A run downloads to the object's end, not past it.
			end = size
		}
		for _, part := range d.rangeParts(start, end, block) {
			o := d.newFileObject(context.WithoutCancel(ctx), filePath, part[0], part[1], src, flightNeeds{deletes: &deletes})
			o.readStarted = true
			r := newReadRun(o)
			r.first = -1
			for j := p[0]; j <= p[1]; j++ {
				if b := &blocks[j]; part[0] <= b.start && b.start < part[1] {
					if r.first < 0 {
						r.first = j
					}
					r.last = j
				}
			}
			if r.first < 0 {
				o.abandon()
				continue
			}
			runs = append(runs, r)
		}
	}

	// The runs are admitted in order, by one goroutine, and each is read by its own once
	// registered. A run serves its bytes to waiting reads as they arrive and
	// stays registered until it is cached; it outlives the read, which may return before
	// its last bytes arrive.
	admitCtx, cancelAdmits := context.WithCancel(ctx)
	defer func() {
		cancelAdmits()
		for _, r := range runs {
			r.done()
		}
	}()
	for _, r := range runs {
		r.unneeded = true
		for j := r.first; j <= r.last; j++ {
			if blocks[j].needed {
				blocks[j].state, blocks[j].run = blockInFlight, r
				r.unneeded = false
			}
		}
	}
	startRuns(admitCtx, runs)

	// Assemble the bytes. A prefetch object is released once the last needed block it
	// holds is copied; a MemoryPrefetchObject is held only while a block is copied from it.
	remaining := make(map[*readRun]int)
	for i := range blocks {
		if r := blocks[i].run; r != nil {
			remaining[r]++
		}
	}
	out := make([]byte, hi-lo)
	var deadline time.Time
	// copyBlock copies the bytes of b from the prefetch object reading them, holding it
	// while it copies. It reports false when they are not read, with the error failing the
	// read.
	copyBlock := func(b *prefetchBlock, dst []byte) (bool, error) {
		var o prefetchObject
		release := func() {}
		if r := b.run; r != nil {
			select {
			case <-r.admitted:
			case <-ctx.Done():
				return false, ctx.Err()
			}
			if r.err != nil {
				return false, ctx.Err()
			}
			if !r.releasedOnAdmit() {
				o = r.got
				release = func() {
					if remaining[r]--; remaining[r] == 0 {
						r.done()
					}
				}
			}
		}
		if b.run == nil {
			// The read joins the block as it chose to when it planned it.
			f.mu.Lock()
			o, release = heldLocked(d.joinableLocked(diskPath, false, b.start, b.end, time.Now(), true))
			f.mu.Unlock()
		} else if b.run.releasedOnAdmit() {
			// A run of the read's own that ended left its bytes cached.
			f.mu.Lock()
			o, release = heldLocked(f.registeredLocked(diskPath, b.run.got))
			f.mu.Unlock()
		}
		var err error = errPrefetchMissed
		if o != nil {
			err = func() error {
				defer release()
				return o.readAt(ctx, b.start, dst)
			}()
		} else {
			release()
		}
		if err == nil {
			return true, nil
		}
		// A failed read of object storage fails the read. An object that caches its bytes
		// and ended before the read reached the block left them in a cached range.
		if err := readFailure(ctx, filePath, fromSource, err); err != nil {
			return false, err
		}
		if o != nil && !o.caches() {
			return false, nil
		}
		return d.readCachedBytes(ctx, filePath, b.start, dst, &deadline), nil
	}
	for i := 0; i < len(blocks); i++ {
		b := &blocks[i]
		if !b.needed {
			continue
		}
		dst := out[b.start-lo : b.end-lo]
		switch b.state {
		case blockInFlight, blockJoined:
			if ok, err := copyBlock(b, dst); !ok {
				return nil, memoryRuns, false, err
			}
		case blockCached:
			j := i
			for j+1 < len(blocks) && blocks[j+1].needed && blocks[j+1].state == blockCached &&
				blocks[j+1].cached.diskPath == b.cached.diskPath {
				j++
			}
			dst = out[b.start-lo : blocks[j].end-lo]
			if !d.readCachedRange(ctx, b.cached, b.start, dst, &deadline) {
				return nil, memoryRuns, false, nil
			}
			i = j
		default:
			return nil, memoryRuns, false, nil
		}
	}
	// Bytes copied across a delete of the object may be of two objects at the path.
	if d.deletes.since(diskPath, deletes) {
		return nil, memoryRuns, false, nil
	}
	return out, memoryRuns, true, nil
}

// readRun is a run of a prefetch or a read, read by its own prefetch object.
type readRun struct {
	first, last int // block indexes, for a read
	object      waitingObject
	// admitted is closed once object is registered, joined the prefetch object got holding
	// its bytes, or will not be read, with err; got is held until done.
	admitted chan struct{}
	got      prefetchObject
	err      error
	doneOnce sync.Once
	// unneeded is set when the read takes no bytes of the run.
	unneeded bool
	// copyHolds is set for a run of a read that holds the MemoryPrefetchObject it got only
	// while it copies from it, releasing the hold it was admitted with.
	copyHolds bool
}

func newReadRun(o waitingObject) *readRun {
	return &readRun{object: o, admitted: make(chan struct{})}
}

// startRuns admits the objects of runs in order, by one goroutine, and runs each by its
// own once registered. Once ctx ends, the object is deleted or the cache closes, the runs
// not admitted end with that error. It returns the channel closed once the last is
// admitted.
func startRuns(ctx context.Context, runs []*readRun) <-chan struct{} {
	if len(runs) == 0 {
		return closedChan
	}
	all := make(chan struct{})
	go func() {
		defer close(all)
		for i, r := range runs {
			if err := r.admit(ctx); err != nil {
				for _, r := range runs[i+1:] {
					r.fail(err)
				}
				return
			}
		}
	}()
	return all
}

// closedChan is a closed channel.
var closedChan = func() <-chan struct{} {
	c := make(chan struct{})
	close(c)
	return c
}()

// admit admits the object of r, waiting holding nothing, and starts its run once
// registered. It returns the error ending the runs after r: ctx's, or the delete or close
// that ended r.
func (r *readRun) admit(ctx context.Context) error {
	var err error
	r.got, err = admit(ctx, r.object)
	r.err = err
	close(r.admitted)
	if r.releasedOnAdmit() {
		r.done()
	}
	if r.got != prefetchObject(r.object) {
		r.object.abandon()
		r.end()
	} else {
		go func() {
			r.object.run()
			r.end()
		}()
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	if errors.Is(err, errPrefetchDeleted) || errors.Is(err, errPrefetchClosed) {
		return err
	}
	return nil
}

// fail ends r, not admitted, with err.
func (r *readRun) fail(err error) {
	r.err = err
	close(r.admitted)
	r.object.abandon()
}

// end frees the object r got when it takes no bytes of it.
func (r *readRun) end() {
	if r.unneeded {
		r.done()
	}
}

// releasedOnAdmit reports whether r, admitted, released the MemoryPrefetchObject it got.
func (r *readRun) releasedOnAdmit() bool {
	_, ok := r.got.(*memoryPrefetchObject)
	return ok && r.copyHolds
}

// done releases the object r got from its admission, once.
func (r *readRun) done() {
	r.doneOnce.Do(func() {
		<-r.admitted
		if r.got != nil {
			r.got.release()
		}
	})
}

// rangeParts returns the parts [start, end) of an object is read as, each by its own
// prefetch object: disk-prefetch-object-split parts at boundaries that are multiples of
// align from start, when it is large enough and few downloads are under way, otherwise
// one.
func (d *DiskCache) rangeParts(start, end, align int64) [][2]int64 {
	whole := [][2]int64{{start, end}}
	if d.objectSplit.parts <= 1 || d.downloading() >= cmp.Or(d.objectSplit.downloads, DefaultDiskPrefetchObjectSplitDownloads) {
		return whole
	}
	bounds := d.objectSplit.bounds(end-start, 0)
	if len(bounds) < 3 {
		return whole
	}
	parts := make([][2]int64, 0, len(bounds)-1)
	for i := 1; i < len(bounds); i++ {
		if bounds[i-1]%align != 0 {
			return whole
		}
		parts = append(parts, [2]int64{start + bounds[i-1], start + bounds[i]})
	}
	return parts
}

// readDownload reads the bytes of download o from off into dst, once they are written. It
// reports whether it read them, and whether the download wrote them.
func readDownload(ctx context.Context, o *objectDownload, off int64, dst []byte) (read, written bool) {
	if _, ok := o.waitFor(ctx, off, off+int64(len(dst)), 0); !ok {
		return false, false
	}
	held, err := o.with(func(f *os.File) error {
		_, err := f.ReadAt(dst, off)
		return err
	})
	return held && err == nil, true
}

// fetchRange returns the bytes [start, end) of the object at filePath from a prefetch
// object holding them: one under way, a MemoryPrefetchObject of at most
// disk-prefetch-memory-max bytes it starts and runs itself, which caches nothing, or,
// unless the read skips disk-cache writes, a FilePrefetchObject it starts. It
// reports whether it returned them, and whether the prefetch object caches them. A read
// whose cache is the index-file cache, index, joins and starts prefetch objects of that
// cache.
func (d *DiskCache) fetchRange(
	ctx context.Context,
	filePath string,
	policy Policy,
	index bool,
	start, end int64,
	src objectSource,
) (content []byte, ok, cached bool, err error) {
	if start < 0 || end <= start {
		return nil, false, false, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, false, false, err
	}
	// The flight caches nothing; the read caches its entries when its policy says so.
	fl := d.newFlight(filePath, start, end, policy, src, flightNeeds{
		joinsCaching: policy.CacheIOEntry() && !policy.Any(SkipDiskCacheWrites),
	})
	fl.index = index
	var o prefetchObject
	var own *filePrefetchObject
	memory := d.memoryBacked(end-start, requestClassOf(ctx) == backgroundRequest)
	if !memory && policy.Any(SkipDiskCacheWrites) {
		// A read that writes no cache, of bytes not held in memory, only joins a prefetch
		// object under way.
		f := &d.prefetchObjects
		f.mu.Lock()
		o, _ = heldLocked(fl.joinLocked(time.Now()))
		f.mu.Unlock()
	} else if !memory {
		deletes := d.deletes.now()
		p := d.newFileObject(context.WithoutCancel(ctx), filePath, start, end, src, flightNeeds{deletes: &deletes})
		p.readStarted, p.holdStarter = true, true
		declined, _ := d.fileDownload(p.diskPath, start, end)
		p.declined = declined != nil
		if o, err = admit(ctx, p); o != prefetchObject(p) {
			p.abandon()
		} else {
			own = p
			defer own.releaseStarter()
			go p.run()
		}
	} else {
		o, err = admit(ctx, fl)
	}
	if err != nil || o == nil {
		return nil, false, false, readFailure(ctx, filePath, fromSource, err)
	}
	defer o.release()
	if o == prefetchObject(fl) {
		go fl.run()
	}
	// A prefetch object that failed to read the bytes fails the read; one that ended
	// without them leaves the read to the caller.
	out := make([]byte, end-start)
	read := o.readAt
	if own != nil {
		read = own.readOwnAt
	}
	if err := read(ctx, start, out); err != nil {
		if err := readFailure(ctx, filePath, fromSource, err); err != nil {
			return nil, false, false, err
		}
		// An object that caches its bytes and ended without them may have found them
		// cached in a range.
		var deadline time.Time
		if o.caches() && d.readCachedBytes(ctx, filePath, start, out, &deadline) {
			return out, true, true, nil
		}
		return nil, false, false, nil
	}
	return out, true, o.caches(), nil
}

// run reads the range of f, registered, through its opener, caches the range when f
// caches, and forgets f. It is canceled by a delete of the object or the closing of the
// cache.
func (f *memoryPrefetchObject) run() {
	d := f.d
	updateCtx := closingValues{Context: f.closing, values: context.WithoutCancel(f.ctx)}
	defer d.prefetchObjects.reads.Done()
	defer f.cancelRead()
	defer context.AfterFunc(f.closing, f.cancelRead)()
	defer func() {
		d.endPrefetch(f.diskPath, f)
		f.release()
	}()
	err := f.readHeld()
	content := f.content()
	if err == nil && len(content) == 0 {
		err = errSourceShort
	}
	f.fail(err)
	if err == nil && !f.skipCache {
		err = d.Update(updateCtx, &IOVector{
			FilePath: f.filePath,
			Policy:   f.policy,
			Entries:  []IOEntry{{Offset: f.start, Size: int64(len(content)), Data: content, readDeletes: f.readDeletes}},
		}, false)
	}
	f.cacheErr = err
}

// closingValues is a context with the values of values that ends once Context does.
type closingValues struct {
	context.Context
	values context.Context
}

func (c closingValues) Value(key any) any { return c.values.Value(key) }

// readHeld reads the range of f from its source.
func (f *memoryPrefetchObject) readHeld() error {
	return f.read(f.ctx)
}

// errPrefetchDeleted is the error of a prefetch of an object deleted before it read all its
// ranges.
var errPrefetchDeleted = moerr.NewInternalErrorNoCtx("fileservice: object deleted during a prefetch")

// errPrefetchClosed is the error of a prefetch the disk cache closed on before it read all
// its ranges.
var errPrefetchClosed = moerr.NewInternalErrorNoCtx("fileservice: disk cache closed during a prefetch")

// flightNeeds is what a prefetch object needs before it registers, besides what its kind
// needs.
type flightNeeds struct {
	// caching is set when it caches its range: it joins only prefetch objects that cache,
	// and is not registered once its range is cached.
	caching bool
	// joinsCaching is set when it caches nothing but the read that starts it caches its
	// entries: it joins, and reads that cache join it, as one that caches.
	joinsCaching bool
	// deletes, when set, is the count of deletes after which it is not registered.
	deletes *uint64
}

// newFlight returns a MemoryPrefetchObject of the bytes [start, end) of the object at
// filePath, read from src, with what needs asks for; it is not registered.
func (d *DiskCache) newFlight(filePath string, start, end int64, policy Policy, src objectSource, needs flightNeeds) *memoryPrefetchObject {
	fl := newMemoryPrefetchObject(start, end, time.Time{})
	fl.prefetchTarget = prefetchTarget{d: d, filePath: filePath, diskPath: d.objectKey(filePath), src: src, needs: needs}
	fl.policy, fl.skipCache = policy, !needs.caching
	return fl
}

// prefetchTarget is the object a prefetch object reads, at diskPath in d, from src,
// what it needs before it registers, and, once registered, closing, done when d closes.
// state is where the prefetch object is in its life, since since; both are guarded by the
// lock of d's prefetch objects.
type prefetchTarget struct {
	d                  *DiskCache
	filePath, diskPath string
	src                objectSource
	needs              flightNeeds
	// index is set on a fill of the index-file cache.
	index   bool
	closing context.Context
	state   objectState
	since   time.Time
	// waitedFrom is when it began to wait for its resources, zero if it has not.
	waitedFrom time.Time
	// background is set when its requests are background.
	background bool
	// fill is the queued fill it is a part of, or nil.
	fill *queuedFill
}

func (t *prefetchTarget) target() *prefetchTarget { return t }

// objectState is where a prefetch object of a run is in its life.
type objectState int

const (
	objectCreated objectState = iota
	objectWaitingBuffer
	objectRegistered
)

func (s objectState) String() string {
	return [...]string{"created", "waiting for a buffer", "registered"}[s]
}

// waitingGauge returns the gauge of the objects in state s, nil when s is not a wait.
func (s objectState) waitingGauge() prometheus.Gauge {
	if s == objectWaitingBuffer {
		return metric.FSPrefetchObjectWaitingBuffer
	}
	return nil
}

// setStateLocked moves o to state s, listing it among the waiting objects while it waits,
// and counts its wait.
func setStateLocked(o waitingObject, s objectState) {
	t := o.target()
	f := &t.d.prefetchObjects
	if t.state != s {
		now := time.Now()
		if g := t.state.waitingGauge(); g != nil {
			g.Dec()
		}
		if g := s.waitingGauge(); g != nil {
			g.Inc()
			if t.waitedFrom.IsZero() {
				t.waitedFrom = now
			}
		} else if !t.waitedFrom.IsZero() {
			// A wait ends, whatever it ends with.
			metric.FSPrefetchObjectWaitDuration.Observe(now.Sub(t.waitedFrom).Seconds())
			t.waitedFrom = time.Time{}
		}
		t.state, t.since = s, now
	}
	if s.waitingGauge() != nil {
		if f.waiting == nil {
			f.waiting = make(map[waitingObject]struct{})
		}
		f.waiting[o] = struct{}{}
	} else {
		delete(f.waiting, o)
	}
}

// waitingObject is a prefetch object that waits, holding nothing, for its resources, then
// registers and runs.
type waitingObject interface {
	prefetchObject
	target() *prefetchTarget
	// joinLocked returns the prefetch object holding its bytes it joins instead.
	joinLocked(now time.Time) prefetchObject
	// fitsLocked reports whether the resources it needs are free.
	fitsLocked() bool
	registerLocked(ctx, closing context.Context, now time.Time)
	run()
	// abandon ends it, never registered.
	abandon()
}

// admit waits until o holds its resources, then registers o and returns it, held, for the
// caller to run. It returns instead, held, the prefetch object reads can join holding the
// bytes of o; neither once they are cached, when o caches, or once the buffer of o, a
// MemoryPrefetchObject, is past its class's share of the flight budget; errPrefetchClosed
// once the cache is closed, and
// errPrefetchDeleted after a delete of the object counted since its needs' deletes. A
// delete or the closing of the cache wakes the objects waiting.
func admit(ctx context.Context, o waitingObject) (prefetchObject, error) {
	t := o.target()
	d, needs := t.d, t.needs
	t.background = requestClassOf(ctx) == backgroundRequest
	start, end := o.span()
	var key string
	if needs.caching {
		key = d.objectKey(t.filePath)
	}
	f := &d.prefetchObjects
	waited, counted := false, false
	defer func() {
		if waited {
			f.mu.Lock()
			if t.state != objectRegistered {
				setStateLocked(o, objectCreated)
			}
			if counted {
				f.foregroundWaiting--
				d.startFillsLocked()
			}
			f.mu.Unlock()
		}
	}()
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		f.mu.Lock()
		closing, ok := f.openLocked()
		if !ok {
			f.mu.Unlock()
			return nil, errPrefetchClosed
		}
		if needs.deletes != nil && d.deletes.since(t.diskPath, *needs.deletes) {
			f.mu.Unlock()
			return nil, errPrefetchDeleted
		}
		if key != "" {
			if _, cached := d.ranges.covering(key, start, end-start); cached {
				f.mu.Unlock()
				return nil, nil
			}
		}
		now := time.Now()
		if j := o.joinLocked(now); j != nil {
			if !t.background {
				d.promoteObjectLocked(j)
			}
			j.hold()
			f.mu.Unlock()
			return j, nil
		}
		if o.fitsLocked() {
			setStateLocked(o, objectRegistered)
			o.registerLocked(ctx, closing, now)
			o.hold()
			f.mu.Unlock()
			return o, nil
		}
		if fl, ok := o.(*memoryPrefetchObject); ok && !d.memoryBacked(fl.end-fl.start, t.background) {
			f.mu.Unlock()
			return nil, nil
		}
		waited = true
		if !t.background && !counted {
			f.foregroundWaiting++
			counted = true
		}
		setStateLocked(o, objectWaitingBuffer)
		freed := f.freed.waitLocked()
		f.mu.Unlock()
		select {
		case <-freed:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// registerLocked charges the buffer of f and registers it, joinable. It outlives the read
// that registers it, not the cache, closing once closing is done, or a delete of the
// object.
func (f *memoryPrefetchObject) registerLocked(ctx, closing context.Context, now time.Time) {
	objs := &f.d.prefetchObjects
	f.began, f.closing = now, closing
	f.owner, f.charge = objs, flightBufSize(f.end-f.start)
	objs.bytes += f.charge
	if f.background {
		objs.background += f.charge
	}
	f.readDeletes = f.d.readDeletes()
	f.ctx, f.cancelRead = context.WithCancel(context.WithoutCancel(ctx))
	objs.registerLocked(f.diskPath, f)
	objs.reads.Add(1)
}

// joinLocked returns the fastest prefetch object reads can join holding the range of f,
// one that caches when f caches.
func (f *memoryPrefetchObject) joinLocked(now time.Time) prefetchObject {
	return f.d.joinableLocked(f.diskPath, f.index, f.start, f.end, now, f.needs.caching || f.needs.joinsCaching)
}

// abandon ends f, never registered.
func (f *memoryPrefetchObject) abandon() {}

// fitsLocked reports whether the buffer of f fits the flight budget now.
func (f *memoryPrefetchObject) fitsLocked() bool {
	return f.d.fitsLocked(flightBufSize(f.end-f.start), f.background)
}

// readFromFlights fills the entries of vector not done, of the object at filePath, from the
// flights of the read's cache reading their bytes, waiting for the bytes to arrive. Each entry holds its flight
// only while it waits for and copies its bytes. One no flight holds, or whose flight ends
// without its bytes, is left to the caller. A read that caches its bytes joins only flights
// that cache theirs.
func (d *DiskCache) readFromFlights(ctx context.Context, filePath string, vector *IOVector, caches bool) error {
	diskPath := d.objectKey(filePath)
	f := &d.prefetchObjects
	f.mu.Lock()
	if len(f.m[diskPath]) == 0 {
		f.mu.Unlock()
		return nil
	}
	inFlight := make([]bool, len(vector.Entries))
	found := false
	now := time.Now()
	for i := range vector.Entries {
		if e := &vector.Entries[i]; e.ranged() && e.materializes(d.memoryPrefetchMax) {
			if d.joinableLocked(diskPath, vector.indexTier, e.Offset, e.Offset+e.Size, now, caches) != nil {
				inFlight[i], found = true, true
			}
		}
	}
	f.mu.Unlock()
	if !found {
		return nil
	}
	for i := range vector.Entries {
		if !inFlight[i] {
			continue
		}
		e := &vector.Entries[i]
		f.mu.Lock()
		fl, release := heldLocked(d.joinableLocked(diskPath, vector.indexTier, e.Offset, e.Offset+e.Size, time.Now(), caches))
		f.mu.Unlock()
		if fl == nil {
			continue
		}
		var decodeErr error
		err := func() error {
			defer release()
			return fl.use(ctx, e.Offset, e.Size, func(bs []byte) error {
				decodeErr = e.readFromThen(ctx, bytes.NewReader(bs), d.cacheDataAllocator, release)
				return decodeErr
			})
		}()
		if decodeErr != nil {
			return decodeErr
		}
		if err != nil {
			// A flight that ended without the bytes leaves them to the caller; a failed
			// read of object storage fails the read.
			if err := readFailure(ctx, filePath, fromSource, err); err != nil {
				return err
			}
			continue
		}
		e.done = true
		e.fromCache = d
	}
	return nil
}

// readCachedBytes reads len(dst) bytes at offset of the object at filePath from what the
// disk cache holds of them: the smallest cached range holding them, or the cached ranges
// holding them together. It reports whether it read them.
func (d *DiskCache) readCachedBytes(ctx context.Context, filePath string, offset int64, dst []byte, deadline *time.Time) bool {
	key, end := d.objectKey(filePath), offset+int64(len(dst))
	if r, ok := smallestContaining(d.ranges.of(key), offset, int64(len(dst)), nil); ok && d.cache.Contains(r.diskPath) {
		return d.readCachedRange(ctx, r, offset, dst, deadline)
	}
	if rs, ok := d.ranges.covering(key, offset, int64(len(dst))); ok {
		for _, r := range rs {
			lo, hi := max(offset, r.offset), min(end, r.offset+r.size)
			if !d.cache.Contains(r.diskPath) || !d.readCachedRange(ctx, r, lo, dst[lo-offset:hi-offset], deadline) {
				return false
			}
		}
		return true
	}
	return false
}

// readCachedRange reads len(dst) bytes at offset of the object from the cached range r. A
// range it cannot read is dropped.
func (d *DiskCache) readCachedRange(ctx context.Context, r cachedRange, offset int64, dst []byte, deadline *time.Time) bool {
	generation, ok := d.paths.startRead(ctx, r.diskPath, deadline)
	if !ok {
		return false
	}
	opened, ok := d.paths.open(ctx, r.diskPath, generation)
	if !ok {
		d.paths.endRead(r.diskPath, generation)
		d.dropCachedRange(ctx, r.diskPath)
		return false
	}
	if _, err := opened.file.ReadAt(dst, offset-r.offset); err != nil {
		d.paths.close(r.diskPath, opened)
		d.dropCachedRange(ctx, r.diskPath)
		return false
	}
	d.paths.close(r.diskPath, opened)
	d.cache.Get(ctx, r.diskPath)
	return true
}

// dropCachedRange removes the cached range at diskPath, whose file is missing, short or
// unreadable, unless the path is being changed.
func (d *DiskCache) dropCachedRange(ctx context.Context, diskPath string) {
	owner, ok := d.paths.tryAcquire(diskPath, nil)
	if !ok {
		return
	}
	defer owner.release()
	if err := os.Remove(diskPath); err != nil && !os.IsNotExist(err) {
		return
	}
	d.ranges.remove(diskPath)
	d.cache.Delete(ctx, diskPath)
}

// cachedFileExists reports whether the cache file at diskPath is on disk; when it is
// missing, its index entry is dropped (see dropMissingFile).
func (d *DiskCache) cachedFileExists(ctx context.Context, diskPath string) bool {
	if _, err := os.Stat(diskPath); os.IsNotExist(err) {
		d.dropMissingFile(ctx, diskPath)
		return false
	}
	return true
}

// dropMissingFile drops the index entry of the cache file at diskPath while the file is
// missing, unless another holder owns its path.
func (d *DiskCache) dropMissingFile(ctx context.Context, diskPath string) {
	owner, ok := d.paths.tryAcquire(diskPath, nil)
	if !ok {
		return
	}
	defer owner.release()
	if _, err := os.Stat(diskPath); !os.IsNotExist(err) {
		return
	}
	d.ranges.remove(diskPath)
	d.cache.Delete(ctx, diskPath)
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
	idle     idleReaders // the reader of the range's source
	// readDeletes marks the range's cache entry; see IOEntry.readDeletes.
	readDeletes uint64
	// refs counts the holders of buf: the flight until it is forgotten, and each read
	// copying from it. The last release keeps buf for reuse and frees its charge.
	refs int
	// owner holds the charge of buf, charge bytes, until the last release.
	owner  *prefetchObjects
	charge int64
	// skipCache leaves the range read out of the disk cache.
	skipCache bool
	// readErr is the error the flight failed to read its range with.
	readErr error

	prefetchTarget
	// policy is the policy its range is cached under.
	policy Policy
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

// release drops a hold. The caller does not hold the owner's lock.
func (f *memoryPrefetchObject) release() {
	f.mu.Lock()
	f.refs--
	last := f.refs == 0
	var buf []byte
	if last {
		buf, f.buf = f.buf, nil
	}
	f.mu.Unlock()
	if last && f.owner != nil {
		f.d.unchargeFlight(f)
	}
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

// heldLocked holds o, when set, for a read that joins it, promoting it, and returns it with
// the function releasing it once.
func heldLocked(o prefetchObject) (prefetchObject, func()) {
	if o == nil {
		return nil, func() {}
	}
	if d := o.target().d; d != nil {
		d.promoteObjectLocked(o)
	}
	o.hold()
	return o, sync.OnceFunc(o.release)
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

// dropPrefetches cancels the prefetch objects of the object at diskPath, wakes the
// prefetches waiting for room, and waits for the objects and their cache writes to end,
// or ctx.
func (d *DiskCache) dropPrefetches(ctx context.Context, diskPath string) error {
	f := &d.prefetchObjects
	f.mu.Lock()
	d.dropFillsLocked(diskPath)
	objects := slices.Clone(f.m[diskPath])
	// Prefetches waiting for room see the delete.
	f.freed.notifyLocked()
	f.mu.Unlock()
	for _, o := range objects {
		o.stop()
	}
	return waitFlights(ctx, objects)
}

// read reads the range of f from its source into buf in one request, publishing each
// chunk as it arrives.
func (f *memoryPrefetchObject) read(ctx context.Context) error {
	buf := getFlightBuf(f.end - f.start)
	f.mu.Lock()
	f.buf = buf
	f.mu.Unlock()
	ctx, idle := newIdleReader(ctx)
	defer idle.Close()
	defer f.idle.add(idle)()
	_, err := f.src.g.Do(ctx, rangeRead{key: f.src.key, start: f.start, end: f.end, buf: buf, progress: fillProgress{flight: f}, idle: idle})
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

// fail finishes the flight, which failed to read its range with err when set and the
// flight was not stopped: err then fails the reads waiting for its bytes.
func (f *memoryPrefetchObject) fail(err error) {
	if err != nil && (sourceStopped(err) || f.ctx != nil && f.ctx.Err() != nil) {
		err = nil
	}
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
func (f *memoryPrefetchObject) eta(now time.Time, _, hi int64) time.Duration {
	return max(rangeReadCosts.cost(hi-f.start)-now.Sub(f.began), 0)
}

// waitFor returns the bytes read once they reach end, or false if the flight finished
// short of it or ctx ended first.
func (f *memoryPrefetchObject) waitFor(ctx context.Context, end int64) ([]byte, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.start+f.n < end && !f.finished {
		ch := f.waiters.add(f.start, end)
		f.mu.Unlock()
		done := f.idle.wait()
		select {
		case <-ch:
		case <-ctx.Done():
		}
		done()
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
	// bytes is the size of the buffers of the flights under way or still held by reads;
	// freed, when set, is closed once it shrinks.
	bytes int64
	// background is the part of bytes charged by background flights.
	background int64
	freed      freedSignal
	// streamBytes is the size of the buffers of the memory-backed streams open.
	streamBytes int64
	closed      bool
	// downloads counts the downloads of file fills in flight.
	downloads downloadCount
	// waiting are the prefetch objects of runs waiting for their resources.
	waiting map[waitingObject]struct{}
	// foregroundWaiting counts the foreground flights waiting for a buffer.
	foregroundWaiting int
	// waitingFills are the fills waiting to start, in order; fills are the fills waiting
	// and those of the whole object running before reads can join their downloads;
	// queuedBytes are the bytes of the waiting background fills.
	waitingFills fillHeap
	fills        map[fillKey][]*queuedFill
	queuedBytes  int64
	fillSeq      uint64
	// backgroundFills counts the background fills running, at most fillsMax, 0 for the
	// governor's background room.
	backgroundFills int
	fillsMax        int
	// runWhole runs a fill of the whole object.
	runWhole func(context.Context, *queuedFill)
	reads    sync.WaitGroup // reads of runs and their cache writes
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
	for len(f.waitingFills) > 0 {
		d.removeWaitingLocked(f.waitingFills[0])
	}
	cancelReads := f.cancelReads
	// The prefetch objects waiting for room see the cache closed.
	f.freed.notifyLocked()
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

// joinsLocked reports whether a read of the bytes [lo, hi) of the cache index names may
// join o: o is a live prefetch object of that cache holding them that reads can join and,
// when cached is set, whose bytes end cached.
func joinsLocked(o prefetchObject, index bool, lo, hi int64, cached bool) bool {
	start, end := o.span()
	return start <= lo && hi <= end && o.target().index == index && o.live() && o.joinable() &&
		(!cached || o.endsCached())
}

// arrivesInTime reports whether n bytes of a file fill that arrive wait from now arrive no
// more than extraWait later than a range read could fetch them.
func arrivesInTime(n int64, wait, extraWait time.Duration) bool {
	return wait <= rangeReadCosts.cost(n)+extraWait
}

// fastestLocked returns the prefetch object of diskPath holding [lo, hi) whose bytes reach
// hi first, of the ones joinsLocked allows. A FilePrefetchObject is joined only when its
// bytes arrive no more than extraWait later than a range read could fetch them.
func (f *prefetchObjects) fastestLocked(diskPath string, index bool, lo, hi int64, now time.Time, extraWait time.Duration, cached bool) prefetchObject {
	var best prefetchObject
	var bestETA time.Duration
	for _, o := range f.m[diskPath] {
		if !joinsLocked(o, index, lo, hi, cached) {
			continue
		}
		eta := o.eta(now, lo, hi)
		if _, ok := o.(*filePrefetchObject); ok && !arrivesInTime(hi-lo, eta, extraWait) {
			continue
		}
		if best == nil || eta < bestETA {
			best, bestETA = o, eta
		}
	}
	return best
}

// joinableLocked is fastestLocked of d's prefetch objects with d's max extra wait.
func (d *DiskCache) joinableLocked(diskPath string, index bool, lo, hi int64, now time.Time, cached bool) prefetchObject {
	return d.prefetchObjects.fastestLocked(diskPath, index, lo, hi, now, d.downloadMaxExtraWait, cached)
}

// registeredLocked returns o while it is a prefetch object of diskPath, nil once it ended.
func (f *prefetchObjects) registeredLocked(diskPath string, o prefetchObject) prefetchObject {
	if slices.Contains(f.m[diskPath], o) {
		return o
	}
	return nil
}

// fastestPrefetch returns the time until the bytes [lo, hi) of diskPath arrive from the
// fastest flight of the disk cache containing them, or false if none does.
func (d *DiskCache) fastestPrefetch(diskPath string, lo, hi int64) (time.Duration, bool) {
	f := &d.prefetchObjects
	f.mu.Lock()
	defer f.mu.Unlock()
	now := time.Now()
	if fl := d.joinableLocked(diskPath, false, lo, hi, now, true); fl != nil {
		return fl.eta(now, lo, hi), true
	}
	return 0, false
}

// endPrefetch finishes a flight the caller started and forgets it. Its buffer stays
// charged until its last hold is released.
func (d *DiskCache) endPrefetch(diskPath string, fl *memoryPrefetchObject) {
	f := &d.prefetchObjects
	f.mu.Lock()
	f.forgetLocked(diskPath, fl)
	if fl.fill != nil {
		d.partEndedLocked(fl.fill, fl.cacheErr)
	}
	f.mu.Unlock()
	fl.finish()
	close(fl.endedCh)
}

// unchargeFlight frees the charge of the released buffer of fl.
func (d *DiskCache) unchargeFlight(fl *memoryPrefetchObject) {
	f := &d.prefetchObjects
	f.mu.Lock()
	defer f.mu.Unlock()
	if fl.background {
		f.background -= fl.charge
	}
	d.unchargeLocked(fl.charge)
}

// unchargeLocked frees bytes of the flight budget, wakes the flights waiting for a buffer
// and starts the waiting fills that fit.
func (d *DiskCache) unchargeLocked(bytes int64) {
	f := &d.prefetchObjects
	f.bytes -= bytes
	f.freed.notifyLocked()
	d.startFillsLocked()
}
