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
	"math/bits"
	"os"
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
	// min is the least size of a cached range read.
	min int64
	// auto raises the unit to the bandwidth-delay product of range reads.
	auto bool
	// narrowOnly limits prefetch ranges to narrow reads of groups that read ranges.
	narrowOnly bool
}

// unit returns the size of prefetch ranges, 0 when they are off.
func (c prefetchRangeConfig) unit() int64 {
	unit := c.min
	if c.auto {
		unit = max(unit, autoPrefetchUnit(rangeReadCosts.bandwidthDelay()))
	}
	return unit
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
// its ranges alone.
func (d *DiskCache) prefetchUnit(vector *IOVector) (int64, bool) {
	unit := d.prefetchRange.unit()
	if unit <= 0 || vector.Policy.Any(SkipDiskCacheWrites) || !vector.Policy.CacheIOEntry() {
		return 0, false
	}
	if d.prefetchRange.narrowOnly && !(vector.Policy.Any(NarrowRead) && vector.Policy.Any(SkipFullFilePreloads)) {
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
)

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

// prefetchRead returns a buffer of the bytes [min, max) of filePath that holds the
// bytes of the entries not done. It splits the unit-aligned window around them into
// blocks and takes each block an entry needs from a cached range, from the read of a
// prefetch range under way that ends first, or from a new read: each run of blocks in
// units the entries take bytes of that are neither cached nor being read and holds a
// needed block is read from object storage by fetch as one range, cached, and served to
// other reads while it is read. It returns false when the bytes are not all read.
func (d *DiskCache) prefetchRead(
	ctx context.Context,
	filePath string,
	policy Policy,
	entries []IOEntry,
	min, max, unit int64,
	fetch func(ctx context.Context, start, end int64) ([]byte, error),
) ([]byte, bool) {
	if min < 0 || max <= min {
		return nil, false
	}
	block := int64(prefetchBlockMax)
	if unit < block || unit%block != 0 {
		block = unit
	}
	windowStart := min / unit * unit
	windowEnd := (max + unit - 1) / unit * unit
	if (windowEnd-windowStart)/block > prefetchWindowBlocks {
		return nil, false
	}
	key := d.rangeObjectKey(filePath)
	diskPath := d.pathForFile(filePath)

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
			b.start, b.end = maxInt64(b.start, min), minInt64(b.end, max)
		}
		if r, ok := d.ranges.containing(key, b.start, b.end-b.start, nil); ok && d.cache.Contains(r.diskPath) {
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
		return nil, false
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
	for i := 0; i < len(blocks); {
		if blocks[i].state != blockMissing || !blocks[i].wanted {
			i++
			continue
		}
		j, needed := i, false
		for j < len(blocks) && blocks[j].state == blockMissing && blocks[j].wanted {
			needed = needed || blocks[j].needed
			j++
		}
		if needed {
			start := windowStart + int64(i)*block
			end := windowStart + int64(j)*block
			fl := &prefetchFlight{start: start, end: end, began: now, done: make(chan struct{})}
			if f.m == nil {
				f.m = make(map[string][]*prefetchFlight)
			}
			f.m[diskPath] = append(f.m[diskPath], fl)
			runs = append(runs, run{first: i, last: j - 1, flight: fl})
			f.reads.Add(1)
		}
		i = j
	}
	f.mu.Unlock()

	// Read the runs. A run serves its bytes to waiting reads as soon as it is read and
	// stays registered until its range file is cached, so no read reads it again.
	for i, r := range runs {
		go func(fl *prefetchFlight) {
			defer f.reads.Done()
			content, err := fetch(ctx, fl.start, fl.end)
			if err == nil && len(content) == 0 {
				err = io.ErrUnexpectedEOF
			}
			fl.finish(content, err)
			if err == nil {
				_ = d.Update(context.WithoutCancel(ctx), &IOVector{
					FilePath: filePath,
					Policy:   policy,
					Entries:  []IOEntry{{Offset: fl.start, Size: int64(len(content)), Data: content}},
				}, false)
			}
			d.endPrefetch(diskPath, fl, content, err)
		}(r.flight)
		for j := r.first; j <= r.last; j++ {
			if blocks[j].needed {
				blocks[j].state, blocks[j].flight = blockInFlight, runs[i].flight
			}
		}
	}

	// Assemble the bytes.
	out := make([]byte, max-min)
	var deadline time.Time
	for i := 0; i < len(blocks); i++ {
		b := &blocks[i]
		if !b.needed {
			continue
		}
		dst := out[b.start-min : b.end-min]
		switch b.state {
		case blockInFlight:
			content, ok := b.flight.wait(ctx)
			if !ok || b.flight.start+int64(len(content)) < b.end {
				return nil, false
			}
			copy(dst, content[b.start-b.flight.start:b.end-b.flight.start])
		case blockCached:
			j := i
			for j+1 < len(blocks) && blocks[j+1].needed && blocks[j+1].state == blockCached &&
				blocks[j+1].cached.diskPath == b.cached.diskPath {
				j++
			}
			dst = out[b.start-min : blocks[j].end-min]
			if !d.readCachedRange(ctx, b.cached, b.start, dst, &deadline) {
				return nil, false
			}
			i = j
		default:
			return nil, false
		}
	}
	return out, true
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

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// prefetchFlight is a prefetch range being read from object storage.
type prefetchFlight struct {
	start, end int64
	began      time.Time
	done       chan struct{}
	once       sync.Once
	content    []byte
	err        error
}

func (f *prefetchFlight) finish(content []byte, err error) {
	f.once.Do(func() {
		f.content, f.err = content, err
		close(f.done)
	})
}

// eta estimates how long after now the flight ends.
func (f *prefetchFlight) eta(now time.Time) time.Duration {
	return max(rangeReadCosts.cost(f.end-f.start)-now.Sub(f.began), 0)
}

// wait returns the content of the flight, or false if it failed or ctx ended first.
func (f *prefetchFlight) wait(ctx context.Context) ([]byte, bool) {
	select {
	case <-f.done:
		return f.content, f.err == nil
	case <-ctx.Done():
		return nil, false
	}
}

// prefetchFlights indexes the prefetch ranges being read by the disk path of their
// object.
type prefetchFlights struct {
	mu     sync.Mutex
	m      map[string][]*prefetchFlight
	closed bool
	reads  sync.WaitGroup // reads of runs and their cache writes
}

// closePrefetches stops new prefetch reads and waits for those under way, or ctx.
func (d *DiskCache) closePrefetches(ctx context.Context) {
	f := &d.prefetchFlights
	f.mu.Lock()
	f.closed = true
	f.mu.Unlock()
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

// fastestLocked returns the flight of diskPath containing [min, max) that ends first.
func (f *prefetchFlights) fastestLocked(diskPath string, min, max int64, now time.Time) (*prefetchFlight, time.Duration) {
	var best *prefetchFlight
	var bestETA time.Duration
	for _, fl := range f.m[diskPath] {
		if fl.start > min || fl.end < max {
			continue
		}
		if eta := fl.eta(now); best == nil || eta < bestETA {
			best, bestETA = fl, eta
		}
	}
	return best, bestETA
}

// fastestPrefetch returns the time until the first flight containing [min, max) of
// diskPath ends, or false if none does.
func (d *DiskCache) fastestPrefetch(diskPath string, min, max int64) (time.Duration, bool) {
	f := &d.prefetchFlights
	f.mu.Lock()
	defer f.mu.Unlock()
	fl, eta := f.fastestLocked(diskPath, min, max, time.Now())
	return eta, fl != nil
}

// endPrefetch publishes the content of a flight the caller started and forgets it.
func (d *DiskCache) endPrefetch(diskPath string, fl *prefetchFlight, content []byte, err error) {
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
	f.mu.Unlock()
	fl.finish(content, err)
}
