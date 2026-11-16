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
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/util/toml"
	"github.com/stretchr/testify/require"
)

type countingStorage struct {
	ObjectStorage
	reads atomic.Int64
	bytes atomic.Int64

	mu     sync.Mutex
	starts []int64
}

func (c *countingStorage) Read(ctx context.Context, key string, min, max *int64) (io.ReadCloser, error) {
	c.reads.Add(1)
	if min != nil && max != nil {
		c.bytes.Add(*max - *min)
	}
	if min != nil && (max != nil || *min > 0) {
		c.mu.Lock()
		c.starts = append(c.starts, *min)
		c.mu.Unlock()
	}
	return c.ObjectStorage.Read(ctx, key, min, max)
}

func newPrefetchRangeFS(t *testing.T, unit int64, size int) (*S3FS, *countingStorage, []byte) {
	ctx := context.Background()
	fs, err := NewS3FS(ctx, ObjectStorageArguments{Name: "s3", Endpoint: "disk", Bucket: t.TempDir()}, CacheConfig{
		DiskPath: ptrTo(t.TempDir()), DiskCapacity: ptrTo[toml.ByteSize](64 << 20),
		MemoryCapacity: ptrTo[toml.ByteSize](0),
	}, nil, false, false)
	require.NoError(t, err)
	fs.diskCache.prefetchRange = prefetchRangeConfig{on: unit > 0, fixed: unit}
	fs.SetAsyncUpdate(false)
	t.Cleanup(func() { fs.Close(ctx) })
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i*7 + i/1021)
	}
	require.NoError(t, fs.Write(ctx, IOVector{FilePath: "object", Entries: []IOEntry{{Size: int64(len(data)), Data: data}}, Policy: SkipDiskCache | SkipMemoryCache}))
	storage := &countingStorage{ObjectStorage: fs.storage}
	fs.storage = storage
	return fs, storage, data
}

// readRangeOf reads the range and waits until the prefetch ranges it read are cached.
func readRangeOf(t *testing.T, fs *S3FS, policy Policy, offset, size int64) []byte {
	vec := &IOVector{FilePath: "object", Policy: policy | SkipFullFilePreloads | SkipMemoryCache,
		Entries: []IOEntry{{Offset: offset, Size: size}}}
	require.NoError(t, fs.Read(context.Background(), vec))
	fs.diskCache.prefetchObjects.reads.Wait()
	data := slices.Clone(vec.Entries[0].Data)
	vec.Release()
	return data
}

// A short range read caches the aligned prefetch range around it, which serves a later
// read within it without object storage.
func TestS3FSPrefetchRangeServesLaterReads(t *testing.T) {
	fs, storage, data := newPrefetchRangeFS(t, 4096, 64<<10)
	require.Equal(t, data[5000:5002], readRangeOf(t, fs, 0, 5000, 2))
	require.Equal(t, int64(1), storage.reads.Load())
	require.Equal(t, int64(4096), storage.bytes.Load())
	require.Equal(t, data[6000:6100], readRangeOf(t, fs, 0, 6000, 100))
	require.Equal(t, data[4096:4097], readRangeOf(t, fs, 0, 4096, 1))
	require.Equal(t, int64(1), storage.reads.Load(), "reads within the prefetch range")
	require.Equal(t, data[9000:9010], readRangeOf(t, fs, 0, 9000, 10))
	require.Equal(t, int64(2), storage.reads.Load(), "a read in another unit")
}

// A read near the end of a unit reads from its own block, and the blocks of the unit
// before it in a range of their own.
func TestS3FSPrefetchRangeStartsAtTheNeededBlock(t *testing.T) {
	fs, storage, data := newPrefetchRangeFS(t, 256<<10, 512<<10)
	require.Equal(t, data[200<<10:200<<10+10], readRangeOf(t, fs, 0, 200<<10, 10))
	storage.mu.Lock()
	starts := slices.Clone(storage.starts)
	storage.mu.Unlock()
	require.ElementsMatch(t, []int64{192 << 10, 0}, starts)
	require.Equal(t, int64(256<<10), storage.bytes.Load())
	require.Equal(t, data[10:20], readRangeOf(t, fs, 0, 10, 10))
	require.Equal(t, int64(2), storage.reads.Load())
}

// A read whose context is done starts no prefetch range.
func TestS3FSPrefetchRangeSkipsCanceledRead(t *testing.T) {
	fs, _, _ := newPrefetchRangeFS(t, 4096, 64<<10)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var opens atomic.Int64
	_, reads, ok := fs.diskCache.prefetchRead(ctx, "object", 0, []IOEntry{{Offset: 5000, Size: 2}}, 5000, 5002, 4096,
		func(context.Context, int64, int64) (io.ReadCloser, error) {
			opens.Add(1)
			return nil, context.Canceled
		})
	fs.diskCache.prefetchObjects.reads.Wait()
	require.False(t, ok)
	require.Zero(t, reads)
	require.Zero(t, opens.Load())
}

// The entries inside a prefetch range are not cached again as ranges of their own.
func TestS3FSPrefetchRangeCachesOnlyTheRange(t *testing.T) {
	fs, _, _ := newPrefetchRangeFS(t, 4096, 64<<10)
	readRangeOf(t, fs, 0, 5000, 2)
	fs.diskCache.ranges.mu.Lock()
	defer fs.diskCache.ranges.mu.Unlock()
	var ranges []cachedRange
	for _, rs := range fs.diskCache.ranges.byObject {
		ranges = append(ranges, rs...)
	}
	require.Len(t, ranges, 1)
	require.Equal(t, int64(4096), ranges[0].offset)
	require.Equal(t, int64(4096), ranges[0].size)
}

// A prefetch range past the end of the object holds the bytes up to the end.
func TestS3FSPrefetchRangeAtObjectEnd(t *testing.T) {
	fs, storage, data := newPrefetchRangeFS(t, 4096, 10000)
	require.Equal(t, data[9990:10000], readRangeOf(t, fs, 0, 9990, 10))
	require.Equal(t, data[8200:8300], readRangeOf(t, fs, 0, 8200, 100))
	require.Equal(t, int64(1), storage.reads.Load())
}

// A read crossing a unit boundary reads the units it spans as one range.
func TestS3FSPrefetchRangeAcrossUnits(t *testing.T) {
	fs, storage, data := newPrefetchRangeFS(t, 4096, 64<<10)
	require.Equal(t, data[4000:4200], readRangeOf(t, fs, 0, 4000, 200))
	require.Equal(t, int64(8192), storage.bytes.Load())
	require.Equal(t, data[8000:8096], readRangeOf(t, fs, 0, 0, 8192)[8000:8096])
	require.Equal(t, int64(1), storage.reads.Load())
}

// Reads wider than the prefetch window, and every read with prefetch ranges turned off,
// read their range alone.
func TestS3FSPrefetchRangeOff(t *testing.T) {
	fs, storage, _ := newPrefetchRangeFS(t, 4096, (prefetchWindowBlocks+2)*4096)
	readRangeOf(t, fs, 0, 100, prefetchWindowBlocks*4096+1)
	require.Equal(t, int64(prefetchWindowBlocks*4096+1), storage.bytes.Load())

	fs, storage, _ = newPrefetchRangeFS(t, 0, 64<<10)
	readRangeOf(t, fs, 0, 5000, 2)
	require.Equal(t, int64(2), storage.bytes.Load())
}

func TestAutoPrefetchUnit(t *testing.T) {
	require.Equal(t, int64(64<<10), autoPrefetchUnit(0))
	require.Equal(t, int64(64<<10), autoPrefetchUnit(1000))
	require.Equal(t, int64(4<<20), autoPrefetchUnit(25e-3*100*(1<<20)))
	require.Equal(t, int64(2<<20), autoPrefetchUnit(2<<20))
	require.Equal(t, int64(8<<20), autoPrefetchUnit(100e-3*2000*(1<<20)))

	rangeCosts(t, 0.025, 100<<20)
	require.Equal(t, int64(4<<20), prefetchRangeConfig{on: true}.unit())
	require.Equal(t, int64(16<<20), prefetchRangeConfig{on: true, fixed: 16 << 20}.unit())
	require.Equal(t, int64(0), prefetchRangeConfig{fixed: 16 << 20}.unit())
}

type blockingStorage struct {
	ObjectStorage
	reads   atomic.Int64
	started chan struct{}
	release chan struct{}
}

func (b *blockingStorage) Read(ctx context.Context, key string, min, max *int64) (io.ReadCloser, error) {
	if b.reads.Add(1) == 1 {
		close(b.started)
		<-b.release
	}
	return b.ObjectStorage.Read(ctx, key, min, max)
}

// A read within a prefetch range under way waits for it instead of reading object
// storage.
func TestS3FSPrefetchRangeReadersShareTheRead(t *testing.T) {
	fs, _, data := newPrefetchRangeFS(t, 4096, 64<<10)
	storage := &blockingStorage{ObjectStorage: fs.storage.(*countingStorage).ObjectStorage,
		started: make(chan struct{}), release: make(chan struct{})}
	fs.storage = storage

	first := make(chan []byte, 1)
	go func() { first <- readRangeOf(t, fs, 0, 5000, 2) }()
	<-storage.started
	second := make(chan []byte, 1)
	go func() { second <- readRangeOf(t, fs, 0, 6000, 100) }()
	require.Eventually(t, func() bool {
		fs.diskCache.prefetchObjects.mu.Lock()
		defer fs.diskCache.prefetchObjects.mu.Unlock()
		return len(fs.diskCache.prefetchObjects.m) == 1
	}, diskCacheLifecycleTestTimeout, time.Millisecond)
	time.Sleep(10 * time.Millisecond)
	close(storage.release)
	require.Equal(t, data[5000:5002], <-first)
	require.Equal(t, data[6000:6100], <-second)
	require.Equal(t, int64(1), storage.reads.Load())
	fs.diskCache.prefetchObjects.mu.Lock()
	defer fs.diskCache.prefetchObjects.mu.Unlock()
	require.Empty(t, fs.diskCache.prefetchObjects.m)
}

// addFlight registers a prefetch range of diskPath being read.
func addFlight(d *DiskCache, diskPath string, start, end int64, began time.Time) *memoryPrefetchObject {
	f := &d.prefetchObjects
	f.mu.Lock()
	defer f.mu.Unlock()
	fl := newMemoryPrefetchObject(start, end, began)
	fl.buf = make([]byte, end-start)
	f.registerLocked(diskPath, fl)
	return fl
}

// deliver publishes content as the next bytes of the flight.
func deliver(fl *memoryPrefetchObject, content []byte) {
	copy(fl.buf[len(fl.content()):], content)
	fl.wrote(len(content))
}

// A reader of a failed prefetch range reads its own range.
func TestS3FSPrefetchRangeWaiterFallsBackOnFailure(t *testing.T) {
	fs, _, data := newPrefetchRangeFS(t, 4096, 64<<10)
	diskPath := fs.diskCache.pathForFile("object")
	fl := addFlight(fs.diskCache, diskPath, 4096, 8192, time.Now())
	storage := fs.storage.(*countingStorage)
	done := make(chan []byte, 1)
	go func() { done <- readRangeOf(t, fs, 0, 5000, 2) }()
	time.Sleep(20 * time.Millisecond)
	require.Zero(t, storage.reads.Load(), "waits for the range being read")
	fs.diskCache.endPrefetch(diskPath, fl)
	require.Equal(t, data[5000:5002], <-done)
	require.Equal(t, int64(1), storage.reads.Load())
	require.Equal(t, int64(2), storage.bytes.Load())
}

// A closed cache starts no prefetch reads.
func TestS3FSPrefetchRangeAfterClose(t *testing.T) {
	fs, storage, data := newPrefetchRangeFS(t, 4096, 64<<10)
	fs.diskCache.closePrefetches(context.Background())
	require.Equal(t, data[5000:5002], readRangeOf(t, fs, 0, 5000, 2))
	require.Equal(t, int64(2), storage.bytes.Load())
}

// A read spanning cached units reads only the units between them, and takes the rest
// from the cache.
func TestS3FSPrefetchRangeMergesCachedUnits(t *testing.T) {
	fs, storage, data := newPrefetchRangeFS(t, 4096, 64<<10)
	readRangeOf(t, fs, 0, 4100, 1)
	readRangeOf(t, fs, 0, 12300, 1)
	require.Equal(t, int64(8192), storage.bytes.Load())
	require.Equal(t, data[4096:16384], readRangeOf(t, fs, 0, 4096, 12288))
	require.Equal(t, int64(3), storage.reads.Load())
	require.Equal(t, int64(3*4096), storage.bytes.Load(), "only the middle unit")
	require.Equal(t, data[4000:16000], readRangeOf(t, fs, 0, 4000, 12000))
	require.Equal(t, int64(4), storage.reads.Load(), "only the first unit")
	require.Equal(t, int64(4*4096), storage.bytes.Load())
}

// A read spanning a unit being read and a missing unit reads only the missing one and
// waits for the other.
func TestS3FSPrefetchRangeNeverReadsABlockTwice(t *testing.T) {
	fs, storage, data := newPrefetchRangeFS(t, 4096, 64<<10)
	diskPath := fs.diskCache.pathForFile("object")
	fl := addFlight(fs.diskCache, diskPath, 4096, 8192, time.Now())
	done := make(chan []byte, 1)
	go func() { done <- readRangeOf(t, fs, 0, 6000, 4000) }()
	require.Eventually(t, func() bool { return storage.reads.Load() == 1 }, diskCacheLifecycleTestTimeout, time.Millisecond)
	require.Equal(t, int64(4096), storage.bytes.Load(), "only the unit not being read")
	deliver(fl, data[4096:8192])
	fs.diskCache.endPrefetch(diskPath, fl)
	require.Equal(t, data[6000:10000], <-done)
	require.Equal(t, int64(1), storage.reads.Load())
}

// A block takes its bytes from the containing flight that ends first.
func TestPrefetchFastestFlight(t *testing.T) {
	rangeCosts(t, 0.01, 1<<20)

	var d DiskCache
	now := time.Now()
	slow := addFlight(&d, "p", 0, 4<<20, now)
	fast := addFlight(&d, "p", 4096, 8192, now)
	d.prefetchObjects.mu.Lock()
	got := d.prefetchObjects.fastestLocked("p", 5000, 5010, now, 0, true)
	eta := got.eta(now, 5000, 5010)
	d.prefetchObjects.mu.Unlock()
	require.Same(t, fast, got)
	require.Less(t, eta, time.Second)
	_, ok := d.fastestPrefetch("p", 9000, 9010)
	require.True(t, ok, "only the slow flight contains it")
	d.endPrefetch("p", fast)
	d.endPrefetch("p", slow)
	_, ok = d.fastestPrefetch("p", 9000, 9010)
	require.False(t, ok)
	require.Empty(t, d.prefetchObjects.m)
}

// A read of entries far apart fetches the units they are in, not the bytes between them.
func TestS3FSPrefetchRangeSkipsGapsBetweenEntries(t *testing.T) {
	fs, storage, data := newPrefetchRangeFS(t, 4096, 64<<10)
	vec := &IOVector{FilePath: "object", Policy: SkipFullFilePreloads | SkipMemoryCache, Entries: []IOEntry{
		{Offset: 100, Size: 10}, {Offset: 40000, Size: 5000},
	}}
	require.NoError(t, fs.Read(context.Background(), vec))
	fs.diskCache.prefetchObjects.reads.Wait()
	require.Equal(t, data[100:110], vec.Entries[0].Data)
	require.Equal(t, data[40000:45000], vec.Entries[1].Data)
	require.Equal(t, int64(2), storage.reads.Load())
	require.Equal(t, int64(4096+8192), storage.bytes.Load())
	require.Equal(t, data[41000:41100], readRangeOf(t, fs, 0, 41000, 100))
	require.Equal(t, int64(2), storage.reads.Load())
}

// A reader waiting on a flight takes its bytes once they arrive, before the flight ends.
func TestS3FSPrefetchRangeServesBytesAsTheyArrive(t *testing.T) {
	fs, storage, data := newPrefetchRangeFS(t, 8192, 64<<10)
	diskPath := fs.diskCache.pathForFile("object")
	fl := addFlight(fs.diskCache, diskPath, 0, 8192, time.Now())
	done := make(chan []byte, 1)
	go func() { done <- readRangeOf(t, fs, 0, 100, 10) }()
	deliver(fl, data[:4096])
	require.Equal(t, data[100:110], <-done)
	require.Zero(t, storage.reads.Load())
	fs.diskCache.endPrefetch(diskPath, fl)
}

// A flight returns false to a reader of bytes it finished without.
func TestPrefetchFlightWaitFor(t *testing.T) {
	fl := newMemoryPrefetchObject(100, 200, time.Now())
	fl.buf = make([]byte, 100)
	fl.wrote(50)
	content, ok := fl.waitFor(context.Background(), 150)
	require.True(t, ok)
	require.Len(t, content, 50)
	fl.finish()
	_, ok = fl.waitFor(context.Background(), 151)
	require.False(t, ok)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, ok = newMemoryPrefetchObject(0, 10, time.Now()).waitFor(ctx, 5)
	require.False(t, ok)
}

// A flight whose source stops sending fails after the idle timeout with the bytes it got.
func TestPrefetchFlightFailsWhenSourceStalls(t *testing.T) {
	idle0 := sourceIdleTimeout
	sourceIdleTimeout = 20 * time.Millisecond
	t.Cleanup(func() { sourceIdleTimeout = idle0 })
	fl := newMemoryPrefetchObject(0, 10, time.Now())
	err := fl.read(context.Background(), func(ctx context.Context, start, end int64) (io.ReadCloser, error) {
		return io.NopCloser(&stalledReader{ctx: ctx, data: []byte("abc")}), nil
	})
	require.ErrorIs(t, err, errSourceIdle)
	require.Equal(t, []byte("abc"), fl.content())
}

// Objects whose paths have a separator read their ranges alone: the range index does
// not hold their range files.
func TestS3FSPrefetchRangeSkipsPathsWithSeparator(t *testing.T) {
	fs, _, _ := newPrefetchRangeFS(t, 4096, 64<<10)
	_, ok := fs.diskCache.prefetchUnit(&IOVector{FilePath: "dir/object", Policy: SkipFullFilePreloads,
		Entries: []IOEntry{{Offset: 0, Size: 10}}})
	require.False(t, ok)
	_, ok = fs.diskCache.prefetchUnit(&IOVector{FilePath: "object", Policy: SkipFullFilePreloads,
		Entries: []IOEntry{{Offset: 0, Size: 10}}})
	require.True(t, ok)
}

// A prefetch read is a sample of the latency of range reads.
func TestS3FSPrefetchRangeSamplesLatency(t *testing.T) {
	rangeCosts(t, 10, rangeReadCosts.get().bytesPerS)
	fs, _, _ := newPrefetchRangeFS(t, 4096, 64<<10)
	readRangeOf(t, fs, 0, 5000, 2)
	require.Less(t, rangeReadCosts.get().latency, 10.0)
}

type trackedReader struct {
	ctx   context.Context
	r     io.ReadCloser
	eof   bool
	gate  chan struct{} // closed to let reads past the first chunk through
	first bool
	ended *atomic.Int64 // readers closed after their end
}

func (t *trackedReader) Read(p []byte) (int, error) {
	if t.first && t.gate != nil {
		<-t.gate
		if err := t.ctx.Err(); err != nil {
			return 0, err
		}
	}
	t.first = true
	n, err := t.r.Read(p)
	if err == io.EOF {
		t.eof = true
	}
	return n, err
}

func (t *trackedReader) Close() error {
	if t.eof {
		t.ended.Add(1)
	}
	return t.r.Close()
}

// A prefetch read reads each range to its end, and finishes after the read that started
// it returns and its context ends.
func TestPrefetchReadOutlivesItsReader(t *testing.T) {
	fs, _, data := newPrefetchRangeFS(t, 128<<10, 256<<10)
	d := fs.diskCache
	gate := make(chan struct{})
	var opened, ended atomic.Int64
	open := func(ctx context.Context, start, end int64) (io.ReadCloser, error) {
		opened.Add(1)
		r, err := fs.storage.Read(ctx, "object", &start, &end)
		if err != nil {
			return nil, err
		}
		return &trackedReader{ctx: ctx, r: r, gate: gate, ended: &ended}, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	entries := []IOEntry{{Offset: 100, Size: 10}}
	content, reads, ok := d.prefetchRead(ctx, "object", SkipFullFilePreloads, entries, 100, 110, 128<<10, open)
	require.True(t, ok)
	require.Equal(t, 1, reads)
	require.Equal(t, data[100:110], content)
	cancel()
	close(gate)
	d.prefetchObjects.reads.Wait()
	require.Equal(t, int64(1), opened.Load())
	require.Equal(t, int64(1), ended.Load(), "read to its end")
	r, ok := d.ranges.containing(d.rangeObjectKey("object"), 0, 128<<10, nil)
	require.True(t, ok, "cached after its reader's context ended")
	require.Equal(t, int64(128<<10), r.size)
}

// Deleting an object deletes its cached ranges: a read of another offset in a range
// cached before the delete reads the object written after it.
func TestDeleteRemovesCachedRanges(t *testing.T) {
	fs, storage, data := newPrefetchRangeFS(t, 4096, 64<<10)
	ctx := context.Background()
	require.Equal(t, data[5000:5002], readRangeOf(t, fs, 0, 5000, 2))
	require.Equal(t, int64(1), storage.reads.Load())
	require.NoError(t, fs.Delete(ctx, "object"))
	require.Empty(t, fs.diskCache.ranges.of(fs.diskCache.rangeObjectKey("object")))
	_, err := fs.diskCache.ranges.covering(fs.diskCache.rangeObjectKey("object"), 6000, 100)
	require.False(t, err, "the deleted object's ranges are not indexed")

	recreated := slices.Clone(data)
	for i := range recreated {
		recreated[i] ^= 0xff
	}
	require.NoError(t, fs.Write(ctx, IOVector{FilePath: "object", Entries: []IOEntry{{Size: int64(len(recreated)), Data: recreated}}, Policy: SkipDiskCache | SkipMemoryCache}))
	require.Equal(t, recreated[6000:6100], readRangeOf(t, fs, 0, 6000, 100))
	require.Equal(t, int64(2), storage.reads.Load(), "the read goes to object storage")
}

// stuckStorage blocks a read until its context is canceled or the test ends.
type stuckStorage struct {
	ObjectStorage
	started chan struct{}
	end     chan struct{}
}

func (b *stuckStorage) Read(ctx context.Context, key string, min, max *int64) (io.ReadCloser, error) {
	b.started <- struct{}{}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-b.end:
		return nil, io.ErrUnexpectedEOF
	}
}

// Closing the disk cache cancels the prefetch range reads under way, which outlive the
// reads that start them.
func TestCloseCancelsPrefetchRangeReads(t *testing.T) {
	fs, _, _ := newPrefetchRangeFS(t, 4096, 64<<10)
	storage := &stuckStorage{ObjectStorage: fs.storage, started: make(chan struct{}, 1), end: make(chan struct{})}
	fs.storage = storage
	t.Cleanup(func() { close(storage.end) })
	readCtx, cancel := context.WithCancel(context.Background())
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		vec := &IOVector{FilePath: "object", Policy: SkipFullFilePreloads | SkipMemoryCache, Entries: []IOEntry{{Offset: 5000, Size: 2}}}
		_ = fs.Read(readCtx, vec)
		vec.Release()
	}()
	<-storage.started
	cancel()
	<-readDone

	closed := make(chan struct{})
	go func() {
		fs.diskCache.Close(context.Background())
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(10 * time.Second):
		t.Fatal("close waits for a blocked prefetch range read")
	}
}

// stallingStorage serves the first after bytes of the first read, then holds it until
// its context ends or release is closed.
type stallingStorage struct {
	ObjectStorage
	after    int
	reads    atomic.Int64
	stalled  chan struct{}
	release  chan struct{}
	canceled atomic.Bool
}

func (s *stallingStorage) Read(ctx context.Context, key string, min, max *int64) (io.ReadCloser, error) {
	r, err := s.ObjectStorage.Read(ctx, key, min, max)
	if err != nil || s.reads.Add(1) > 1 {
		return r, err
	}
	return &stallingReader{r: r, s: s, ctx: ctx, left: s.after}, nil
}

type stallingReader struct {
	r    io.ReadCloser
	s    *stallingStorage
	ctx  context.Context
	left int
}

func (r *stallingReader) Read(p []byte) (int, error) {
	if r.left == 0 {
		r.left = -1
		close(r.s.stalled)
		select {
		case <-r.ctx.Done():
			r.s.canceled.Store(true)
			return 0, r.ctx.Err()
		case <-r.s.release:
		}
	}
	if r.left > 0 && len(p) > r.left {
		p = p[:r.left]
	}
	n, err := r.r.Read(p)
	if r.left > 0 {
		r.left -= n
	}
	return n, err
}

func (r *stallingReader) Close() error { return r.r.Close() }

// Deleting an object stops the read of a prefetch range under way before its range is
// cached: after a delete and recreation, another offset of that range is read from the
// new object.
func TestDeleteDropsPrefetchRangeUnderWay(t *testing.T) {
	fs, _, data := newPrefetchRangeFS(t, 64<<10, 256<<10)
	ctx := context.Background()
	storage := &stallingStorage{ObjectStorage: fs.storage, after: 4096, stalled: make(chan struct{}), release: make(chan struct{})}
	fs.storage = storage

	vec := &IOVector{FilePath: "object", Policy: SkipFullFilePreloads | SkipMemoryCache, Entries: []IOEntry{{Offset: 100, Size: 10}}}
	require.NoError(t, fs.Read(ctx, vec))
	require.Equal(t, data[100:110], vec.Entries[0].Data)
	vec.Release()
	<-storage.stalled

	require.NoError(t, fs.Delete(ctx, "object"))
	require.True(t, storage.canceled.Load(), "the delete cancels the read under way")
	close(storage.release)
	fs.diskCache.prefetchObjects.reads.Wait()
	require.Empty(t, fs.diskCache.ranges.of(fs.diskCache.rangeObjectKey("object")))

	recreated := slices.Clone(data)
	for i := range recreated {
		recreated[i] ^= 0xff
	}
	require.NoError(t, fs.Write(ctx, IOVector{FilePath: "object", Entries: []IOEntry{{Size: int64(len(recreated)), Data: recreated}}, Policy: SkipDiskCache | SkipMemoryCache}))
	require.Equal(t, recreated[30000:30010], readRangeOf(t, fs, 0, 30000, 10))
}

// A flight's buffer is kept for reuse only once the flight is forgotten and no read copies
// from it. Buffers are of a power-of-two size up to 32 MiB, past which a flight allocates
// its own.
func TestPrefetchFlightBufferReusedAfterLastHolder(t *testing.T) {
	fl := newMemoryPrefetchObject(0, 100<<10, time.Now())
	fl.buf = getFlightBuf(100 << 10)
	require.Equal(t, 128<<10, cap(fl.buf))
	fl.hold()
	fl.release()
	require.NotNil(t, fl.buf, "the flight still holds it")
	fl.release()
	require.Nil(t, fl.buf)

	require.Equal(t, int64(64<<10), flightBufSize(1))
	require.Equal(t, int64(32<<20), flightBufSize(20<<20))
	require.Equal(t, int64(40<<20), flightBufSize(40<<20), "past the pooled sizes, its own size")
	require.Len(t, getFlightBuf(40<<20), 40<<20)
	putFlightBuf(make([]byte, 40<<20))
	putFlightBuf(make([]byte, 100))
}

// The bytes of the flights under way are a share of the memory available, measured at
// most once per refresh, and at least one column prefetch's bytes.
func TestFlightBytesFollowAvailableMemory(t *testing.T) {
	prev := memoryAvailable
	t.Cleanup(func() { memoryAvailable = prev })
	var calls int
	avail := uint64(10 << 30)
	memoryAvailable = func() (uint64, bool) { calls++; return avail, true }
	d := &DiskCache{memoryPrefetchMax: 32 << 20}
	share := int64(avail / 100 * flightMemoryPercent)
	require.Equal(t, share, d.flightBytes())
	avail = 1 << 20
	require.Equal(t, share, d.flightBytes(), "measured once per refresh")
	require.Equal(t, 1, calls)
	d.flightBudget.at.Store(0)
	require.Equal(t, int64(32<<20), d.flightBytes(), "at least one MemoryPrefetchObject")
	memoryAvailable = func() (uint64, bool) { return 0, false }
	d.flightBudget.at.Store(0)
	require.Equal(t, int64(prefetchFlightBytesMax), d.flightBytes())
	d.prefetchFlightBytes = 7
	require.Equal(t, int64(7), d.flightBytes())
}

// A read of a stopped MemoryPrefetchObject misses its bytes; a read of one whose read
// failed gets the read's error.
func TestMemoryPrefetchObjectUseErrors(t *testing.T) {
	ctx := context.Background()
	noop := func([]byte) error { return nil }

	stopped := newMemoryPrefetchObject(0, 10, time.Now())
	stopped.fail(context.Canceled)
	require.ErrorIs(t, stopped.use(ctx, 0, 10, noop), errPrefetchMissed)

	readErr := errors.New("read failed")
	failed := newMemoryPrefetchObject(0, 10, time.Now())
	failed.fail(readErr)
	require.ErrorIs(t, failed.use(ctx, 0, 10, noop), readErr)

	short := newMemoryPrefetchObject(0, 10, time.Now())
	short.buf = make([]byte, 10)
	deliver(short, []byte{1, 2, 3})
	short.finish()
	var got []byte
	require.NoError(t, short.use(ctx, 1, 2, func(bs []byte) error { got = append(got, bs...); return nil }))
	require.Equal(t, []byte{2, 3}, got)
	require.ErrorIs(t, short.use(ctx, 0, 10, noop), errPrefetchMissed)
}

// A range read takes its bytes from a prefetch object that caches nothing: a read that
// caches its entries caches them, and one that caches whole objects caches no byte.
func TestS3FSRangeReadCachesItsEntriesNotItsPrefetchObject(t *testing.T) {
	fs, storage, data := newPrefetchRangeFS(t, 4096, 64<<10)
	read := func(policy Policy, rangeRead bool) {
		vec := &IOVector{FilePath: "object", Policy: policy | SkipMemoryCache,
			Entries: []IOEntry{{Offset: 5000, Size: 10}, {Offset: 5010, Size: 10}}}
		if rangeRead {
			// A read that caches whole objects reads ranges when it does not download one.
			require.NoError(t, fs.read(context.Background(), vec, true))
		} else {
			require.NoError(t, fs.Read(context.Background(), vec))
		}
		fs.diskCache.prefetchObjects.reads.Wait()
		require.Equal(t, data[5000:5010], vec.Entries[0].Data)
		require.Equal(t, data[5010:5020], vec.Entries[1].Data)
		vec.Release()
	}
	cacheFiles := func() []string {
		var files []string
		require.NoError(t, filepath.Walk(fs.diskCache.path, func(p string, info os.FileInfo, err error) error {
			if err == nil && !info.IsDir() && strings.HasSuffix(p, cacheFileSuffix) {
				files = append(files, filepath.Base(p))
			}
			return err
		}))
		slices.Sort(files)
		return files
	}

	read(0, true)
	require.Equal(t, int64(1), storage.reads.Load())
	require.Empty(t, cacheFiles())

	read(SkipFullFilePreloads|ExactRead, false)
	require.Equal(t, int64(2), storage.reads.Load())
	require.Equal(t, []string{
		filepath.Base(fs.diskCache.pathForIOEntry("object", IOEntry{Offset: 5000, Size: 10})),
		filepath.Base(fs.diskCache.pathForIOEntry("object", IOEntry{Offset: 5010, Size: 10})),
	}, cacheFiles(), "the entries, not the prefetch object's range")
}
