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
	"slices"
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

func newPrefetchRangeFS(t *testing.T, unit int64, narrowOnly bool, size int) (*S3FS, *countingStorage, []byte) {
	ctx := context.Background()
	fs, err := NewS3FS(ctx, ObjectStorageArguments{Name: "s3", Endpoint: "disk", Bucket: t.TempDir()}, CacheConfig{
		DiskPath: ptrTo(t.TempDir()), DiskCapacity: ptrTo[toml.ByteSize](64 << 20),
		MemoryCapacity: ptrTo[toml.ByteSize](0),
	}, nil, false, false)
	require.NoError(t, err)
	fs.diskCache.prefetchRange = prefetchRangeConfig{on: unit > 0, fixed: unit, narrowOnly: narrowOnly}
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
	fs.diskCache.prefetchFlights.reads.Wait()
	data := slices.Clone(vec.Entries[0].Data)
	vec.Release()
	return data
}

// A short range read caches the aligned prefetch range around it, which serves a later
// read within it without object storage.
func TestS3FSPrefetchRangeServesLaterReads(t *testing.T) {
	fs, storage, data := newPrefetchRangeFS(t, 4096, false, 64<<10)
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
	fs, storage, data := newPrefetchRangeFS(t, 256<<10, false, 512<<10)
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
	fs, _, _ := newPrefetchRangeFS(t, 4096, false, 64<<10)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var opens atomic.Int64
	_, reads, ok := fs.diskCache.prefetchRead(ctx, "object", 0, []IOEntry{{Offset: 5000, Size: 2}}, 5000, 5002, 4096,
		func(context.Context, int64, int64) (io.ReadCloser, error) {
			opens.Add(1)
			return nil, context.Canceled
		})
	fs.diskCache.prefetchFlights.reads.Wait()
	require.False(t, ok)
	require.Zero(t, reads)
	require.Zero(t, opens.Load())
}

// The entries inside a prefetch range are not cached again as ranges of their own.
func TestS3FSPrefetchRangeCachesOnlyTheRange(t *testing.T) {
	fs, _, _ := newPrefetchRangeFS(t, 4096, false, 64<<10)
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
	fs, storage, data := newPrefetchRangeFS(t, 4096, false, 10000)
	require.Equal(t, data[9990:10000], readRangeOf(t, fs, 0, 9990, 10))
	require.Equal(t, data[8200:8300], readRangeOf(t, fs, 0, 8200, 100))
	require.Equal(t, int64(1), storage.reads.Load())
}

// A read crossing a unit boundary reads the units it spans as one range.
func TestS3FSPrefetchRangeAcrossUnits(t *testing.T) {
	fs, storage, data := newPrefetchRangeFS(t, 4096, false, 64<<10)
	require.Equal(t, data[4000:4200], readRangeOf(t, fs, 0, 4000, 200))
	require.Equal(t, int64(8192), storage.bytes.Load())
	require.Equal(t, data[8000:8096], readRangeOf(t, fs, 0, 0, 8192)[8000:8096])
	require.Equal(t, int64(1), storage.reads.Load())
}

// Reads wider than the prefetch window, and every read with the option off, read
// their range alone.
func TestS3FSPrefetchRangeOff(t *testing.T) {
	fs, storage, _ := newPrefetchRangeFS(t, 4096, false, (prefetchWindowBlocks+2)*4096)
	readRangeOf(t, fs, 0, 100, prefetchWindowBlocks*4096+1)
	require.Equal(t, int64(prefetchWindowBlocks*4096+1), storage.bytes.Load())

	fs, storage, _ = newPrefetchRangeFS(t, 0, false, 64<<10)
	readRangeOf(t, fs, 0, 5000, 2)
	require.Equal(t, int64(2), storage.bytes.Load())
}

// With narrow-only, only narrow reads of groups that read ranges prefetch.
func TestS3FSPrefetchRangeNarrowOnly(t *testing.T) {
	fs, storage, _ := newPrefetchRangeFS(t, 4096, true, 64<<10)
	readRangeOf(t, fs, 0, 5000, 2)
	require.Equal(t, int64(2), storage.bytes.Load())
	readRangeOf(t, fs, NarrowRead, 9000, 2)
	require.Equal(t, int64(4098), storage.bytes.Load())
}

func TestDiskPrefetchRangeConfig(t *testing.T) {
	var c CacheConfig
	require.Equal(t, prefetchRangeConfig{on: true}, c.diskPrefetchRange())
	c.DiskPrefetchRangeAligned = ptrTo(false)
	require.Equal(t, prefetchRangeConfig{}, c.diskPrefetchRange())
	c.DiskPrefetchRangeNarrowOnly = true
	c.DiskPrefetchRangeAligned = ptrTo(true)
	require.Equal(t, prefetchRangeConfig{on: true, narrowOnly: true}, c.diskPrefetchRange())
}

func TestAutoPrefetchUnit(t *testing.T) {
	require.Equal(t, int64(64<<10), autoPrefetchUnit(0))
	require.Equal(t, int64(64<<10), autoPrefetchUnit(1000))
	require.Equal(t, int64(4<<20), autoPrefetchUnit(25e-3*100*(1<<20)))
	require.Equal(t, int64(2<<20), autoPrefetchUnit(2<<20))
	require.Equal(t, int64(8<<20), autoPrefetchUnit(100e-3*2000*(1<<20)))

	latency0, bytesPerS0 := rangeReadCosts.latency, rangeReadCosts.bytesPerS
	t.Cleanup(func() { rangeReadCosts.latency, rangeReadCosts.bytesPerS = latency0, bytesPerS0 })
	rangeReadCosts.latency, rangeReadCosts.bytesPerS = 0.025, 100<<20
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
	fs, _, data := newPrefetchRangeFS(t, 4096, false, 64<<10)
	storage := &blockingStorage{ObjectStorage: fs.storage.(*countingStorage).ObjectStorage,
		started: make(chan struct{}), release: make(chan struct{})}
	fs.storage = storage

	first := make(chan []byte, 1)
	go func() { first <- readRangeOf(t, fs, 0, 5000, 2) }()
	<-storage.started
	second := make(chan []byte, 1)
	go func() { second <- readRangeOf(t, fs, 0, 6000, 100) }()
	require.Eventually(t, func() bool {
		fs.diskCache.prefetchFlights.mu.Lock()
		defer fs.diskCache.prefetchFlights.mu.Unlock()
		return len(fs.diskCache.prefetchFlights.m) == 1
	}, diskCacheLifecycleTestTimeout, time.Millisecond)
	time.Sleep(10 * time.Millisecond)
	close(storage.release)
	require.Equal(t, data[5000:5002], <-first)
	require.Equal(t, data[6000:6100], <-second)
	require.Equal(t, int64(1), storage.reads.Load())
	fs.diskCache.prefetchFlights.mu.Lock()
	defer fs.diskCache.prefetchFlights.mu.Unlock()
	require.Empty(t, fs.diskCache.prefetchFlights.m)
}

// addFlight registers a prefetch range of diskPath being read.
func addFlight(d *DiskCache, diskPath string, start, end int64, began time.Time) *prefetchFlight {
	f := &d.prefetchFlights
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.m == nil {
		f.m = make(map[string][]*prefetchFlight)
	}
	fl := newPrefetchFlight(start, end, began)
	f.m[diskPath] = append(f.m[diskPath], fl)
	return fl
}

// deliver publishes content as the next bytes of the flight.
func deliver(fl *prefetchFlight, content []byte) {
	copy(fl.buf[len(fl.content()):], content)
	fl.wrote(len(content))
}

// A reader of a failed prefetch range reads its own range.
func TestS3FSPrefetchRangeWaiterFallsBackOnFailure(t *testing.T) {
	fs, _, data := newPrefetchRangeFS(t, 4096, false, 64<<10)
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
	fs, storage, data := newPrefetchRangeFS(t, 4096, false, 64<<10)
	fs.diskCache.closePrefetches(context.Background())
	require.Equal(t, data[5000:5002], readRangeOf(t, fs, 0, 5000, 2))
	require.Equal(t, int64(2), storage.bytes.Load())
}

// A read spanning cached units reads only the units between them, and takes the rest
// from the cache.
func TestS3FSPrefetchRangeMergesCachedUnits(t *testing.T) {
	fs, storage, data := newPrefetchRangeFS(t, 4096, false, 64<<10)
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
	fs, storage, data := newPrefetchRangeFS(t, 4096, false, 64<<10)
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
	latency0, bytesPerS0 := rangeReadCosts.latency, rangeReadCosts.bytesPerS
	t.Cleanup(func() { rangeReadCosts.latency, rangeReadCosts.bytesPerS = latency0, bytesPerS0 })
	rangeReadCosts.latency, rangeReadCosts.bytesPerS = 0.01, 1<<20

	var d DiskCache
	now := time.Now()
	slow := addFlight(&d, "p", 0, 4<<20, now)
	fast := addFlight(&d, "p", 4096, 8192, now)
	d.prefetchFlights.mu.Lock()
	got, eta := d.prefetchFlights.fastestLocked("p", 5000, 5010, now)
	d.prefetchFlights.mu.Unlock()
	require.Same(t, fast, got)
	require.Less(t, eta, time.Second)
	_, ok := d.fastestPrefetch("p", 9000, 9010)
	require.True(t, ok, "only the slow flight contains it")
	d.endPrefetch("p", fast)
	d.endPrefetch("p", slow)
	_, ok = d.fastestPrefetch("p", 9000, 9010)
	require.False(t, ok)
	require.Empty(t, d.prefetchFlights.m)
}

// A read of entries far apart fetches the units they are in, not the bytes between them.
func TestS3FSPrefetchRangeSkipsGapsBetweenEntries(t *testing.T) {
	fs, storage, data := newPrefetchRangeFS(t, 4096, false, 64<<10)
	vec := &IOVector{FilePath: "object", Policy: SkipFullFilePreloads | SkipMemoryCache, Entries: []IOEntry{
		{Offset: 100, Size: 10}, {Offset: 40000, Size: 5000},
	}}
	require.NoError(t, fs.Read(context.Background(), vec))
	fs.diskCache.prefetchFlights.reads.Wait()
	require.Equal(t, data[100:110], vec.Entries[0].Data)
	require.Equal(t, data[40000:45000], vec.Entries[1].Data)
	require.Equal(t, int64(2), storage.reads.Load())
	require.Equal(t, int64(4096+8192), storage.bytes.Load())
	require.Equal(t, data[41000:41100], readRangeOf(t, fs, 0, 41000, 100))
	require.Equal(t, int64(2), storage.reads.Load())
}

// A reader waiting on a flight takes its bytes once they arrive, before the flight ends.
func TestS3FSPrefetchRangeServesBytesAsTheyArrive(t *testing.T) {
	fs, storage, data := newPrefetchRangeFS(t, 8192, false, 64<<10)
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
	fl := newPrefetchFlight(100, 200, time.Now())
	copy(fl.buf, make([]byte, 50))
	fl.wrote(50)
	content, ok := fl.waitFor(context.Background(), 150)
	require.True(t, ok)
	require.Len(t, content, 50)
	fl.finish()
	_, ok = fl.waitFor(context.Background(), 151)
	require.False(t, ok)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, ok = newPrefetchFlight(0, 10, time.Now()).waitFor(ctx, 5)
	require.False(t, ok)
}

// Objects whose paths have a separator read their ranges alone: the range index does
// not hold their range files.
func TestS3FSPrefetchRangeSkipsPathsWithSeparator(t *testing.T) {
	fs, _, _ := newPrefetchRangeFS(t, 4096, false, 64<<10)
	_, ok := fs.diskCache.prefetchUnit(&IOVector{FilePath: "dir/object", Policy: SkipFullFilePreloads,
		Entries: []IOEntry{{Offset: 0, Size: 10}}})
	require.False(t, ok)
	_, ok = fs.diskCache.prefetchUnit(&IOVector{FilePath: "object", Policy: SkipFullFilePreloads,
		Entries: []IOEntry{{Offset: 0, Size: 10}}})
	require.True(t, ok)
}

// A prefetch read is a sample of the latency of range reads.
func TestS3FSPrefetchRangeSamplesLatency(t *testing.T) {
	latency0 := rangeReadCosts.latency
	t.Cleanup(func() { rangeReadCosts.latency = latency0 })
	rangeReadCosts.latency = 10
	fs, _, _ := newPrefetchRangeFS(t, 4096, false, 64<<10)
	readRangeOf(t, fs, 0, 5000, 2)
	require.Less(t, rangeReadCosts.latency, 10.0)
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
	fs, _, data := newPrefetchRangeFS(t, 128<<10, false, 256<<10)
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
	d.prefetchFlights.reads.Wait()
	require.Equal(t, int64(1), opened.Load())
	require.Equal(t, int64(1), ended.Load(), "read to its end")
	r, ok := d.ranges.containing(d.rangeObjectKey("object"), 0, 128<<10, nil)
	require.True(t, ok, "cached after its reader's context ended")
	require.Equal(t, int64(128<<10), r.size)
}
