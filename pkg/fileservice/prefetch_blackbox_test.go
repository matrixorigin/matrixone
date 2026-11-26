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
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// blackBoxStorage holds each range read until the channel gated returns for it is closed,
// none for nil, and counts the reads of each object under way and in all.
type blackBoxStorage struct {
	ObjectStorage
	gated func(key string, offset int64) chan struct{}
	gate  chan struct{}

	mu    sync.Mutex
	cur   map[string]int
	most  map[string]int
	total map[string]int
}

// newBlackBoxStorage returns a storage holding the reads gated accepts until its gate is
// closed.
func newBlackBoxStorage(upstream ObjectStorage, gated func(key string, offset int64) bool) *blackBoxStorage {
	g := &blackBoxStorage{ObjectStorage: upstream, gate: make(chan struct{}),
		cur: map[string]int{}, most: map[string]int{}, total: map[string]int{}}
	g.gated = func(key string, offset int64) chan struct{} {
		if gated(key, offset) {
			return g.gate
		}
		return nil
	}
	return g
}

func (g *blackBoxStorage) Read(ctx context.Context, key string, from, to *int64) (io.ReadCloser, error) {
	var offset int64
	if from != nil {
		offset = *from
	}
	g.mu.Lock()
	g.cur[key]++
	g.total[key]++
	g.most[key] = max(g.most[key], g.cur[key])
	g.mu.Unlock()
	defer func() {
		g.mu.Lock()
		g.cur[key]--
		g.mu.Unlock()
	}()
	if gate := g.gated(key, offset); gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return g.ObjectStorage.Read(ctx, key, from, to)
}

func (g *blackBoxStorage) counts(key string) (cur, most, total int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.cur[key], g.most[key], g.total[key]
}

func columnRanges(n int, stride, size int64) []PrefetchRange {
	ranges := make([]PrefetchRange, n)
	for i := range ranges {
		ranges[i] = PrefetchRange{Offset: int64(i) * stride, Size: size}
	}
	return ranges
}

func readColumnRange(ctx context.Context, fs *S3FS, r PrefetchRange) ([]byte, error) {
	vec := &IOVector{FilePath: "object", Policy: ColumnPrefetch | SkipFullFilePreloads | SkipMemoryCache,
		Entries: []IOEntry{{Offset: r.Offset, Size: r.Size}}}
	if err := fs.Read(ctx, vec); err != nil {
		return nil, err
	}
	defer vec.Release()
	return append([]byte(nil), vec.Entries[0].Data...), nil
}

// A column read that joined a prefetch of the object returns once its range is read, while
// the prefetch's later ranges still wait.
func TestBlackBoxJoinedColumnReadReturnsWhileRangesWait(t *testing.T) {
	fs, _, data := newPrefetchRangeFS(t, 0, 300<<10)
	ranges := columnRanges(3, 40<<10, 1<<10)
	fs.diskCache.prefetchFlightBytes = flightBufSize(ranges[0].Size)
	first, rest := make(chan struct{}), make(chan struct{})
	g := newBlackBoxStorage(fs.governor.raw, func(string, int64) bool { return false })
	g.gated = func(_ string, offset int64) chan struct{} {
		if offset == ranges[0].Offset {
			return first
		}
		return rest
	}
	fs.governor.raw = g
	releaseFirst, releaseRest := sync.OnceFunc(func() { close(first) }), sync.OnceFunc(func() { close(rest) })
	defer releaseFirst()
	defer releaseRest()
	ctx := context.Background()
	key := fs.pathToKey("object")
	require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "object", Size: 3 << 10, Ranges: ranges, Scan: &PrefetchScan{}}))
	require.Eventually(t, func() bool { cur, _, _ := g.counts(key); return cur == 1 }, 5*time.Second, time.Millisecond)
	readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	type result struct {
		data []byte
		err  error
	}
	done := make(chan result, 1)
	go func() {
		got, err := readColumnRange(readCtx, fs, ranges[0])
		done <- result{got, err}
	}()
	require.Never(t, func() bool { return len(done) > 0 }, 100*time.Millisecond, time.Millisecond, "the read waits for its range")
	releaseFirst()
	r := <-done
	require.NoError(t, r.err, "the read returns while the later ranges wait")
	require.Equal(t, data[ranges[0].Offset:ranges[0].Offset+ranges[0].Size], r.data)
}

// The memory flights of a background column prefetch run at most the background room left.
func TestBlackBoxColumnPrefetchHoldsDownloadConnections(t *testing.T) {
	fs, _, _ := newPrefetchRangeFS(t, 0, 300<<10)
	limitRoom(t, fs.governor, 18, 2)
	g := newBlackBoxStorage(fs.governor.raw, func(string, int64) bool { return true })
	fs.governor.raw = g
	release := sync.OnceFunc(func() { close(g.gate) })
	defer release()
	ranges := columnRanges(8, 20<<10, 1<<10)
	require.NoError(t, fs.PrefetchObject(context.Background(), ObjectPrefetch{FilePath: "object", Size: 8 << 10, Ranges: ranges, Scan: &PrefetchScan{}}))
	key := fs.pathToKey("object")
	require.Eventually(t, func() bool { cur, _, _ := g.counts(key); return cur == 2 }, 5*time.Second, time.Millisecond)
	require.Never(t, func() bool { cur, _, _ := g.counts(key); return cur > 2 }, 100*time.Millisecond, time.Millisecond)
	release()
	require.Eventually(t, func() bool { _, _, total := g.counts(key); return total == len(ranges) }, 5*time.Second, time.Millisecond)
	_, most, _ := g.counts(key)
	require.Equal(t, 2, most)
}

// A file-backed column prefetch starts no range of an object deleted while it runs.
func TestBlackBoxColumnPrefetchStopsAfterDelete(t *testing.T) {
	fs, _, _ := newPrefetchRangeFS(t, 0, 300<<10)
	fs.diskCache.memoryPrefetchMax = 1
	g := newBlackBoxStorage(fs.governor.raw, func(string, int64) bool { return true })
	fs.governor.raw = g
	release := sync.OnceFunc(func() { close(g.gate) })
	defer release()
	ranges := columnRanges(14, 20<<10, 1<<10)
	ctx := context.Background()
	require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "object", Size: 14 << 10, Ranges: ranges, Scan: &PrefetchScan{}}))
	key := fs.pathToKey("object")
	require.Eventually(t, func() bool { cur, _, _ := g.counts(key); return cur == fs.diskCache.prefetchShare() }, 5*time.Second, time.Millisecond)
	require.NoError(t, fs.Delete(ctx, "object"))
	release()
	require.Eventually(t, func() bool { return !fs.prefetcher.isRunning("object") }, 5*time.Second, time.Millisecond)
	_, _, total := g.counts(key)
	require.LessOrEqual(t, total, fs.diskCache.prefetchShare(), "no range starts after the delete")
}

// A request for an object downloading does not take over what an ended scan deferred behind
// it, so another object's live request is still downloaded.
func TestBlackBoxEndedDeferredPrefetchNotInherited(t *testing.T) {
	fs, _, data := newPrefetchRangeFS(t, 0, 300<<10)
	ctx := context.Background()
	require.NoError(t, fs.Write(ctx, IOVector{FilePath: "other", Entries: []IOEntry{{Size: int64(len(data)), Data: data}},
		Policy: SkipDiskCache | SkipMemoryCache}))
	objectKey, otherKey := fs.pathToKey("object"), fs.pathToKey("other")
	g := newBlackBoxStorage(fs.governor.raw, func(key string, _ int64) bool { return key == objectKey })
	fs.governor.raw = g
	release := sync.OnceFunc(func() { close(g.gate) })
	defer release()
	limit := fs.prefetchQueueLimit()()
	require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "object", Size: 1 << 10,
		Ranges: []PrefetchRange{{Offset: 0, Size: 1 << 10}}, Scan: &PrefetchScan{}}))
	require.Eventually(t, func() bool { cur, _, _ := g.counts(objectKey); return cur == 1 }, 5*time.Second, time.Millisecond)
	ended := &PrefetchScan{}
	require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "object", Size: limit - (1 << 10),
		Ranges: []PrefetchRange{{Offset: 100 << 10, Size: limit - (1 << 10)}}, Scan: ended}))
	ended.End()
	require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "object", Size: 1 << 10,
		Ranges: []PrefetchRange{{Offset: 200 << 10, Size: 1 << 10}}, Scan: &PrefetchScan{}}))
	require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "other", Size: 1 << 10,
		Ranges: []PrefetchRange{{Offset: 0, Size: 1 << 10}}, Scan: &PrefetchScan{}}))
	require.Eventually(t, func() bool { _, _, total := g.counts(otherKey); return total == 1 }, 5*time.Second, time.Millisecond,
		"the live request of another object is downloaded")
}

// foregroundReadOfWaitingRange starts a background column prefetch of two ranges, holds the
// first range it reads from storage, and once it does reads the other range in the
// foreground; it returns the bytes read, the range's offset, and the error.
func foregroundReadOfWaitingRange(t *testing.T, fs *S3FS, ranges []PrefetchRange) ([]byte, int64, error) {
	held := make(chan struct{})
	t.Cleanup(sync.OnceFunc(func() { close(held) }))
	var heldOffset atomic.Int64
	heldOffset.Store(-1)
	g := newBlackBoxStorage(fs.governor.raw, func(string, int64) bool { return false })
	g.gated = func(_ string, offset int64) chan struct{} {
		if heldOffset.CompareAndSwap(-1, offset) {
			return held
		}
		return nil
	}
	fs.governor.raw = g
	ctx := context.Background()
	key := fs.pathToKey("object")
	require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "object", Size: 2 << 10, Ranges: ranges, Scan: &PrefetchScan{}}))
	require.Eventually(t, func() bool { cur, _, _ := g.counts(key); return cur == 1 }, 5*time.Second, time.Millisecond)
	r := ranges[0]
	if heldOffset.Load() == r.Offset {
		r = ranges[1]
	}
	readCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	got, err := readColumnRange(readCtx, fs, r)
	return got, r.Offset, err
}

// A column read of a range a background prefetch has not started, as it waits for a
// download connection, reads its own bytes: the range is not joinable before it holds one.
func TestBlackBoxForegroundReadNotHeldByWaitingConnection(t *testing.T) {
	fs, _, data := newPrefetchRangeFS(t, 0, 300<<10)
	limitRoom(t, fs.governor, 2, 1)
	ranges := columnRanges(2, 40<<10, 1<<10)
	fs.diskCache.prefetchFlightBytes = 4 * flightBufSize(ranges[0].Size)
	got, offset, err := foregroundReadOfWaitingRange(t, fs, ranges)
	require.NoError(t, err)
	require.Equal(t, data[offset:offset+ranges[0].Size], got)
}

// A column prefetch whose range cannot get a download connection holds no buffer, so a
// column read of another range of its object, waiting for buffers, gets one and returns.
func TestBlackBoxForegroundReadNotHeldByBufferOfWaitingRange(t *testing.T) {
	fs, _, data := newPrefetchRangeFS(t, 0, 300<<10)
	ctx := context.Background()
	require.NoError(t, fs.Write(ctx, IOVector{FilePath: "other", Entries: []IOEntry{{Size: int64(len(data)), Data: data}},
		Policy: SkipDiskCache | SkipMemoryCache}))
	ranges := columnRanges(2, 40<<10, 1<<10)
	limitRoom(t, fs.governor, 2, 1)
	fs.diskCache.prefetchFlightBytes = flightBufSize(ranges[0].Size)
	objectKey, otherKey := fs.pathToKey("object"), fs.pathToKey("other")
	g := newBlackBoxStorage(fs.governor.raw, func(key string, _ int64) bool { return key == otherKey })
	fs.governor.raw = g
	release := sync.OnceFunc(func() { close(g.gate) })
	defer release()
	require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "other", Size: int64(len(data)), Scan: &PrefetchScan{}}))
	require.Eventually(t, func() bool { cur, _, _ := g.counts(otherKey); return cur == 1 }, 5*time.Second, time.Millisecond,
		"the whole download of other holds the connection")
	require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "object", Size: ranges[0].Size,
		Ranges: ranges[:1], Scan: &PrefetchScan{}}))
	require.Eventually(t, func() bool { return fs.prefetcher.isRunning("object") }, 5*time.Second, time.Millisecond)
	readCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	got, err := readColumnRange(readCtx, fs, ranges[1])
	require.NoError(t, err)
	require.Equal(t, data[ranges[1].Offset:ranges[1].Offset+ranges[1].Size], got)
	_, _, total := g.counts(objectKey)
	require.Equal(t, 1, total, "only the read reads the object")
}

// A column read of a range of a running prefetch whose range waits for a connection, held
// by another object's download, reads its own bytes; once the connection is free, the
// prefetch caches the range.
func TestBlackBoxColumnReadNotHeldByPrefetchWaitingForConnection(t *testing.T) {
	fs, _, data := newPrefetchRangeFS(t, 0, 300<<10)
	ctx := context.Background()
	require.NoError(t, fs.Write(ctx, IOVector{FilePath: "other", Entries: []IOEntry{{Size: int64(len(data)), Data: data}},
		Policy: SkipDiskCache | SkipMemoryCache}))
	ranges := columnRanges(2, 40<<10, 1<<10)
	limitRoom(t, fs.governor, 2, 1)
	objectKey, otherKey := fs.pathToKey("object"), fs.pathToKey("other")
	g := newBlackBoxStorage(fs.governor.raw, func(key string, _ int64) bool { return key == otherKey })
	fs.governor.raw = g
	release := sync.OnceFunc(func() { close(g.gate) })
	defer release()
	require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "other", Size: int64(len(data)), Scan: &PrefetchScan{}}))
	require.Eventually(t, func() bool { cur, _, _ := g.counts(otherKey); return cur == 1 }, 5*time.Second, time.Millisecond,
		"the whole download of other holds the connection")
	require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "object", Size: 2 << 10, Ranges: ranges, Scan: &PrefetchScan{}}))
	require.Eventually(t, func() bool { return fs.prefetcher.isRunning("object") }, 5*time.Second, time.Millisecond)
	readCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	got, err := readColumnRange(readCtx, fs, ranges[0])
	require.NoError(t, err)
	require.Equal(t, data[ranges[0].Offset:ranges[0].Offset+ranges[0].Size], got)
	require.True(t, fs.prefetcher.isRunning("object"), "the prefetch still waits")
	release()
	require.Eventually(t, func() bool { return !fs.prefetcher.isRunning("object") }, 5*time.Second, time.Millisecond)
	key := fs.diskCache.objectKey("object")
	for _, r := range ranges {
		_, cached := fs.diskCache.ranges.covering(key, r.Offset, r.Size)
		require.True(t, cached)
	}
	// The prefetch's range the read read was registered before the read cached it.
	_, _, total := g.counts(objectKey)
	require.LessOrEqual(t, total, 3)
}

// A column read of a range of a running prefetch of ranges downloading to files, whose
// range waits for a connection held by another object's download, reads its own bytes.
func TestBlackBoxColumnReadNotHeldByFilePrefetchWaitingForConnection(t *testing.T) {
	fs, _, data := newPrefetchRangeFS(t, 0, 300<<10)
	ctx := context.Background()
	require.NoError(t, fs.Write(ctx, IOVector{FilePath: "other", Entries: []IOEntry{{Size: int64(len(data)), Data: data}},
		Policy: SkipDiskCache | SkipMemoryCache}))
	ranges := columnRanges(2, 40<<10, 16<<10)
	limitRoom(t, fs.governor, 2, 1)
	fs.diskCache.memoryPrefetchMax = 16 << 10
	otherKey := fs.pathToKey("other")
	g := newBlackBoxStorage(fs.governor.raw, func(key string, _ int64) bool { return key == otherKey })
	fs.governor.raw = g
	release := sync.OnceFunc(func() { close(g.gate) })
	defer release()
	require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "other", Size: int64(len(data)), Scan: &PrefetchScan{}}))
	require.Eventually(t, func() bool { cur, _, _ := g.counts(otherKey); return cur == 1 }, 5*time.Second, time.Millisecond,
		"the whole download of other holds the connection")
	require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "object", Size: 32 << 10, Ranges: ranges, Scan: &PrefetchScan{}}))
	require.Eventually(t, func() bool { return fs.prefetcher.isRunning("object") }, 5*time.Second, time.Millisecond)
	readCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	got, err := readColumnRange(readCtx, fs, ranges[0])
	require.NoError(t, err)
	require.Equal(t, data[ranges[0].Offset:ranges[0].Offset+ranges[0].Size], got)
}

// A column read of an object whose whole-object prefetch waits for a connection, held by
// another object's download, reads its own bytes.
func TestBlackBoxColumnReadNotHeldByWholeObjectPrefetchWaitingForConnection(t *testing.T) {
	fs, _, data := newPrefetchRangeFS(t, 0, 300<<10)
	ctx := context.Background()
	require.NoError(t, fs.Write(ctx, IOVector{FilePath: "other", Entries: []IOEntry{{Size: int64(len(data)), Data: data}},
		Policy: SkipDiskCache | SkipMemoryCache}))
	limitRoom(t, fs.governor, 2, 1)
	otherKey := fs.pathToKey("other")
	g := newBlackBoxStorage(fs.governor.raw, func(key string, _ int64) bool { return key == otherKey })
	fs.governor.raw = g
	release := sync.OnceFunc(func() { close(g.gate) })
	defer release()
	require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "other", Size: int64(len(data)), Scan: &PrefetchScan{}}))
	require.Eventually(t, func() bool { cur, _, _ := g.counts(otherKey); return cur == 1 }, 5*time.Second, time.Millisecond,
		"the whole download of other holds the connection")
	require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "object", Size: int64(len(data)), Scan: &PrefetchScan{}}))
	require.Eventually(t, func() bool { return fs.prefetcher.isRunning("object") }, 5*time.Second, time.Millisecond)
	r := PrefetchRange{Offset: 40 << 10, Size: 1 << 10}
	readCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	got, err := readColumnRange(readCtx, fs, r)
	require.NoError(t, err)
	require.Equal(t, data[r.Offset:r.Offset+r.Size], got)
	require.True(t, fs.prefetcher.isRunning("object"), "the prefetch still waits")
}

// A memory-backed column prefetch whose ranges wait for a connection ends once its object
// is deleted, while the download holding the connection still runs.
func TestBlackBoxMemoryColumnPrefetchWaitingForConnectionEndsOnDelete(t *testing.T) {
	fs, _, data := newPrefetchRangeFS(t, 0, 300<<10)
	ctx := context.Background()
	require.NoError(t, fs.Write(ctx, IOVector{FilePath: "other", Entries: []IOEntry{{Size: int64(len(data)), Data: data}},
		Policy: SkipDiskCache | SkipMemoryCache}))
	ranges := columnRanges(2, 40<<10, 1<<10)
	limitRoom(t, fs.governor, 2, 1)
	objectKey, otherKey := fs.pathToKey("object"), fs.pathToKey("other")
	g := newBlackBoxStorage(fs.governor.raw, func(key string, _ int64) bool { return key == otherKey })
	fs.governor.raw = g
	release := sync.OnceFunc(func() { close(g.gate) })
	defer release()
	require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "other", Size: int64(len(data)), Scan: &PrefetchScan{}}))
	require.Eventually(t, func() bool { cur, _, _ := g.counts(otherKey); return cur == 1 }, 5*time.Second, time.Millisecond,
		"the whole download of other holds the connection")
	require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "object", Size: 2 << 10, Ranges: ranges, Scan: &PrefetchScan{}}))
	require.Eventually(t, func() bool { return fs.prefetcher.isRunning("object") }, 5*time.Second, time.Millisecond)
	require.NoError(t, fs.Delete(ctx, "object"))
	require.Eventually(t, func() bool { return !fs.prefetcher.isRunning("object") }, 5*time.Second, time.Millisecond)
	require.True(t, fs.prefetcher.isRunning("other"))
	_, _, total := g.counts(objectKey)
	require.Zero(t, total)
}

// A whole-object prefetch waiting for room is registered, and ends on a delete of its
// object without reading it.
func TestBlackBoxWholeObjectPrefetchWaitingForConnection(t *testing.T) {
	fs, _, data := newPrefetchRangeFS(t, 0, 300<<10)
	ctx := context.Background()
	require.NoError(t, fs.Write(ctx, IOVector{FilePath: "other", Entries: []IOEntry{{Size: int64(len(data)), Data: data}},
		Policy: SkipDiskCache | SkipMemoryCache}))
	limitRoom(t, fs.governor, 2, 1)
	objectKey, otherKey := fs.pathToKey("object"), fs.pathToKey("other")
	g := newBlackBoxStorage(fs.governor.raw, func(key string, _ int64) bool { return key == otherKey })
	fs.governor.raw = g
	release := sync.OnceFunc(func() { close(g.gate) })
	defer release()
	require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "other", Size: int64(len(data)), Scan: &PrefetchScan{}}))
	require.Eventually(t, func() bool { cur, _, _ := g.counts(otherKey); return cur == 1 }, 5*time.Second, time.Millisecond,
		"the whole download of other holds the connection")
	require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "object", Size: int64(len(data)), Scan: &PrefetchScan{}}))
	d := fs.diskCache
	diskPath := d.objectKey("object")
	require.Eventually(t, func() bool { return waitingForConn(d, diskPath) }, 5*time.Second, time.Millisecond,
		"the whole object waits for room")
	require.NoError(t, fs.Delete(ctx, "object"))
	require.Eventually(t, func() bool { return !fs.prefetcher.isRunning("object") }, 5*time.Second, time.Millisecond)
	require.False(t, waitingForConn(d, diskPath))
	require.True(t, fs.prefetcher.isRunning("other"))
	_, _, total := g.counts(objectKey)
	require.Zero(t, total)
	release()
	require.Eventually(t, func() bool { return !fs.prefetcher.isRunning("other") }, 5*time.Second, time.Millisecond)
	require.Zero(t, heldRoom(d.governor))
}

// A memory-backed column prefetch starts no range of an object deleted while its ranges
// wait for buffers.
func TestBlackBoxMemoryColumnPrefetchStopsAfterDelete(t *testing.T) {
	fs, _, _ := newPrefetchRangeFS(t, 0, 300<<10)
	ranges := columnRanges(6, 20<<10, 1<<10)
	fs.diskCache.prefetchFlightBytes = flightBufSize(ranges[0].Size)
	g := newBlackBoxStorage(fs.governor.raw, func(string, int64) bool { return true })
	fs.governor.raw = g
	release := sync.OnceFunc(func() { close(g.gate) })
	defer release()
	ctx := context.Background()
	require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "object", Size: 6 << 10, Ranges: ranges, Scan: &PrefetchScan{}}))
	key := fs.pathToKey("object")
	require.Eventually(t, func() bool { cur, _, _ := g.counts(key); return cur == 1 }, 5*time.Second, time.Millisecond)
	require.NoError(t, fs.Delete(ctx, "object"))
	release()
	require.Eventually(t, func() bool { return !fs.prefetcher.isRunning("object") }, 5*time.Second, time.Millisecond)
	_, _, total := g.counts(key)
	require.Equal(t, 1, total, "no range starts after the delete")
}

// A prefetch range of at least the split size is read as parts at once, each its own
// prefetch object cached as its own range file; a read across the parts is served from
// the cache.
func TestBlackBoxPrefetchRangeSplitIntoParts(t *testing.T) {
	fs, _, data := newPrefetchRangeFS(t, 0, 2<<20)
	fs.diskCache.objectSplit = objectSplitConfig{parts: 2, min: 512 << 10}
	key := fs.pathToKey("object")
	g := newBlackBoxStorage(fs.governor.raw, func(k string, _ int64) bool { return k == key })
	fs.governor.raw = g
	release := sync.OnceFunc(func() { close(g.gate) })
	defer release()
	ctx := context.Background()
	r := PrefetchRange{Offset: 64 << 10, Size: 1 << 20}
	require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "object", Size: 2 << 20, Ranges: []PrefetchRange{r}, Scan: &PrefetchScan{}}))
	require.Eventually(t, func() bool { cur, _, _ := g.counts(key); return cur == 2 }, 5*time.Second, time.Millisecond,
		"the parts are read at once")
	release()
	require.Eventually(t, func() bool { return !fs.prefetcher.isRunning("object") }, 5*time.Second, time.Millisecond)
	cached, ok := fs.diskCache.ranges.covering(fs.diskCache.objectKey("object"), r.Offset, r.Size)
	require.True(t, ok)
	require.Len(t, cached, 2, "each part is its own range file")
	off := r.Offset + 512<<10 - 100
	got, err := readColumnRange(ctx, fs, PrefetchRange{Offset: off, Size: 200})
	require.NoError(t, err)
	require.Equal(t, data[off:off+200], got)
	_, _, total := g.counts(key)
	require.Equal(t, 2, total, "the read across the parts is served from the cache")
}

// Reads of the same run past the bytes of a MemoryPrefetchObject that wait for a
// connection, held by another object's download, download the run once.
func TestBlackBoxConcurrentReadsOfFileRunDownloadOnce(t *testing.T) {
	const unit = 64 << 10
	fs, _, data := newPrefetchRangeFS(t, unit, 1<<20)
	ctx := context.Background()
	require.NoError(t, fs.Write(ctx, IOVector{FilePath: "other", Entries: []IOEntry{{Size: int64(len(data)), Data: data}},
		Policy: SkipDiskCache | SkipMemoryCache}))
	limitRoom(t, fs.governor, 2, 1)
	fs.diskCache.memoryPrefetchMax = 2 * unit
	objectKey, otherKey := fs.pathToKey("object"), fs.pathToKey("other")
	g := newBlackBoxStorage(fs.governor.raw, func(key string, _ int64) bool { return key == otherKey })
	fs.governor.raw = g
	release := sync.OnceFunc(func() { close(g.gate) })
	defer release()
	require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "other", Size: int64(len(data)), Scan: &PrefetchScan{}}))
	require.Eventually(t, func() bool { cur, _, _ := g.counts(otherKey); return cur == 1 }, 5*time.Second, time.Millisecond,
		"the whole download of other holds the connection")
	freeForeground := occupy(t, fs.governor, foregroundRequest, 1)
	const readers = 3
	errs := make(chan error, readers)
	for range readers {
		go func() {
			vec := &IOVector{FilePath: "object", Policy: SkipFullFilePreloads | SkipMemoryCache,
				Entries: []IOEntry{{Offset: 0, Size: 4 * unit}}}
			err := fs.Read(ctx, vec)
			if err == nil && !bytes.Equal(data[:4*unit], vec.Entries[0].Data) {
				err = errors.New("wrong bytes")
			}
			vec.Release()
			errs <- err
		}()
	}
	require.Never(t, func() bool { return len(errs) > 0 }, 100*time.Millisecond, time.Millisecond, "the reads wait for room")
	_, _, total := g.counts(objectKey)
	require.Zero(t, total)
	release()
	freeForeground()
	for range readers {
		require.NoError(t, <-errs)
	}
	_, _, total = g.counts(objectKey)
	require.Equal(t, 1, total, "the run downloads once")
}

// A range of a prefetch that ended frees its buffer while an earlier range of the prefetch
// still reads, so a read of another range takes the buffer and returns.
func TestBlackBoxEndedPrefetchRangeFreesItsBuffer(t *testing.T) {
	const unit = 64 << 10
	fs, _, data := newPrefetchRangeFS(t, unit, 4*unit)
	ctx := context.Background()
	d := fs.diskCache
	d.prefetchFlightBytes = 2 * flightBufSize(unit)
	key := fs.pathToKey("object")
	g := newBlackBoxStorage(fs.governor.raw, func(k string, offset int64) bool { return k == key && offset == 0 })
	fs.governor.raw = g
	release := sync.OnceFunc(func() { close(g.gate) })
	t.Cleanup(release)
	require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "object", Size: 2 * unit,
		Ranges: []PrefetchRange{{Offset: 0, Size: unit}, {Offset: 2 * unit, Size: unit}}, Scan: &PrefetchScan{}}))
	f := &d.prefetchObjects
	diskPath := d.objectKey("object")
	require.Eventually(t, func() bool {
		f.mu.Lock()
		defer f.mu.Unlock()
		return len(f.m[diskPath]) == 1 && f.bytes == flightBufSize(unit)
	}, 5*time.Second, time.Millisecond, "the second range ended and freed its buffer while the first reads")

	read := make(chan []byte, 1)
	go func() {
		vec := &IOVector{FilePath: "object", Policy: SkipFullFilePreloads | SkipMemoryCache,
			Entries: []IOEntry{{Offset: 3*unit + 100, Size: 10}}}
		if err := fs.Read(ctx, vec); err != nil {
			read <- nil
			return
		}
		read <- bytes.Clone(vec.Entries[0].Data)
		vec.Release()
	}()
	var got []byte
	require.Eventually(t, func() bool {
		select {
		case got = <-read:
			return true
		default:
			return false
		}
	}, 5*time.Second, time.Millisecond, "a read of another range takes the freed buffer")
	require.Equal(t, data[3*unit+100:3*unit+110], got)
	cur, _, _ := g.counts(key)
	require.Equal(t, 1, cur, "the first range still reads")
	release()
	f.reads.Wait()
}

// The ranges of a prefetch waiting for the only connection take no goroutine each, and a
// delete of the object ends them all.
func TestBlackBoxWideRangePrefetchWaitingTakesNoGoroutinePerRange(t *testing.T) {
	const n = 2048
	fs, _, _ := newPrefetchRangeFS(t, 0, n*64)
	ctx := context.Background()
	d := fs.diskCache
	limitRoom(t, d.governor, 1, 0)
	f := &d.prefetchObjects
	waiting := func() int {
		_, waiting := d.governor.counts()
		return waiting[backgroundRequest]
	}
	before := runtime.NumGoroutine()
	require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "object", Size: n * 16,
		Ranges: columnRanges(n, 64, 16), Scan: &PrefetchScan{}}))
	require.Eventually(t, func() bool { return waiting() == d.prefetchShare() }, 5*time.Second, time.Millisecond,
		"the first ranges of the prefetch's share wait for room")
	require.Less(t, runtime.NumGoroutine()-before, 32, "the ranges not admitted take no goroutine")

	require.NoError(t, fs.Delete(ctx, "object"))
	require.Eventually(t, func() bool {
		return waiting() == 0 && !fs.prefetcher.isRunning("object") && runtime.NumGoroutine()-before < 32
	}, 5*time.Second, time.Millisecond, "the delete ends every range")
	f.mu.Lock()
	require.Zero(t, f.bytes)
	f.mu.Unlock()
	require.Zero(t, heldRoom(d.governor), "no range took room")
}

// A prefetch that joined a range download a read started keeps it downloading once the
// read leaves, and the range is cached.
func TestBlackBoxPrefetchKeepsJoinedReadDownload(t *testing.T) {
	const unit = 64 << 10
	fs, storage, data := newPrefetchRangeFS(t, unit, 4*unit)
	d := fs.diskCache
	d.memoryPrefetchMax = unit / 2
	entered, gate := make(chan struct{}), make(chan struct{})
	var once sync.Once
	d.hooks.beforeCacheTempFileCreate = func() {
		once.Do(func() {
			close(entered)
			<-gate
		})
	}
	release := sync.OnceFunc(func() { close(gate) })
	t.Cleanup(release)

	readCtx, cancelRead := context.WithCancel(context.Background())
	readDone := make(chan error, 1)
	go func() {
		vec := &IOVector{FilePath: "object", Policy: SkipFullFilePreloads | SkipMemoryCache,
			Entries: []IOEntry{{Offset: 100, Size: 10}}}
		readDone <- fs.Read(readCtx, vec)
		vec.Release()
	}()
	<-entered
	f := &d.prefetchObjects
	diskPath := d.objectKey("object")
	var started *filePrefetchObject
	f.mu.Lock()
	for _, o := range f.m[diskPath] {
		if p, ok := o.(*filePrefetchObject); ok && p.readStarted {
			started = p
		}
	}
	f.mu.Unlock()
	require.NotNil(t, started, "the read started a range download")

	ctx := context.Background()
	require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "object", Size: unit,
		Ranges: []PrefetchRange{{Offset: 0, Size: unit}}, Scan: &PrefetchScan{}}))
	holds := func() int {
		started.mu.Lock()
		defer started.mu.Unlock()
		return started.holds
	}
	require.Eventually(t, func() bool { return holds() == 2 }, 5*time.Second, time.Millisecond, "the prefetch joined it")
	cancelRead()
	require.ErrorIs(t, <-readDone, context.Canceled)
	require.NoError(t, started.ctx.Err(), "the download the prefetch holds goes on")

	release()
	require.Eventually(t, func() bool { return !fs.prefetcher.isRunning("object") }, 5*time.Second, time.Millisecond)
	f.reads.Wait()
	reads := storage.reads.Load()
	require.Equal(t, data[100:110], readRangeOf(t, fs, 0, 100, 10))
	require.Equal(t, reads, storage.reads.Load(), "the range was cached")
}

// A prefetch of many disjoint ranges reads at most its share of them at once, so the
// prefetch of another object reads while it waits.
func TestBlackBoxWideRangePrefetchLeavesConnectionsToOthers(t *testing.T) {
	fs, _, data := newPrefetchRangeFS(t, 0, 300<<10)
	ctx := context.Background()
	require.NoError(t, fs.Write(ctx, IOVector{FilePath: "other", Entries: []IOEntry{{Size: 64 << 10, Data: data[:64<<10]}}, Policy: SkipDiskCache | SkipMemoryCache}))
	share := fs.diskCache.prefetchShare()
	require.Less(t, share, fs.governor.backgroundMax)
	key := fs.pathToKey("object")
	g := newBlackBoxStorage(fs.governor.raw, func(k string, _ int64) bool { return k == key })
	fs.governor.raw = g
	release := sync.OnceFunc(func() { close(g.gate) })
	t.Cleanup(release)
	ranges := columnRanges(14, 20<<10, 1<<10)
	require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "object", Size: 300 << 10, Ranges: ranges, Scan: &PrefetchScan{}}))
	require.Eventually(t, func() bool { cur, _, _ := g.counts(key); return cur == share }, 5*time.Second, time.Millisecond)
	require.Never(t, func() bool { cur, _, _ := g.counts(key); return cur > share }, 100*time.Millisecond, time.Millisecond)
	require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "other", Size: 64 << 10, Ranges: []PrefetchRange{{Offset: 0, Size: 1 << 10}}, Scan: &PrefetchScan{}}))
	otherKey := fs.diskCache.objectKey("other")
	require.Eventually(t, func() bool { _, ok := fs.diskCache.ranges.covering(otherKey, 0, 1<<10); return ok },
		5*time.Second, time.Millisecond, "the other object's prefetch read while the wide prefetch waited")
	release()
	require.Eventually(t, func() bool { _, _, total := g.counts(key); return total == len(ranges) }, 5*time.Second, time.Millisecond)
}
