// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// You may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// Distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// Limitations under the License.

package fileservice

import (
	"context"
	"io"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A range read whose prefetch run is a FilePrefetchObject must not join a whole-object
// Download whose bytes the cost model says arrive far later than its own range read would.
func TestDiskCacheTransitionFileRunJoinsSlowWholeDownload(t *testing.T) {
	const size = 8 << 20
	fs, _, data := newPrefetchRangeFS(t, 1<<20, size)
	fs.diskCache.memoryPrefetchMax = 256 << 10
	rate0 := downloadRates.rate()
	downloadRates.set(1 << 20)
	t.Cleanup(func() { downloadRates.set(rate0) })

	key := fs.pathToKey("object")
	// The whole-object download stalls on its first byte.
	g := newBlackBoxStorage(fs.storage, func(k string, offset int64) bool { return k == key && offset == 0 })
	fs.storage = g
	release := sync.OnceFunc(func() { close(g.gate) })
	defer release()

	ctx := context.Background()
	require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "object", Size: size, Scan: &PrefetchScan{}}))
	diskPath := fs.diskCache.pathForFile("object")
	require.Eventually(t, func() bool {
		cur, _, _ := g.counts(key)
		return cur == 1 && fs.diskCache.download(diskPath) != nil
	}, 5*time.Second, time.Millisecond)

	readCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	off := int64(6 << 20)
	vec := &IOVector{FilePath: "object", Policy: SkipFullFilePreloads | SkipMemoryCache,
		Entries: []IOEntry{{Offset: off, Size: 64 << 10}}}
	start := time.Now()
	err := fs.Read(readCtx, vec)
	elapsed := time.Since(start)
	var got []byte
	if err == nil {
		got = slices.Clone(vec.Entries[0].Data)
	}
	vec.Release()
	require.NoError(t, err, "elapsed %v", elapsed)
	require.Equal(t, data[off:off+64<<10], got)
	require.Less(t, elapsed, time.Second, "the read reads its own range rather than wait for the stalled download")
}

// ProbeStallStorage returns readers of whole reads that send their first n bytes, then wait
// For their context to end.
type probeStallStorage struct {
	ObjectStorage
	n     int64
	reads atomic.Int64
}

type probeStallReader struct {
	ctx  context.Context
	r    io.ReadCloser
	left int64
}

func (s *probeStallReader) Read(b []byte) (int, error) {
	if s.left <= 0 {
		<-s.ctx.Done()
		return 0, s.ctx.Err()
	}
	if int64(len(b)) > s.left {
		b = b[:s.left]
	}
	n, err := s.r.Read(b)
	s.left -= int64(n)
	return n, err
}

func (s *probeStallReader) Close() error { return s.r.Close() }

func (s *probeStallStorage) Read(ctx context.Context, key string, min, max *int64) (io.ReadCloser, error) {
	s.reads.Add(1)
	r, err := s.ObjectStorage.Read(ctx, key, min, max)
	if err != nil || max != nil {
		return r, err
	}
	return &probeStallReader{ctx: ctx, r: r, left: s.n}, nil
}

// A whole-object download its read no longer waits for, whose source stalls, stops
// And frees its connection, async slot, update slot and the download.
func TestDiskCacheTransitionAbandonedStalledDownloadFreesResources(t *testing.T) {
	shortIdle(t, 50*time.Millisecond)
	fs, _, data := newPrefetchRangeFS(t, 0, 300<<10)
	fs.SetAsyncUpdate(true)
	fs.storage = &probeStallStorage{ObjectStorage: fs.storage, n: 64 << 10}
	got, err := readFullObjectOf(fs, 10, 10)
	require.NoError(t, err)
	require.Equal(t, data[10:20], got)
	d := fs.diskCache
	diskPath := d.pathForFile("object")
	require.Eventually(t, func() bool {
		d.async.mu.Lock()
		pending := len(d.async.mu.pending)
		d.async.mu.Unlock()
		return d.downloadConns.Load() == 0 && !d.isUpdating(diskPath) && d.download(diskPath) == nil &&
			pending == 0 && len(backgroundFills) == 0
	}, 5*time.Second, time.Millisecond)
	require.False(t, d.cache.Contains(diskPath))
}

// An entry read before a delete, whose async publication waits while the object is
// Deleted and rewritten with new content, is not published; a later read gets the new bytes.
func TestDiskCacheTransitionAsyncEntryPublicationAfterDeleteRewrite(t *testing.T) {
	fs, _, data := newPrefetchRangeFS(t, 0, 64<<10)
	fs.SetAsyncUpdate(true)
	d := fs.diskCache
	ctx := context.Background()
	hold, held := make(chan struct{}), make(chan struct{})
	var once sync.Once
	d.beforeFilePublication = func() {
		once.Do(func() {
			close(held)
			<-hold
		})
	}
	vec := &IOVector{FilePath: "object", Policy: SkipFullFilePreloads | SkipMemoryCache | ExactRead,
		Entries: []IOEntry{{Offset: 100, Size: 10}}}
	require.NoError(t, fs.Read(ctx, vec))
	require.Equal(t, data[100:110], vec.Entries[0].Data)
	vec.Release()
	<-held
	require.NoError(t, fs.Delete(ctx, "object"))
	v2 := make([]byte, len(data))
	for i := range v2 {
		v2[i] = ^data[i]
	}
	require.NoError(t, fs.Write(ctx, IOVector{FilePath: "object", Entries: []IOEntry{{Size: int64(len(v2)), Data: v2}},
		Policy: SkipDiskCache | SkipMemoryCache}))
	close(hold)
	d.Flush(ctx)
	vec = &IOVector{FilePath: "object", Policy: SkipFullFilePreloads | SkipMemoryCache | ExactRead,
		Entries: []IOEntry{{Offset: 100, Size: 10}}}
	require.NoError(t, fs.Read(ctx, vec))
	require.Equal(t, v2[100:110], vec.Entries[0].Data, "no stale bytes after delete and rewrite")
	vec.Release()
}

// A disk-cache read under way when the cache closes keeps no file open past it.
func TestDiskCacheTransitionReadDuringCloseKeepsNoOpenFile(t *testing.T) {
	fs, _, data := newPrefetchRangeFS(t, 0, 64<<10)
	ctx := context.Background()
	d := fs.diskCache
	require.NoError(t, d.Update(ctx, &IOVector{FilePath: "object", Entries: []IOEntry{{Offset: 0, Size: 100, Data: data[:100]}}}, false))
	diskPath := d.pathForIOEntry("object", IOEntry{Offset: 0, Size: 100})
	require.True(t, d.cache.Contains(diskPath))
	// The read has started its read of the path when the cache closes.
	var deadline time.Time
	generation, ok := d.startPathReadWithin(ctx, diskPath, &deadline, shortIOWaitDuration)
	require.True(t, ok)
	d.Close(ctx)
	opened, ok := d.openReadFile(ctx, diskPath, generation)
	require.True(t, ok)
	d.closeReadFile(opened)
	d.finishPathRead(diskPath, generation)
	d.updatingPaths.L.Lock()
	kept := len(d.updatingPaths.openFiles.m)
	d.updatingPaths.L.Unlock()
	require.Zero(t, kept, "a file kept open after Close is never closed")
}
