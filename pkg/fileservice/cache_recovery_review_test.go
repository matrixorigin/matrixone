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
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/require"
)

func TestIndexEvictionWithRenameFailureOverCapacity(t *testing.T) {
	for _, tc := range []struct {
		name     string
		readOnly bool
	}{
		{name: "writable_directory"},
		{name: "read_only_directory", readOnly: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.readOnly && !permissionsEnforced(t) {
				t.Skip("filesystem permissions are not enforced")
			}
			ctx := context.Background()
			var capacity atomic.Int64
			capacity.Store(1 << 20)
			c, err := NewIndexFileCache(ctx, t.TempDir(), capacity.Load, "", 0)
			require.NoError(t, err)
			t.Cleanup(func() { c.Close(ctx) })
			<-c.disk.load.done

			written, err := c.Put(ctx, "entry", recoveryFiles(bytes.Repeat([]byte{1}, 128<<10)))
			require.NoError(t, err)
			require.True(t, written)
			entry, err := c.disk.pathForDir("entry")
			require.NoError(t, err)
			parent := filepath.Dir(entry)
			if tc.readOnly {
				require.NoError(t, os.Chmod(parent, 0o500))
				t.Cleanup(func() { require.NoError(t, os.Chmod(parent, 0o755)) })
			}

			attempts := 0
			var restoreErr error
			c.disk.hooks.beforeEvictionStart = func(path string) {
				if path != entry {
					return
				}
				attempts++
				if tc.readOnly && attempts == 3 {
					// Release the forced failure after observing that eviction retries
					// the same path with no change in capacity or permissions.
					restoreErr = os.Chmod(parent, 0o755)
				}
			}
			t.Cleanup(func() { c.disk.hooks.beforeEvictionStart = nil })
			require.Positive(t, c.disk.cache.Used())
			capacity.Store(0)
			c.disk.Evict(ctx, nil)
			require.NoError(t, restoreErr)
			require.Equal(t, 1, attempts, "eviction should finish after one attempt at the same path")
		})
	}
}

func TestColumnPrefetchRefillsMissingFileRegardlessOfRangeOrder(t *testing.T) {
	for _, tc := range []struct {
		name      string
		coldFirst bool
	}{
		{name: "missing_range_first"},
		{name: "cold_range_first", coldFirst: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			fs, storage, _ := newPrefetchRangeFS(t, 0, 300<<10)
			<-fs.diskCache.load.done
			missing := PrefetchRange{Offset: 40 << 10, Size: 4 << 10}
			cold := PrefetchRange{Offset: 80 << 10, Size: 4 << 10}
			prefetch := func(ranges ...PrefetchRange) {
				t.Helper()
				require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{
					FilePath: "object", Size: int64(len(ranges)) * missing.Size, Ranges: ranges,
				}))
				waitPrefetcherIdle(t, fs)
			}

			prefetch(missing)
			path := fs.diskCache.pathForIOEntry("object", missing.entry())
			require.FileExists(t, path)
			require.NoError(t, os.Remove(path))
			require.True(t, fs.diskCache.cache.Contains(path))
			before := storage.reads.Load()

			if tc.coldFirst {
				prefetch(cold, missing)
			} else {
				prefetch(missing, cold)
			}
			require.FileExists(t, fs.diskCache.pathForIOEntry("object", cold.entry()))
			_, err := os.Stat(path)
			require.NoError(t, err, "a prefetch of both ranges should restore the missing file")
			require.Equal(t, int64(2), storage.reads.Load()-before)
		})
	}
}

func TestColumnReadPropagatesJoinedPrefetchSourceError(t *testing.T) {
	for _, tc := range []struct {
		name       string
		fileBacked bool
	}{
		{name: "memory_prefetch"},
		{name: "file_prefetch", fileBacked: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			t.Cleanup(cancel)
			fs, storage, _ := newPrefetchRangeFS(t, 0, 300<<10)
			<-fs.diskCache.load.done
			r := PrefetchRange{Offset: 40 << 10, Size: 4 << 10}
			if tc.fileBacked {
				fs.diskCache.memoryPrefetchMax = r.Size - 1
			}
			s := &failOnceStorage{ObjectStorage: storage, fail: r.Offset, gate: make(chan struct{})}
			fs.storage = s
			release := sync.OnceFunc(func() { close(s.gate) })
			t.Cleanup(release)
			require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{
				FilePath: "object", Size: r.Size, Ranges: []PrefetchRange{r},
			}))
			require.Eventually(t, s.failed.Load, 5*time.Second, time.Millisecond, "prefetch entered storage")

			finished := make(chan struct{})
			defer func() {
				release()
				cancel()
				<-finished
			}()
			result := make(chan error, 1)
			go func() {
				defer close(finished)
				_, err := readColumnRange(ctx, fs, r)
				result <- err
			}()
			diskPath := fs.diskCache.pathForFile("object")
			require.Eventually(t, func() bool {
				if !tc.fileBacked {
					return memoryWaiters(fs, diskPath) == 1
				}
				o, _ := fs.diskCache.fileDownload(diskPath, r.Offset, r.Offset+r.Size)
				if o == nil {
					return false
				}
				o.mu.Lock()
				defer o.mu.Unlock()
				return len(o.waiters) == 1
			}, 5*time.Second, time.Millisecond, "the foreground read joined the stalled prefetch")
			release()
			err := <-result
			waitPrefetcherIdle(t, fs)
			require.ErrorContains(t, err, "probe: storage failed",
				"the joined read should receive the source error (successful fallback opens: %d)", storage.reads.Load())
			require.Zero(t, storage.reads.Load())
		})
	}
}

func TestIndexObjectReadRejectsEmptyRange(t *testing.T) {
	for _, cached := range []bool{false, true} {
		name := "index_cache_miss"
		if cached {
			name = "index_cache_hit"
		}
		t.Run(name, func(t *testing.T) {
			fs, storage, data := newIndexObjectFS(t, true)
			if cached {
				cacheIndexObject(t, fs, "object", data)
				require.Equal(t, data[100:110], readIndexObjectRange(t, fs, IndexFileObject, 100, 10))
				require.Zero(t, storage.reads.Load())
			}
			vector := &IOVector{
				FilePath: "object",
				Policy:   IndexFileObject | SkipMemoryCache | SkipFullFilePreloads,
				Entries:  []IOEntry{{Offset: 100, Size: 0}},
			}
			defer vector.Release()
			err := fs.Read(context.Background(), vector)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrEmptyRange), "expected ErrEmptyRange, got %v", err)
		})
	}
}

func TestRangeIndexSurvivesOlderEviction(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		reindexDuringEviction bool
	}{
		{name: "evict_without_reindex"},
		{name: "evict_after_read_reindexes", reindexDuringEviction: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			fs, storage, data := newPrefetchRangeFS(t, 0, 300<<10)
			d := fs.diskCache
			<-d.load.done
			r := PrefetchRange{Offset: 40 << 10, Size: 4 << 10}
			require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{
				FilePath: "object", Size: r.Size, Ranges: []PrefetchRange{r},
			}))
			waitPrefetcherIdle(t, fs)
			path := d.pathForIOEntry("object", r.entry())
			key := d.rangeObjectKey("object")
			require.True(t, d.cache.Contains(path))
			require.FileExists(t, path)

			d.hooks.beforeEvictionStart = func(evicted string) {
				if evicted != path || !tc.reindexDuringEviction {
					return
				}
				require.False(t, d.cache.Contains(path), "old cache index record was removed")
				vector := &IOVector{FilePath: "object", Policy: SkipFullFilePreloads, Entries: []IOEntry{r.entry()}}
				defer vector.Release()
				require.NoError(t, d.Read(ctx, vector))
				if vector.Entries[0].done {
					require.Equal(t, data[r.Offset:r.Offset+r.Size], vector.Entries[0].Data)
				}
				t.Logf("read reindexed old file: %t", d.cache.Contains(path))
			}
			t.Cleanup(func() { d.hooks.beforeEvictionStart = nil })
			d.cache.ForceEvict(ctx, d.cache.Used())
			if !d.cache.Contains(path) {
				// A completed eviction need not keep the range. Refill it to
				// establish the same warm postcondition as a successful reindex.
				require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{
					FilePath: "object", Size: r.Size, Ranges: []PrefetchRange{r},
				}))
				waitPrefetcherIdle(t, fs)
			}
			require.True(t, d.cache.Contains(path))
			require.FileExists(t, path)
			want := PrefetchRange{Offset: r.Offset + 1, Size: r.Size - 2}
			_, indexed := d.ranges.containing(key, want.Offset, want.Size, nil)
			before := storage.reads.Load()
			got, err := readColumnRange(ctx, fs, want)
			require.NoError(t, err)
			require.Equal(t, data[want.Offset:want.Offset+want.Size], got)
			require.Equal(t, before, storage.reads.Load(),
				"a warm subrange should not open storage (range indexed before read: %t)", indexed)
		})
	}
}

func TestIndexObjectReadToEndUsesWarmCache(t *testing.T) {
	for _, tc := range []struct {
		name      string
		readToEnd bool
	}{
		{name: "known_size"},
		{name: "read_to_end", readToEnd: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs, storage, data := newIndexObjectFS(t, true)
			cacheIndexObject(t, fs, "object", data)
			offset := int64(len(data) - 32)
			size := int64(len(data)) - offset
			if tc.readToEnd {
				size = -1
			}
			got := readIndexObjectRange(t, fs, IndexFileObject|SkipFullFilePreloads, offset, size)
			require.Equal(t, data[offset:], got)
			require.Zero(t, storage.reads.Load(), "a warm index-file entry should serve the read")
		})
	}
}

func TestIndexObjectReadRefreshesIdleTTL(t *testing.T) {
	for _, tc := range []struct {
		name     string
		sinceUse time.Duration
	}{
		{name: "outside_touch_interval", sinceUse: 2 * indexObjectTouch},
		{name: "inside_touch_interval", sinceUse: time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			fs, storage, data := newIndexObjectFS(t, true)
			cacheIndexObject(t, fs, "object", data)
			require.Equal(t, data[10:20], readIndexObjectRange(t, fs, IndexFileObject, 10, 10))
			path, ok := fs.indexObjectCached("object")
			require.True(t, ok)
			c := fs.indexFileCache
			c.idleTTL = 2 * time.Second

			// Age the previous use in both places without waiting for the touch interval.
			lastRead := time.Now().Add(-tc.sinceUse)
			require.NoError(t, os.Chtimes(path, lastRead, lastRead))
			fs.indexObjects.mu.Lock()
			fs.indexObjects.touched[path] = lastRead
			fs.indexObjects.mu.Unlock()

			require.Equal(t, data[20:30], readIndexObjectRange(t, fs, IndexFileObject, 20, 10))
			require.Zero(t, storage.reads.Load())
			removed, err := c.collectIdle(ctx, time.Now().Add(1500*time.Millisecond))
			require.NoError(t, err)
			require.Zero(t, removed, "idle collection should keep the index object that was just read")
		})
	}
}

func TestReadStartedSplitDownloadWaitsForItsPart(t *testing.T) {
	for _, tc := range []struct {
		name   string
		delays bool
	}{
		{name: "part_arrives_before_join_deadline"},
		{name: "part_arrives_after_join_deadline", delays: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				const partSize = prefetchBlockMax
				download := &objectDownload{parts: []downloadPart{
					{end: partSize}, {start: partSize, end: 2 * partSize},
				}}
				download.begin(1)
				reader := newFullObjectDiskCacheReader(&IOVector{Entries: []IOEntry{{Offset: partSize, Size: 8}}})
				source := bytes.Repeat([]byte{3}, 2*partSize)
				type result struct {
					captured bool
					err      error
				}
				done := make(chan result, 1)
				go func() {
					captured, err := reader.capture(ctx, download, bytes.NewReader(source), "object")
					done <- result{captured, err}
				}()
				synctest.Wait()
				download.mu.Lock()
				waiters := len(download.waiters)
				download.mu.Unlock()
				require.Equal(t, 1, waiters, "the initiating read is waiting for its bytes")
				if tc.delays {
					// The clock is simulated. Advance past the bound for a joining read
					// while both parts are still gated.
					time.Sleep(maxIOWaitDuration + time.Nanosecond)
				}
				download.advance(1, 8)
				got := <-done
				require.NoError(t, got.err)
				require.True(t, got.captured, "the initiating read should take its bytes while the other part is still pending")
				require.Equal(t, source[partSize:partSize+8], reader.captures[0].data.Bytes())
			})
		})
	}
}
