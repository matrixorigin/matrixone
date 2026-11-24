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
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/stretchr/testify/require"
)

func TestIndexObjectFillWithDiskPolicy(t *testing.T) {
	for _, tc := range []struct {
		name   string
		policy Policy
	}{
		{name: "default_policy"},
		{name: "skip_disk_writes", policy: SkipDiskCacheWrites},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs, _, data := newIndexObjectFS(t, true)
			// Unlike SkipCacheWrites, skipping the table-data tier still permits an index fill.
			vec := &IOVector{FilePath: "object", Policy: IndexFileObject | tc.policy,
				Entries: []IOEntry{{Offset: 100, Size: 64}}}
			defer vec.Release()
			require.NoError(t, fs.Read(context.Background(), vec))
			require.Equal(t, data[100:164], vec.Entries[0].Data)
			_, cached := fs.indexObjectCached("object")
			require.True(t, cached, "the read should cache the whole object in the index-file tier")
			require.Zero(t, fs.diskCache.cache.Used(), "the table-data tier should stay empty")
		})
	}
}

func TestIndexObjectRangeDoesNotFillDiskCache(t *testing.T) {
	for _, tc := range []struct {
		name          string
		cacheWritable bool
	}{
		{name: "skip_disk_writes"},
		{name: "narrow_index_object_read", cacheWritable: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs, _, data := newIndexObjectFS(t, true)
			policy := Policy(IndexFileObject | ExactColumnsRead)
			if !tc.cacheWritable {
				policy |= SkipDiskCacheWrites
			}
			require.Equal(t, data[100:164], readIndexObjectRange(t, fs, policy, 100, 64))
			d := fs.diskCache
			rangePath := d.pathForIOEntry("object", IOEntry{Offset: 100, Size: 64})
			require.False(t, d.cache.Contains(rangePath), "a narrow index-object read should not cache a range in the table-data tier")
		})
	}
}

func TestIndexPrefetchRefillsMissingDataFile(t *testing.T) {
	for _, missing := range []bool{false, true} {
		name := "existing_file"
		if missing {
			name = "missing_file"
		}
		t.Run(name, func(t *testing.T) {
			fs, _, data := newIndexObjectFS(t, true)
			cacheIndexObject(t, fs, "object", data)
			entryDir, cached := fs.indexObjectCached("object")
			require.True(t, cached)
			file := filepath.Join(entryDir, IndexObjectFile)
			if missing {
				require.NoError(t, os.Remove(file))
			}
			ctx := WithFileServicePolicy(context.Background(), IndexFileObject)
			require.NoError(t, fs.PrefetchFile(ctx, "object"))
			require.FileExists(t, file, "prefetch should leave a readable data file in the index-file cache")
		})
	}
}

func TestIndexObjectActiveReadSurvivesEviction(t *testing.T) {
	for _, explicitHold := range []bool{true, false} {
		name := "explicit_hold"
		if !explicitHold {
			name = "read_hold"
		}
		t.Run(name, func(t *testing.T) {
			fs, storage, data := newIndexObjectFS(t, true)
			cacheIndexObject(t, fs, "object", data)
			cache := fs.indexFileCache
			if explicitHold {
				hold, err := cache.Hold(IndexObjectEntry("object"))
				require.NoError(t, err)
				defer hold.Release()
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			entered := make(chan struct{})
			resume := make(chan struct{})
			vec := &IOVector{FilePath: "object", Policy: IndexFileObject | SkipMemoryCache,
				Entries: []IOEntry{{Offset: 100, Size: 64, ToCacheData: func(ctx context.Context, r io.Reader, data []byte, alloc CacheDataAllocator) (fscache.Data, error) {
					close(entered)
					<-resume
					return CacheOriginalData(ctx, r, data, alloc)
				}}}}
			defer vec.Release()
			done := make(chan struct{})
			var readErr error
			go func() {
				defer close(done)
				readErr = fs.Read(ctx, vec)
			}()
			defer func() {
				close(resume)
				<-done
				require.NoError(t, readErr)
				require.Zero(t, storage.reads.Load(), "the reader should be served by the index file")
			}()
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatal("the read never entered the decoder")
			}
			used := cache.disk.cache.Used()
			require.Positive(t, used)
			remaining := cache.disk.cache.ForceEvictWithWait(ctx, used)
			require.Equal(t, used, remaining, "eviction should skip an index object with an active read")
		})
	}
}

func TestMixedIndexObjectReadDoesNotFillDiskCache(t *testing.T) {
	for _, stream := range []bool{false, true} {
		name := "buffered_entries"
		if stream {
			name = "buffered_entry_and_writer"
		}
		t.Run(name, func(t *testing.T) {
			fs, _, data := newIndexObjectFS(t, true)
			vec := &IOVector{FilePath: "object", Policy: IndexFileObject | SkipMemoryCache,
				Entries: []IOEntry{{Offset: 100, Size: 64}, {Offset: 164, Size: 64}}}
			defer vec.Release()
			var out bytes.Buffer
			if stream {
				vec.Entries[1].WriterForRead = &out
			}
			require.NoError(t, fs.Read(context.Background(), vec))
			require.Equal(t, data[100:164], vec.Entries[0].Data)
			if stream {
				require.Equal(t, data[164:228], out.Bytes())
			} else {
				require.Equal(t, data[164:228], vec.Entries[1].Data)
			}
			require.Zero(t, fs.diskCache.cache.Used(), "an index-object read must not fill the table-data cache")
		})
	}
}

func TestIndexPrefetchWhileDiskPrefetchRunning(t *testing.T) {
	for _, concurrent := range []bool{false, true} {
		name := "after_disk_fill"
		if concurrent {
			name = "during_disk_fill"
		}
		t.Run(name, func(t *testing.T) {
			fs, _, data := newIndexObjectFS(t, true)
			storage := newBlackBoxStorage(fs.storage, func(string, int64) bool { return true })
			fs.storage = storage
			resume := sync.OnceFunc(func() { close(storage.gate) })
			defer resume()
			ctx := context.Background()
			req := ObjectPrefetch{FilePath: "object", Size: int64(len(data))}
			require.NoError(t, fs.PrefetchObject(ctx, req))
			require.Eventually(t, func() bool {
				cur, _, _ := storage.counts(fs.pathToKey(req.FilePath))
				return cur == 1
			}, 5*time.Second, time.Millisecond, "the table-data download should be under way")
			if !concurrent {
				resume()
				waitPrefetcherIdle(t, fs)
			}
			indexCtx := WithFileServicePolicy(ctx, IndexFileObject)
			require.NoError(t, fs.PrefetchObject(indexCtx, req))
			resume()
			waitPrefetcherIdle(t, fs)
			require.True(t, fs.diskCache.cache.Contains(fs.diskCache.pathForFile(req.FilePath)))
			_, cached := fs.indexObjectCached(req.FilePath)
			require.True(t, cached, "the index-object prefetch should fill its own cache tier")
		})
	}
}

func TestIndexObjectExternalRemovalIsCacheMiss(t *testing.T) {
	for _, pooled := range []bool{false, true} {
		name := "no_retained_file"
		if pooled {
			name = "default_open_file_pool"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			fs, storage, data := newIndexObjectFS(t, true)
			cache := fs.indexFileCache
			if !pooled {
				cache.disk.paths.L.Lock()
				cache.disk.paths.openFiles.limit = -1
				cache.disk.paths.L.Unlock()
			}
			cacheIndexObject(t, fs, "object", data)
			entryDir, cached := fs.indexObjectCached("object")
			require.True(t, cached)
			// Give the second cache its own path owners, as in a different process.
			other, err := NewIndexFileCache(ctx, cache.Path(), fscache.ConstCapacity(64<<20), "other", 0)
			require.NoError(t, err)
			defer other.Close(ctx)
			<-other.disk.load.done
			const policy = IndexFileObject | ExactColumnsRead | SkipCacheWrites
			require.Equal(t, data[100:164], readIndexObjectRange(t, fs, policy, 100, 64))
			require.Zero(t, storage.reads.Load(), "the initial read should come from the cached file")

			require.NoError(t, other.Delete(ctx, IndexObjectEntry("object")))
			require.NoDirExists(t, entryDir)
			require.Equal(t, data[100:164], readIndexObjectRange(t, fs, policy, 100, 64))
			require.EqualValues(t, 1, storage.reads.Load(), "a file removed by another cache instance should be a cache miss")
		})
	}
}

func TestIndexQuotaErrorDoesNotEvictTableCache(t *testing.T) {
	for _, quotaExceeded := range []bool{false, true} {
		name := "successful_index_fill"
		if quotaExceeded {
			name = "index_fill_exceeds_quota"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			fs, _, data := newIndexObjectFS(t, true)
			d := fs.diskCache
			require.NoError(t, d.setFileContent(ctx, "bystander", data, false))
			warmFile := d.pathForFile("bystander")
			require.FileExists(t, warmFile)
			if quotaExceeded {
				syncFile := d.fileSync
				indexDir := fs.indexFileCache.Path()
				d.fileSync = func(f *os.File) error {
					if filepath.Dir(f.Name()) == indexDir {
						return syscall.EDQUOT
					}
					return syncFile(f)
				}
			}
			indexCtx := WithFileServicePolicy(ctx, IndexFileObject)
			require.NoError(t, fs.PrefetchFile(indexCtx, "object"), "an index-cache write failure is best effort")
			_, cached := fs.indexObjectCached("object")
			require.Equal(t, !quotaExceeded, cached)
			require.FileExists(t, warmFile, "an index-only disk quota failure must not evict table-data files")
		})
	}
}

type reviewIndexJoinContext struct {
	context.Context
	beforeJoin func()
}

func (c reviewIndexJoinContext) Value(key any) any {
	if _, ok := key.(downloadStartedKey); ok {
		c.beforeJoin()
	}
	return c.Context.Value(key)
}

func TestIndexReadSeesConcurrentPublication(t *testing.T) {
	for _, overlap := range []bool{false, true} {
		name := "published_before_read"
		if overlap {
			name = "published_after_cache_lookup"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			fs, _, data := newIndexObjectFS(t, true)
			storage := newBlackBoxStorage(fs.storage, func(string, int64) bool { return true })
			fs.storage = storage
			resumeStorage := sync.OnceFunc(func() { close(storage.gate) })
			fillDone := make(chan struct{})
			var fillErr error
			go func() {
				defer close(fillDone)
				fillErr = fs.PrefetchFile(WithFileServicePolicy(ctx, IndexFileObject), "object")
			}()
			defer func() {
				resumeStorage()
				<-fillDone
			}()
			objectKey := fs.pathToKey("object")
			require.Eventually(t, func() bool {
				cur, _, _ := storage.counts(objectKey)
				return cur == 1
			}, time.Second, time.Millisecond)

			atJoin := make(chan struct{})
			resumeJoin := make(chan struct{})
			unblockJoin := sync.OnceFunc(func() { close(resumeJoin) })
			var readCtx context.Context = ctx
			if overlap {
				// With disk reads skipped, the first lookup of downloadStartedKey is
				// inside readIndexDownload, after the index-file lookup missed.
				readCtx = reviewIndexJoinContext{Context: ctx, beforeJoin: sync.OnceFunc(func() {
					close(atJoin)
					<-resumeJoin
				})}
			} else {
				resumeStorage()
				<-fillDone
				require.NoError(t, fillErr)
			}
			vec := &IOVector{FilePath: "object", Policy: IndexFileObject | SkipDiskCacheReads | SkipMemoryCache | ExactColumnsRead,
				Entries: []IOEntry{{Offset: 100, Size: 64}}}
			defer vec.Release()
			readDone := make(chan struct{})
			var readErr error
			go func() {
				defer close(readDone)
				readErr = fs.Read(readCtx, vec)
			}()
			defer func() {
				unblockJoin()
				resumeStorage()
				<-readDone
			}()
			if overlap {
				select {
				case <-atJoin:
				case <-ctx.Done():
					t.Fatal("the read never tried to join the index download")
				}
				resumeStorage()
				<-fillDone
				require.NoError(t, fillErr)
				_, cached := fs.indexObjectCached("object")
				require.True(t, cached)
				require.Nil(t, fs.diskCache.download(fs.diskCache.pathForFile("object")))
			}
			unblockJoin()
			<-readDone
			require.NoError(t, readErr)
			require.Equal(t, data[100:164], vec.Entries[0].Data)
			_, _, total := storage.counts(objectKey)
			require.Equal(t, 1, total, "the read should use the index file published before it checked the download registry")
		})
	}
}
