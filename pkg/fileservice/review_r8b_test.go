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
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStartupLoadPreservesRefilledFile(t *testing.T) {
	for _, tc := range []struct {
		name       string
		deletePath string
	}{
		{name: "baseline_unrelated_delete", deletePath: "unrelated"},
		{name: "delete_and_refill_same_object", deletePath: "object"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			d := newLifecycleTestDiskCache(t)
			path := d.pathForFile("object")
			require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
			require.NoError(t, os.WriteFile(path, []byte("old"), 0o644))

			found, resume, loaded := make(chan struct{}), make(chan struct{}), make(chan struct{})
			unblock := sync.OnceFunc(func() { close(resume) })
			d.hooks.beforeLoadFile = func(file string) {
				if file == path {
					close(found)
					<-resume
				}
			}
			go func() {
				defer close(loaded)
				d.loadCache(ctx)
			}()
			defer func() { unblock(); <-loaded }()
			select {
			case <-found:
			case <-ctx.Done():
				t.Fatal("startup load did not find the old file")
			}

			require.NoError(t, d.DeletePaths(ctx, []string{tc.deletePath}))
			want := "old"
			if tc.deletePath == "object" {
				want = "replacement"
				require.NoError(t, d.SetFile(ctx, "object", func(context.Context) (io.ReadCloser, error) {
					return io.NopCloser(strings.NewReader(want)), nil
				}))
				got, err := os.ReadFile(path)
				require.NoError(t, err)
				require.Equal(t, want, string(got), "refill published before the loader resumes")
			}

			unblock()
			<-loaded
			got, err := os.ReadFile(path)
			require.NoError(t, err, "startup load removed the current cache file")
			require.Equal(t, want, string(got))
			require.True(t, d.cache.Contains(path))
		})
	}
}

func TestMemoryPrefetchDeliversCallbacksAfterGoexit(t *testing.T) {
	for _, tc := range []struct {
		name string
		exit func()
	}{
		{name: "baseline_callback_returns", exit: func() {}},
		{name: "first_callback_exits_goroutine", exit: runtime.Goexit},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const unit = 64 << 10
			fs, _, data := newPrefetchRangeFS(t, unit, 2*unit)
			<-fs.diskCache.load.done
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			first, second := make(chan struct{}), make(chan struct{})
			ctx = OnDiskCacheWritten(ctx, func(string, IOEntry) {
				defer close(first)
				tc.exit()
			})
			ctx = OnDiskCacheWritten(ctx, func(string, IOEntry) { close(second) })
			vector := &IOVector{FilePath: "object", Policy: SkipFullFilePreloads | SkipMemoryCache,
				Entries: []IOEntry{{Offset: 16, Size: 64}}}
			defer vector.Release()
			require.NoError(t, fs.Read(ctx, vector))
			require.Equal(t, data[16:80], vector.Entries[0].Data)
			select {
			case <-first:
			case <-ctx.Done():
				t.Fatal("the first callback was never called")
			}
			require.Eventually(t, func() bool { return fs.diskCache.async.flightCopies.Load() == 0 },
				time.Second, time.Millisecond, "the callback runner must release its copy")
			require.True(t, fs.diskCache.cache.Contains(fs.diskCache.pathForIOEntry("object", IOEntry{Size: unit})))
			select {
			case <-second:
			default:
				t.Fatal("the range was published and its callback copy released, but the second callback was never called")
			}
		})
	}
}

func TestDeleteRetryInvalidatesRange(t *testing.T) {
	for _, tc := range []struct {
		name          string
		readOnlyShard bool
	}{
		{name: "baseline_writable_shard"},
		{name: "first_delete_cannot_unlink_range", readOnlyShard: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.readOnlyShard && !permissionsEnforced(t) {
				t.Skip("filesystem permissions are not enforced")
			}
			ctx := context.Background()
			fs, _, data := newPrefetchRangeFS(t, 0, 1024)
			<-fs.diskCache.load.done
			const offset, size = 100, 4
			rangeEntry := IOEntry{Offset: offset, Size: size}
			require.Equal(t, data[offset:offset+size], readRangeOf(t, fs, ExactRead, offset, size))
			path := fs.diskCache.pathForIOEntry("object", rangeEntry)
			require.FileExists(t, path)
			if tc.readOnlyShard {
				parent := filepath.Dir(path)
				require.NoError(t, os.Chmod(parent, 0o555))
				t.Cleanup(func() { require.NoError(t, os.Chmod(parent, 0o755)) })
				require.ErrorIs(t, fs.Delete(ctx, "object"), os.ErrPermission)
				require.FileExists(t, path)
				require.NoError(t, os.Chmod(parent, 0o755))
			} else {
				require.NoError(t, fs.Delete(ctx, "object"))
			}
			require.NoError(t, fs.Delete(ctx, "object"), "the delete retry succeeds")
			replacement := make([]byte, len(data))
			copy(replacement[offset:], "new!")
			require.NoError(t, fs.Write(ctx, IOVector{FilePath: "object", Policy: SkipAllCache,
				Entries: []IOEntry{{Size: int64(len(replacement)), Data: replacement}}}))
			got := readRangeOf(t, fs, ExactRead, offset, size)
			require.Equal(t, "new!", string(got), "a successful delete retry must invalidate the old cached range")
		})
	}
}

func TestIndexPrefetchRepairsNonFileData(t *testing.T) {
	for _, tc := range []struct {
		name        string
		isDirectory bool
	}{
		{name: "baseline_missing_data"},
		{name: "data_replaced_by_directory", isDirectory: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs, storage, data := newIndexObjectFS(t, true)
			cacheIndexObject(t, fs, "object", data)
			path, ok := fs.indexObjectCached("object")
			require.True(t, ok)
			name := filepath.Join(path, IndexObjectFile)
			require.NoError(t, os.Remove(name))
			if tc.isDirectory {
				require.NoError(t, os.Mkdir(name, 0o755))
			}
			ctx := WithFileServicePolicy(context.Background(), IndexFileObject)
			require.NoError(t, fs.PrefetchFile(ctx, "object"))
			require.EqualValues(t, 1, storage.reads.Load(), "prefetch must replace the invalid index-cache entry from storage")
			require.Equal(t, data[100:104], readIndexObjectRange(t, fs, IndexFileObject, 100, 4))
			require.EqualValues(t, 1, storage.reads.Load(), "the repaired entry serves the next read")
		})
	}
}

func TestDropCachedRangeKeepsRangeUntilUnlinked(t *testing.T) {
	for _, tc := range []struct {
		name          string
		readOnlyShard bool
	}{
		{name: "baseline_writable_shard"},
		{name: "drop_cannot_unlink_range", readOnlyShard: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.readOnlyShard && !permissionsEnforced(t) {
				t.Skip("filesystem permissions are not enforced")
			}
			ctx := context.Background()
			fs, _, data := newPrefetchRangeFS(t, 0, 1024)
			<-fs.diskCache.load.done
			const offset, size = 100, 4
			require.Equal(t, data[offset:offset+size], readRangeOf(t, fs, ExactRead, offset, size))
			path := fs.diskCache.pathForIOEntry("object", IOEntry{Offset: offset, Size: size})
			require.NoError(t, os.Truncate(path, 1))
			if tc.readOnlyShard {
				parent := filepath.Dir(path)
				require.NoError(t, os.Chmod(parent, 0o555))
				t.Cleanup(func() { require.NoError(t, os.Chmod(parent, 0o755)) })
				fs.diskCache.dropCachedRange(ctx, path)
				require.FileExists(t, path)
				require.NoError(t, os.Chmod(parent, 0o755))
			} else {
				fs.diskCache.dropCachedRange(ctx, path)
			}
			require.NoError(t, fs.Delete(ctx, "object"))
			require.NoFileExists(t, path, "a delete after a failed drop must remove the range file")
			require.False(t, fs.diskCache.cache.Contains(path))
		})
	}
}

func TestIndexReadRepairsNonFileData(t *testing.T) {
	for _, tc := range []struct {
		name        string
		isDirectory bool
	}{
		{name: "baseline_missing_data"},
		{name: "data_replaced_by_directory", isDirectory: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs, storage, data := newIndexObjectFS(t, true)
			cacheIndexObject(t, fs, "object", data)
			path, ok := fs.indexObjectCached("object")
			require.True(t, ok)
			name := filepath.Join(path, IndexObjectFile)
			require.NoError(t, os.Remove(name))
			if tc.isDirectory {
				require.NoError(t, os.Mkdir(name, 0o755))
			}
			require.Equal(t, data[100:104], readIndexObjectRange(t, fs, IndexFileObject, 100, 4))
			require.EqualValues(t, 1, storage.reads.Load())
			require.True(t, fs.indexObjectHeld("object"), "the read must replace the invalid index-cache entry")
			require.Equal(t, data[100:104], readIndexObjectRange(t, fs, IndexFileObject, 100, 4))
			require.EqualValues(t, 1, storage.reads.Load(), "the repaired entry serves the next read")
		})
	}
}
