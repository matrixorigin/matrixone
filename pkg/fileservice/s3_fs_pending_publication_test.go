// Copyright 2022 Matrix Origin
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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/util/toml"
	"github.com/stretchr/testify/require"
)

// Count bytes consumed, not just requested ranges: a correct result alone can
// hide a second full-object read while the first cache file is being published.
type publicationReadStorage struct {
	ObjectStorage
	bytes      atomic.Int64
	opens      atomic.Int64
	closes     atomic.Int64
	beforeRead func(context.Context) error
}

type publicationVectorCache struct {
	reads   atomic.Int64
	updates atomic.Int64
	last    *IOVector
}

var _ IOVectorCache = (*publicationVectorCache)(nil)

func (c *publicationVectorCache) Read(context.Context, *IOVector) error {
	c.reads.Add(1)
	return nil
}

func (c *publicationVectorCache) Update(_ context.Context, vector *IOVector, _ bool) error {
	c.updates.Add(1)
	c.last = vector
	return nil
}

func (c *publicationVectorCache) Flush(context.Context) {}

func (c *publicationVectorCache) DeletePaths(context.Context, []string) error { return nil }

func (c *publicationVectorCache) Evict(_ context.Context, done chan int64) {
	if done != nil {
		done <- 0
	}
}

func (c *publicationVectorCache) Close(context.Context) {}

func (s *publicationReadStorage) Read(ctx context.Context, key string, min, max *int64) (io.ReadCloser, error) {
	if s.beforeRead != nil {
		if err := s.beforeRead(ctx); err != nil {
			return nil, err
		}
	}
	r, err := s.ObjectStorage.Read(ctx, key, min, max)
	if err != nil {
		return nil, err
	}
	s.opens.Add(1)
	return &readCloser{
		r: readerFunc(func(p []byte) (int, error) {
			n, err := r.Read(p)
			s.bytes.Add(int64(n))
			return n, err
		}),
		closeFunc: func() error { s.closes.Add(1); return r.Close() },
	}, nil
}

// This test intentionally blocks asynchronous disk-cache publication. Keep
// the FileService operation contexts independent from fixture setup and the
// publication barrier; this is only a per-phase liveness guard.
const pendingPublicationTestWatchdog = 30 * time.Second

func runPendingPublicationPhase(t *testing.T, phase string, release func(), run func(context.Context) error) error {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx) }()

	timer := time.NewTimer(pendingPublicationTestWatchdog)
	defer timer.Stop()
	select {
	case err := <-done:
		cancel()
		return err
	case <-timer.C:
		cancel()
		if release != nil {
			release()
		}
		err := <-done
		t.Fatalf("%s did not complete within %s; returned %v after cancellation", phase, pendingPublicationTestWatchdog, err)
		return err
	}
}

func waitPendingPublicationSignal(t *testing.T, phase string, signal <-chan struct{}, release func()) {
	t.Helper()
	timer := time.NewTimer(pendingPublicationTestWatchdog)
	defer timer.Stop()
	select {
	case <-signal:
	case <-timer.C:
		if release != nil {
			release()
		}
		t.Fatalf("%s did not occur within %s", phase, pendingPublicationTestWatchdog)
	}
}

func waitPendingPublicationResult(t *testing.T, phase string, done <-chan error, cancel, release func()) error {
	t.Helper()
	timer := time.NewTimer(pendingPublicationTestWatchdog)
	defer timer.Stop()
	select {
	case err := <-done:
		return err
	case <-timer.C:
		cancel()
		if release != nil {
			release()
		}
		err := <-done
		t.Fatalf("%s did not complete within %s; returned %v after cancellation", phase, pendingPublicationTestWatchdog, err)
		return err
	}
}

func TestS3FSDefaultPolicyReadWhileFullObjectPublicationPending(t *testing.T) {
	for _, mode := range []string{"sparse", "dense", "partial-hit", "read-to-end", "cancel-between-entries", "error-between-entries", "publication-failure", "custom-cache"} {
		t.Run(mode, func(t *testing.T) {
			setupCtx, cancelSetup := context.WithCancel(context.Background())
			defer cancelSetup()
			fs, err := NewS3FS(setupCtx, ObjectStorageArguments{
				Name: "s3", Endpoint: "disk", Bucket: t.TempDir(),
			}, CacheConfig{
				DiskPath: ptrTo(t.TempDir()), DiskCapacity: ptrTo[toml.ByteSize](32 << 20),
				MemoryCapacity: ptrTo[toml.ByteSize](0),
			}, nil, false, false)
			require.NoError(t, err)
			unblock := func() {}
			t.Cleanup(func() { unblock(); fs.Close(context.Background()) })
			data := make([]byte, (9<<20)+4096)
			for i := range data {
				data[i] = byte(i*31 + i/251)
			}
			require.NoError(t, runPendingPublicationPhase(t, "initial object write", nil, func(ctx context.Context) error {
				return fs.Write(ctx, IOVector{FilePath: "object", Entries: []IOEntry{{Size: int64(len(data)), Data: data}}, Policy: SkipDiskCache | SkipMemoryCache})
			}))
			storage := &publicationReadStorage{ObjectStorage: fs.storage}
			fs.storage = storage
			if mode == "partial-hit" {
				warm := &IOVector{FilePath: "object", Entries: []IOEntry{{Offset: 2048, Size: 3}}, Policy: SkipFullFilePreloads}
				defer warm.Release()
				require.NoError(t, runPendingPublicationPhase(t, "partial-hit warm read", nil, func(ctx context.Context) error {
					return fs.Read(ctx, warm)
				}))
				require.NoError(t, runPendingPublicationPhase(t, "partial-hit warm flush", nil, func(ctx context.Context) error {
					fs.FlushCache(ctx)
					return ctx.Err()
				}))
			}
			leaderBytes := storage.bytes.Load()
			started := make(chan struct{})
			release := make(chan struct{})
			unblock = sync.OnceFunc(func() { close(release) })
			var once sync.Once
			var syncCalls int
			fs.diskCache.fileSync = func(*os.File) error {
				once.Do(func() { close(started) })
				<-release
				syncCalls++
				if mode == "publication-failure" && syncCalls == 1 {
					return errors.New("injected publication sync failure")
				}
				return nil
			}
			leader := &IOVector{FilePath: "object", Entries: []IOEntry{{Offset: 1024, Size: 4}}}
			defer leader.Release()
			require.NoError(t, runPendingPublicationPhase(t, "leader read", unblock, func(ctx context.Context) error {
				return fs.Read(ctx, leader)
			}))
			waitPendingPublicationSignal(t, "cache finalizer start", started, unblock)
			require.Equal(t, data[1024:1028], leader.Entries[0].Data)
			require.Equal(t, int64(len(data)), storage.bytes.Load()-leaderBytes)
			require.False(t, fs.ioMerger.IsMerging(fs.readMergeKey(leader)))
			diskPath := fs.diskCache.pathForFile("object")
			require.True(t, fs.diskCache.isUpdating(diskPath))
			require.NoFileExists(t, diskPath)
			require.False(t, fs.diskCache.cache.Contains(diskPath))

			secondOffset := int64(9 << 20)
			if mode == "dense" {
				secondOffset = 4112
			}
			follower := &IOVector{FilePath: "object", Entries: []IOEntry{{Offset: 4096, Size: 13}, {Offset: secondOffset, Size: 17}}}
			if mode == "partial-hit" {
				follower.Entries = append([]IOEntry{{Offset: 2048, Size: 3}}, follower.Entries...)
			}
			if mode == "read-to-end" {
				follower.Entries = []IOEntry{{Offset: 9 << 20, Size: -1}}
			}
			var vectorCache *publicationVectorCache
			if mode == "custom-cache" {
				vectorCache = new(publicationVectorCache)
				follower.Caches = []IOVectorCache{vectorCache}
			}
			defer follower.Release()
			before := storage.bytes.Load()
			opensBefore := storage.opens.Load()
			if mode == "cancel-between-entries" || mode == "error-between-entries" {
				readCtx, stop := context.WithCancel(context.Background())
				defer stop()
				secondRead := make(chan struct{})
				injected := errors.New("injected range failure")
				var calls int
				storage.beforeRead = func(ctx context.Context) error {
					calls++
					if calls == 2 {
						close(secondRead)
						if mode == "error-between-entries" {
							return injected
						}
						<-ctx.Done()
						return ctx.Err()
					}
					return nil
				}
				done := make(chan error, 1)
				go func() { done <- fs.Read(readCtx, follower) }()
				waitPendingPublicationSignal(t, "second bounded read start", secondRead, func() {
					stop()
					unblock()
					<-done
				})
				stop()
				err := waitPendingPublicationResult(t, "cancelled follower read", done, stop, unblock)
				if mode == "error-between-entries" {
					require.ErrorIs(t, err, injected)
				} else {
					require.ErrorIs(t, err, context.Canceled)
				}
				storage.beforeRead = nil
				require.Equal(t, int64(13), storage.bytes.Load()-before)
				require.Equal(t, data[4096:4109], follower.Entries[0].Data)
				require.Empty(t, follower.Entries[1].Data)
				require.Equal(t, int64(1), storage.opens.Load()-opensBefore)
			} else {
				require.NoError(t, runPendingPublicationPhase(t, "follower read", unblock, func(ctx context.Context) error {
					return fs.Read(ctx, follower)
				}))
				for _, entry := range follower.Entries {
					require.Equal(t, data[entry.Offset:entry.Offset+entry.Size], entry.Data)
				}
				wantBytes := int64(30)
				wantOpens := int64(2)
				if mode == "dense" {
					wantBytes = 33
					wantOpens = 1
				}
				if mode == "read-to-end" {
					wantBytes = 4096
					wantOpens = 1
				}
				t.Logf("consumed_bytes=%d object_reads=%d", storage.bytes.Load()-before, storage.opens.Load()-opensBefore)
				require.Equal(t, wantBytes, storage.bytes.Load()-before, "pending publication must not trigger another full-object read")
				require.Equal(t, wantOpens, storage.opens.Load()-opensBefore)
				if mode == "custom-cache" {
					require.Equal(t, int64(1), vectorCache.reads.Load())
					require.Equal(t, int64(1), vectorCache.updates.Load())
					require.Same(t, follower, vectorCache.last)
					require.Len(t, vectorCache.last.Entries, 2)
				}
			}
			require.Equal(t, storage.opens.Load(), storage.closes.Load())
			require.False(t, fs.ioMerger.IsMerging(fs.readMergeKey(follower)))
			require.NoFileExists(t, diskPath)
			require.False(t, fs.diskCache.cache.Contains(diskPath))
			unblock()
			require.NoError(t, runPendingPublicationPhase(t, "publication flush", unblock, func(ctx context.Context) error {
				fs.FlushCache(ctx)
				return ctx.Err()
			}))
			require.False(t, fs.diskCache.isUpdating(diskPath))
			if mode == "publication-failure" {
				require.NoFileExists(t, diskPath)
				require.False(t, fs.diskCache.cache.Contains(diskPath))
				retry := &IOVector{FilePath: "object", Entries: []IOEntry{{Offset: 1024, Size: 4}}}
				defer retry.Release()
				require.NoError(t, runPendingPublicationPhase(t, "publication retry read", unblock, func(ctx context.Context) error {
					return fs.Read(ctx, retry)
				}))
				require.Equal(t, data[1024:1028], retry.Entries[0].Data)
				require.NoError(t, runPendingPublicationPhase(t, "publication retry flush", unblock, func(ctx context.Context) error {
					fs.FlushCache(ctx)
					return ctx.Err()
				}))
				require.False(t, fs.diskCache.isUpdating(diskPath))
			}
			require.FileExists(t, diskPath)
			cached, err := os.ReadFile(diskPath)
			require.NoError(t, err)
			require.Equal(t, data, cached)
			var tempFiles []string
			require.NoError(t, filepath.WalkDir(fs.diskCache.path, func(path string, entry os.DirEntry, err error) error {
				if err == nil && !entry.IsDir() && filepath.Ext(path) == ".tmp" {
					tempFiles = append(tempFiles, path)
				}
				return err
			}))
			require.Empty(t, tempFiles)
			before = storage.bytes.Load()
			hit := &IOVector{FilePath: "object", Entries: []IOEntry{{Offset: 4096, Size: 13}, {Offset: secondOffset, Size: 17}}}
			defer hit.Release()
			require.NoError(t, runPendingPublicationPhase(t, "published cache hit", nil, func(ctx context.Context) error {
				return fs.Read(ctx, hit)
			}))
			require.Equal(t, data[4096:4109], hit.Entries[0].Data)
			require.Equal(t, data[secondOffset:secondOffset+17], hit.Entries[1].Data)
			require.Equal(t, before, storage.bytes.Load(), "published file must satisfy the next read")
		})
	}
}
