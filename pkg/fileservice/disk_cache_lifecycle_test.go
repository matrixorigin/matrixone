// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/stretchr/testify/require"
)

const diskCacheLifecycleTestTimeout = time.Second

func newLifecycleTestDiskCache(t *testing.T) *DiskCache {
	t.Helper()
	cache, err := NewDiskCache(
		context.Background(),
		t.TempDir(),
		fscache.ConstCapacity(1<<20),
		nil,
		false,
		nil,
		"",
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		cache.Close(context.Background())
	})
	return cache
}

type notifyingDiskCacheLocker struct {
	sync.Mutex
	unlocked chan struct{}
}

type blockingDiskCacheWriter struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (w *blockingDiskCacheWriter) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.started) })
	<-w.release
	return len(p), nil
}

func (l *notifyingDiskCacheLocker) Unlock() {
	l.Mutex.Unlock()
	l.unlocked <- struct{}{}
}

func installNotifyingDiskCacheLocker(cache *DiskCache) *notifyingDiskCacheLocker {
	locker := &notifyingDiskCacheLocker{unlocked: make(chan struct{}, 16)}
	cache.updatingPaths.Cond = sync.NewCond(locker)
	return locker
}

func requireDiskCacheUnlock(t *testing.T, locker *notifyingDiskCacheLocker) {
	t.Helper()
	select {
	case <-locker.unlocked:
	case <-time.After(diskCacheLifecycleTestTimeout):
		t.Fatal("disk-cache operation did not reach the expected wait phase")
	}
}

func TestDiskCacheIOEntryWaitHonorsCancellation(t *testing.T) {
	cache := newLifecycleTestDiskCache(t)
	locker := installNotifyingDiskCacheLocker(cache)
	diskPath := cache.pathForIOEntry("foo", IOEntry{Offset: 0, Size: 1})
	release := cache.startUpdate(diskPath)
	requireDiskCacheUnlock(t, locker)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- cache.Read(ctx, &IOVector{
			FilePath: "foo",
			Entries:  []IOEntry{{Offset: 0, Size: 1}},
		})
	}()
	requireDiskCacheUnlock(t, locker)
	cancel()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(diskCacheLifecycleTestTimeout):
		release()
		<-done
		t.Fatal("canceled disk-cache read remained blocked by an IOEntry update")
	}
	release()
}

func TestDiskCacheIOEntryWaitIsBoundedDuringAsyncFinalize(t *testing.T) {
	cache := newLifecycleTestDiskCache(t)
	originalShortWait := shortIOWaitDuration
	shortIOWaitDuration = 5 * time.Millisecond
	t.Cleanup(func() { shortIOWaitDuration = originalShortWait })

	syncStarted := make(chan struct{})
	releaseSync := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(releaseSync) })
	t.Cleanup(unblock)
	cache.fileSync = func(file *os.File) error {
		close(syncStarted)
		<-releaseSync
		return file.Sync()
	}
	require.NoError(t, cache.Update(context.Background(), &IOVector{
		FilePath: "foo",
		Entries:  []IOEntry{{Offset: 0, Size: 1, Data: []byte("x")}},
	}, true))
	select {
	case <-syncStarted:
	case <-time.After(diskCacheLifecycleTestTimeout):
		t.Fatal("async IOEntry finalizer did not start")
	}

	vector := &IOVector{FilePath: "foo", Entries: []IOEntry{{Offset: 0, Size: 1}}}
	readDone := make(chan error, 1)
	go func() { readDone <- cache.Read(context.Background(), vector) }()
	select {
	case err := <-readDone:
		require.NoError(t, err)
	case <-time.After(diskCacheLifecycleTestTimeout):
		unblock()
		<-readDone
		t.Fatal("IOEntry cache read waited for async disk finalization")
	}
	require.False(t, vector.Entries[0].done)

	unblock()
	flushCtx, cancel := context.WithTimeout(context.Background(), diskCacheLifecycleTestTimeout)
	defer cancel()
	cache.Flush(flushCtx)
	require.NoError(t, flushCtx.Err())
	require.NoError(t, cache.Read(context.Background(), vector))
	require.True(t, vector.Entries[0].done)
	require.Equal(t, []byte("x"), vector.Entries[0].Data)
}

func TestDiskCacheReadSharesWaitBudgetAcrossEntries(t *testing.T) {
	originalShortWait := shortIOWaitDuration
	shortIOWaitDuration = 20 * time.Millisecond
	t.Cleanup(func() { shortIOWaitDuration = originalShortWait })

	for _, tc := range []struct {
		name      string
		holdPaths func(*DiskCache, []IOEntry) []string
	}{
		{
			name: "full file",
			holdPaths: func(cache *DiskCache, _ []IOEntry) []string {
				return []string{cache.pathForFile("foo")}
			},
		},
		{
			name: "distinct ranges",
			holdPaths: func(cache *DiskCache, entries []IOEntry) []string {
				paths := make([]string, 0, len(entries))
				for _, entry := range entries {
					paths = append(paths, cache.pathForIOEntry("foo", entry))
				}
				return paths
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cache := newLifecycleTestDiskCache(t)
			entries := make([]IOEntry, 64)
			for i := range entries {
				entries[i] = IOEntry{Offset: int64(i), Size: 1}
			}

			var releases []func()
			for _, path := range tc.holdPaths(cache, entries) {
				releases = append(releases, cache.startUpdate(path))
			}
			releaseAll := sync.OnceFunc(func() {
				for _, release := range releases {
					release()
				}
			})
			t.Cleanup(releaseAll)

			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()
			require.NoError(t, cache.Read(ctx, &IOVector{
				FilePath: "foo",
				Entries:  entries,
			}))
			require.NoError(t, ctx.Err(), "one Read exhausted a per-entry wait budget")
			releaseAll()
		})
	}
}

func TestDiskCacheSynchronousUpdateWaitHonorsCancellation(t *testing.T) {
	cache := newLifecycleTestDiskCache(t)
	locker := installNotifyingDiskCacheLocker(cache)
	diskPath := cache.pathForIOEntry("foo", IOEntry{Offset: 0, Size: 1})
	release := cache.startUpdate(diskPath)
	requireDiskCacheUnlock(t, locker)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- cache.Update(ctx, &IOVector{
			FilePath: "foo",
			Entries:  []IOEntry{{Offset: 0, Size: 1, Data: []byte("x")}},
		}, false)
	}()
	requireDiskCacheUnlock(t, locker)
	cancel()

	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(diskCacheLifecycleTestTimeout):
		release()
		<-done
		t.Fatal("canceled disk-cache update remained blocked by another update")
	}
	release()
}

func TestDiskCacheDeletePathsWaitHonorsCancellation(t *testing.T) {
	cache := newLifecycleTestDiskCache(t)
	locker := installNotifyingDiskCacheLocker(cache)
	release := cache.startUpdate(cache.pathForFile("foo"))
	requireDiskCacheUnlock(t, locker)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- cache.DeletePaths(ctx, []string{"foo"})
	}()
	requireDiskCacheUnlock(t, locker)
	cancel()

	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(diskCacheLifecycleTestTimeout):
		release()
		<-done
		t.Fatal("canceled disk-cache delete remained blocked by an update")
	}
	release()
}

func TestDiskCacheCleanupDoesNotBlockIndependentPath(t *testing.T) {
	cache := newLifecycleTestDiskCache(t)
	cleanupEntered := make(chan struct{})
	releaseCleanup := make(chan struct{})
	doneFirst := cache.startUpdateWithCleanup("first", func() error {
		close(cleanupEntered)
		<-releaseCleanup
		return nil
	})
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- doneFirst()
	}()
	<-cleanupEntered

	secondStarted := make(chan func(), 1)
	go func() {
		secondStarted <- cache.startUpdate("second")
	}()

	select {
	case doneSecond := <-secondStarted:
		doneSecond()
	case <-time.After(diskCacheLifecycleTestTimeout):
		close(releaseCleanup)
		require.NoError(t, <-firstDone)
		doneSecond := <-secondStarted
		doneSecond()
		t.Fatal("cleanup for one path held the global update lock")
	}
	close(releaseCleanup)
	require.NoError(t, <-firstDone)
}

func TestDiskCacheCleanupKeepsSamePathExcluded(t *testing.T) {
	cache := newLifecycleTestDiskCache(t)
	cleanupEntered := make(chan struct{})
	releaseCleanup := make(chan struct{})
	doneFirst := cache.startUpdateWithCleanup("same", func() error {
		close(cleanupEntered)
		<-releaseCleanup
		return nil
	})
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- doneFirst()
	}()
	<-cleanupEntered

	waitCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := cache.startUpdateContext(waitCtx, "same")
	require.ErrorIs(t, err, context.DeadlineExceeded)

	close(releaseCleanup)
	require.NoError(t, <-firstDone)
	doneSecond, err := cache.startUpdateContext(context.Background(), "same")
	require.NoError(t, err)
	doneSecond()
}

func TestDiskCacheEvictionTransfersToCleanupOwner(t *testing.T) {
	cache := newLifecycleTestDiskCache(t)
	ctx := context.Background()
	diskPath := cache.pathForFile("foo")
	require.NoError(t, os.WriteFile(diskPath, []byte("payload"), 0o644))
	cache.cache.Set(ctx, diskPath, struct{}{}, int64(len("payload")))

	cleanupObservedIndex := make(chan bool, 1)
	releaseCleanup := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(releaseCleanup) })
	t.Cleanup(unblock)
	doneUpdate := cache.startUpdateWithCleanup(diskPath, func() error {
		cleanupObservedIndex <- cache.cache.Contains(diskPath)
		<-releaseCleanup
		return nil
	})
	updateDone := make(chan error, 1)
	go func() { updateDone <- doneUpdate() }()

	select {
	case contained := <-cleanupObservedIndex:
		require.True(t, contained)
	case <-time.After(diskCacheLifecycleTestTimeout):
		t.Fatal("cleanup owner did not reach its index check")
	}

	cache.cache.ForceEvictWithWait(ctx, int64(len("payload")))
	require.False(t, cache.cache.Contains(diskPath))
	unblock()
	select {
	case err := <-updateDone:
		require.NoError(t, err)
	case <-time.After(diskCacheLifecycleTestTimeout):
		t.Fatal("cleanup owner did not release the path")
	}
	_, err := os.Stat(diskPath)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestDiskCacheEvictionTransfersToPlainPathOwner(t *testing.T) {
	cache := newLifecycleTestDiskCache(t)
	ctx := context.Background()
	diskPath := cache.pathForFile("foo")
	require.NoError(t, os.WriteFile(diskPath, []byte("payload"), 0o644))
	cache.cache.Set(ctx, diskPath, struct{}{}, int64(len("payload")))

	release := cache.startUpdate(diskPath)
	releaseOnce := sync.OnceFunc(release)
	t.Cleanup(releaseOnce)
	cache.cache.ForceEvictWithWait(ctx, int64(len("payload")))
	require.False(t, cache.cache.Contains(diskPath))
	_, err := os.Stat(diskPath)
	require.NoError(t, err)

	releaseOnce()
	_, err = os.Stat(diskPath)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestDiskCacheOpenReaderCannotReindexEvictedPath(t *testing.T) {
	tests := []struct {
		name  string
		evict func(context.Context, *DiskCache, string)
	}{
		{
			name: "Delete",
			evict: func(ctx context.Context, cache *DiskCache, diskPath string) {
				cache.cache.Delete(ctx, diskPath)
			},
		},
		{
			name: "ForceEvict",
			evict: func(ctx context.Context, cache *DiskCache, _ string) {
				cache.cache.ForceEvictWithWait(ctx, cache.cache.Used())
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cache := newLifecycleTestDiskCache(t)
			ctx := context.Background()
			require.NoError(t, cache.SetFile(ctx, "foo", func(context.Context) (io.ReadCloser, error) {
				return io.NopCloser(strings.NewReader("ab")), nil
			}))

			diskPath := cache.pathForFile("foo")
			writer := &blockingDiskCacheWriter{
				started: make(chan struct{}),
				release: make(chan struct{}),
			}
			releaseWriter := sync.OnceFunc(func() { close(writer.release) })
			t.Cleanup(releaseWriter)
			vector := &IOVector{
				FilePath: "foo",
				Entries: []IOEntry{
					{Offset: 0, Size: 1, WriterForRead: writer},
					{Offset: 1, Size: 1},
				},
			}
			readDone := make(chan error, 1)
			go func() { readDone <- cache.Read(ctx, vector) }()

			select {
			case <-writer.started:
			case <-time.After(diskCacheLifecycleTestTimeout):
				t.Fatal("disk-cache reader did not open the full-file cache entry")
			}
			test.evict(ctx, cache, diskPath)
			require.False(t, cache.cache.Contains(diskPath))
			_, err := os.Stat(diskPath)
			require.ErrorIs(t, err, os.ErrNotExist)

			releaseWriter()
			select {
			case err := <-readDone:
				require.NoError(t, err)
			case <-time.After(diskCacheLifecycleTestTimeout):
				t.Fatal("disk-cache reader did not finish after eviction")
			}
			require.True(t, vector.Entries[0].done)
			require.True(t, vector.Entries[1].done)
			require.Equal(t, []byte("b"), vector.Entries[1].Data)
			require.False(t, cache.cache.Contains(diskPath))
			_, err = os.Stat(diskPath)
			require.ErrorIs(t, err, os.ErrNotExist)
			cache.updatingPaths.L.Lock()
			activeReadGenerations := len(cache.updatingPaths.readGenerations)
			cache.updatingPaths.L.Unlock()
			require.Zero(t, activeReadGenerations)
		})
	}
}

func TestDiskCacheCurrentReaderReindexesUntrackedPath(t *testing.T) {
	cache := newLifecycleTestDiskCache(t)
	ctx := context.Background()
	diskPath := cache.pathForFile("foo")
	require.NoError(t, os.WriteFile(diskPath, []byte("ab"), 0o644))
	require.False(t, cache.cache.Contains(diskPath))

	vector := &IOVector{
		FilePath: "foo",
		Entries:  []IOEntry{{Offset: 1, Size: 1}},
	}
	require.NoError(t, cache.Read(ctx, vector))
	require.True(t, vector.Entries[0].done)
	require.Equal(t, []byte("b"), vector.Entries[0].Data)
	require.True(t, cache.cache.Contains(diskPath))
	_, err := os.Stat(diskPath)
	require.NoError(t, err)
	cache.updatingPaths.L.Lock()
	activeReadGenerations := len(cache.updatingPaths.readGenerations)
	cache.updatingPaths.L.Unlock()
	require.Zero(t, activeReadGenerations)
}

func TestDiskCacheCompletedOwnerCannotReleaseNextGeneration(t *testing.T) {
	cache := newLifecycleTestDiskCache(t)
	doneFirst := cache.startUpdate("same")
	doneFirst()

	doneSecond := cache.startUpdate("same")
	doneFirst()
	require.True(t, cache.isUpdating("same"))
	doneSecond()
	require.False(t, cache.isUpdating("same"))
}

func TestDiskCacheAsyncUpdateReturnsBeforePathAvailable(t *testing.T) {
	cache := newLifecycleTestDiskCache(t)
	diskPath := cache.pathForIOEntry("foo", IOEntry{Offset: 0, Size: 1})
	release := cache.startUpdate(diskPath)
	data := []byte("x")
	written := make(chan IOEntry, 1)
	ctx := OnDiskCacheWritten(context.Background(), func(_ string, entry IOEntry) {
		written <- entry
	})

	done := make(chan error, 1)
	go func() {
		done <- cache.Update(ctx, &IOVector{
			FilePath: "foo",
			Entries:  []IOEntry{{Offset: 0, Size: 1, Data: data}},
		}, true)
	}()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(diskCacheLifecycleTestTimeout):
		release()
		<-done
		t.Fatal("async disk-cache update waited for the current path owner")
	}

	select {
	case <-written:
		t.Fatal("async callback ran before the cache file was written")
	default:
	}
	data[0] = 'y'
	release()
	flushCtx, cancel := context.WithTimeout(context.Background(), diskCacheLifecycleTestTimeout)
	defer cancel()
	cache.Flush(flushCtx)
	require.NoError(t, flushCtx.Err())
	writtenEntry := <-written
	require.Equal(t, []byte("x"), writtenEntry.Data)

	vector := &IOVector{FilePath: "foo", Entries: []IOEntry{{Offset: 0, Size: 1}}}
	defer vector.Release()
	require.NoError(t, cache.Read(context.Background(), vector))
	require.True(t, vector.Entries[0].done)
	require.Equal(t, []byte("x"), vector.Entries[0].Data)
}

func TestDiskCacheAsyncCallbackCanReenterCompletionMethods(t *testing.T) {
	for _, test := range []struct {
		name string
		call func(context.Context, *DiskCache)
	}{
		{
			name: "flush",
			call: func(ctx context.Context, cache *DiskCache) { cache.Flush(ctx) },
		},
		{
			name: "close",
			call: func(ctx context.Context, cache *DiskCache) { cache.Close(ctx) },
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			cache := newLifecycleTestDiskCache(t)
			callbackCtx, cancel := context.WithCancel(context.Background())
			defer cancel()
			callbackStarted := make(chan struct{})
			callbackDone := make(chan struct{})
			ctx := OnDiskCacheWritten(context.Background(), func(string, IOEntry) {
				close(callbackStarted)
				test.call(callbackCtx, cache)
				close(callbackDone)
			})

			require.NoError(t, cache.Update(ctx, &IOVector{
				FilePath: "foo",
				Entries:  []IOEntry{{Offset: 0, Size: 1, Data: []byte("x")}},
			}, true))
			select {
			case <-callbackStarted:
			case <-time.After(diskCacheLifecycleTestTimeout):
				t.Fatal("async callback did not start")
			}
			select {
			case <-callbackDone:
			case <-time.After(diskCacheLifecycleTestTimeout):
				cancel()
				<-callbackDone
				t.Fatalf("async callback deadlocked while reentering %s", test.name)
			}
			require.Eventually(t, func() bool {
				cache.async.mu.Lock()
				defer cache.async.mu.Unlock()
				return len(cache.async.slots) == 0 && cache.async.mu.pendingBytes == 0
			}, diskCacheLifecycleTestTimeout, time.Millisecond)
		})
	}
}

func TestDiskCacheAsyncCallbacksRemainOrderedAndBounded(t *testing.T) {
	cache := newLifecycleTestDiskCache(t)
	cache.fileSync = func(*os.File) error { return nil }
	releaseCallbacks := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(releaseCallbacks) })
	t.Cleanup(unblock)
	callbackStarted := make(chan struct{})
	var startedOnce sync.Once
	var callbackMu sync.Mutex
	var callbackPaths []string
	ctx := OnDiskCacheWritten(context.Background(), func(filePath string, _ IOEntry) {
		startedOnce.Do(func() { close(callbackStarted) })
		<-releaseCallbacks
		callbackMu.Lock()
		callbackPaths = append(callbackPaths, filePath)
		callbackMu.Unlock()
	})

	expectedPaths := make([]string, 0, cap(cache.async.slots))
	for i := 0; i < cap(cache.async.slots); i++ {
		filePath := fmt.Sprintf("file-%02d", i)
		expectedPaths = append(expectedPaths, filePath)
		require.NoError(t, cache.Update(ctx, &IOVector{
			FilePath: filePath,
			Entries:  []IOEntry{{Offset: 0, Size: 1, Data: []byte("x")}},
		}, true))
	}
	select {
	case <-callbackStarted:
	case <-time.After(diskCacheLifecycleTestTimeout):
		t.Fatal("async callback did not start")
	}
	flushCtx, cancel := context.WithTimeout(context.Background(), diskCacheLifecycleTestTimeout)
	defer cancel()
	cache.Flush(flushCtx)
	require.NoError(t, flushCtx.Err())

	require.NoError(t, cache.Update(ctx, &IOVector{
		FilePath: "overflow",
		Entries:  []IOEntry{{Offset: 0, Size: 1, Data: []byte("y")}},
	}, true))
	cache.async.mu.Lock()
	require.Len(t, cache.async.slots, cap(cache.async.slots))
	require.Equal(t, int64(cap(cache.async.slots)), cache.async.mu.pendingBytes)
	require.Equal(t, int64(1), cache.async.mu.dropped)
	cache.async.mu.Unlock()

	unblock()
	require.Eventually(t, func() bool {
		cache.async.mu.Lock()
		defer cache.async.mu.Unlock()
		return len(cache.async.slots) == 0 && cache.async.mu.pendingBytes == 0
	}, diskCacheLifecycleTestTimeout, time.Millisecond)
	callbackMu.Lock()
	require.Equal(t, expectedPaths, callbackPaths)
	callbackMu.Unlock()
}

func TestDiskCacheAsyncSetFileReturnsBeforeFinalize(t *testing.T) {
	cache := newLifecycleTestDiskCache(t)
	syncStarted := make(chan struct{})
	releaseSync := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(releaseSync) }) }
	t.Cleanup(unblock)
	cache.fileSync = func(file *os.File) error {
		close(syncStarted)
		<-releaseSync
		return file.Sync()
	}

	readerOpened := false
	require.NoError(t, cache.setFile(
		context.Background(),
		"foo",
		func(context.Context) (io.ReadCloser, error) {
			readerOpened = true
			return io.NopCloser(strings.NewReader("payload")), nil
		},
		true,
	))
	require.True(t, readerOpened)
	select {
	case <-syncStarted:
	case <-time.After(diskCacheLifecycleTestTimeout):
		t.Fatal("async full-file finalizer did not start")
	}

	diskPath := cache.pathForFile("foo")
	_, err := os.Stat(diskPath)
	require.ErrorIs(t, err, os.ErrNotExist)
	require.True(t, cache.isUpdating(diskPath))

	duplicateReaderOpened := false
	require.NoError(t, cache.setFile(
		context.Background(),
		"foo",
		func(context.Context) (io.ReadCloser, error) {
			duplicateReaderOpened = true
			return io.NopCloser(strings.NewReader("duplicate")), nil
		},
		true,
	))
	require.False(t, duplicateReaderOpened)

	unblock()
	flushCtx, cancel := context.WithTimeout(context.Background(), diskCacheLifecycleTestTimeout)
	defer cancel()
	cache.Flush(flushCtx)
	require.NoError(t, flushCtx.Err())
	require.False(t, cache.isUpdating(diskPath))
	data, err := os.ReadFile(diskPath)
	require.NoError(t, err)
	require.Equal(t, []byte("payload"), data)
	require.False(t, cache.writeFailures.failed.Load())
}

func TestDiskCacheAsyncSetFileSkipsSourceWhenFinalizeCapacityIsFull(t *testing.T) {
	cache := newLifecycleTestDiskCache(t)
	for i := 0; i < cap(cache.async.slots); i++ {
		cache.async.slots <- struct{}{}
	}
	t.Cleanup(func() {
		for i := 0; i < cap(cache.async.slots); i++ {
			<-cache.async.slots
		}
	})

	readerOpened := false
	require.NoError(t, cache.setFile(
		context.Background(),
		"foo",
		func(context.Context) (io.ReadCloser, error) {
			readerOpened = true
			return io.NopCloser(strings.NewReader("payload")), nil
		},
		true,
	))
	require.False(t, readerOpened)
	require.False(t, cache.isUpdating(cache.pathForFile("foo")))
	require.Equal(t, int64(1), cache.async.mu.dropped)
}

func TestDiskCacheAsyncSetFileSourceErrorReleasesOwnership(t *testing.T) {
	cache := newLifecycleTestDiskCache(t)
	expectedErr := io.ErrUnexpectedEOF
	err := cache.setFile(
		context.Background(),
		"foo",
		func(context.Context) (io.ReadCloser, error) {
			return nil, expectedErr
		},
		true,
	)
	require.ErrorIs(t, err, expectedErr)
	require.False(t, cache.isUpdating(cache.pathForFile("foo")))
	require.Empty(t, cache.async.slots)
	cache.async.mu.Lock()
	require.Empty(t, cache.async.mu.pending)
	cache.async.mu.Unlock()
	require.False(t, cache.writeFailures.failed.Load())
}

func TestDiskCacheAsyncSetFileFinalizeErrorIsFailOpenAndObservable(t *testing.T) {
	cache := newLifecycleTestDiskCache(t)
	core, logs := observer.New(zap.DebugLevel)
	cache.writeFailures.logger = logutil.NewRateLimitedLoggerWithConfig(
		zap.New(core),
		logutil.RateLimitedLoggerConfig{MaxKeys: 1},
	)
	cache.fileSync = func(*os.File) error { return os.ErrPermission }

	require.NoError(t, cache.setFile(
		context.Background(),
		"foo",
		func(context.Context) (io.ReadCloser, error) {
			return io.NopCloser(strings.NewReader("payload")), nil
		},
		true,
	))
	flushCtx, cancel := context.WithTimeout(context.Background(), diskCacheLifecycleTestTimeout)
	defer cancel()
	cache.Flush(flushCtx)
	require.NoError(t, flushCtx.Err())
	require.Equal(t, 1, logs.FilterMessage("write disk cache error").Len())
	require.True(t, cache.writeFailures.failed.Load())
	require.False(t, cache.isUpdating(cache.pathForFile("foo")))
	require.Empty(t, cache.async.slots)
	_, err := os.Stat(cache.pathForFile("foo"))
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestDiskCacheCloseDrainsQueuedFileFinalize(t *testing.T) {
	cache := newLifecycleTestDiskCache(t)
	syncStarted := make(chan struct{})
	releaseSync := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(releaseSync) }) }
	t.Cleanup(unblock)
	cache.fileSync = func(file *os.File) error {
		close(syncStarted)
		<-releaseSync
		return file.Sync()
	}

	open := func(value string) func(context.Context) (io.ReadCloser, error) {
		return func(context.Context) (io.ReadCloser, error) {
			return io.NopCloser(strings.NewReader(value)), nil
		}
	}
	require.NoError(t, cache.setFile(context.Background(), "first", open("first"), true))
	select {
	case <-syncStarted:
	case <-time.After(diskCacheLifecycleTestTimeout):
		t.Fatal("first full-file finalizer did not start")
	}
	require.NoError(t, cache.setFile(context.Background(), "queued", open("queued"), true))
	queuedPath := cache.pathForFile("queued")
	require.True(t, cache.isUpdating(queuedPath))
	// Canceling a queued finalizer only cleans its temporary artifact; it must
	// not claim that a previous disk-write failure recovered.
	cache.writeFailures.failed.Store(true)

	closeCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	cache.Close(closeCtx)
	require.ErrorIs(t, closeCtx.Err(), context.DeadlineExceeded)
	require.False(t, cache.isUpdating(queuedPath))
	require.True(t, cache.writeFailures.failed.Load())
	_, err := os.Stat(queuedPath)
	require.ErrorIs(t, err, os.ErrNotExist)

	unblock()
	finishCtx, finishCancel := context.WithTimeout(context.Background(), diskCacheLifecycleTestTimeout)
	defer finishCancel()
	cache.Close(finishCtx)
	require.NoError(t, finishCtx.Err())
}

func TestDiskCacheCloseOwnsAsyncLoaderGeneration(t *testing.T) {
	dir := t.TempDir()
	diskPath := filepath.Join(dir, "fullfoo"+cacheFileSuffix)
	require.NoError(t, os.WriteFile(diskPath, []byte("old"), 0o644))

	loaderReachedEviction := make(chan struct{})
	releaseLoader := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(releaseLoader) })
	t.Cleanup(unblock)
	var capacityCalls atomic.Int32
	var signalLoader sync.Once
	capacity := func() int64 {
		if capacityCalls.Add(1) > 1 {
			signalLoader.Do(func() { close(loaderReachedEviction) })
			<-releaseLoader
			return 0
		}
		return 1 << 20
	}

	oldCache, err := NewDiskCache(
		context.Background(),
		dir,
		capacity,
		nil,
		true,
		nil,
		"",
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		unblock()
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			diskCacheLifecycleTestTimeout,
		)
		defer cleanupCancel()
		oldCache.Close(cleanupCtx)
	})
	select {
	case <-loaderReachedEviction:
	case <-time.After(diskCacheLifecycleTestTimeout):
		t.Fatal("async loader did not reach eviction")
	}

	expiredCtx, expiredCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer expiredCancel()
	oldCache.Close(expiredCtx)
	require.ErrorIs(t, expiredCtx.Err(), context.DeadlineExceeded)
	select {
	case <-oldCache.load.done:
		t.Fatal("Close returned before the old loader generation stopped")
	default:
	}

	unblock()
	finishCtx, finishCancel := context.WithTimeout(context.Background(), diskCacheLifecycleTestTimeout)
	defer finishCancel()
	oldCache.Close(finishCtx)
	require.NoError(t, finishCtx.Err())
	select {
	case <-oldCache.load.done:
	default:
		t.Fatal("successful Close did not join the old loader generation")
	}

	require.NoError(t, os.WriteFile(diskPath, []byte("new"), 0o644))
	newCache, err := NewDiskCache(
		context.Background(),
		dir,
		fscache.ConstCapacity(1<<20),
		nil,
		false,
		nil,
		"",
	)
	require.NoError(t, err)
	t.Cleanup(func() { newCache.Close(context.Background()) })
	require.True(t, newCache.cache.Contains(diskPath))
	data, err := os.ReadFile(diskPath)
	require.NoError(t, err)
	require.Equal(t, []byte("new"), data)
}

func TestDiskCacheCloseCancelsActiveFileFinalizeBeforePublish(t *testing.T) {
	cache := newLifecycleTestDiskCache(t)
	syncStarted := make(chan struct{})
	releaseSync := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(releaseSync) })
	t.Cleanup(unblock)
	cache.fileSync = func(file *os.File) error {
		close(syncStarted)
		<-releaseSync
		return file.Sync()
	}

	require.NoError(t, cache.setFile(
		context.Background(),
		"active",
		func(context.Context) (io.ReadCloser, error) {
			return io.NopCloser(strings.NewReader("payload")), nil
		},
		true,
	))
	select {
	case <-syncStarted:
	case <-time.After(diskCacheLifecycleTestTimeout):
		t.Fatal("async full-file finalizer did not start")
	}

	closeCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	cache.Close(closeCtx)
	require.ErrorIs(t, closeCtx.Err(), context.DeadlineExceeded)
	unblock()

	finishCtx, finishCancel := context.WithTimeout(context.Background(), diskCacheLifecycleTestTimeout)
	defer finishCancel()
	cache.Close(finishCtx)
	require.NoError(t, finishCtx.Err())
	diskPath := cache.pathForFile("active")
	require.False(t, cache.isUpdating(diskPath))
	_, err := os.Stat(diskPath)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestDiskCacheCloseWinsAtFilePublicationBoundary(t *testing.T) {
	cache := newLifecycleTestDiskCache(t)
	publicationReached := make(chan struct{})
	releasePublication := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(releasePublication) })
	t.Cleanup(unblock)
	cache.beforeFilePublication = func() {
		close(publicationReached)
		<-releasePublication
	}

	require.NoError(t, cache.setFile(
		context.Background(),
		"boundary",
		func(context.Context) (io.ReadCloser, error) {
			return io.NopCloser(strings.NewReader("payload")), nil
		},
		true,
	))
	select {
	case <-publicationReached:
	case <-time.After(diskCacheLifecycleTestTimeout):
		t.Fatal("async finalizer did not reach the publication boundary")
	}

	closeCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	cache.Close(closeCtx)
	require.ErrorIs(t, closeCtx.Err(), context.DeadlineExceeded)
	unblock()

	finishCtx, finishCancel := context.WithTimeout(context.Background(), diskCacheLifecycleTestTimeout)
	defer finishCancel()
	cache.Close(finishCtx)
	require.NoError(t, finishCtx.Err())
	diskPath := cache.pathForFile("boundary")
	require.False(t, cache.isUpdating(diskPath))
	_, err := os.Stat(diskPath)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestDiskCachePublicationClaimWinsBeforeClose(t *testing.T) {
	cache := newLifecycleTestDiskCache(t)
	diskPath := cache.pathForFile("claimed")
	require.True(t, cache.tryReserveAsyncFileFinalize(diskPath))
	doneUpdate, ok := cache.tryStartUpdateWithCleanup(
		diskPath,
		func() error { return cache.removeUnindexedFile(diskPath) },
	)
	require.True(t, ok)
	tempFile, err := os.CreateTemp(cache.path, "claimed-*"+cacheFileTempSuffix)
	require.NoError(t, err)
	_, err = tempFile.WriteString("payload")
	require.NoError(t, err)

	claimReached := make(chan struct{})
	releaseClaim := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(releaseClaim) })
	t.Cleanup(unblock)
	finalizeDone := make(chan error, 1)
	go func() {
		finalizeErr := cache.finalizeFile(
			cache.async.ctx,
			diskPath,
			tempFile,
			func() bool {
				claimed := cache.claimAsyncPublication()
				close(claimReached)
				<-releaseClaim
				return claimed
			},
		)
		cleanupErr := errors.Join(
			cleanupDiskCacheTempFile(tempFile),
			doneUpdate(),
		)
		cache.releaseAsyncFileFinalizeReservation(diskPath)
		finalizeDone <- errors.Join(finalizeErr, cleanupErr)
	}()
	select {
	case <-claimReached:
	case <-time.After(diskCacheLifecycleTestTimeout):
		t.Fatal("async finalizer did not claim the publication generation")
	}

	closeCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	cache.Close(closeCtx)
	require.ErrorIs(t, closeCtx.Err(), context.DeadlineExceeded)
	unblock()
	select {
	case err := <-finalizeDone:
		require.NoError(t, err)
	case <-time.After(diskCacheLifecycleTestTimeout):
		t.Fatal("claimed publication did not complete")
	}

	finishCtx, finishCancel := context.WithTimeout(context.Background(), diskCacheLifecycleTestTimeout)
	defer finishCancel()
	cache.Close(finishCtx)
	require.NoError(t, finishCtx.Err())
	data, err := os.ReadFile(diskPath)
	require.NoError(t, err)
	require.Equal(t, []byte("payload"), data)
}

func TestDiskCacheCloseTracksReservedFileFinalize(t *testing.T) {
	cache := newLifecycleTestDiskCache(t)
	reader, writer := io.Pipe()
	readerOpened := make(chan struct{})
	closeWriter := sync.OnceFunc(func() { _ = writer.Close() })
	t.Cleanup(closeWriter)
	setFileDone := make(chan error, 1)
	go func() {
		setFileDone <- cache.setFile(
			context.Background(),
			"reserved",
			func(context.Context) (io.ReadCloser, error) {
				close(readerOpened)
				return reader, nil
			},
			true,
		)
	}()
	select {
	case <-readerOpened:
	case <-time.After(diskCacheLifecycleTestTimeout):
		closeWriter()
		<-setFileDone
		t.Fatal("async full-file source reader did not open")
	}

	const numClosers = 4
	closeResults := make(chan error, numClosers)
	for i := 0; i < numClosers; i++ {
		go func() {
			closeCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			cache.Close(closeCtx)
			closeResults <- closeCtx.Err()
		}()
	}
	for i := 0; i < numClosers; i++ {
		select {
		case err := <-closeResults:
			require.ErrorIs(t, err, context.DeadlineExceeded)
		case <-time.After(diskCacheLifecycleTestTimeout):
			t.Fatal("concurrent Close did not honor its deadline")
		}
	}

	_, err := writer.Write([]byte("payload"))
	require.NoError(t, err)
	closeWriter()
	select {
	case err := <-setFileDone:
		require.NoError(t, err)
	case <-time.After(diskCacheLifecycleTestTimeout):
		t.Fatal("async SetFile did not transfer canceled finalizer cleanup")
	}
	finishCtx, finishCancel := context.WithTimeout(context.Background(), diskCacheLifecycleTestTimeout)
	defer finishCancel()
	cache.Close(finishCtx)
	require.NoError(t, finishCtx.Err())
	_, err = os.Stat(cache.pathForFile("reserved"))
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestDiskCacheCloseDeadlineDoesNotWaitForQueuedCleanup(t *testing.T) {
	cache := newLifecycleTestDiskCache(t)
	diskPath := cache.pathForFile("queued")
	cleanupStarted := make(chan struct{})
	releaseCleanup := make(chan struct{})
	unblockCleanup := sync.OnceFunc(func() { close(releaseCleanup) })
	t.Cleanup(unblockCleanup)
	doneUpdate := cache.startUpdateWithCleanup(diskPath, func() error {
		close(cleanupStarted)
		<-releaseCleanup
		return nil
	})

	tempFile, err := os.CreateTemp(cache.path, "close-deadline-*")
	require.NoError(t, err)
	a := &cache.async
	a.mu.Lock()
	a.slots <- struct{}{}
	a.mu.idle = make(chan struct{})
	a.mu.pending[diskPath] = struct{}{}
	a.mu.Unlock()
	a.jobs <- &diskCacheAsyncUpdate{
		diskPath: diskPath,
		finalize: &diskCacheAsyncFileFinalize{
			file:       tempFile,
			doneUpdate: doneUpdate,
		},
	}

	closeCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	closeReturned := make(chan struct{})
	go func() {
		cache.Close(closeCtx)
		close(closeReturned)
	}()

	select {
	case <-cleanupStarted:
	case <-time.After(diskCacheLifecycleTestTimeout):
		unblockCleanup()
		<-closeReturned
		t.Fatal("queued cleanup did not start during close")
	}
	select {
	case <-closeReturned:
		require.ErrorIs(t, closeCtx.Err(), context.DeadlineExceeded)
	case <-time.After(diskCacheLifecycleTestTimeout):
		unblockCleanup()
		<-closeReturned
		t.Fatal("Close exceeded its context deadline while queued cleanup was blocked")
	}
	require.True(t, cache.isUpdating(diskPath))

	unblockCleanup()
	finishCtx, finishCancel := context.WithTimeout(context.Background(), diskCacheLifecycleTestTimeout)
	defer finishCancel()
	cache.Close(finishCtx)
	require.NoError(t, finishCtx.Err())
	require.False(t, cache.isUpdating(diskPath))
	require.Empty(t, cache.async.slots)
	cache.async.mu.Lock()
	require.Empty(t, cache.async.mu.pending)
	cache.async.mu.Unlock()
	_, err = os.Stat(tempFile.Name())
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestDiskCacheAsyncUpdateMemoryIsBounded(t *testing.T) {
	cache := newLifecycleTestDiskCache(t)
	cache.async.mu.Lock()
	cache.async.mu.maxPendingBytes = 1
	cache.async.mu.Unlock()

	release := cache.startUpdate(cache.pathForIOEntry("foo", IOEntry{Offset: 0, Size: 1}))
	require.NoError(t, cache.Update(context.Background(), &IOVector{
		FilePath: "foo",
		Entries:  []IOEntry{{Offset: 0, Size: 1, Data: []byte("x")}},
	}, true))
	require.NoError(t, cache.Update(context.Background(), &IOVector{
		FilePath: "bar",
		Entries:  []IOEntry{{Offset: 0, Size: 1, Data: []byte("y")}},
	}, true))

	cache.async.mu.Lock()
	require.Equal(t, int64(1), cache.async.mu.pendingBytes)
	require.Len(t, cache.async.mu.pending, 1)
	require.Equal(t, int64(1), cache.async.mu.dropped)
	cache.async.mu.Unlock()

	release()
	flushCtx, cancel := context.WithTimeout(context.Background(), diskCacheLifecycleTestTimeout)
	defer cancel()
	cache.Flush(flushCtx)
	require.NoError(t, flushCtx.Err())
	cache.async.mu.Lock()
	require.Zero(t, cache.async.mu.pendingBytes)
	cache.async.mu.Unlock()
}

func TestDiskCacheAsyncUpdateRejectsCanceledCaller(t *testing.T) {
	cache := newLifecycleTestDiskCache(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := cache.Update(ctx, &IOVector{
		FilePath: "foo",
		Entries:  []IOEntry{{Offset: 0, Size: 1, Data: []byte("x")}},
	}, true)
	require.ErrorIs(t, err, context.Canceled)
	cache.async.mu.Lock()
	require.Empty(t, cache.async.mu.pending)
	require.Zero(t, cache.async.mu.pendingBytes)
	cache.async.mu.Unlock()
}

func TestDiskCacheCloseCancelsQueuedUpdateWaits(t *testing.T) {
	cache := newLifecycleTestDiskCache(t)
	release := cache.startUpdate(cache.pathForIOEntry("foo", IOEntry{Offset: 0, Size: 1}))
	require.NoError(t, cache.Update(context.Background(), &IOVector{
		FilePath: "foo",
		Entries:  []IOEntry{{Offset: 0, Size: 1, Data: []byte("x")}},
	}, true))

	closeCtx, cancel := context.WithTimeout(context.Background(), diskCacheLifecycleTestTimeout)
	defer cancel()
	cache.Close(closeCtx)
	require.NoError(t, closeCtx.Err())
	cache.async.mu.Lock()
	require.True(t, cache.async.mu.closed)
	require.Zero(t, cache.async.mu.pendingBytes)
	require.Empty(t, cache.async.mu.pending)
	cache.async.mu.Unlock()
	release()
}

func TestDiskCacheWriteErrorsAndAlternatingRecoveryAreRateLimited(t *testing.T) {
	cache := newLifecycleTestDiskCache(t)
	core, logs := observer.New(zap.DebugLevel)
	cache.writeFailures.logger = logutil.NewRateLimitedLoggerWithConfig(
		zap.New(core),
		logutil.RateLimitedLoggerConfig{MaxKeys: 1},
	)

	require.NoError(t, os.RemoveAll(cache.path))
	require.NoError(t, os.WriteFile(cache.path, []byte("not-a-directory"), 0o644))
	for i := 0; i < 10; i++ {
		require.NoError(t, cache.Update(context.Background(), &IOVector{
			FilePath: "broken",
			Entries:  []IOEntry{{Offset: int64(i), Size: 1, Data: []byte("x")}},
		}, false))
	}
	require.Equal(t, 1, logs.FilterMessage("write disk cache error").Len())
	require.True(t, cache.writeFailures.failed.Load())

	require.NoError(t, os.Remove(cache.path))
	require.NoError(t, os.MkdirAll(cache.path, 0o755))
	require.NoError(t, cache.Update(context.Background(), &IOVector{
		FilePath: "recovered",
		Entries:  []IOEntry{{Offset: 0, Size: 1, Data: []byte("x")}},
	}, false))
	require.Equal(t, 1, logs.FilterMessage("disk cache write recovered").Len())
	require.False(t, cache.writeFailures.failed.Load())

	for i := 0; i < 10; i++ {
		require.NoError(t, os.RemoveAll(cache.path))
		require.NoError(t, os.WriteFile(cache.path, []byte("not-a-directory"), 0o644))
		require.NoError(t, cache.Update(context.Background(), &IOVector{
			FilePath: "broken-again",
			Entries:  []IOEntry{{Offset: int64(i), Size: 1, Data: []byte("x")}},
		}, false))

		require.NoError(t, os.Remove(cache.path))
		require.NoError(t, os.MkdirAll(cache.path, 0o755))
		require.NoError(t, cache.Update(context.Background(), &IOVector{
			FilePath: "recovered-again",
			Entries:  []IOEntry{{Offset: int64(i), Size: 1, Data: []byte("x")}},
		}, false))
	}
	require.Equal(t, 1, logs.FilterMessage("write disk cache error").Len())
	require.Equal(t, 1, logs.FilterMessage("disk cache write recovered").Len())
	require.False(t, cache.writeFailures.failed.Load())
}

func TestDiskCacheCacheHitDoesNotClaimWriteRecovery(t *testing.T) {
	cache := newLifecycleTestDiskCache(t)
	cache.writeFailures.logger = logutil.NewRateLimitedLoggerWithConfig(
		zap.NewNop(),
		logutil.RateLimitedLoggerConfig{MaxKeys: 1},
	)
	ctx := context.Background()
	require.NoError(t, cache.SetFile(ctx, "cached", func(context.Context) (io.ReadCloser, error) {
		return io.NopCloser(strings.NewReader("cached")), nil
	}))

	cache.fileSync = func(*os.File) error { return os.ErrPermission }
	require.NoError(t, cache.SetFile(ctx, "broken", func(context.Context) (io.ReadCloser, error) {
		return io.NopCloser(strings.NewReader("broken")), nil
	}))
	require.True(t, cache.writeFailures.failed.Load())

	readerOpened := false
	require.NoError(t, cache.SetFile(ctx, "cached", func(context.Context) (io.ReadCloser, error) {
		readerOpened = true
		return io.NopCloser(strings.NewReader("duplicate")), nil
	}))
	require.False(t, readerOpened)
	require.True(t, cache.writeFailures.failed.Load())

	cache.fileSync = func(file *os.File) error { return file.Sync() }
	require.NoError(t, cache.SetFile(ctx, "recovered", func(context.Context) (io.ReadCloser, error) {
		return io.NopCloser(strings.NewReader("recovered")), nil
	}))
	require.False(t, cache.writeFailures.failed.Load())
}
