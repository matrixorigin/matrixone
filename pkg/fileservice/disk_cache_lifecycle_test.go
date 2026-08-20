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
	"io"
	"os"
	"strings"
	"sync"
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
