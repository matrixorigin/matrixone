// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package fulltext2

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func testPoolKey(id string, size int64, value string) baseFileKey {
	sum := md5.Sum([]byte(value))
	return baseFileKey{db: "db", index: "idx", metadata: "meta", id: id, checksum: hex.EncodeToString(sum[:]), size: size}
}

func testPoolFill(t *testing.T, fills *atomic.Int32, value string) func(context.Context) (*baseFileHandle, error) {
	t.Helper()
	dir := t.TempDir()
	return func(context.Context) (*baseFileHandle, error) {
		fills.Add(1)
		f, err := os.CreateTemp(dir, "ft2-pool")
		if err != nil {
			return nil, err
		}
		if _, err = f.WriteString(value); err != nil {
			_ = f.Close()
			return nil, err
		}
		if err = f.Sync(); err != nil {
			_ = f.Close()
			return nil, err
		}
		if _, err = f.Seek(0, 0); err != nil {
			_ = f.Close()
			return nil, err
		}
		return &baseFileHandle{file: f, path: f.Name()}, nil
	}
}

func TestBaseFilePoolConcurrentFillAndIndependentMaps(t *testing.T) {
	pool := newBaseFilePool(64, 2)
	defer pool.Close()
	var fills atomic.Int32
	key := testPoolKey("s0", 8, "abcdefgh")
	fill := testPoolFill(t, &fills, "abcdefgh")

	const n = 8
	start := make(chan struct{})
	results := make(chan error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			lease, err := pool.acquire(context.Background(), key, fill)
			if err != nil {
				results <- err
				return
			}
			data, err := lease.MapReadOnly()
			// Use the real failed-munmap ownership contract even when a worker
			// fails: Free retains the Segment in the pool until cleanup succeeds.
			seg := &Segment{mmapData: data, mmapRelease: lease.Release, mmapRetryPool: pool}
			defer seg.Free()
			if err == nil && !bytes.Equal([]byte("abcdefgh"), data) {
				err = fmt.Errorf("unexpected mapping contents: %q", data)
			}
			results <- err
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	for err := range results {
		require.NoError(t, err)
	}
	require.Zero(t, pool.deferredCount())
	require.Equal(t, int32(1), fills.Load(), "one FILLING owner must publish one immutable file")

	lease, err := pool.acquire(context.Background(), key, func(context.Context) (*baseFileHandle, error) {
		t.Fatal("a READY hit must not refill")
		return nil, nil
	})
	require.NoError(t, err)
	data, err := lease.MapReadOnly()
	require.NoError(t, err)
	require.Equal(t, []byte("abcdefgh"), data)
	require.NoError(t, munmap(data))
	lease.Release()
}

func TestBaseFilePoolCapacityEvictsOnlyIdleFiles(t *testing.T) {
	pool := newBaseFilePool(8, 1)
	defer pool.Close()
	var fills atomic.Int32

	first, err := pool.acquire(context.Background(), testPoolKey("s0", 8, "12345678"), testPoolFill(t, &fills, "12345678"))
	require.NoError(t, err)
	firstData, err := first.MapReadOnly()
	require.NoError(t, err)
	require.NoError(t, munmap(firstData))
	firstPath := first.entry.handle.path
	first.Release()
	require.FileExists(t, firstPath)

	second, err := pool.acquire(context.Background(), testPoolKey("s1", 8, "abcdefgh"), testPoolFill(t, &fills, "abcdefgh"))
	require.NoError(t, err)
	require.NoError(t, munmap(mustMapLease(t, second)))
	require.NoFileExists(t, firstPath, "only an idle READY file may be evicted")
	second.Release()
	require.Equal(t, int32(2), fills.Load())
}

func TestBaseFilePoolPinnedCapacityIsAnOptimizationMiss(t *testing.T) {
	pool := newBaseFilePool(16, 1)
	defer pool.Close()
	var fills atomic.Int32
	pinned, err := pool.acquire(context.Background(), testPoolKey("pinned", 8, "12345678"), testPoolFill(t, &fills, "12345678"))
	require.NoError(t, err)
	defer pinned.Release()

	_, err = pool.acquire(context.Background(), testPoolKey("blocked", 8, "abcdefgh"), testPoolFill(t, &fills, "abcdefgh"))
	require.ErrorIs(t, err, errBaseFilePoolCapacity)
	require.Equal(t, int32(1), fills.Load(), "a pinned READY file must not be evicted to admit another pooled file")
}

func TestBaseFilePoolRetiredPinnedFileStillConsumesFDQuota(t *testing.T) {
	pool := newBaseFilePool(16, 1)
	defer pool.Close()
	var fills atomic.Int32
	key := testPoolKey("retired", 8, "12345678")
	first, err := pool.acquire(context.Background(), key, testPoolFill(t, &fills, "12345678"))
	require.NoError(t, err)
	data, err := first.MapReadOnly()
	require.NoError(t, err)
	pooledFile := first.entry.handle.file
	_, err = pooledFile.WriteAt([]byte{'X'}, 0)
	require.NoError(t, err)

	var unexpectedRefill atomic.Bool
	second, err := pool.acquire(context.Background(), key, func(context.Context) (*baseFileHandle, error) {
		unexpectedRefill.Store(true)
		return nil, errors.New("unexpected corrupt-hit refill")
	})
	require.NoError(t, err)
	_, err = second.MapReadOnly()
	require.ErrorContains(t, err, "checksum mismatch")
	second.Release()
	require.False(t, unexpectedRefill.Load())

	_, err = pool.acquire(context.Background(), testPoolKey("next", 8, "abcdefgh"), testPoolFill(t, &fills, "abcdefgh"))
	require.ErrorIs(t, err, errBaseFilePoolCapacity, "the retired file remains an open FD while first lease is pinned")
	require.Equal(t, int32(1), fills.Load())

	require.NoError(t, munmap(data))
	first.Release()
	next, err := pool.acquire(context.Background(), testPoolKey("next", 8, "abcdefgh"), testPoolFill(t, &fills, "abcdefgh"))
	require.NoError(t, err, "new FD admission becomes possible after the retired lease closes")
	require.NoError(t, munmap(mustMapLease(t, next)))
	next.Release()
	require.Equal(t, int32(2), fills.Load())
}

func TestBaseFilePoolFailedFillKeepsFDReservationUntilClose(t *testing.T) {
	pool := newBaseFilePool(16, 1)
	defer pool.Close()

	closeEntered := make(chan struct{})
	releaseClose := make(chan struct{})
	var releaseOnce sync.Once
	var workers sync.WaitGroup
	var closeOnce sync.Once
	previousHook := baseFilePoolBeforeCloseHandle
	baseFilePoolBeforeCloseHandle = func() {
		closeOnce.Do(func() { close(closeEntered) })
		<-releaseClose
	}
	t.Cleanup(func() { baseFilePoolBeforeCloseHandle = previousHook })
	// Independent rescue must run before the global hook is restored, including
	// when the capacity assertion or a bounded wait fails.
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(releaseClose) })
		workers.Wait()
	})

	fillDir := t.TempDir()
	failedFill := func(context.Context) (*baseFileHandle, error) {
		f, err := os.CreateTemp(fillDir, "ft2-failed-fill")
		if err != nil {
			return nil, err
		}
		if _, err = f.WriteString("12345678"); err != nil {
			_ = f.Close()
			return nil, err
		}
		if _, err = f.Seek(0, 0); err != nil {
			_ = f.Close()
			return nil, err
		}
		return &baseFileHandle{file: f, path: f.Name()}, errors.New("synthetic fill failure")
	}

	firstResult := make(chan error, 1)
	workers.Add(1)
	go func() {
		defer workers.Done()
		_, err := pool.acquire(context.Background(), testPoolKey("failed", 8, "12345678"), failedFill)
		firstResult <- err
	}()
	select {
	case <-closeEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("failed fill did not enter handle cleanup")
	}

	// The failed fill has left the directory, but its FD is still inside
	// closeHandle.  A second admission must see the reserved slot.
	secondResult := make(chan error, 1)
	fill := testPoolFill(t, new(atomic.Int32), "abcdefgh")
	workers.Add(1)
	go func() {
		defer workers.Done()
		lease, err := pool.acquire(context.Background(), testPoolKey("blocked", 8, "abcdefgh"), fill)
		if lease != nil {
			lease.Release()
		}
		secondResult <- err
	}()
	select {
	case err := <-secondResult:
		require.ErrorIs(t, err, errBaseFilePoolCapacity)
	case <-time.After(2 * time.Second):
		t.Fatal("second admission blocked behind failed handle cleanup")
	}

	releaseOnce.Do(func() { close(releaseClose) })
	select {
	case err := <-firstResult:
		require.Error(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("failed fill did not finish after cleanup was released")
	}

	lease, err := pool.acquire(context.Background(), testPoolKey("after", 8, "abcdefgh"), testPoolFill(t, new(atomic.Int32), "abcdefgh"))
	require.NoError(t, err, "the FD slot is reusable after the failed handle is actually closed")
	data, err := lease.MapReadOnly()
	require.NoError(t, err)
	require.NoError(t, munmap(data))
	lease.Release()
}

func TestBaseFilePoolDeferredValidationMappingConsumesByteQuota(t *testing.T) {
	const size = int64(8)
	pool := newBaseFilePool(size, 2)
	defer pool.Close()
	key := testPoolKey("deferred", size, "abcdefgh")
	var fills atomic.Int32
	original := munmapFn
	t.Cleanup(func() { munmapFn = original })
	munmapFn = func([]byte) error {
		return errors.New("synthetic deferred munmap failure")
	}
	fill := func(context.Context) (*baseFileHandle, error) {
		fills.Add(1)
		f, err := os.CreateTemp(t.TempDir(), "ft2-deferred")
		require.NoError(t, err)
		_, err = f.WriteString("abcdefgh")
		require.NoError(t, err)
		data, err := mmapReadOnly(f)
		require.NoError(t, err)
		return &baseFileHandle{file: f, path: f.Name(), validationData: data}, errors.New("synthetic validation failure")
	}

	_, err := pool.acquire(context.Background(), key, fill)
	require.Error(t, err)
	require.Equal(t, size, pool.deferredBytes, "a failed validation mapping transfers fill reservation into deferred bytes")
	require.Equal(t, int32(1), fills.Load())

	_, err = pool.acquire(context.Background(), testPoolKey("next", size, "12345678"), func(context.Context) (*baseFileHandle, error) {
		t.Fatal("a deferred mapping must consume the entire byte budget")
		return nil, nil
	})
	require.ErrorIs(t, err, errBaseFilePoolCapacity)
	require.Equal(t, int32(1), fills.Load())

	munmapFn = original
	pool.retryDeferred()
	require.Zero(t, pool.deferredBytes)
	require.Zero(t, pool.deferredCount())
	lease, err := pool.acquire(context.Background(), testPoolKey("after-cleanup", size, "abcdefgh"), testPoolFill(t, &fills, "abcdefgh"))
	require.NoError(t, err, "capacity admission must recover after deferred mapping cleanup")
	data, err := lease.MapReadOnly()
	require.NoError(t, err)
	require.NoError(t, munmap(data))
	lease.Release()
	require.Equal(t, int32(2), fills.Load())
}

func mustMapLease(t *testing.T, lease *baseFileLease) []byte {
	t.Helper()
	data, err := lease.MapReadOnly()
	require.NoError(t, err)
	return data
}

func TestBaseFilePoolPinnedFileSurvivesCloseUntilRelease(t *testing.T) {
	pool := newBaseFilePool(8, 1)
	var fills atomic.Int32
	lease, err := pool.acquire(context.Background(), testPoolKey("s0", 8, "12345678"), testPoolFill(t, &fills, "12345678"))
	require.NoError(t, err)
	data, err := lease.MapReadOnly()
	require.NoError(t, err)
	path := lease.entry.handle.path
	pool.Close()
	require.FileExists(t, path, "Close must not remove a file pinned by a live Segment")
	require.NoError(t, munmap(data))
	lease.Release()
	require.Eventually(t, func() bool {
		_, statErr := os.Stat(path)
		return os.IsNotExist(statErr)
	}, time.Second, time.Millisecond)
}

func TestBaseFilePoolClassifiesLeaderCancellationForHealthyWaiter(t *testing.T) {
	pool := newBaseFilePool(64, 1)
	defer pool.Close()
	key := testPoolKey("cancel", 8, "abcdefgh")
	started := make(chan struct{})
	fill := func(ctx context.Context) (*baseFileHandle, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	leaderResult := make(chan error, 1)
	go func() {
		_, err := pool.acquire(leaderCtx, key, fill)
		leaderResult <- err
	}()
	<-started
	waiterResult := make(chan error, 1)
	go func() {
		_, err := pool.acquire(context.Background(), key, fill)
		waiterResult <- err
	}()
	for {
		pool.mu.Lock()
		entry := pool.entries[key]
		ready := entry != nil && entry.waiters == 1
		pool.mu.Unlock()
		if ready {
			break
		}
		runtime.Gosched()
	}
	cancelLeader()
	require.Error(t, <-leaderResult)
	require.ErrorIs(t, <-waiterResult, errBaseFilePoolLeaderCanceled)
}

func TestBaseFilePoolCloseClassifiesCanceledFill(t *testing.T) {
	pool := newBaseFilePool(64, 1)
	key := testPoolKey("close", 8, "abcdefgh")
	started := make(chan struct{})
	fill := func(ctx context.Context) (*baseFileHandle, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	result := make(chan error, 1)
	go func() {
		_, err := pool.acquire(context.Background(), key, fill)
		result <- err
	}()
	<-started
	pool.Close()
	require.ErrorIs(t, <-result, errBaseFilePoolClosed)
}

func TestBaseFilePoolContextCancellationWhileFilling(t *testing.T) {
	pool := newBaseFilePool(8, 1)
	defer pool.Close()
	key := testPoolKey("s0", 8, "12345678")
	started := make(chan struct{})
	finish := make(chan struct{})
	fill := func(context.Context) (*baseFileHandle, error) {
		close(started)
		<-finish
		f, err := os.CreateTemp(t.TempDir(), "ft2-pool")
		if err != nil {
			return nil, err
		}
		_, _ = f.WriteString("12345678")
		_, _ = f.Seek(0, 0)
		return &baseFileHandle{file: f, path: f.Name()}, nil
	}
	ownerDone := make(chan *baseFileLease, 1)
	go func() {
		lease, _ := pool.acquire(context.Background(), key, fill)
		ownerDone <- lease
	}()
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	_, err := pool.acquire(ctx, key, func(context.Context) (*baseFileHandle, error) {
		t.Fatal("the waiter must not become a second filler")
		return nil, nil
	})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	close(finish)
	owner := <-ownerDone
	require.NotNil(t, owner)
	owner.Release()
}

func TestBaseFilePoolFillFailurePropagatesToExistingWaiters(t *testing.T) {
	pool := newBaseFilePool(8, 1)
	defer pool.Close()
	key := testPoolKey("failed", 8, "12345678")
	started := make(chan struct{})
	finish := make(chan struct{})
	fillErr := errors.New("synthetic immutable base fill failure")
	fill := func(context.Context) (*baseFileHandle, error) {
		close(started)
		<-finish
		return nil, fillErr
	}
	ownerDone := make(chan error, 1)
	go func() {
		_, err := pool.acquire(context.Background(), key, fill)
		ownerDone <- err
	}()
	<-started

	var unexpectedFill atomic.Bool
	waiterDone := make(chan error, 1)
	go func() {
		_, err := pool.acquire(context.Background(), key, func(context.Context) (*baseFileHandle, error) {
			unexpectedFill.Store(true)
			return nil, errors.New("unexpected second fill")
		})
		waiterDone <- err
	}()
	require.Eventually(t, func() bool {
		pool.mu.Lock()
		defer pool.mu.Unlock()
		entry := pool.entries[key]
		return entry != nil && entry.waiters == 1
	}, time.Second, time.Millisecond)
	close(finish)
	require.ErrorIs(t, <-ownerDone, fillErr)
	require.ErrorIs(t, <-waiterDone, fillErr)
	require.False(t, unexpectedFill.Load(), "an existing waiter must not become a second filler")

	var fills atomic.Int32
	retry, err := pool.acquire(context.Background(), key, testPoolFill(t, &fills, "12345678"))
	require.NoError(t, err, "a later independent request may retry after the failed generation")
	retry.Release()
	require.Equal(t, int32(1), fills.Load())
}
