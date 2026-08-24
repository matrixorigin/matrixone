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

package backup

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/stretchr/testify/require"
)

func newBackupObject(idByte byte) (string, *objectio.BackupObject) {
	id := types.Uuid{idByte}
	name := objectio.BuildObjectName(&id, 0)
	location := objectio.BuildLocation(
		name,
		objectio.NewExtent(0, 0, 1, 1),
		0,
		0,
	)
	return name.String(), &objectio.BackupObject{
		Location: location,
		NeedCopy: true,
	}
}

type cancelBlockingReadFS struct {
	fileservice.FileService
	entered chan struct{}
	once    sync.Once
}

func (f *cancelBlockingReadFS) Read(ctx context.Context, _ *fileservice.IOVector) error {
	f.once.Do(func() {
		close(f.entered)
	})
	<-ctx.Done()
	return ctx.Err()
}

func TestParallelCopyDataHonorsCallerCancellation(t *testing.T) {
	srcMemory, err := fileservice.NewMemoryFS("src", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	src := &cancelBlockingReadFS{
		FileService: srcMemory,
		entered:     make(chan struct{}),
	}
	dst, err := fileservice.NewMemoryFS("dst", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)

	name, backupObject := newBackupObject(1)
	files := map[string]*objectio.BackupObject{name: backupObject}
	ctx, cancel := context.WithCancel(t.Context())
	result := make(chan error, 1)
	go func() {
		_, err := parallelCopyData(ctx, src, dst, files, 1, nil)
		result <- err
	}()

	select {
	case <-src.entered:
	case <-time.After(time.Second):
		t.Fatal("copy did not reach the source read")
	}
	cancel()

	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("parallelCopyData did not return after caller cancellation")
	}
}

type retryingReadFS struct {
	fileservice.FileService
	entered chan struct{}
	once    sync.Once
}

func (f *retryingReadFS) Read(context.Context, *fileservice.IOVector) error {
	f.once.Do(func() {
		close(f.entered)
	})
	return errors.New("connection reset by peer")
}

func TestCopyFileWithRetryStopsDuringBackoff(t *testing.T) {
	srcMemory, err := fileservice.NewMemoryFS("src", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	src := &retryingReadFS{
		FileService: srcMemory,
		entered:     make(chan struct{}),
	}
	dst, err := fileservice.NewMemoryFS("dst", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	result := make(chan error, 1)
	go func() {
		_, err := CopyFileWithRetry(ctx, src, dst, "object", "")
		result <- err
	}()

	select {
	case <-src.entered:
	case <-time.After(time.Second):
		t.Fatal("copy did not make its first attempt")
	}
	cancel()

	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("CopyFileWithRetry did not stop during retry backoff")
	}
}

func TestParallelCopyDataReturnsMissingSourceObject(t *testing.T) {
	src, err := fileservice.NewMemoryFS("src", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	dst, err := fileservice.NewMemoryFS("dst", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)

	name, backupObject := newBackupObject(1)
	files := map[string]*objectio.BackupObject{
		name: backupObject,
	}

	copied, err := parallelCopyData(t.Context(), src, dst, files, 1, nil)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound), err)
	require.Empty(t, copied)
}

type failFirstReadFS struct {
	fileservice.FileService
	failure error
	mu      sync.Mutex
	reads   int
	block   chan struct{}
}

func (f *failFirstReadFS) Read(ctx context.Context, vector *fileservice.IOVector) error {
	f.mu.Lock()
	f.reads++
	read := f.reads
	f.mu.Unlock()
	if read == 1 {
		return f.failure
	}
	select {
	case <-f.block:
		return errors.New("unexpected admitted copy")
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestParallelCopyDataStopsAdmissionAfterFailure(t *testing.T) {
	srcMemory, err := fileservice.NewMemoryFS("src", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	failure := errors.New("copy failed")
	src := &failFirstReadFS{
		FileService: srcMemory,
		failure:     failure,
		block:       make(chan struct{}),
	}
	dst, err := fileservice.NewMemoryFS("dst", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	firstName, firstObject := newBackupObject(1)
	secondName, secondObject := newBackupObject(2)
	files := map[string]*objectio.BackupObject{
		firstName:  firstObject,
		secondName: secondObject,
	}

	result := make(chan error, 1)
	go func() {
		_, err := parallelCopyData(t.Context(), src, dst, files, 1, nil)
		result <- err
	}()

	select {
	case err := <-result:
		require.ErrorIs(t, err, failure)
	case <-time.After(time.Second):
		close(src.block)
		<-result
		t.Fatal("parallelCopyData admitted another copy after a permanent failure")
	}
	src.mu.Lock()
	defer src.mu.Unlock()
	require.Equal(t, 1, src.reads)
}

type failAndBlockReadFS struct {
	fileservice.FileService
	failure    error
	allEntered chan struct{}
	release    chan struct{}
	mu         sync.Mutex
	entered    int
}

func (f *failAndBlockReadFS) Read(context.Context, *fileservice.IOVector) error {
	f.mu.Lock()
	f.entered++
	read := f.entered
	if read == 2 {
		close(f.allEntered)
	}
	f.mu.Unlock()
	<-f.allEntered
	if read == 1 {
		return f.failure
	}
	<-f.release
	return errors.New("sibling released")
}

func TestParallelCopyDataWaitsForScheduledSiblingAfterFailure(t *testing.T) {
	srcMemory, err := fileservice.NewMemoryFS("src", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	failure := errors.New("copy failed")
	src := &failAndBlockReadFS{
		FileService: srcMemory,
		failure:     failure,
		allEntered:  make(chan struct{}),
		release:     make(chan struct{}),
	}
	dst, err := fileservice.NewMemoryFS("dst", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	firstName, firstObject := newBackupObject(1)
	secondName, secondObject := newBackupObject(2)
	files := map[string]*objectio.BackupObject{
		firstName:  firstObject,
		secondName: secondObject,
	}

	result := make(chan error, 1)
	go func() {
		_, err := parallelCopyData(t.Context(), src, dst, files, 2, nil)
		result <- err
	}()
	<-src.allEntered

	select {
	case err := <-result:
		close(src.release)
		t.Fatalf("parallelCopyData returned before its scheduled sibling: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(src.release)
	select {
	case err := <-result:
		require.ErrorIs(t, err, failure)
	case <-time.After(time.Second):
		t.Fatal("parallelCopyData did not return after its scheduled sibling completed")
	}
}
