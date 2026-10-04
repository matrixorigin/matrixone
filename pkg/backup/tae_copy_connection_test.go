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
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/stretchr/testify/require"
)

type copyConnectionPool struct {
	permits  chan struct{}
	abort    chan struct{}
	stopOnce sync.Once
}

func newCopyConnectionPool(size int) *copyConnectionPool {
	return &copyConnectionPool{
		permits: make(chan struct{}, size),
		abort:   make(chan struct{}),
	}
}

func (p *copyConnectionPool) acquire(ctx context.Context) error {
	select {
	case p.permits <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-p.abort:
		return context.Canceled
	}
}

func (p *copyConnectionPool) release() {
	<-p.permits
}

func (p *copyConnectionPool) stop() {
	p.stopOnce.Do(func() {
		close(p.abort)
	})
}

type pooledReadFS struct {
	fileservice.FileService
	pool          *copyConnectionPool
	data          []byte
	expectedReads int
	allReads      chan struct{}
	mu            sync.Mutex
	reads         int
	closes        atomic.Int32
}

func (f *pooledReadFS) Read(ctx context.Context, vector *fileservice.IOVector) error {
	if err := f.pool.acquire(ctx); err != nil {
		return err
	}

	f.mu.Lock()
	f.reads++
	if f.reads == f.expectedReads {
		close(f.allReads)
	}
	f.mu.Unlock()

	select {
	case <-f.allReads:
	case <-ctx.Done():
		f.pool.release()
		return ctx.Err()
	case <-f.pool.abort:
		f.pool.release()
		return context.Canceled
	}

	reader := &pooledReadCloser{
		reader: bytes.NewReader(f.data),
		release: sync.OnceFunc(func() {
			f.pool.release()
		}),
		onClose: func() {
			f.closes.Add(1)
		},
	}
	*vector.Entries[0].ReadCloserForRead = reader
	return nil
}

type pooledReadCloser struct {
	reader   *bytes.Reader
	release  func()
	onClose  func()
	closeOne sync.Once
}

func (r *pooledReadCloser) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if err != nil {
		r.release()
	}
	return n, err
}

func (r *pooledReadCloser) Close() error {
	r.closeOne.Do(func() {
		r.release()
		r.onClose()
	})
	return nil
}

type pooledCopyFS struct {
	fileservice.FileService
	pool   *copyConnectionPool
	copies atomic.Int32
}

func (f *pooledCopyFS) Write(ctx context.Context, vector fileservice.IOVector) error {
	if err := f.pool.acquire(ctx); err != nil {
		return err
	}
	defer f.pool.release()
	return f.FileService.Write(ctx, vector)
}

func (f *pooledCopyFS) CopyObject(
	ctx context.Context,
	srcFS fileservice.FileService,
	srcPath string,
	dstPath string,
) (bool, error) {
	if err := f.pool.acquire(ctx); err != nil {
		return false, err
	}
	defer f.pool.release()

	src, ok := srcFS.(*pooledReadFS)
	if !ok {
		return false, nil
	}
	err := f.FileService.Write(ctx, fileservice.IOVector{
		FilePath: dstPath,
		Entries: []fileservice.IOEntry{{
			Offset:         0,
			Size:           int64(len(src.data)),
			ReaderForWrite: bytes.NewReader(src.data),
		}},
	})
	if err != nil {
		return false, err
	}
	f.copies.Add(1)
	return true, nil
}

func TestCopyFileUsesProviderCopyBeforeOpeningSourceReaders(t *testing.T) {
	const workers = 2
	content := []byte("backup object contents")
	pool := newCopyConnectionPool(workers)
	t.Cleanup(pool.stop)

	srcMemory, err := fileservice.NewMemoryFS("src", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	src := &pooledReadFS{
		FileService:   srcMemory,
		pool:          pool,
		data:          content,
		expectedReads: workers,
		allReads:      make(chan struct{}),
	}
	dstMemory, err := fileservice.NewMemoryFS("dst", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	dst := &pooledCopyFS{
		FileService: dstMemory,
		pool:        pool,
	}

	results := make(chan struct {
		checksum []byte
		err      error
	}, workers)
	for i := 0; i < workers; i++ {
		name := string(rune('a' + i))
		go func() {
			checksum, err := CopyFile(t.Context(), src, dst, name, "")
			results <- struct {
				checksum []byte
				err      error
			}{checksum: checksum, err: err}
		}()
	}

	expected := sha256.Sum256(content)
	for i := 0; i < workers; i++ {
		select {
		case result := <-results:
			require.NoError(t, result.err)
			require.Equal(t, expected[:], result.checksum)
		case <-time.After(time.Second):
			pool.stop()
			for j := i; j < workers; j++ {
				select {
				case <-results:
				case <-time.After(time.Second):
				}
			}
			t.Fatal("CopyFile deadlocked while source reads held every shared connection")
		}
	}
	src.mu.Lock()
	require.Zero(t, src.reads)
	src.mu.Unlock()
	require.Zero(t, src.closes.Load())
	require.Equal(t, int32(workers), dst.copies.Load())
}

type countingReadFS struct {
	fileservice.FileService
	reads atomic.Int32
}

func (f *countingReadFS) Read(ctx context.Context, vector *fileservice.IOVector) error {
	f.reads.Add(1)
	return f.FileService.Read(ctx, vector)
}

type rejectingCopyFS struct {
	fileservice.FileService
}

func (f *rejectingCopyFS) CopyObject(
	context.Context,
	fileservice.FileService,
	string,
	string,
) (bool, error) {
	return false, nil
}

type flakyChecksumCopyFS struct {
	fileservice.FileService
	data   []byte
	copies atomic.Int32
	reads  atomic.Int32
}

func (f *flakyChecksumCopyFS) CopyObject(
	ctx context.Context,
	_ fileservice.FileService,
	_ string,
	dstPath string,
) (bool, error) {
	if f.copies.Add(1) > 1 {
		return false, errors.New("provider copy invoked more than once")
	}
	err := f.FileService.Write(ctx, fileservice.IOVector{
		FilePath: dstPath,
		Entries: []fileservice.IOEntry{{
			Offset:         0,
			Size:           int64(len(f.data)),
			ReaderForWrite: bytes.NewReader(f.data),
		}},
	})
	return err == nil, err
}

func (f *flakyChecksumCopyFS) Read(ctx context.Context, vector *fileservice.IOVector) error {
	if f.reads.Add(1) == 1 {
		return io.ErrUnexpectedEOF
	}
	return f.FileService.Read(ctx, vector)
}

func TestCopyFileFallbackReadsSourceOnce(t *testing.T) {
	content := []byte("backup object contents")
	srcMemory, err := fileservice.NewMemoryFS("src", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	require.NoError(t, srcMemory.Write(t.Context(), fileservice.IOVector{
		FilePath: "object",
		Entries: []fileservice.IOEntry{{
			Offset:         0,
			Size:           int64(len(content)),
			ReaderForWrite: bytes.NewReader(content),
		}},
	}))
	src := &countingReadFS{FileService: srcMemory}
	dstMemory, err := fileservice.NewMemoryFS("dst", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	dst := &rejectingCopyFS{FileService: dstMemory}

	checksum, err := CopyFile(t.Context(), src, dst, "object", "")
	require.NoError(t, err)
	expected := sha256.Sum256(content)
	require.Equal(t, expected[:], checksum)
	require.Equal(t, int32(1), src.reads.Load())
}

func TestCopyFileWithRetryDoesNotRepeatSuccessfulProviderCopy(t *testing.T) {
	content := []byte("backup object contents")
	src, err := fileservice.NewMemoryFS("src", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	dstMemory, err := fileservice.NewMemoryFS("dst", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	dst := &flakyChecksumCopyFS{
		FileService: dstMemory,
		data:        content,
	}

	checksum, err := CopyFileWithRetry(t.Context(), src, dst, "object", "")
	require.NoError(t, err)
	expected := sha256.Sum256(content)
	require.Equal(t, expected[:], checksum)
	require.Equal(t, int32(1), dst.copies.Load())
	require.Equal(t, int32(2), dst.reads.Load())
}

var _ io.ReadCloser = new(pooledReadCloser)
var _ fileservice.ObjectCopier = new(pooledCopyFS)
var _ fileservice.ObjectCopier = new(rejectingCopyFS)
var _ fileservice.ObjectCopier = new(flakyChecksumCopyFS)
