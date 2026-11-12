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
	"encoding/hex"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
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

// emptyObjectCopyFS simulates a provider-side copy whose checksum read
// surfaces an object-storage read-open failure with no reader, exactly like
// the MinIO SDK does for a zero-length object, over a real backing FS. A
// repeated provider copy fails loudly instead of silently repeating work.
type emptyObjectCopyFS struct {
	fileservice.FileService
	content  []byte
	readErr  error
	statErrs []error
	onStat   func()
	copies   atomic.Int32
	reads    atomic.Int32
	stats    atomic.Int32
}

func (f *emptyObjectCopyFS) CopyObject(
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
			Size:           int64(len(f.content)),
			ReaderForWrite: bytes.NewReader(f.content),
		}},
	})
	return err == nil, err
}

func (f *emptyObjectCopyFS) Read(ctx context.Context, vector *fileservice.IOVector) error {
	f.reads.Add(1)
	if f.readErr != nil {
		// The read-open failure returns before any reader pointer is filled.
		return f.readErr
	}
	return f.FileService.Read(ctx, vector)
}

func (f *emptyObjectCopyFS) StatFile(ctx context.Context, name string) (*fileservice.DirEntry, error) {
	attempt := f.stats.Add(1)
	if f.onStat != nil {
		f.onStat()
	}
	if len(f.statErrs) > 0 {
		idx := int(attempt) - 1
		if idx >= len(f.statErrs) {
			idx = len(f.statErrs) - 1
		}
		if statErr := f.statErrs[idx]; statErr != nil {
			return nil, statErr
		}
	}
	return f.FileService.StatFile(ctx, name)
}

func newProviderCopyEndpoints(
	t *testing.T,
	content []byte,
	readErr error,
	statErrs ...error,
) (*fileservice.MemoryFS, *emptyObjectCopyFS, *fileservice.MemoryFS) {
	t.Helper()
	src, err := fileservice.NewMemoryFS("src", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	backing, err := fileservice.NewMemoryFS("dst", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	dst := &emptyObjectCopyFS{
		FileService: backing,
		content:     content,
		readErr:     readErr,
		statErrs:    statErrs,
	}
	return src, dst, backing
}

// TestCopyFileProviderEmptyObject proves the public no-retry copy root
// accepts a provider-copied zero-length object whose checksum read surfaces
// the SDK read-EOF failure instead of a reader, that the real target keeps
// its stat size of zero, and that the result survives the real backup
// manifest writer plus the package-local sidecar-verified readback helper.
func TestCopyFileProviderEmptyObject(t *testing.T) {
	src, dst, backing := newProviderCopyEndpoints(t, nil, io.EOF)
	checksum, err := CopyFile(t.Context(), src, dst, "object", "")
	require.NoError(t, err)
	require.Equal(t, emptyContentSHA256Hex, hex.EncodeToString(checksum))
	require.EqualValues(t, 1, dst.copies.Load())
	require.EqualValues(t, 1, dst.reads.Load())
	require.EqualValues(t, 1, dst.stats.Load())
	entry, err := backing.StatFile(t.Context(), "object")
	require.NoError(t, err)
	require.EqualValues(t, 0, entry.Size)

	// Real metadata consumer: the public empty-object result must survive the
	// actual backup manifest writer plus the package-local sidecar-verified
	// readback helper, not only a sum comparison. This pins the manifest
	// contract for a size-0 entry; it does not claim an empty plain object
	// is a restorable empty TAE object.
	meta, err := fileservice.NewMemoryFS("meta", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	const (
		backupTime = "2026-11-09 18:00:00"
		backupTS   = "1794230630262503021-0"
		backupTyp  = "full"
	)
	file := &taeFile{
		path:     "object",
		size:     entry.Size,
		checksum: checksum,
		needCopy: true,
		ts:       types.BuildTS(1794230630262503021, 0),
	}
	require.NoError(t, saveTaeFilesList(t.Context(), meta, []*taeFile{file}, backupTime, backupTS, backupTyp))
	listData, err := readFileAndCheck(t.Context(), meta, taeList)
	require.NoError(t, err)
	rows, err := fromCsvBytes(listData)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, []string{"object", "0", emptyContentSHA256Hex, "true", backupTS}, rows[0])
	sumData, err := readFileAndCheck(t.Context(), meta, taeSum)
	require.NoError(t, err)
	sumRows, err := fromCsvBytes(sumData)
	require.NoError(t, err)
	require.Len(t, sumRows, 1)
	require.Equal(t, []string{backupTime, "0", backupTS, backupTyp}, sumRows[0])
	// The .sha256 sidecar stays the raw 32-byte digest, not a hex string.
	saved, err := readFile(t.Context(), meta, taeList+".sha256")
	require.NoError(t, err)
	require.Len(t, saved, 32)
	digest := sha256.Sum256(listData)
	require.Equal(t, digest[:], saved)
}

// TestCopyFileWithRetryProviderEmptyObject is the retrying public root for
// the same empty target: a success on the first checksum phase leaves the
// provider copy at exactly one call.
func TestCopyFileWithRetryProviderEmptyObject(t *testing.T) {
	src, dst, _ := newProviderCopyEndpoints(t, nil, io.EOF)
	checksum, err := CopyFileWithRetry(t.Context(), src, dst, "object", "")
	require.NoError(t, err)
	require.Equal(t, emptyContentSHA256Hex, hex.EncodeToString(checksum))
	require.EqualValues(t, 1, dst.copies.Load())
	require.EqualValues(t, 1, dst.reads.Load())
	require.EqualValues(t, 1, dst.stats.Load())
}

func TestCopyFileWithRetryProviderOneByteObject(t *testing.T) {
	content := []byte("x")
	src, dst, backing := newProviderCopyEndpoints(t, content, nil)
	checksum, err := CopyFileWithRetry(t.Context(), src, dst, "object", "")
	require.NoError(t, err)
	want := sha256.Sum256(content)
	require.Equal(t, want[:], checksum)
	data, err := readFile(t.Context(), backing, "object")
	require.NoError(t, err)
	require.Equal(t, content, data)
	require.EqualValues(t, 1, dst.copies.Load())
}

// TestCopyFileWithRetryEOFWithNonEmptyTarget keeps the original failure for
// an EOF on a non-empty provider target, including the fact that the
// provider-written bytes are not rolled back.
func TestCopyFileWithRetryEOFWithNonEmptyTarget(t *testing.T) {
	content := []byte("data")
	src, dst, backing := newProviderCopyEndpoints(t, content, io.EOF)
	_, err := CopyFileWithRetry(t.Context(), src, dst, "object", "")
	require.ErrorIs(t, err, io.EOF)
	require.EqualValues(t, 1, dst.copies.Load())
	require.EqualValues(t, 1, dst.reads.Load())
	require.EqualValues(t, 1, dst.stats.Load())
	// The provider-written target exists; a checksum failure keeps it.
	data, err := readFile(t.Context(), backing, "object")
	require.NoError(t, err)
	require.Equal(t, content, data)
}

func TestCopyFileWithRetryProviderStatFailures(t *testing.T) {
	t.Run("non_retryable_error_surfaces", func(t *testing.T) {
		src, dst, _ := newProviderCopyEndpoints(t, nil, io.EOF,
			errors.New("head not answerable"))
		_, err := CopyFileWithRetry(t.Context(), src, dst, "object", "")
		require.ErrorContains(t, err, "head not answerable")
		require.EqualValues(t, 1, dst.copies.Load())
		require.EqualValues(t, 1, dst.reads.Load())
		require.EqualValues(t, 1, dst.stats.Load())
	})
	t.Run("retryable_error_rechecks_checksum_only", func(t *testing.T) {
		timeout := errors.New("i/o timeout")
		src, dst, _ := newProviderCopyEndpoints(t, nil, io.EOF, timeout, timeout, nil)
		checksum, err := CopyFileWithRetry(t.Context(), src, dst, "object", "")
		require.NoError(t, err)
		require.Equal(t, emptyContentSHA256Hex, hex.EncodeToString(checksum))
		// Two transient stats re-run only the checksum read and stat.
		require.EqualValues(t, 1, dst.copies.Load())
		require.EqualValues(t, 3, dst.reads.Load())
		require.EqualValues(t, 3, dst.stats.Load())
	})
	t.Run("canceled_error_keeps_causality", func(t *testing.T) {
		src, dst, _ := newProviderCopyEndpoints(t, nil, io.EOF, context.Canceled)
		_, err := CopyFileWithRetry(t.Context(), src, dst, "object", "")
		require.ErrorIs(t, err, context.Canceled)
		require.EqualValues(t, 1, dst.copies.Load())
	})
	t.Run("canceled_in_stat_stops_after_first_attempt", func(t *testing.T) {
		// The cancellation is observed inside the stat of the first
		// checksum attempt, which stops the phase at that attempt. This
		// does not wait out a live backoff timer and claims no timer-wait
		// coverage.
		ctx, cancel := context.WithCancel(t.Context())
		src, dst, _ := newProviderCopyEndpoints(t, nil, io.EOF,
			errors.New("i/o timeout"))
		dst.onStat = cancel
		_, err := CopyFileWithRetry(ctx, src, dst, "object", "")
		require.ErrorIs(t, err, context.Canceled)
		// The provider copy already succeeded; only the checksum phase ran.
		require.EqualValues(t, 1, dst.copies.Load())
		require.EqualValues(t, 1, dst.reads.Load())
		require.EqualValues(t, 1, dst.stats.Load())
	})
	t.Run("canceled_with_nonzero_size_keeps_causality", func(t *testing.T) {
		// A stat that succeeds with a nonzero size while its ctx gets
		// canceled must return the cancellation, not the read-EOF
		// substitute failure, and must never repeat the provider copy.
		ctx, cancel := context.WithCancel(t.Context())
		content := []byte("data")
		src, dst, _ := newProviderCopyEndpoints(t, content, io.EOF)
		dst.onStat = cancel
		_, err := CopyFileWithRetry(ctx, src, dst, "object", "")
		require.ErrorIs(t, err, context.Canceled)
		require.NotErrorIs(t, err, io.EOF)
		require.EqualValues(t, 1, dst.copies.Load())
		require.EqualValues(t, 1, dst.reads.Load())
		require.EqualValues(t, 1, dst.stats.Load())
	})
}

var _ fileservice.ObjectCopier = new(emptyObjectCopyFS)
