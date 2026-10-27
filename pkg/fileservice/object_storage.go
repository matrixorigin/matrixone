// Copyright 2023 Matrix Origin
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
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"iter"
	"runtime"
	"sync"
	"time"

	"golang.org/x/sync/semaphore"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// objectExists keeps the context and provider-specific not-found handling shared
// by object stores whose stat result is irrelevant to existence.
func objectExists[T any](
	ctx context.Context, key string,
	stat func(context.Context, string) (T, error), isNotFound func(error) bool,
) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	_, err := stat(ctx, key)
	if err != nil {
		if isNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

const smallObjectThreshold = 64 * (1 << 20)
const (
	// defaultParallelMultipartPartSize defines the default per-part size for parallel multipart uploads.
	defaultParallelMultipartPartSize = 64 * (1 << 20)
	// minMultipartPartSize is the minimum allowed part size for S3-compatible multipart uploads.
	minMultipartPartSize = 5 * (1 << 20)
	// maxMultipartPartSize is the maximum allowed part size for S3-compatible multipart uploads.
	maxMultipartPartSize = 5 * (1 << 30)
	// maxMultipartParts is the maximum allowed parts for S3-compatible multipart uploads.
	maxMultipartParts = 10000
)

var (
	parallelUploadSemaphoreOnce sync.Once
	parallelUploadSemaphore     chan struct{}

	parallelUploadBufferBudgetOnce sync.Once
	parallelUploadBufferBudget     *weightedUploadBufferBudget
)

type weightedUploadBufferBudget struct {
	semaphore *semaphore.Weighted
	capacity  int64
}

func getParallelUploadSemaphore() chan struct{} {
	parallelUploadSemaphoreOnce.Do(func() {
		parallelUploadSemaphore = make(chan struct{}, runtime.NumCPU())
	})
	return parallelUploadSemaphore
}

func getParallelUploadBufferBudget() *weightedUploadBufferBudget {
	parallelUploadBufferBudgetOnce.Do(func() {
		capacity := int64(runtime.NumCPU()) * int64(defaultParallelMultipartPartSize)
		if capacity < 1 {
			capacity = 1
		}
		parallelUploadBufferBudget = &weightedUploadBufferBudget{
			semaphore: semaphore.NewWeighted(capacity),
			capacity:  capacity,
		}
	})
	return parallelUploadBufferBudget
}

func acquireParallelUploadBufferBudget(ctx context.Context, bytes int64) (int64, error) {
	budget := getParallelUploadBufferBudget()
	if bytes < 1 {
		bytes = 1
	}
	if bytes > budget.capacity {
		return 0, moerr.NewInvalidInputNoCtxf(
			"multipart part size %d exceeds shared upload buffer budget %d",
			bytes,
			budget.capacity,
		)
	}
	if err := budget.semaphore.Acquire(ctx, bytes); err != nil {
		return 0, err
	}
	return bytes, nil
}

func releaseParallelUploadBufferBudget(bytes int64) {
	if bytes <= 0 {
		return
	}
	getParallelUploadBufferBudget().semaphore.Release(bytes)
}

// parallelUploadPart owns the budget reserved for its buffer until released.
type parallelUploadPart struct {
	buf    []byte
	n      int
	tokens int64
}

func releaseParallelUploadPart(part *parallelUploadPart) {
	if part == nil {
		return
	}
	releaseParallelUploadBufferBudget(part.tokens)
}

func readParallelUploadPart(ctx context.Context, r io.Reader, partSize int64) (*parallelUploadPart, error) {
	tokens, err := acquireParallelUploadBufferBudget(ctx, partSize)
	if err != nil {
		return nil, err
	}
	raw := make([]byte, partSize)
	n, err := io.ReadFull(r, raw)
	switch {
	case errors.Is(err, io.EOF):
		releaseParallelUploadBufferBudget(tokens)
		return nil, io.EOF
	case errors.Is(err, io.ErrUnexpectedEOF):
		return &parallelUploadPart{buf: raw, n: n, tokens: tokens}, io.EOF
	case err != nil:
		releaseParallelUploadBufferBudget(tokens)
		return nil, err
	default:
		return &parallelUploadPart{buf: raw, n: n, tokens: tokens}, nil
	}
}

// uploadParallelParts owns each part until its upload finishes or is rejected.
func uploadParallelParts[T any](ctx context.Context, cancel context.CancelFunc, r io.Reader, options ParallelMultipartOption, firstPart *parallelUploadPart, upload func(context.Context, int32, *parallelUploadPart) (T, error)) ([]T, error) {
	type partJob struct {
		num  int32
		part *parallelUploadPart
	}

	var (
		partNum   int32
		parts     []T
		partsLock sync.Mutex
		wg        sync.WaitGroup
		errOnce   sync.Once
		firstErr  error
	)

	setErr := func(e error) {
		if e == nil {
			return
		}
		errOnce.Do(func() {
			firstErr = e
			cancel()
		})
	}

	uploadSlots := make(chan struct{}, options.Concurrency)
	startPartUpload := func(job partJob) bool {
		select {
		case uploadSlots <- struct{}{}:
		case <-ctx.Done():
			releaseParallelUploadPart(job.part)
			setErr(ctx.Err())
			return false
		}
		select {
		case getParallelUploadSemaphore() <- struct{}{}:
		case <-ctx.Done():
			<-uploadSlots
			releaseParallelUploadPart(job.part)
			setErr(ctx.Err())
			return false
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				<-getParallelUploadSemaphore()
				<-uploadSlots
			}()
			if options.beforePartUpload != nil {
				options.beforePartUpload()
			}
			if ctxErr := ctx.Err(); ctxErr != nil {
				setErr(ctxErr)
				releaseParallelUploadPart(job.part)
				return
			}
			result, uploadErr := upload(ctx, job.num, job.part)
			if uploadErr != nil {
				setErr(uploadErr)
				releaseParallelUploadPart(job.part)
				return
			}
			releaseParallelUploadPart(job.part)
			partsLock.Lock()
			parts = append(parts, result)
			partsLock.Unlock()
		}()
		return true
	}

	sendJob := func(part *parallelUploadPart) bool {
		partNum++
		if partNum > maxMultipartParts {
			setErr(moerr.NewInternalErrorNoCtxf("too many parts for multipart upload: %d", partNum))
			releaseParallelUploadPart(part)
			return false
		}
		job := partJob{
			num:  partNum,
			part: part,
		}
		return startPartUpload(job)
	}

	if sendJob(firstPart) {
		for {
			part, readErr := readParallelUploadPart(ctx, r, options.PartSize)
			if errors.Is(readErr, io.EOF) && part == nil {
				break
			}
			if readErr != nil && !errors.Is(readErr, io.EOF) {
				setErr(readErr)
				releaseParallelUploadPart(part)
				break
			}
			if part == nil || part.n == 0 {
				releaseParallelUploadPart(part)
				break
			}
			if !sendJob(part) {
				break
			}
			if errors.Is(readErr, io.EOF) {
				break
			}
		}
	}

	wg.Wait()

	if firstErr != nil {
		return nil, firstErr
	}
	if len(parts) != int(partNum) {
		return nil, moerr.NewInternalErrorNoCtxf("multipart upload incomplete, expect %d parts got %d", partNum, len(parts))
	}
	return parts, nil
}

func normalizeParallelOption(opt *ParallelMultipartOption) ParallelMultipartOption {
	res := ParallelMultipartOption{}
	if opt != nil {
		res = *opt
	}
	if res.PartSize <= 0 {
		res.PartSize = defaultParallelMultipartPartSize
	}
	if res.PartSize < minMultipartPartSize {
		res.PartSize = minMultipartPartSize
	}
	if res.PartSize > maxMultipartPartSize {
		res.PartSize = maxMultipartPartSize
	}
	if res.Concurrency <= 0 {
		res.Concurrency = runtime.NumCPU()
	}
	if res.Concurrency < 1 {
		res.Concurrency = 1
	}
	return res
}

// readObjectRange applies the object storage half-open byte range to a seekable reader.
// The caller retains responsibility for closing source, including on error.
func readObjectRange(source io.ReadSeeker, key string, min, max *int64) (io.Reader, error) {
	var reader io.Reader
	reader = source

	if min != nil {
		pos, err := source.Seek(*min, io.SeekStart)
		if err != nil {
			return nil, err
		}
		if pos != *min {
			return nil, moerr.NewEmptyRangeNoCtx(key)
		}
	}

	if max != nil {
		limit := *max
		if min != nil {
			limit -= *min
		}
		if limit <= 0 {
			return nil, moerr.NewEmptyRangeNoCtx(key)
		}
		reader = io.LimitReader(reader, limit)
	}
	return reader, nil
}

type ObjectStorage interface {
	// List lists objects with specified prefix
	List(
		ctx context.Context,
		prefix string,
	) iter.Seq2[*DirEntry, error]

	// Stat returns informations about an object
	Stat(
		ctx context.Context,
		key string,
	) (
		size int64,
		err error,
	)

	// Exists reports whether specified object exists
	Exists(
		ctx context.Context,
		key string,
	) (
		bool,
		error,
	)

	// Write writes an object
	Write(
		ctx context.Context,
		key string,
		r io.Reader,
		sizeHint *int64,
		expire *time.Time,
	) (
		err error,
	)

	// Read returns an io.Reader for specified object range
	Read(
		ctx context.Context,
		key string,
		min *int64,
		max *int64,
	) (
		r io.ReadCloser,
		err error,
	)

	// Delete deletes objects
	Delete(
		ctx context.Context,
		keys ...string,
	) (
		err error,
	)
}

// objectStorageIdentityReader is an optional provider capability used by
// format-neutral conditional FileService reads. Implementations must preserve
// expected.VersionID or expected.ETag across every internal retry.
type objectStorageIdentityReader interface {
	StatObjectIdentity(ctx context.Context, key string) (ObjectIdentity, error)
	ReadObjectWithIdentity(
		ctx context.Context,
		key string,
		min *int64,
		max *int64,
		expected ObjectIdentity,
	) (io.ReadCloser, error)
}

// mappedErrorReadCloser keeps provider error normalization outside the
// retryable reader. A conditional GET may succeed initially and then reopen
// after a transport failure; errors from that later reopen are returned by
// Read rather than by the constructor and must obey the same public contract.
type mappedErrorReadCloser struct {
	io.ReadCloser
	mapError func(error) error
}

func (r *mappedErrorReadCloser) Read(buffer []byte) (int, error) {
	n, err := r.ReadCloser.Read(buffer)
	if err != nil {
		err = r.mapError(err)
	}
	return n, err
}

func (r *mappedErrorReadCloser) Close() error {
	err := r.ReadCloser.Close()
	if err != nil {
		err = r.mapError(err)
	}
	return err
}

func mapReadCloserErrors(reader io.ReadCloser, mapError func(error) error) io.ReadCloser {
	return &mappedErrorReadCloser{ReadCloser: reader, mapError: mapError}
}

// objectStorageCopier is implemented by object-store SDK adapters that can
// ask the provider to copy an object without downloading it through CN.
type objectStorageCopier interface {
	CopyObject(
		ctx context.Context,
		src ObjectStorage,
		srcKey string,
		dstKey string,
	) (copied bool, err error)
}

// objectStorageCopyCredentialDomain is an opaque identity for the credentials
// used by an object-store client. Provider-side copies are safe only when the
// destination client can prove it uses the same credentials as the source.
// The digest avoids retaining credential material solely for this comparison.
type objectStorageCopyCredentialDomain struct {
	digest [sha256.Size]byte
	valid  bool
}

func newObjectStorageCopyCredentialDomain(keyID, keySecret string, extras ...string) objectStorageCopyCredentialDomain {
	if keyID == "" || keySecret == "" {
		return objectStorageCopyCredentialDomain{}
	}
	hasher := sha256.New()
	var size [8]byte
	writePart := func(value string) {
		binary.BigEndian.PutUint64(size[:], uint64(len(value)))
		_, _ = hasher.Write(size[:])
		_, _ = hasher.Write([]byte(value))
	}
	writePart(keyID)
	writePart(keySecret)
	for _, extra := range extras {
		writePart(extra)
	}
	var domain objectStorageCopyCredentialDomain
	copy(domain.digest[:], hasher.Sum(nil))
	domain.valid = true
	return domain
}

func (d objectStorageCopyCredentialDomain) matches(other objectStorageCopyCredentialDomain) bool {
	return d.valid && other.valid && d.digest == other.digest
}

// ParallelMultipartWriter is implemented by storages that support parallel multipart uploads.
type ParallelMultipartWriter interface {
	SupportsParallelMultipart() bool
	WriteMultipartParallel(
		ctx context.Context,
		key string,
		r io.Reader,
		sizeHint *int64,
		opt *ParallelMultipartOption,
	) error
}

// ParallelMultipartOption controls part size and parallelism of multipart uploads.
type ParallelMultipartOption struct {
	// PartSize configures each part size; defaults to 64MB if zero.
	PartSize int64
	// Concurrency configures worker count; defaults to runtime.NumCPU() if zero.
	Concurrency int
	// Expire sets object expiration.
	Expire *time.Time
	// beforePartUpload is a test-only hook; do not set it in production.
	// It runs after a worker owns its slots, before checking cancellation.
	beforePartUpload func()
}
