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
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"iter"
	"os"
	"runtime"
	"slices"
	"sync"
	"time"

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

// deleteObjectKeys preserves single-object and provider-specific batch paths.
// A zero batch limit leaves batching to the provider.
func deleteObjectKeys[T any](
	ctx context.Context, keys []string, batchLimit int,
	deleteSingle func(context.Context, string) error,
	deleteBatch func(context.Context, ...string) (T, error),
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(keys) == 0 {
		return nil
	}
	if len(keys) == 1 {
		return deleteSingle(ctx, keys[0])
	}
	if batchLimit <= 0 {
		batchLimit = len(keys)
	}
	for start := 0; start < len(keys); start += batchLimit {
		end := min(start+batchLimit, len(keys))
		if _, err := deleteBatch(ctx, keys[start:end]...); err != nil {
			return err
		}
	}
	return nil
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

// partBuffers keeps released part buffers for reuse, a pool for each part size.
var partBuffers sync.Map // int64 -> *sync.Pool

// partBufferCharge returns the bytes of the buffer partBuffer returns.
func partBufferCharge(size, partSize int64) int64 {
	if size != partSize {
		return flightBufSize(size)
	}
	return size
}

// partBuffer returns a buffer of size bytes, for a part of partSize bytes: from the pool
// of partSize when size is partSize, otherwise from the flight buffers, and the function
// keeping it for reuse.
func partBuffer(size, partSize int64) ([]byte, func([]byte)) {
	if size != partSize {
		return getFlightBuf(size), putFlightBuf
	}
	p, _ := partBuffers.LoadOrStore(size, new(sync.Pool))
	pool := p.(*sync.Pool)
	put := func(b []byte) {
		b = b[:cap(b)]
		pool.Put(&b)
	}
	if b, ok := pool.Get().(*[]byte); ok {
		return *b, put
	}
	return make([]byte, size), put
}

// writePart is a part of a write read from the caller's reader: n bytes in buf, taken of
// the memory of its writer, or in an unlinked temporary file.
type writePart struct {
	buf    []byte
	put    func([]byte)
	file   *os.File
	n      int64
	w      partWriter
	class  requestClass
	charge int64
}

// body returns the bytes of p.
func (p *writePart) body() io.ReaderAt {
	if p.file != nil {
		return p.file
	}
	return bytes.NewReader(p.buf[:p.n])
}

// release frees the buffer or the file of p.
func (p *writePart) release() {
	if p == nil {
		return
	}
	if p.file != nil {
		_ = p.file.Close()
		p.file = nil
	}
	if p.buf != nil {
		p.put(p.buf)
		p.buf = nil
		p.w.freeMemory(p.charge, p.class)
	}
}

// readWritePart reads at most size bytes of r into a part of a write in parts of partSize
// bytes: into a buffer when the memory of w fits it now, otherwise into an unlinked
// temporary file. It returns io.EOF, with the part, when r ends within it.
func readWritePart(ctx context.Context, w partWriter, r io.Reader, size, partSize int64) (*writePart, error) {
	p := &writePart{w: w, class: requestClassOf(ctx)}
	var err error
	if charge := partBufferCharge(size, partSize); w.takeMemory(charge, p.class) {
		p.charge = charge
		p.buf, p.put = partBuffer(size, partSize)
		var n int
		n, err = io.ReadFull(r, p.buf)
		p.n = int64(n)
		if errors.Is(err, io.ErrUnexpectedEOF) {
			err = io.EOF
		}
	} else {
		if p.file, err = createUnlinkedTemp("mo-fs-part-*"); err != nil {
			return nil, err
		}
		p.n, err = io.CopyN(p.file, r, size)
	}
	if err != nil && !errors.Is(err, io.EOF) {
		p.release()
		return nil, err
	}
	return p, err
}

// createUnlinkedTemp returns a temporary file of the process, already removed.
func createUnlinkedTemp(pattern string) (*os.File, error) {
	f, err := os.CreateTemp("", pattern)
	if err != nil {
		return nil, err
	}
	if err := os.Remove(f.Name()); err != nil {
		_ = f.Close()
		return nil, err
	}
	return f, nil
}

// uploadParts uploads the parts next returns, part by part, at most options.Concurrency at
// once, each in its own request, and returns them in order. next returns nil after the
// last part; its error, or a failed upload, cancels the parts under way.
func uploadParts(ctx context.Context, w partWriter, key, uploadID string, options ParallelMultipartOption, next func() (io.ReaderAt, int64, func(), error)) ([]multipartPart, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var (
		parts    []multipartPart
		mu       sync.Mutex
		wg       sync.WaitGroup
		firstErr error
		num      int32
	)
	setErr := func(err error) {
		mu.Lock()
		if firstErr == nil {
			firstErr = err
			cancel()
		}
		mu.Unlock()
	}
	slots := make(chan struct{}, options.Concurrency)
	for {
		body, size, release, err := next()
		if err != nil {
			setErr(err)
			break
		}
		if body == nil {
			break
		}
		if num++; num > maxMultipartParts {
			release()
			setErr(moerr.NewInternalErrorNoCtxf("too many parts for multipart upload: %d", num))
			break
		}
		select {
		case slots <- struct{}{}:
		case <-ctx.Done():
		}
		if err := ctx.Err(); err != nil {
			release()
			setErr(err)
			break
		}
		wg.Add(1)
		go func(num int32) {
			defer wg.Done()
			defer func() { <-slots }()
			defer release()
			if options.beforePartUpload != nil {
				options.beforePartUpload()
			}
			if err := ctx.Err(); err != nil {
				setErr(err)
				return
			}
			res, err := w.Do(ctx, uploadPartRequest{key: key, uploadID: uploadID, num: num, body: body, size: size})
			if err != nil {
				setErr(err)
				return
			}
			mu.Lock()
			parts = append(parts, multipartPart{num: num, etag: res.id})
			mu.Unlock()
		}(num)
	}
	wg.Wait()
	if firstErr != nil {
		return nil, firstErr
	}
	if len(parts) != int(num) {
		return nil, moerr.NewInternalErrorNoCtxf("multipart upload incomplete, expect %d parts got %d", num, len(parts))
	}
	slices.SortFunc(parts, func(a, b multipartPart) int { return cmp.Compare(a.num, b.num) })
	return parts, nil
}

// multipartStorage is object storage that writes an object in parts; each call is one
// request.
type multipartStorage interface {
	createMultipart(ctx context.Context, key string, expire *time.Time) (uploadID string, err error)
	uploadPart(ctx context.Context, key, uploadID string, num int32, body io.ReaderAt, size int64) (etag string, err error)
	completeMultipart(ctx context.Context, key, uploadID string, parts []multipartPart) error
	abortMultipart(ctx context.Context, key, uploadID string) error
}

type multipartPart struct {
	num  int32
	etag string
}

// partWriter makes the requests of a write in parts and holds the memory of the parts it
// reads from callers.
type partWriter interface {
	Do(ctx context.Context, req request) (result, error)
	takeMemory(n int64, c requestClass) bool
	freeMemory(n int64, c requestClass)
}

// storageParts makes the requests of a write in parts on s itself; the parts it reads
// are always held in memory.
type storageParts struct {
	s interface {
		ObjectStorage
		multipartStorage
	}
}

func (p storageParts) Do(ctx context.Context, req request) (result, error) {
	return makeRequest(ctx, p.s, req)
}

func (storageParts) takeMemory(int64, requestClass) bool { return true }

func (storageParts) freeMemory(int64, requestClass) {}

const multipartAbortTimeout = 30 * time.Second

// multipartUpload creates a multipart upload of key, uploads the parts next returns, and
// completes it, or aborts it on failure.
func multipartUpload(ctx context.Context, w partWriter, key string, options ParallelMultipartOption, next func() (io.ReaderAt, int64, func(), error)) (err error) {
	created, err := w.Do(ctx, createMultipartRequest{key: key, expire: options.Expire})
	if err != nil {
		return err
	}
	uploadID := created.id
	defer func() {
		if err != nil {
			abortCtx, abortCancel := context.WithTimeout(context.WithoutCancel(ctx), multipartAbortTimeout)
			defer abortCancel()
			_, abortErr := w.Do(abortCtx, abortMultipartRequest{key: key, uploadID: uploadID})
			err = errors.Join(err, abortErr)
		}
	}()
	parts, err := uploadParts(ctx, w, key, uploadID, options, next)
	if err != nil {
		return err
	}
	_, err = w.Do(ctx, completeMultipartRequest{key: key, uploadID: uploadID, parts: parts})
	return err
}

// writeParts writes r as the object key: with one put when it ends within its first part,
// otherwise as a multipart upload. Each part is read from r before its request is made,
// into memory of w when it fits at once, otherwise into a temporary file.
func writeParts(ctx context.Context, w partWriter, key string, r io.Reader, sizeHint *int64, opt *ParallelMultipartOption) (err error) {
	defer wrapSizeMismatchErr(&err)
	options := normalizeParallelOption(opt)
	firstSize := options.PartSize
	if sizeHint != nil {
		r = &exactSizeReader{R: r, Expected: *sizeHint, Key: key}
		if parts := (*sizeHint + options.PartSize - 1) / options.PartSize; parts > maxMultipartParts {
			return moerr.NewInternalErrorNoCtxf("too many parts for multipart upload: %d", parts)
		}
		firstSize = min(*sizeHint, options.PartSize)
	}
	first, err := readWritePart(ctx, w, r, firstSize, options.PartSize)
	defer func() { first.release() }()
	if err == nil && sizeHint != nil && *sizeHint <= options.PartSize {
		// The reader ends within the part: a longer one fails here.
		if _, err = r.Read(make([]byte, 1)); err == nil || errors.Is(err, io.EOF) {
			err = io.EOF
		}
	}
	if errors.Is(err, io.EOF) {
		_, err = w.Do(ctx, putRequest{key: key, body: first.body(), size: first.n, expire: options.Expire})
		return err
	}
	if err != nil {
		return err
	}
	ended := false
	return multipartUpload(ctx, w, key, options, func() (io.ReaderAt, int64, func(), error) {
		if first != nil {
			p := first
			first = nil
			return p.body(), p.n, p.release, nil
		}
		if ended {
			return nil, 0, nil, nil
		}
		p, err := readWritePart(ctx, w, r, options.PartSize, options.PartSize)
		if errors.Is(err, io.EOF) {
			ended = true
			if p.n == 0 {
				p.release()
				return nil, 0, nil, nil
			}
			err = nil
		}
		if err != nil {
			return nil, 0, nil, err
		}
		return p.body(), p.n, p.release, nil
	})
}

// writeBytes writes the size bytes of body, which are in memory, as the object key: with
// one put when they fit a part, otherwise as a multipart upload of sections of body. It
// copies no bytes and takes no memory of w.
func writeBytes(ctx context.Context, w partWriter, key string, body io.ReaderAt, size int64, opt *ParallelMultipartOption) error {
	options := normalizeParallelOption(opt)
	if size <= options.PartSize {
		_, err := w.Do(ctx, putRequest{key: key, body: body, size: size, expire: options.Expire})
		return err
	}
	if parts := (size + options.PartSize - 1) / options.PartSize; parts > maxMultipartParts {
		return moerr.NewInternalErrorNoCtxf("too many parts for multipart upload: %d", parts)
	}
	var off int64
	return multipartUpload(ctx, w, key, options, func() (io.ReaderAt, int64, func(), error) {
		if off >= size {
			return nil, 0, nil, nil
		}
		n := min(options.PartSize, size-off)
		section := io.NewSectionReader(body, off, n)
		off += n
		return section, n, func() {}, nil
	})
}

// bodyOf returns the bytes r reads as a ReaderAt and their size: r itself, from where it
// is, when it is a section, otherwise the bytes read from it.
func bodyOf(r io.Reader) (io.ReaderAt, int64, error) {
	if s, ok := r.(*io.SectionReader); ok {
		pos, err := s.Seek(0, io.SeekCurrent)
		if err != nil {
			return nil, 0, err
		}
		return io.NewSectionReader(s, pos, s.Size()-pos), s.Size() - pos, nil
	}
	data, err := io.ReadAll(r)
	return bytes.NewReader(data), int64(len(data)), err
}

// bytesReaderAt reads the bytes of slices one after another.
type bytesReaderAt [][]byte

func (b bytesReaderAt) ReadAt(p []byte, off int64) (int, error) {
	n := 0
	for _, s := range b {
		if off >= int64(len(s)) {
			off -= int64(len(s))
			continue
		}
		c := copy(p[n:], s[off:])
		n += c
		off = 0
		if n == len(p) {
			return n, nil
		}
	}
	return n, io.EOF
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
