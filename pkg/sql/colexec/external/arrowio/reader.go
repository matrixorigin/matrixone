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

// Package arrowio provides bounded Arrow IPC File and Stream readers over
// MatrixOne FileService. It deliberately has no Flight/Flight SQL dependency.
package arrowio

import (
	"context"
	"errors"
	"io"
	"math"
	"sync"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
)

const (
	DefaultMaxMetadataBytes      int64 = 1 << 20
	DefaultMaxBodyBytes          int64 = 256 << 20
	DefaultMaxDecodedRecordBytes int64 = 256 << 20
)

type Container uint8

const (
	ContainerAuto Container = iota
	ContainerFile
	ContainerStream
)

type Options struct {
	MaxMetadataBytes      int64
	MaxBodyBytes          int64
	MaxDecodedRecordBytes int64
	Allocator             memory.Allocator
	ExpectedIdentity      *fileservice.ObjectIdentity
	FileShard             *FileShard
}

type Reader interface {
	Schema() *arrow.Schema
	Next() bool
	RecordBatch() arrow.RecordBatch
	Err() error
	Close() error
}

type ipcRecordReader struct {
	reader             *ipc.Reader
	close              func() error
	allocator          *admissionAllocator
	rangeMessageReader *rangeMessageReader
	err                error
}

func (r *ipcRecordReader) Schema() *arrow.Schema {
	if r == nil || r.reader == nil {
		return nil
	}
	return r.reader.Schema()
}

func (r *ipcRecordReader) Next() (ok bool) {
	if r == nil || r.reader == nil || r.err != nil {
		return false
	}
	var checkpoint uint64
	if r.allocator != nil {
		checkpoint = r.allocator.checkpoint()
	}
	var rangeCheckpoint uint64
	if r.rangeMessageReader != nil {
		rangeCheckpoint = r.rangeMessageReader.checkpoint()
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			if allocationErr, matched := recoveredAllocationError(recovered); matched {
				r.err = allocationErr
			} else {
				r.err = moerr.NewInvalidInputNoCtxf("invalid Arrow IPC record batch: %v", recovered)
			}
			ok = false
		}
		if !ok && r.allocator != nil {
			// Arrow-Go can recover a panic internally after allocating a body
			// buffer but before installing an ArrayData owner. Only allocations
			// created by this failed Next call are unowned. Older allocations may
			// still be retained by already-published record batches or MO vectors.
			r.allocator.releaseAfter(checkpoint)
		}
		if !ok && r.rangeMessageReader != nil {
			// An invalid uncompressed array can panic after Arrow-Go has
			// retained ArrayData but before it publishes the array or record.
			// Arrow-Go recovers that panic internally, so its ordinary message
			// release cannot close the unmatched retain. Abort only a range
			// first published by this failed Next call; older ranges may still
			// back record batches or MO vectors retained by the caller.
			r.rangeMessageReader.abortAfter(rangeCheckpoint)
		}
	}()
	return r.reader.Next()
}

func (r *ipcRecordReader) RecordBatch() arrow.RecordBatch {
	if r == nil || r.reader == nil {
		return nil
	}
	return r.reader.RecordBatch()
}

func (r *ipcRecordReader) Err() error {
	if r == nil {
		return nil
	}
	if r.reader == nil {
		return r.err
	}
	return errors.Join(r.err, r.reader.Err())
}

func (r *ipcRecordReader) Close() error {
	if r == nil {
		return nil
	}
	if r.reader != nil {
		r.reader.Release()
		r.reader = nil
	}
	r.rangeMessageReader = nil
	// Do not sweep the allocator here. Arrow buffers retained by a caller are
	// allowed to outlive the reader and will free themselves through their
	// allocator. Failed Next generations are swept at the Next boundary.
	r.allocator = nil
	if r.close != nil {
		close := r.close
		r.close = nil
		return close()
	}
	return nil
}

func normalizeOptions(options Options) (Options, error) {
	if options.MaxMetadataBytes == 0 {
		options.MaxMetadataBytes = DefaultMaxMetadataBytes
	}
	if options.MaxBodyBytes == 0 {
		options.MaxBodyBytes = DefaultMaxBodyBytes
	}
	if options.MaxDecodedRecordBytes == 0 {
		options.MaxDecodedRecordBytes = DefaultMaxDecodedRecordBytes
	}
	if options.MaxMetadataBytes < 4 || options.MaxMetadataBytes > int64(math.MaxInt) ||
		options.MaxBodyBytes <= 0 || options.MaxBodyBytes > int64(math.MaxInt) ||
		options.MaxDecodedRecordBytes <= 0 || options.MaxDecodedRecordBytes > int64(math.MaxInt) {
		return options, moerr.NewInvalidInputNoCtx("invalid Arrow IPC size limits")
	}
	return options, nil
}

// Open selects IPC File or Stream. Auto probes only the final Arrow file magic;
// arbitrary binary input is still validated by the selected official decoder.
func Open(
	ctx context.Context,
	fs fileservice.FileService,
	path string,
	size int64,
	container Container,
	admission fileservice.RangeReadAdmission,
	options Options,
) (_ Reader, retErr error) {
	options, err := normalizeOptions(options)
	if err != nil {
		return nil, err
	}
	if fs == nil || path == "" || size < 0 || admission == nil {
		return nil, moerr.NewInvalidInput(ctx, "invalid Arrow IPC source")
	}
	rangeReader := fileservice.NewLeasedRangeReader(fs)
	if options.ExpectedIdentity != nil {
		expected := *options.ExpectedIdentity
		if err := expected.Validate(); err != nil {
			return nil, err
		}
		if expected.Size != size {
			return nil, moerr.NewInvalidInputf(ctx,
				"Arrow object identity size %d does not match planned size %d", expected.Size, size)
		}
		conditional, ok := rangeReader.(fileservice.ConditionalLeasedRangeReader)
		if !ok {
			return nil, moerr.NewNotSupported(ctx, "conditional Arrow object reads")
		}
		rangeReader = fixedIdentityRangeReader{reader: conditional, expected: expected}
	}
	var ownedAllocator *admissionAllocator
	if options.Allocator == nil {
		ownedAllocator = newAdmissionAllocator(ctx, admission)
		options.Allocator = ownedAllocator
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			if allocationErr, matched := recoveredAllocationError(recovered); matched {
				if ownedAllocator != nil {
					ownedAllocator.releaseAll()
				}
				retErr = allocationErr
				return
			}
			if ownedAllocator != nil {
				ownedAllocator.releaseAll()
			}
			retErr = moerr.NewInvalidInputf(ctx, "invalid Arrow IPC input: %v", recovered)
		}
		if retErr != nil && ownedAllocator != nil {
			ownedAllocator.releaseAll()
		}
	}()
	if container == ContainerAuto {
		container, err = detectContainer(ctx, rangeReader, path, size, admission)
		if err != nil {
			return nil, err
		}
	}
	var result Reader
	switch container {
	case ContainerFile:
		result, err = openFile(ctx, rangeReader, path, size, admission, options)
	case ContainerStream:
		if options.FileShard != nil {
			return nil, moerr.NewInvalidInput(ctx, "Arrow IPC Stream cannot use a record-batch shard")
		}
		result, err = openStream(ctx, fs, path, options)
	default:
		return nil, moerr.NewInvalidInputf(ctx, "invalid Arrow IPC container %d", container)
	}
	if err != nil {
		return nil, err
	}
	if ownedAllocator != nil {
		ipcReader, ok := result.(*ipcRecordReader)
		if !ok {
			_ = result.Close()
			return nil, moerr.NewInternalErrorNoCtx("Arrow IPC facade returned an unmanaged reader")
		}
		ipcReader.allocator = ownedAllocator
	}
	return result, nil
}

// DetectContainer performs the same identity-locked tail probe as Open
// without decoding schema or record data.
func DetectContainer(
	ctx context.Context,
	fs fileservice.FileService,
	path string,
	size int64,
	admission fileservice.RangeReadAdmission,
	options Options,
) (Container, error) {
	options, err := normalizeOptions(options)
	if err != nil {
		return 0, err
	}
	if fs == nil || path == "" || size < 0 || admission == nil {
		return 0, moerr.NewInvalidInput(ctx, "invalid Arrow IPC container probe")
	}
	rangeReader := fileservice.NewLeasedRangeReader(fs)
	if options.ExpectedIdentity != nil {
		expected := *options.ExpectedIdentity
		if err := expected.Validate(); err != nil {
			return 0, err
		}
		if expected.Size != size {
			return 0, moerr.NewInvalidInputf(ctx,
				"Arrow object identity size %d does not match planned size %d", expected.Size, size)
		}
		conditional, ok := rangeReader.(fileservice.ConditionalLeasedRangeReader)
		if !ok {
			return 0, moerr.NewNotSupported(ctx, "conditional Arrow object reads")
		}
		rangeReader = fixedIdentityRangeReader{reader: conditional, expected: expected}
	}
	return detectContainer(ctx, rangeReader, path, size, admission)
}

const arrowAllocatorAlignment = 64

type allocationPanic struct{ err error }

func recoveredAllocationError(value any) (error, bool) {
	p, ok := value.(allocationPanic)
	if !ok {
		return nil, false
	}
	return p.err, true
}

// admissionAllocator makes Arrow-Go decoded/metadata buffers participate in
// the same pre-allocation admission protocol as leased FileService ranges.
// Arrow's Allocator API cannot return errors, so a private panic is recovered
// exactly at the facade's Open/Next boundaries and converted back to an error.
type admissionAllocator struct {
	ctx       context.Context
	admission fileservice.RangeReadAdmission
	base      memory.Allocator
	mu        sync.Mutex
	nextID    uint64
	allocated map[uintptr]admissionAllocation
}

type admissionAllocation struct {
	buffer []byte
	lease  fileservice.CapacityLease
	id     uint64
}

func newAdmissionAllocator(ctx context.Context, admission fileservice.RangeReadAdmission) *admissionAllocator {
	return &admissionAllocator{
		ctx: ctx, admission: admission, base: memory.DefaultAllocator,
		allocated: make(map[uintptr]admissionAllocation),
	}
}

func (a *admissionAllocator) Allocate(size int) []byte {
	if size <= 0 {
		return nil
	}
	if size > int(^uint(0)>>1)-arrowAllocatorAlignment {
		panic(allocationPanic{err: moerr.NewInvalidInputNoCtx("Arrow allocation size overflows")})
	}
	upper := int64(size) + arrowAllocatorAlignment
	reservation, err := a.admission.Reserve(a.ctx, upper)
	if err != nil {
		panic(allocationPanic{err: err})
	}
	var buffer []byte
	func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				reservation.Abort()
				panic(recovered)
			}
		}()
		buffer = a.base.Allocate(size)
	}()
	actualCapacity := int64(cap(buffer))
	if actualCapacity <= 0 || actualCapacity > upper {
		reservation.Abort()
		a.base.Free(buffer)
		panic("Arrow allocator exceeded its reserved capacity")
	}
	lease, err := reservation.Commit(actualCapacity)
	if err != nil {
		reservation.Abort()
		a.base.Free(buffer)
		panic(allocationPanic{err: err})
	}
	key := arrowBufferKey(buffer)
	a.mu.Lock()
	if _, exists := a.allocated[key]; exists {
		a.mu.Unlock()
		lease.Release()
		a.base.Free(buffer)
		panic("Arrow allocator returned duplicate live backing")
	}
	a.nextID++
	a.allocated[key] = admissionAllocation{buffer: buffer, lease: lease, id: a.nextID}
	a.mu.Unlock()
	return buffer
}

func (a *admissionAllocator) Reallocate(size int, buffer []byte) []byte {
	if size <= cap(buffer) {
		return buffer[:size]
	}
	replacement := a.Allocate(size)
	copy(replacement, buffer)
	a.Free(buffer)
	return replacement
}

func (a *admissionAllocator) Free(buffer []byte) {
	key := arrowBufferKey(buffer)
	if key == 0 {
		return
	}
	a.mu.Lock()
	allocation, exists := a.allocated[key]
	delete(a.allocated, key)
	a.mu.Unlock()
	if !exists {
		// releaseAll owns abandoned allocations on decoder failure. Arrow-Go
		// cleanup may still invoke Free while unwinding; it must not double-free.
		return
	}
	a.base.Free(allocation.buffer)
	if allocation.lease != nil {
		allocation.lease.Release()
	}
}

func arrowBufferKey(buffer []byte) uintptr {
	if cap(buffer) == 0 {
		return 0
	}
	return uintptr(unsafe.Pointer(unsafe.SliceData(buffer[:1])))
}

func (a *admissionAllocator) releaseAll() {
	a.releaseAfter(0)
}

// checkpoint identifies an allocation generation boundary. It is used to
// reclaim only buffers abandoned by one failed decoder call without touching
// older buffers that may still be retained by published zero-copy views.
func (a *admissionAllocator) checkpoint() uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.nextID
}

func (a *admissionAllocator) releaseAfter(checkpoint uint64) {
	a.mu.Lock()
	allocated := make([]admissionAllocation, 0)
	for key, allocation := range a.allocated {
		if allocation.id > checkpoint {
			allocated = append(allocated, allocation)
			delete(a.allocated, key)
		}
	}
	a.mu.Unlock()
	for _, allocation := range allocated {
		a.base.Free(allocation.buffer)
		if allocation.lease != nil {
			allocation.lease.Release()
		}
	}
}

var _ memory.Allocator = (*admissionAllocator)(nil)

func detectContainer(
	ctx context.Context,
	reader fileservice.LeasedRangeReader,
	path string,
	size int64,
	admission fileservice.RangeReadAdmission,
) (Container, error) {
	if size >= int64(len(ipc.Magic)) && admission != nil {
		lease, err := reader.ReadRangeLease(
			ctx, path, size-int64(len(ipc.Magic)), int64(len(ipc.Magic)), admission,
		)
		if err != nil {
			return 0, err
		}
		isFile := string(lease.Bytes()) == string(ipc.Magic)
		lease.Release()
		if isFile {
			return ContainerFile, nil
		}
	}
	return ContainerStream, nil
}

type fixedIdentityRangeReader struct {
	reader   fileservice.ConditionalLeasedRangeReader
	expected fileservice.ObjectIdentity
}

func (r fixedIdentityRangeReader) ReadRangeLease(
	ctx context.Context,
	path string,
	offset, size int64,
	admission fileservice.RangeReadAdmission,
) (fileservice.RangeLease, error) {
	return r.reader.ReadRangeLeaseWithIdentity(ctx, path, offset, size, r.expected, admission)
}

func openStream(
	ctx context.Context,
	fs fileservice.FileService,
	path string,
	options Options,
) (Reader, error) {
	var stream io.ReadCloser
	if options.ExpectedIdentity != nil {
		identityFS, ok := fs.(fileservice.ObjectIdentityFileService)
		if !ok {
			return nil, moerr.NewNotSupported(ctx, "conditional Arrow stream reads")
		}
		var err error
		stream, err = identityFS.OpenReadWithIdentity(ctx, path, 0, -1, *options.ExpectedIdentity)
		if err != nil {
			return nil, err
		}
	} else {
		vector := &fileservice.IOVector{
			FilePath: path,
			Policy:   fileservice.SkipAllCache,
			Entries: []fileservice.IOEntry{{
				Offset: 0, Size: -1, ReadCloserForRead: &stream,
			}},
		}
		if err := fs.Read(ctx, vector); err != nil {
			vector.ReleaseReadResultOnError()
			return nil, err
		}
	}
	if stream == nil {
		return nil, moerr.NewInvalidInput(ctx, "Arrow IPC Stream reader is missing")
	}
	readerOptions := []ipc.Option{
		ipc.WithAllocator(options.Allocator),
		ipc.WithMetadataSizeLimit(options.MaxMetadataBytes),
		ipc.WithBodySizeLimit(options.MaxBodyBytes),
		ipc.WithEnsureNativeEndian(true),
	}
	// Arrow-Go's stream messageReader limits the on-wire body only. The facade
	// reader additionally validates buffer descriptors and the declared total
	// decompressed size before Arrow-Go can allocate decode buffers.
	messageReader := newStreamMessageReader(ctx, stream, options)
	reader, err := ipc.NewReaderFromMessageReader(
		messageReader,
		readerOptions...,
	)
	if err != nil {
		messageReader.Release()
		stream.Close()
		return nil, moerr.NewInvalidInputf(ctx, "invalid Arrow IPC Stream: %v", err)
	}
	return &ipcRecordReader{reader: reader, close: stream.Close}, nil
}
