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

package arrowio

import (
	"context"
	"encoding/binary"
	"io"
	"math"
	"sort"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/arrowipc"
	"github.com/matrixorigin/matrixone/pkg/container/arrowipc/ipcflatbuf"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
)

const (
	ipcContinuationToken = uint32(0xffffffff)
	ipcBlockSize         = 24
)

type fileBlock struct {
	offset   int64
	metadata int64
	body     int64
}

func openFile(
	ctx context.Context,
	rangeReader fileservice.LeasedRangeReader,
	path string,
	size int64,
	admission fileservice.RangeReadAdmission,
	options Options,
) (Reader, error) {
	recordBlocks, dictionaryBlocks, err := readFooterBlocks(
		ctx, rangeReader, path, size, admission, options,
	)
	if err != nil {
		return nil, err
	}
	blocks, err := mergeFileBlocks(recordBlocks, dictionaryBlocks)
	if err != nil {
		return nil, moerr.NewInvalidInputf(ctx, "invalid Arrow IPC File block ordering: %v", err)
	}
	if options.FileShard != nil {
		blocks, err = selectFileShardBlocks(recordBlocks, dictionaryBlocks, *options.FileShard)
		if err != nil {
			return nil, moerr.NewInvalidInputf(ctx, "invalid Arrow IPC File shard: %v", err)
		}
	}

	metadataReader := &rangeReadAtSeeker{
		ctx: ctx, reader: rangeReader, path: path, size: size, admission: admission,
	}
	// Arrow-Go's FileReader does not currently release its dictionary memo from
	// Close. Isolate the footer/schema probe in its own admission allocator and
	// release that probe as soon as the immutable schema has been extracted.
	// The actual range-message reader below replays dictionaries under its own
	// normal ref-counted lifetime.
	probeAllocator := newAdmissionAllocator(ctx, admission)
	defer probeAllocator.releaseAll()
	fileReader, err := ipc.NewFileReader(
		metadataReader,
		ipc.WithAllocator(probeAllocator),
		ipc.WithMetadataSizeLimit(options.MaxMetadataBytes),
		ipc.WithBodySizeLimit(options.MaxBodyBytes),
		ipc.WithEnsureNativeEndian(true),
	)
	if err != nil {
		return nil, moerr.NewInvalidInputf(ctx, "invalid Arrow IPC File footer: %v", err)
	}
	schema := fileReader.Schema()
	records := fileReader.NumRecords()
	_ = fileReader.Close()
	if schema == nil || records != len(recordBlocks) {
		return nil, moerr.NewInvalidInput(ctx, "Arrow IPC File footer record count is inconsistent")
	}

	messageReader, err := newRangeMessageReader(
		ctx, rangeReader, path, admission, schema, blocks, options,
	)
	if err != nil {
		return nil, err
	}
	reader, err := ipc.NewReaderFromMessageReader(
		messageReader,
		ipc.WithAllocator(options.Allocator),
		ipc.WithMetadataSizeLimit(options.MaxMetadataBytes),
		ipc.WithBodySizeLimit(options.MaxBodyBytes),
		ipc.WithEnsureNativeEndian(true),
	)
	if err != nil {
		messageReader.Release()
		return nil, moerr.NewInvalidInputf(ctx, "invalid Arrow IPC File schema: %v", err)
	}
	return &ipcRecordReader{reader: reader, rangeMessageReader: messageReader}, nil
}

// FileShard is one independently decodable contiguous record-batch interval.
// RequiredDictionaryBlockIndices are indices in the footer dictionary vector.
type FileShard struct {
	RecordBatchStart               int32
	RecordBatchEnd                 int32
	RequiredDictionaryBlockIndices []int32
}

func selectFileShardBlocks(
	records []fileBlock,
	dictionaries []fileBlock,
	shard FileShard,
) ([]fileBlock, error) {
	start, end := int(shard.RecordBatchStart), int(shard.RecordBatchEnd)
	if start < 0 || start >= end || end > len(records) {
		return nil, moerr.NewInvalidInputNoCtxf("record interval [%d,%d) is outside [0,%d)", start, end, len(records))
	}
	lastRecordOffset := records[end-1].offset
	expectedDictionaries := make([]int32, 0, len(dictionaries))
	for index, block := range dictionaries {
		if block.offset < lastRecordOffset {
			expectedDictionaries = append(expectedDictionaries, int32(index))
		}
	}
	if len(shard.RequiredDictionaryBlockIndices) != len(expectedDictionaries) {
		return nil, moerr.NewInvalidInputNoCtxf("dictionary closure has %d blocks, expected %d",
			len(shard.RequiredDictionaryBlockIndices), len(expectedDictionaries))
	}
	selectedDictionaries := make([]fileBlock, 0, len(expectedDictionaries))
	for index, expected := range expectedDictionaries {
		if shard.RequiredDictionaryBlockIndices[index] != expected {
			return nil, moerr.NewInvalidInputNoCtxf("dictionary closure index %d is %d, expected %d",
				index, shard.RequiredDictionaryBlockIndices[index], expected)
		}
		selectedDictionaries = append(selectedDictionaries, dictionaries[expected])
	}
	return mergeFileBlocks(records[start:end], selectedDictionaries)
}

func readFooterBlocks(
	ctx context.Context,
	reader fileservice.LeasedRangeReader,
	path string,
	size int64,
	admission fileservice.RangeReadAdmission,
	options Options,
) (records []fileBlock, dictionaries []fileBlock, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			records = nil
			dictionaries = nil
			err = moerr.NewInvalidInputf(ctx, "invalid Arrow IPC File footer: %v", recovered)
		}
	}()
	tailSize := int64(4 + len(ipc.Magic))
	if size <= int64(2*len(ipc.Magic))+4 {
		return nil, nil, moerr.NewInvalidInputf(ctx, "Arrow IPC File is too small: %d", size)
	}
	tail, err := reader.ReadRangeLease(ctx, path, size-tailSize, tailSize, admission)
	if err != nil {
		return nil, nil, err
	}
	tailBytes := tail.Bytes()
	if len(tailBytes) != int(tailSize) || string(tailBytes[4:]) != string(ipc.Magic) {
		tail.Release()
		return nil, nil, moerr.NewInvalidInput(ctx, "Arrow IPC File closing magic is invalid")
	}
	footerLength := int64(binary.LittleEndian.Uint32(tailBytes[:4]))
	tail.Release()
	if footerLength < 4 || footerLength > options.MaxMetadataBytes || footerLength > size-tailSize-int64(len(ipc.Magic)) {
		return nil, nil, moerr.NewInvalidInputf(ctx, "Arrow IPC File footer length %d is invalid", footerLength)
	}
	footerStart := size - tailSize - footerLength
	footer, err := reader.ReadRangeLease(ctx, path, footerStart, footerLength, admission)
	if err != nil {
		return nil, nil, err
	}
	defer footer.Release()
	footerBytes := footer.Bytes()
	if len(footerBytes) < 4 {
		return nil, nil, moerr.NewInvalidInput(ctx, "Arrow IPC File footer is truncated")
	}
	root := binary.LittleEndian.Uint32(footerBytes)
	if uint64(root) >= uint64(len(footerBytes)) {
		return nil, nil, moerr.NewInvalidInput(ctx, "Arrow IPC File footer root is out of bounds")
	}
	footerMetadata := ipcflatbuf.GetRootAsFooter(footerBytes)
	if err := arrowipc.ValidateSchemaMetadata(
		ctx, footerMetadata.Schema(nil), len(footerBytes),
	); err != nil {
		return nil, nil, err
	}
	dictionaryCount := footerMetadata.DictionariesLength()
	recordCount := footerMetadata.RecordBatchesLength()
	if dictionaryCount < 0 || recordCount < 0 ||
		uint64(dictionaryCount) > uint64(len(footerBytes))/ipcBlockSize ||
		uint64(recordCount) > uint64(len(footerBytes))/ipcBlockSize {
		return nil, nil, moerr.NewInvalidInput(ctx, "Arrow IPC File block vectors are out of bounds")
	}
	dictionaries, err = flatbufferBlocks(
		dictionaryCount, footerStart, options, footerMetadata.Dictionaries,
	)
	if err != nil {
		return nil, nil, moerr.NewInvalidInputf(ctx, "invalid Arrow IPC dictionary blocks: %v", err)
	}
	records, err = flatbufferBlocks(
		recordCount, footerStart, options, footerMetadata.RecordBatches,
	)
	if err != nil {
		return nil, nil, moerr.NewInvalidInputf(ctx, "invalid Arrow IPC record blocks: %v", err)
	}
	return records, dictionaries, nil
}

func mergeFileBlocks(records, dictionaries []fileBlock) ([]fileBlock, error) {
	blocks := make([]fileBlock, 0, len(records)+len(dictionaries))
	blocks = append(blocks, records...)
	blocks = append(blocks, dictionaries...)
	sort.Slice(blocks, func(i, j int) bool { return blocks[i].offset < blocks[j].offset })
	previousEnd := int64(len(ipc.Magic))
	for i, block := range blocks {
		if block.offset < previousEnd {
			return nil, moerr.NewInvalidInputNoCtxf("block %d overlaps the previous block", i)
		}
		previousEnd = block.offset + block.metadata + block.body
	}
	return blocks, nil
}

func flatbufferBlocks(
	count int,
	footerStart int64,
	options Options,
	read func(*ipcflatbuf.Block, int) bool,
) ([]fileBlock, error) {
	if count == 0 {
		return nil, nil
	}
	blocks := make([]fileBlock, count)
	previousEnd := int64(len(ipc.Magic))
	for i := 0; i < count; i++ {
		var metadataBlock ipcflatbuf.Block
		if !read(&metadataBlock, i) {
			return nil, moerr.NewInvalidInputNoCtxf("block %d is missing", i)
		}
		block := fileBlock{
			offset:   metadataBlock.Offset(),
			metadata: int64(metadataBlock.MetadataLength()),
			body:     metadataBlock.BodyLength(),
		}
		if block.offset < previousEnd || block.offset%8 != 0 || block.metadata < 4 || block.metadata%8 != 0 ||
			block.body < 0 || block.body%8 != 0 || block.metadata > options.MaxMetadataBytes ||
			block.body > options.MaxBodyBytes || block.metadata > math.MaxInt64-block.body ||
			block.offset > footerStart || block.metadata+block.body > footerStart-block.offset {
			return nil, moerr.NewInvalidInputNoCtxf("block %d has invalid offset or length", i)
		}
		previousEnd = block.offset + block.metadata + block.body
		blocks[i] = block
	}
	return blocks, nil
}

type rangeReadAtSeeker struct {
	ctx       context.Context
	reader    fileservice.LeasedRangeReader
	path      string
	size      int64
	offset    int64
	admission fileservice.RangeReadAdmission
}

func (r *rangeReadAtSeeker) Read(p []byte) (int, error) {
	n, err := r.ReadAt(p, r.offset)
	r.offset += int64(n)
	return n, err
}

func (r *rangeReadAtSeeker) ReadAt(p []byte, offset int64) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if offset < 0 || offset > r.size || int64(len(p)) > r.size-offset {
		return 0, io.EOF
	}
	lease, err := r.reader.ReadRangeLease(r.ctx, r.path, offset, int64(len(p)), r.admission)
	if err != nil {
		return 0, err
	}
	n := copy(p, lease.Bytes())
	lease.Release()
	if n != len(p) {
		return n, io.ErrUnexpectedEOF
	}
	return n, nil
}

func (r *rangeReadAtSeeker) Seek(offset int64, whence int) (int64, error) {
	next := offset
	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		next = r.offset + offset
	case io.SeekEnd:
		next = r.size + offset
	default:
		return 0, moerr.NewInvalidInputNoCtx("invalid Arrow IPC seek whence")
	}
	if next < 0 || next > r.size {
		return 0, moerr.NewInvalidInputNoCtx("invalid Arrow IPC seek offset")
	}
	r.offset = next
	return next, nil
}

type rangeMessageReader struct {
	refs                  atomic.Int64
	ctx                   context.Context
	reader                fileservice.LeasedRangeReader
	path                  string
	admission             fileservice.RangeReadAdmission
	maxDecodedRecordBytes int64
	blocks                []fileBlock
	next                  int
	current               *ipc.Message
	currentAllocator      *rangeLeaseAllocator
	generation            uint64
	schema                *ipc.Message
}

func newRangeMessageReader(
	ctx context.Context,
	reader fileservice.LeasedRangeReader,
	path string,
	admission fileservice.RangeReadAdmission,
	schema *arrow.Schema,
	blocks []fileBlock,
	options Options,
) (*rangeMessageReader, error) {
	payload := ipc.GetSchemaPayload(schema, options.Allocator)
	defer payload.Release()
	meta := payload.Meta()
	if meta == nil {
		return nil, moerr.NewInternalErrorNoCtx("Arrow schema payload metadata is missing")
	}
	body := memory.NewBufferBytes(nil)
	schemaMessage := ipc.NewMessage(meta, body)
	meta.Release()
	body.Release()
	readerValue := &rangeMessageReader{
		ctx: ctx, reader: reader, path: path, admission: admission,
		maxDecodedRecordBytes: options.MaxDecodedRecordBytes,
		blocks:                blocks, next: -1, schema: schemaMessage,
	}
	readerValue.refs.Store(1)
	return readerValue, nil
}

func (r *rangeMessageReader) Retain() {
	if r == nil {
		panic("retain released Arrow range message reader")
	}
	for {
		refs := r.refs.Load()
		if refs <= 0 {
			panic("retain released Arrow range message reader")
		}
		if r.refs.CompareAndSwap(refs, refs+1) {
			return
		}
	}
}

func (r *rangeMessageReader) Release() {
	if r == nil {
		return
	}
	refs := r.refs.Add(-1)
	if refs < 0 {
		panic("Arrow range message reader release underflow")
	}
	if refs != 0 {
		return
	}
	r.releaseCurrent()
	if r.schema != nil {
		r.schema.Release()
		r.schema = nil
	}
}

// checkpoint identifies the last range generation published to Arrow-Go.
// A failed decoder call may abort only a newer current generation.
func (r *rangeMessageReader) checkpoint() uint64 {
	if r == nil {
		return 0
	}
	return r.generation
}

func (r *rangeMessageReader) releaseCurrent() {
	if r == nil {
		return
	}
	r.currentAllocator = nil
	if r.current != nil {
		r.current.Release()
		r.current = nil
	}
}

// abortAfter releases the current message and forcibly terminates its leased
// range when Arrow-Go abandoned an intermediate owner during a failed decode.
// rangeLeaseAllocator makes a later ref-count cleanup idempotent.
func (r *rangeMessageReader) abortAfter(checkpoint uint64) {
	if r == nil || r.generation <= checkpoint || r.currentAllocator == nil {
		return
	}
	allocator := r.currentAllocator
	r.currentAllocator = nil
	if r.current != nil {
		r.current.Release()
		r.current = nil
	}
	allocator.release()
}

func (r *rangeMessageReader) Message() (message *ipc.Message, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			message = nil
			err = moerr.NewInvalidInputf(r.ctx, "invalid Arrow IPC File message: %v", recovered)
		}
	}()
	// Message invalidates the preceding message even when the next read fails.
	// Any published record has retained its own buffer references by this point.
	r.releaseCurrent()
	if err := r.ctx.Err(); err != nil {
		return nil, err
	}
	if r.next == -1 {
		r.current = r.schema
		r.schema = nil
		r.next = 0
		return r.current, nil
	}
	if r.next >= len(r.blocks) {
		return nil, io.EOF
	}
	block := r.blocks[r.next]
	r.next++
	lease, err := r.reader.ReadRangeLease(
		r.ctx, r.path, block.offset, block.metadata+block.body, r.admission,
	)
	if err != nil {
		return nil, err
	}
	var allocator *rangeLeaseAllocator
	message, allocator, err = messageFromRangeLease(r.ctx, lease, block, r.maxDecodedRecordBytes)
	if err != nil {
		lease.Release()
		return nil, err
	}
	r.current = message
	r.currentAllocator = allocator
	r.generation++
	return message, nil
}

type rangeLeaseAllocator struct {
	lease    fileservice.RangeLease
	released atomic.Bool
}

func (a *rangeLeaseAllocator) Allocate(int) []byte {
	panic("range lease allocator cannot allocate")
}
func (a *rangeLeaseAllocator) Reallocate(int, []byte) []byte {
	panic("range lease allocator cannot reallocate")
}
func (a *rangeLeaseAllocator) Free([]byte) {
	a.release()
}

func (a *rangeLeaseAllocator) release() {
	// Keep lease immutable: forced cleanup can race Arrow-Go's late Free, and
	// the successful CAS is the sole owner allowed to release it.
	if a != nil && a.released.CompareAndSwap(false, true) && a.lease != nil {
		a.lease.Release()
	}
}

func messageFromRangeLease(
	ctx context.Context,
	lease fileservice.RangeLease,
	block fileBlock,
	maxDecodedRecordBytes int64,
) (message *ipc.Message, allocator *rangeLeaseAllocator, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			if allocator != nil {
				allocator.release()
			} else {
				lease.Release()
			}
			panic(recovered)
		}
	}()
	bytes := lease.Bytes()
	if int64(len(bytes)) != block.metadata+block.body || len(bytes) < 4 {
		return nil, nil, moerr.NewInvalidInputNoCtx("Arrow IPC File block range is truncated")
	}
	metadataBytes := bytes[:int(block.metadata)]
	prefix := 0
	switch binary.LittleEndian.Uint32(metadataBytes[:4]) {
	case 0:
	case ipcContinuationToken:
		prefix = 8
	default:
		prefix = 4
	}
	if int(block.metadata) < prefix+4 {
		return nil, nil, moerr.NewInvalidInputNoCtx("Arrow IPC File metadata prefix is invalid")
	}
	if _, err := inspectIPCMessageMetadata(
		ctx, metadataBytes[prefix:], block.body, block.body,
		bytes[int(block.metadata):], true, maxDecodedRecordBytes,
	); err != nil {
		return nil, nil, err
	}
	allocator = &rangeLeaseAllocator{lease: lease}
	owner := memory.NewBufferWithAllocator(bytes, allocator)
	meta := memory.SliceBuffer(owner, prefix, int(block.metadata)-prefix)
	body := memory.SliceBuffer(owner, int(block.metadata), int(block.body))
	owner.Release()
	message = ipc.NewMessage(meta, body)
	meta.Release()
	body.Release()
	return message, allocator, nil
}
