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
	"math"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/external/arrowio/ipcflatbuf"
)

const (
	messageHeaderDictionaryBatch = byte(2)
	messageHeaderRecordBatch     = byte(3)
)

// RecordBatchInfo is the footer-stable planning metadata for one record block.
type RecordBatchInfo struct {
	Index     int32
	Rows      int64
	WireBytes int64
}

// DictionaryBlockInfo describes one dictionary epoch transition in footer
// order. A non-delta block establishes an ID exactly once; subsequent blocks
// for that ID must be deltas.
type DictionaryBlockInfo struct {
	Index     int32
	ID        int64
	IsDelta   bool
	Rows      int64
	WireBytes int64
}

// FilePlan is an immutable, bounded description of an IPC File. Schema is the
// official Arrow-Go decoded schema; payload bodies are not read by planning.
type FilePlan struct {
	Schema        *arrow.Schema
	RecordBatches []RecordBatchInfo
	Dictionaries  []DictionaryBlockInfo
	recordBlocks  []fileBlock
	dictBlocks    []fileBlock
}

// Shard returns a self-contained decoder interval and its conservative
// dictionary closure. Replaying every dictionary transition preceding the last
// selected record is safe even when projection-specific dependencies are not
// provable.
func (p *FilePlan) Shard(start, end int) (FileShard, int64, int64, error) {
	if p == nil || start < 0 || start >= end || end > len(p.RecordBatches) {
		return FileShard{}, 0, 0, moerr.NewInvalidInputNoCtx("invalid Arrow IPC File shard interval")
	}
	shard := FileShard{RecordBatchStart: int32(start), RecordBatchEnd: int32(end)}
	var rows, wireBytes int64
	for _, record := range p.RecordBatches[start:end] {
		if record.Rows > 0 && rows > maxInt64-record.Rows {
			return FileShard{}, 0, 0, moerr.NewInvalidInputNoCtx("Arrow shard row count overflows")
		}
		if record.WireBytes > 0 && wireBytes > maxInt64-record.WireBytes {
			return FileShard{}, 0, 0, moerr.NewInvalidInputNoCtx("Arrow shard wire size overflows")
		}
		rows += record.Rows
		wireBytes += record.WireBytes
	}
	lastRecordOffset := p.recordBlocks[end-1].offset
	for index, dictionary := range p.dictBlocks {
		if dictionary.offset >= lastRecordOffset {
			continue
		}
		shard.RequiredDictionaryBlockIndices = append(
			shard.RequiredDictionaryBlockIndices, int32(index),
		)
		if dictionary.metadata+dictionary.body > maxInt64-wireBytes {
			return FileShard{}, 0, 0, moerr.NewInvalidInputNoCtx("Arrow shard wire size overflows")
		}
		wireBytes += dictionary.metadata + dictionary.body
	}
	return shard, rows, wireBytes, nil
}

const maxInt64 = int64(^uint64(0) >> 1)

// InspectFile reads only the bounded footer, schema metadata, and block
// metadata. It validates dictionary base/delta ordering before any shard can
// be published.
func InspectFile(
	ctx context.Context,
	fs fileservice.FileService,
	path string,
	size int64,
	admission fileservice.RangeReadAdmission,
	options Options,
) (_ *FilePlan, retErr error) {
	options, err := normalizeOptions(options)
	if err != nil {
		return nil, err
	}
	if fs == nil || path == "" || size < 0 || admission == nil {
		return nil, moerr.NewInvalidInput(ctx, "invalid Arrow IPC File planning source")
	}
	if options.FileShard != nil {
		return nil, moerr.NewInvalidInput(ctx, "Arrow IPC File planning cannot consume a shard")
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
	ownedAllocator := newAdmissionAllocator(ctx, admission)
	defer ownedAllocator.releaseAll()
	options.Allocator = ownedAllocator
	defer func() {
		if recovered := recover(); recovered != nil {
			if allocationErr, matched := recoveredAllocationError(recovered); matched {
				retErr = allocationErr
				return
			}
			retErr = moerr.NewInvalidInputf(ctx, "invalid Arrow IPC File planning metadata: %v", recovered)
		}
	}()

	records, dictionaries, err := readFooterBlocks(ctx, rangeReader, path, size, admission, options)
	if err != nil {
		return nil, err
	}
	if _, err = mergeFileBlocks(records, dictionaries); err != nil {
		return nil, moerr.NewInvalidInputf(ctx, "invalid Arrow IPC File block ordering: %v", err)
	}
	schema, err := inspectFileSchema(ctx, rangeReader, path, size, admission, options, len(records))
	if err != nil {
		return nil, err
	}
	plan := &FilePlan{
		Schema: schema, RecordBatches: make([]RecordBatchInfo, len(records)),
		Dictionaries: make([]DictionaryBlockInfo, len(dictionaries)),
		recordBlocks: records, dictBlocks: dictionaries,
	}
	baseSeen := make(map[int64]struct{}, len(dictionaries))
	for index, block := range dictionaries {
		metadata, err := inspectFileBlockMetadata(ctx, rangeReader, path, block, admission)
		if err != nil {
			return nil, err
		}
		if metadata.headerType != messageHeaderDictionaryBatch {
			return nil, moerr.NewInvalidInputf(ctx,
				"Arrow footer dictionary block %d contains message type %d", index, metadata.headerType)
		}
		if err := acceptDictionaryTransition(baseSeen, metadata.dictionaryID, metadata.isDelta); err != nil {
			return nil, err
		}
		plan.Dictionaries[index] = DictionaryBlockInfo{
			Index: int32(index), ID: metadata.dictionaryID, IsDelta: metadata.isDelta,
			Rows: metadata.rows, WireBytes: block.metadata + block.body,
		}
	}
	for index, block := range records {
		metadata, err := inspectFileBlockMetadata(ctx, rangeReader, path, block, admission)
		if err != nil {
			return nil, err
		}
		if metadata.headerType != messageHeaderRecordBatch {
			return nil, moerr.NewInvalidInputf(ctx,
				"Arrow footer record block %d contains message type %d", index, metadata.headerType)
		}
		plan.RecordBatches[index] = RecordBatchInfo{
			Index: int32(index), Rows: metadata.rows, WireBytes: block.metadata + block.body,
		}
	}
	return plan, nil
}

func acceptDictionaryTransition(baseSeen map[int64]struct{}, id int64, isDelta bool) error {
	if isDelta {
		if _, ok := baseSeen[id]; !ok {
			return moerr.NewInvalidInputNoCtxf("Arrow dictionary %d delta precedes its base", id)
		}
		return nil
	}
	if _, exists := baseSeen[id]; exists {
		return moerr.NewInvalidInputNoCtxf("Arrow dictionary %d has a replacement base", id)
	}
	baseSeen[id] = struct{}{}
	return nil
}

func inspectFileSchema(
	ctx context.Context,
	rangeReader fileservice.LeasedRangeReader,
	path string,
	size int64,
	admission fileservice.RangeReadAdmission,
	options Options,
	expectedRecords int,
) (*arrow.Schema, error) {
	reader, err := ipc.NewFileReader(
		&rangeReadAtSeeker{ctx: ctx, reader: rangeReader, path: path, size: size, admission: admission},
		ipc.WithAllocator(options.Allocator),
		ipc.WithMetadataSizeLimit(options.MaxMetadataBytes),
		ipc.WithBodySizeLimit(options.MaxBodyBytes),
		ipc.WithEnsureNativeEndian(true),
	)
	if err != nil {
		return nil, moerr.NewInvalidInputf(ctx, "invalid Arrow IPC File footer: %v", err)
	}
	schema := reader.Schema()
	records := reader.NumRecords()
	_ = reader.Close()
	if schema == nil || records != expectedRecords {
		return nil, moerr.NewInvalidInput(ctx, "Arrow IPC File footer record count is inconsistent")
	}
	return schema, nil
}

type inspectedBlockMetadata struct {
	headerType   byte
	rows         int64
	dictionaryID int64
	isDelta      bool
	bodyBytes    int64
}

func inspectFileBlockMetadata(
	ctx context.Context,
	reader fileservice.LeasedRangeReader,
	path string,
	block fileBlock,
	admission fileservice.RangeReadAdmission,
) (inspectedBlockMetadata, error) {
	lease, err := reader.ReadRangeLease(ctx, path, block.offset, block.metadata, admission)
	if err != nil {
		return inspectedBlockMetadata{}, err
	}
	defer lease.Release()
	return inspectFileBlockMetadataBytes(ctx, lease.Bytes(), block)
}

func inspectFileBlockMetadataBytes(
	ctx context.Context,
	data []byte,
	block fileBlock,
) (inspectedBlockMetadata, error) {
	if int64(len(data)) != block.metadata || len(data) < 4 {
		return inspectedBlockMetadata{}, moerr.NewInvalidInput(ctx, "Arrow IPC block metadata is truncated")
	}
	prefix := 4
	if len(data) >= 8 && binary.LittleEndian.Uint32(data) == ipcContinuationToken {
		prefix = 8
	} else if binary.LittleEndian.Uint32(data) == 0 {
		prefix = 0
	}
	if len(data)-prefix < 4 {
		return inspectedBlockMetadata{}, moerr.NewInvalidInput(ctx, "Arrow IPC block metadata prefix is invalid")
	}
	return inspectIPCMessageMetadata(
		ctx, data[prefix:], block.body, block.body, nil, false, DefaultMaxDecodedRecordBytes,
	)
}

// inspectIPCMessageMetadata validates the untrusted FlatBuffers envelope and,
// when the body is present, the total decoded size declared by compressed
// buffer prefixes. bodyEnvelopeBytes is -1 for a stream whose exact body size
// is not known until the metadata has been inspected; otherwise it includes
// at most seven bytes of IPC alignment padding.
func inspectIPCMessageMetadata(
	ctx context.Context,
	payload []byte,
	maxBodyBytes int64,
	bodyEnvelopeBytes int64,
	body []byte,
	validateBody bool,
	maxDecodedRecordBytes int64,
) (_ inspectedBlockMetadata, retErr error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			retErr = moerr.NewInvalidInputf(ctx, "invalid Arrow IPC message metadata: %v", recovered)
		}
	}()
	if len(payload) < 4 {
		return inspectedBlockMetadata{}, moerr.NewInvalidInput(ctx, "Arrow IPC message metadata is truncated")
	}
	root := binary.LittleEndian.Uint32(payload)
	if uint64(root) >= uint64(len(payload)) {
		return inspectedBlockMetadata{}, moerr.NewInvalidInput(ctx, "Arrow IPC message root is out of bounds")
	}
	message := ipcflatbuf.GetRootAsMessage(payload)
	headerType := message.HeaderType()
	if headerType == 0 {
		return inspectedBlockMetadata{}, moerr.NewInvalidInput(ctx, "Arrow IPC message header is missing")
	}
	bodyLength := message.BodyLength()
	if bodyLength < 0 || bodyLength > maxBodyBytes || bodyLength > int64(math.MaxInt) {
		return inspectedBlockMetadata{}, moerr.NewInvalidInputf(ctx,
			"Arrow IPC message body length %d exceeds limit %d", bodyLength, maxBodyBytes)
	}
	if bodyEnvelopeBytes >= 0 &&
		(bodyLength > bodyEnvelopeBytes || bodyEnvelopeBytes-bodyLength >= 8) {
		return inspectedBlockMetadata{}, moerr.NewInvalidInputf(ctx,
			"Arrow IPC message body length %d does not match envelope body length %d",
			bodyLength, bodyEnvelopeBytes)
	}
	if validateBody && (int64(len(body)) < bodyLength || int64(len(body))-bodyLength >= 8) {
		return inspectedBlockMetadata{}, moerr.NewInvalidInputf(ctx,
			"Arrow IPC message body length %d does not match available body length %d",
			bodyLength, len(body))
	}
	result := inspectedBlockMetadata{headerType: byte(headerType), bodyBytes: bodyLength}
	switch headerType {
	case ipcflatbuf.MessageHeaderSchema:
		var schema ipcflatbuf.Schema
		if !message.Schema(&schema) {
			return inspectedBlockMetadata{}, moerr.NewInvalidInput(ctx, "Arrow schema header is missing")
		}
		if err := validateIPCSchemaMetadata(ctx, &schema, len(payload)); err != nil {
			return inspectedBlockMetadata{}, err
		}
	case ipcflatbuf.MessageHeaderRecordBatch:
		var record ipcflatbuf.RecordBatch
		if !message.RecordBatch(&record) {
			return inspectedBlockMetadata{}, moerr.NewInvalidInput(ctx, "Arrow record data header is missing")
		}
		if err := validateRecordBatchMetadata(
			ctx, &record, len(payload), bodyLength, body, validateBody, maxDecodedRecordBytes,
		); err != nil {
			return inspectedBlockMetadata{}, err
		}
		result.rows = record.Length()
	case ipcflatbuf.MessageHeaderDictionaryBatch:
		var dictionary ipcflatbuf.DictionaryBatch
		if !message.DictionaryBatch(&dictionary) {
			return inspectedBlockMetadata{}, moerr.NewInvalidInput(ctx, "Arrow dictionary header is missing")
		}
		var record ipcflatbuf.RecordBatch
		if !dictionary.Data(&record) {
			return inspectedBlockMetadata{}, moerr.NewInvalidInput(ctx, "Arrow dictionary data header is missing")
		}
		if err := validateRecordBatchMetadata(
			ctx, &record, len(payload), bodyLength, body, validateBody, maxDecodedRecordBytes,
		); err != nil {
			return inspectedBlockMetadata{}, err
		}
		result.dictionaryID = dictionary.ID()
		result.isDelta = dictionary.IsDelta()
		result.rows = record.Length()
	default:
		return result, nil
	}
	return result, nil
}

// validateRecordBatchMetadata closes a gap in Arrow-Go's panic-to-error path:
// malformed buffer descriptors can panic after the message body has been
// retained but before an ArrayData owner exists to release it. Validate every
// descriptor while the FileService lease still has a single, explicit owner.
func validateRecordBatchMetadata(
	ctx context.Context,
	record *ipcflatbuf.RecordBatch,
	metadataBytes int,
	bodyBytes int64,
	body []byte,
	validateBody bool,
	maxDecodedRecordBytes int64,
) error {
	rows := record.Length()
	if rows < 0 || rows > int64(math.MaxInt) {
		return moerr.NewInvalidInputf(ctx, "Arrow IPC message has invalid row count %d", rows)
	}

	nodeCount := record.NodesLength()
	if nodeCount < 0 || nodeCount > metadataBytes/16 {
		return moerr.NewInvalidInputf(ctx, "Arrow IPC field-node count %d exceeds metadata", nodeCount)
	}
	var node ipcflatbuf.FieldNode
	for index := 0; index < nodeCount; index++ {
		if !record.Nodes(&node, index) {
			return moerr.NewInvalidInputf(ctx, "Arrow IPC field node %d is missing", index)
		}
		length, nullCount := node.Length(), node.NullCount()
		if length < 0 || nullCount < 0 || nullCount > length {
			return moerr.NewInvalidInputf(ctx,
				"Arrow IPC field node %d has invalid length %d and null count %d",
				index, length, nullCount)
		}
	}

	compression := record.Compression(nil)
	if compression != nil {
		codec := compression.Codec()
		if codec != ipcflatbuf.CompressionTypeLZ4Frame && codec != ipcflatbuf.CompressionTypeZSTD {
			return moerr.NewInvalidInputf(ctx, "Arrow IPC compression codec %d is unsupported", codec)
		}
		if method := compression.Method(); method != ipcflatbuf.BodyCompressionMethodBuffer {
			return moerr.NewInvalidInputf(ctx, "Arrow IPC compression method %d is unsupported", method)
		}
	}

	bufferCount := record.BuffersLength()
	if bufferCount < 0 || bufferCount > metadataBytes/16 {
		return moerr.NewInvalidInputf(ctx, "Arrow IPC buffer count %d exceeds metadata", bufferCount)
	}
	var buffer ipcflatbuf.Buffer
	var decodedBytes int64
	for index := 0; index < bufferCount; index++ {
		if !record.Buffers(&buffer, index) {
			return moerr.NewInvalidInputf(ctx, "Arrow IPC buffer %d is missing", index)
		}
		offset, length := buffer.Offset(), buffer.Length()
		if offset < 0 || length < 0 || offset > bodyBytes || length > bodyBytes-offset {
			return moerr.NewInvalidInputf(ctx,
				"Arrow IPC buffer %d range [%d,%d) exceeds message body %d",
				index, offset, offset+length, bodyBytes)
		}
		if !validateBody {
			continue
		}
		decodedLength := length
		if compression != nil && length != 0 {
			if length < 8 {
				return moerr.NewInvalidInputf(ctx,
					"Arrow IPC compressed buffer %d is shorter than its decoded-size prefix", index)
			}
			declared := int64(binary.LittleEndian.Uint64(body[int(offset) : int(offset)+8]))
			switch {
			case declared == -1:
				decodedLength = length - 8
			case declared < 0 || declared > int64(math.MaxInt):
				return moerr.NewInvalidInputf(ctx,
					"Arrow IPC compressed buffer %d has invalid decoded size %d", index, declared)
			default:
				decodedLength = declared
			}
		}
		if decodedLength > maxDecodedRecordBytes-decodedBytes {
			return moerr.NewInvalidInputf(ctx,
				"Arrow IPC decoded record body exceeds limit %d", maxDecodedRecordBytes)
		}
		decodedBytes += decodedLength
	}

	variadicCount := record.VariadicBufferCountsLength()
	if variadicCount < 0 || variadicCount > metadataBytes/8 {
		return moerr.NewInvalidInputf(ctx,
			"Arrow IPC variadic-buffer count %d exceeds metadata", variadicCount)
	}
	var variadicBuffers int64
	for index := 0; index < variadicCount; index++ {
		count := record.VariadicBufferCounts(index)
		if count < 0 || count > int64(bufferCount)-variadicBuffers {
			return moerr.NewInvalidInputf(ctx,
				"Arrow IPC variadic-buffer count %d at index %d exceeds buffer count %d",
				count, index, bufferCount)
		}
		variadicBuffers += count
	}
	return nil
}
