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

// Package arrowipc validates Arrow IPC framing and FlatBuffers metadata at a
// shared trust boundary. Object/range transport and consumer-specific type
// policy deliberately remain outside this package.
package arrowipc

import (
	"context"
	"encoding/binary"
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/arrowipc/ipcflatbuf"
)

const (
	// DefaultMaxMetadataBytes is a structural safety ceiling, not a transport
	// frame limit. A consumer may negotiate a lower value but must not raise it.
	DefaultMaxMetadataBytes int64 = 1 << 20
	// ContinuationToken prefixes modern stream-framed Arrow IPC metadata.
	ContinuationToken = uint32(math.MaxUint32)

	MessageHeaderSchema          = byte(ipcflatbuf.MessageHeaderSchema)
	MessageHeaderDictionaryBatch = byte(ipcflatbuf.MessageHeaderDictionaryBatch)
	MessageHeaderRecordBatch     = byte(ipcflatbuf.MessageHeaderRecordBatch)
)

// ValidationOptions supplies caller-owned limits. BodyEnvelopeBytes is -1
// when the exact framed body size is not known yet; otherwise it may include
// at most seven bytes of IPC alignment padding.
type ValidationOptions struct {
	// MaxMetadataBytes bounds the complete framed metadata input.
	MaxMetadataBytes int64
	// MaxBodyBytes bounds Message.bodyLength before any decoder allocation.
	MaxBodyBytes int64
	// BodyEnvelopeBytes is the body span supplied by the outer transport. Use
	// -1 only while inspecting metadata before that envelope is available.
	BodyEnvelopeBytes int64
	// Body is read only for compressed buffers' decoded-size prefixes.
	Body []byte
	// ValidateBody enables body-range and decoded-size inspection.
	ValidateBody bool
	// MaxDecodedRecordBytes bounds the sum of all decoded Arrow buffers.
	MaxDecodedRecordBytes int64
}

// MessageInfo is transport-neutral metadata needed by File, Stream, and
// Flight consumers before a decoder may allocate or retain the message body.
// It contains no object identity or invocation sequence because those are
// owned by the transport that supplied the message.
type MessageInfo struct {
	// HeaderType identifies Schema, DictionaryBatch, or RecordBatch metadata.
	HeaderType byte
	// Rows is populated for record and dictionary batches.
	Rows int64
	// DictionaryID and IsDelta are populated only for dictionary batches.
	DictionaryID int64
	IsDelta      bool
	// BodyBytes is the validated Message.bodyLength, excluding IPC padding.
	BodyBytes int64
}

// Metadata accepts a raw Message flatbuffer, continuation framing, or the
// legacy four-byte length prefix and returns a bounded raw metadata view. The
// caller may lower maxBytes, but values above DefaultMaxMetadataBytes are
// clamped so this trust-boundary ceiling cannot be bypassed accidentally.
func Metadata(ctx context.Context, wire []byte, maxBytes int64) ([]byte, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if maxBytes > DefaultMaxMetadataBytes {
		maxBytes = DefaultMaxMetadataBytes
	}
	if maxBytes < 4 || int64(len(wire)) > maxBytes {
		return nil, moerr.NewInvalidInputf(ctx,
			"Arrow IPC metadata length %d exceeds limit %d", len(wire), maxBytes)
	}
	if len(wire) < 4 {
		return nil, moerr.NewInvalidInput(ctx, "Arrow IPC metadata is truncated")
	}
	if binary.LittleEndian.Uint32(wire[:4]) == ContinuationToken {
		if len(wire) < 8 {
			return nil, moerr.NewInvalidInput(ctx, "Arrow IPC continuation header is truncated")
		}
		length := uint64(binary.LittleEndian.Uint32(wire[4:8]))
		if length == 0 || length > uint64(len(wire)-8) {
			return nil, moerr.NewInvalidInput(ctx, "Arrow IPC metadata length is invalid")
		}
		return wire[8 : 8+length], nil
	}
	length := uint64(binary.LittleEndian.Uint32(wire[:4]))
	if length != 0 && length == uint64(len(wire)-4) {
		return wire[4:], nil
	}
	return wire, nil
}

// InspectMessage validates the complete untrusted IPC metadata graph and, when
// requested, compressed-buffer decoded sizes before a downstream decoder can
// allocate from values controlled by the message. Success is structural only:
// callers still enforce metadata version, expected header kind, schema/type
// identity, row cardinality, and protocol sequence as their own contracts.
func InspectMessage(
	ctx context.Context,
	wire []byte,
	options ValidationOptions,
) (_ MessageInfo, retErr error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if options.MaxMetadataBytes == 0 {
		options.MaxMetadataBytes = DefaultMaxMetadataBytes
	}
	if options.MaxBodyBytes < 0 || options.MaxDecodedRecordBytes <= 0 {
		return MessageInfo{}, moerr.NewInvalidInput(ctx, "invalid Arrow IPC validation limits")
	}
	metadata, err := Metadata(ctx, wire, options.MaxMetadataBytes)
	if err != nil {
		return MessageInfo{}, err
	}
	// FlatBuffers' generated accessors panic on some malformed offset graphs.
	// Recovery belongs at this shared trust boundary, before consumer-specific
	// code can allocate or retain attacker-described buffers.
	defer func() {
		if recovered := recover(); recovered != nil {
			retErr = moerr.NewInvalidInputf(ctx, "invalid Arrow IPC message metadata: %v", recovered)
		}
	}()
	root := binary.LittleEndian.Uint32(metadata)
	if uint64(root) >= uint64(len(metadata)) {
		return MessageInfo{}, moerr.NewInvalidInput(ctx, "Arrow IPC message root is out of bounds")
	}
	message := ipcflatbuf.GetRootAsMessage(metadata)
	headerType := message.HeaderType()
	if headerType == 0 {
		return MessageInfo{}, moerr.NewInvalidInput(ctx, "Arrow IPC message header is missing")
	}
	if headerType != ipcflatbuf.MessageHeaderSchema &&
		headerType != ipcflatbuf.MessageHeaderDictionaryBatch &&
		headerType != ipcflatbuf.MessageHeaderRecordBatch {
		return MessageInfo{}, moerr.NewInvalidInputf(ctx,
			"unsupported Arrow IPC message header %d", headerType)
	}
	bodyLength := message.BodyLength()
	if bodyLength < 0 || bodyLength > options.MaxBodyBytes || bodyLength > int64(math.MaxInt) {
		return MessageInfo{}, moerr.NewInvalidInputf(ctx,
			"Arrow IPC message body length %d exceeds limit %d", bodyLength, options.MaxBodyBytes)
	}
	if options.BodyEnvelopeBytes >= 0 &&
		(bodyLength > options.BodyEnvelopeBytes || options.BodyEnvelopeBytes-bodyLength >= 8) {
		return MessageInfo{}, moerr.NewInvalidInputf(ctx,
			"Arrow IPC message body length %d does not match envelope body length %d",
			bodyLength, options.BodyEnvelopeBytes)
	}
	if options.ValidateBody &&
		(int64(len(options.Body)) < bodyLength || int64(len(options.Body))-bodyLength >= 8) {
		return MessageInfo{}, moerr.NewInvalidInputf(ctx,
			"Arrow IPC message body length %d does not match available body length %d",
			bodyLength, len(options.Body))
	}
	if headerType == ipcflatbuf.MessageHeaderSchema && bodyLength != 0 {
		return MessageInfo{}, moerr.NewInvalidInputf(ctx,
			"invalid Arrow IPC schema message body length %d", bodyLength)
	}

	result := MessageInfo{HeaderType: byte(headerType), BodyBytes: bodyLength}
	switch headerType {
	case ipcflatbuf.MessageHeaderSchema:
		var schema ipcflatbuf.Schema
		if !message.Schema(&schema) {
			return MessageInfo{}, moerr.NewInvalidInput(ctx, "Arrow schema header is missing")
		}
		if err := ValidateSchemaMetadata(ctx, &schema, len(metadata)); err != nil {
			return MessageInfo{}, err
		}
	case ipcflatbuf.MessageHeaderRecordBatch:
		var record ipcflatbuf.RecordBatch
		if !message.RecordBatch(&record) {
			return MessageInfo{}, moerr.NewInvalidInput(ctx, "Arrow record data header is missing")
		}
		if err := validateRecordBatchMetadata(ctx, &record, len(metadata), bodyLength, options); err != nil {
			return MessageInfo{}, err
		}
		result.Rows = record.Length()
	case ipcflatbuf.MessageHeaderDictionaryBatch:
		var dictionary ipcflatbuf.DictionaryBatch
		if !message.DictionaryBatch(&dictionary) {
			return MessageInfo{}, moerr.NewInvalidInput(ctx, "Arrow dictionary header is missing")
		}
		var record ipcflatbuf.RecordBatch
		if !dictionary.Data(&record) {
			return MessageInfo{}, moerr.NewInvalidInput(ctx, "Arrow dictionary data header is missing")
		}
		if err := validateRecordBatchMetadata(ctx, &record, len(metadata), bodyLength, options); err != nil {
			return MessageInfo{}, err
		}
		result.DictionaryID = dictionary.ID()
		result.IsDelta = dictionary.IsDelta()
		result.Rows = record.Length()
	}
	return result, nil
}

func validateRecordBatchMetadata(
	ctx context.Context,
	record *ipcflatbuf.RecordBatch,
	metadataBytes int,
	bodyBytes int64,
	options ValidationOptions,
) error {
	rows := record.Length()
	if rows < 0 || rows > int64(math.MaxInt) {
		return moerr.NewInvalidInputf(ctx, "Arrow IPC message has invalid row count %d", rows)
	}

	nodeCount := record.NodesLength()
	if nodeCount < 0 || nodeCount > metadataBytes/16 {
		return moerr.NewInvalidInputf(ctx, "Arrow IPC field-node count %d exceeds metadata", nodeCount)
	}
	// FieldNodes drive downstream array lengths and null allocations. Merely
	// checking the vector byte range is insufficient; every tuple is validated.
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
	// Buffer ranges are checked with subtraction to avoid offset+length overflow.
	// When compressed, the first eight bytes are an Arrow decoded-size prefix;
	// the aggregate decoded budget is enforced before decompression.
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
		if offset%8 != 0 {
			return moerr.NewInvalidInputf(ctx,
				"Arrow IPC buffer %d has unaligned buffer offset %d", index, offset)
		}
		if !options.ValidateBody {
			continue
		}
		decodedLength := length
		if compression != nil && length != 0 {
			if length < 8 {
				return moerr.NewInvalidInputf(ctx,
					"Arrow IPC compressed buffer %d is shorter than its decoded-size prefix", index)
			}
			declared := int64(binary.LittleEndian.Uint64(options.Body[int(offset) : int(offset)+8]))
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
		if decodedLength > options.MaxDecodedRecordBytes-decodedBytes {
			return moerr.NewInvalidInputf(ctx,
				"Arrow IPC decoded record body exceeds limit %d", options.MaxDecodedRecordBytes)
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
