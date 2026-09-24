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

package arrowipc

import (
	"bytes"
	"context"
	"encoding/binary"
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/arrowipc/ipcflatbuf"
)

func TestMetadataAcceptsRawAndFramedMessages(t *testing.T) {
	raw := []byte{0, 0, 0, 0, 9}
	metadata, err := Metadata(context.Background(), raw, DefaultMaxMetadataBytes)
	require.NoError(t, err)
	require.Equal(t, raw, metadata)

	legacy := []byte{1, 0, 0, 0, 7}
	metadata, err = Metadata(context.Background(), legacy, DefaultMaxMetadataBytes)
	require.NoError(t, err)
	require.Equal(t, []byte{7}, metadata)

	continuation := make([]byte, 9)
	binary.LittleEndian.PutUint32(continuation, ContinuationToken)
	binary.LittleEndian.PutUint32(continuation[4:], 1)
	metadata, err = Metadata(context.Background(), continuation, DefaultMaxMetadataBytes)
	require.NoError(t, err)
	require.Equal(t, []byte{0}, metadata)
}

func TestMetadataRejectsMalformedOrOversizedFraming(t *testing.T) {
	_, err := Metadata(context.Background(), nil, DefaultMaxMetadataBytes)
	require.ErrorContains(t, err, "truncated")
	_, err = Metadata(context.Background(), make([]byte, 5), 4)
	require.ErrorContains(t, err, "exceeds limit")
	_, err = Metadata(context.Background(), []byte{0xff, 0xff, 0xff, 0xff}, DefaultMaxMetadataBytes)
	require.ErrorContains(t, err, "continuation header")

	continuation := make([]byte, 9)
	binary.LittleEndian.PutUint32(continuation, math.MaxUint32)
	binary.LittleEndian.PutUint32(continuation[4:], 2)
	_, err = Metadata(context.Background(), continuation, DefaultMaxMetadataBytes)
	require.ErrorContains(t, err, "length is invalid")

	// A consumer cannot turn the shared structural limit into an allocation
	// escape hatch merely by supplying a larger local option.
	_, err = Metadata(context.Background(), make([]byte, DefaultMaxMetadataBytes+1), 2*DefaultMaxMetadataBytes)
	require.ErrorContains(t, err, "exceeds limit 1048576")
}

func TestInspectMessageValidatesGeneratedSchemaBeforeConsumerPolicy(t *testing.T) {
	// Generate through Arrow-Go so this package test covers the public IPC wire
	// shape without depending on a hand-authored FlatBuffers fixture.
	schema := arrow.NewSchema([]arrow.Field{{
		Name: "value", Type: arrow.PrimitiveTypes.Int64,
	}}, nil)
	var stream bytes.Buffer
	writer := ipc.NewWriter(&stream, ipc.WithSchema(schema))
	require.NoError(t, writer.Close())

	wire := firstStreamMetadata(t, stream.Bytes())
	info, err := InspectMessage(context.Background(), wire, ValidationOptions{
		MaxBodyBytes:          0,
		BodyEnvelopeBytes:     0,
		MaxDecodedRecordBytes: 1,
	})
	require.NoError(t, err)
	require.Equal(t, MessageHeaderSchema, info.HeaderType)
	require.Zero(t, info.BodyBytes)

	malformed := append([]byte(nil), wire...)
	metadata, err := Metadata(context.Background(), malformed, DefaultMaxMetadataBytes)
	require.NoError(t, err)
	// Keep the malformed root distinct from the continuation token so the
	// framing parser reaches the FlatBuffers root bounds check.
	binary.LittleEndian.PutUint32(metadata, math.MaxUint32-1)
	_, err = InspectMessage(context.Background(), malformed, ValidationOptions{
		MaxBodyBytes:          0,
		BodyEnvelopeBytes:     0,
		MaxDecodedRecordBytes: 1,
	})
	require.ErrorContains(t, err, "root is out of bounds")
}

func TestInspectMessageRejectsInvalidBodyEnvelopeSentinel(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{
		Name: "value", Type: arrow.PrimitiveTypes.Int64,
	}}, nil)
	var stream bytes.Buffer
	writer := ipc.NewWriter(&stream, ipc.WithSchema(schema))
	require.NoError(t, writer.Close())
	wire := firstStreamMetadata(t, stream.Bytes())

	_, err := InspectMessage(context.Background(), wire, ValidationOptions{
		MaxBodyBytes:          0,
		BodyEnvelopeBytes:     -2,
		MaxDecodedRecordBytes: 1,
	})
	require.ErrorContains(t, err, "body envelope")
}

func TestInspectMessageRejectsUnsupportedHeaderType(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{
		Name: "value", Type: arrow.PrimitiveTypes.Int64,
	}}, nil)
	var stream bytes.Buffer
	writer := ipc.NewWriter(&stream, ipc.WithSchema(schema))
	require.NoError(t, writer.Close())
	wire := firstStreamMetadata(t, stream.Bytes())
	metadata, err := Metadata(context.Background(), wire, DefaultMaxMetadataBytes)
	require.NoError(t, err)

	root := binary.LittleEndian.Uint32(metadata)
	message := flatbuffers.Table{Bytes: metadata, Pos: flatbuffers.UOffsetT(root)}
	headerOffset := flatbuffers.UOffsetT(message.Offset(6))
	require.NotZero(t, headerOffset)
	metadata[headerOffset+message.Pos] = 4 // Tensor, not an IPC scan message.

	_, err = InspectMessage(context.Background(), metadata, ValidationOptions{
		MaxBodyBytes:          0,
		BodyEnvelopeBytes:     0,
		MaxDecodedRecordBytes: 1,
	})
	require.ErrorContains(t, err, "unsupported Arrow IPC message header")
}

func TestInspectMessageRejectsSchemaBody(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{
		Name: "value", Type: arrow.PrimitiveTypes.Int64,
	}}, nil)
	var stream bytes.Buffer
	writer := ipc.NewWriter(&stream, ipc.WithSchema(schema))
	alloc := memory.NewGoAllocator()
	values := array.NewInt64Builder(alloc)
	values.Append(1)
	record := array.NewRecordBatch(schema, []arrow.Array{values.NewArray()}, 1)
	values.Release()
	require.NoError(t, writer.Write(record))
	record.Release()
	require.NoError(t, writer.Close())
	metadata := streamMetadataAt(t, stream.Bytes(), 1)

	root := binary.LittleEndian.Uint32(metadata)
	message := flatbuffers.Table{Bytes: metadata, Pos: flatbuffers.UOffsetT(root)}
	headerOffset := flatbuffers.UOffsetT(message.Offset(6))
	require.NotZero(t, headerOffset)
	metadata[headerOffset+message.Pos] = byte(ipcflatbuf.MessageHeaderSchema)

	_, err := InspectMessage(context.Background(), metadata, ValidationOptions{
		MaxBodyBytes:          8,
		BodyEnvelopeBytes:     8,
		MaxDecodedRecordBytes: 1,
	})
	require.ErrorContains(t, err, "schema message body")
}

func TestInspectMessageRejectsUnalignedBufferOffset(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{
		Name: "value", Type: arrow.PrimitiveTypes.Int64,
	}}, nil)
	var stream bytes.Buffer
	writer := ipc.NewWriter(&stream, ipc.WithSchema(schema))
	alloc := memory.NewGoAllocator()
	values := array.NewInt64Builder(alloc)
	values.Append(1)
	record := array.NewRecordBatch(schema, []arrow.Array{values.NewArray()}, 1)
	values.Release()
	require.NoError(t, writer.Write(record))
	record.Release()
	require.NoError(t, writer.Close())
	metadata := streamMetadataAt(t, stream.Bytes(), 1)

	root := binary.LittleEndian.Uint32(metadata)
	messageTable := flatbuffers.Table{Bytes: metadata, Pos: flatbuffers.UOffsetT(root)}
	headerOffset := flatbuffers.UOffsetT(messageTable.Offset(8))
	require.NotZero(t, headerOffset)
	var recordTable flatbuffers.Table
	messageTable.Union(&recordTable, headerOffset)
	buffersOffset := flatbuffers.UOffsetT(recordTable.Offset(8))
	require.NotZero(t, buffersOffset)
	bufferPos := recordTable.Vector(buffersOffset)
	binary.LittleEndian.PutUint64(recordTable.Bytes[bufferPos:], 1)

	bodyLength := ipcflatbuf.GetRootAsMessage(metadata).BodyLength()
	_, err := InspectMessage(context.Background(), metadata, ValidationOptions{
		MaxBodyBytes:          bodyLength,
		BodyEnvelopeBytes:     bodyLength,
		MaxDecodedRecordBytes: 1,
	})
	require.ErrorContains(t, err, "unaligned buffer offset")
}

func TestInspectMessageRejectsOverlappingBufferRanges(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "left", Type: arrow.PrimitiveTypes.Int64},
		{Name: "right", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	var stream bytes.Buffer
	writer := ipc.NewWriter(&stream, ipc.WithSchema(schema))
	alloc := memory.NewGoAllocator()
	left := array.NewInt64Builder(alloc)
	left.Append(1)
	right := array.NewInt64Builder(alloc)
	right.Append(2)
	record := array.NewRecordBatch(schema, []arrow.Array{left.NewArray(), right.NewArray()}, 1)
	left.Release()
	right.Release()
	require.NoError(t, writer.Write(record))
	record.Release()
	require.NoError(t, writer.Close())
	metadata := streamMetadataAt(t, stream.Bytes(), 1)

	root := binary.LittleEndian.Uint32(metadata)
	messageTable := flatbuffers.Table{Bytes: metadata, Pos: flatbuffers.UOffsetT(root)}
	headerOffset := flatbuffers.UOffsetT(messageTable.Offset(8))
	require.NotZero(t, headerOffset)
	var recordTable flatbuffers.Table
	messageTable.Union(&recordTable, headerOffset)
	buffersOffset := flatbuffers.UOffsetT(recordTable.Offset(8))
	require.NotZero(t, buffersOffset)
	bufferPos := recordTable.Vector(buffersOffset)
	firstOffset := binary.LittleEndian.Uint64(recordTable.Bytes[bufferPos+16:])
	firstLength := binary.LittleEndian.Uint64(recordTable.Bytes[bufferPos+24:])
	require.Greater(t, firstLength, uint64(0))
	// Point the second non-empty logical buffer at the first buffer. Both
	// ranges remain aligned and in bounds, but they no longer describe the
	// serialized RecordBatch layout.
	binary.LittleEndian.PutUint64(recordTable.Bytes[bufferPos+48:], firstOffset)

	bodyLength := ipcflatbuf.GetRootAsMessage(metadata).BodyLength()
	_, err := InspectMessage(context.Background(), metadata, ValidationOptions{
		MaxBodyBytes:          bodyLength,
		BodyEnvelopeBytes:     bodyLength,
		MaxDecodedRecordBytes: 1,
	})
	require.ErrorContains(t, err, "overlaps buffer")
}

func firstStreamMetadata(t *testing.T, stream []byte) []byte {
	return streamMetadataAt(t, stream, 0)
}

func streamMetadataAt(t *testing.T, stream []byte, target int) []byte {
	t.Helper()
	position := 0
	for index := 0; index <= target; index++ {
		require.LessOrEqual(t, position+8, len(stream))
		require.Equal(t, ContinuationToken, binary.LittleEndian.Uint32(stream[position:]))
		length := int(binary.LittleEndian.Uint32(stream[position+4:]))
		require.Positive(t, length)
		metadataStart := position + 8
		metadataEnd := metadataStart + length
		require.LessOrEqual(t, metadataEnd, len(stream))
		if index == target {
			return stream[metadataStart:metadataEnd]
		}
		metadata := stream[metadataStart:metadataEnd]
		message := ipcflatbuf.GetRootAsMessage(metadata)
		bodyLength := message.BodyLength()
		require.GreaterOrEqual(t, bodyLength, int64(0))
		position = metadataStart + (length+7)/8*8 + (int(bodyLength)+7)/8*8
	}
	t.Fatalf("stream message %d is missing", target)
	return nil
}
