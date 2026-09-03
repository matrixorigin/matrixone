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
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/stretchr/testify/require"
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
	binary.LittleEndian.PutUint32(metadata, math.MaxUint32)
	_, err = InspectMessage(context.Background(), malformed, ValidationOptions{
		MaxBodyBytes:          0,
		BodyEnvelopeBytes:     0,
		MaxDecodedRecordBytes: 1,
	})
	require.ErrorContains(t, err, "root is out of bounds")
}

func firstStreamMetadata(t *testing.T, stream []byte) []byte {
	t.Helper()
	require.GreaterOrEqual(t, len(stream), 8)
	require.Equal(t, ContinuationToken, binary.LittleEndian.Uint32(stream))
	length := int(binary.LittleEndian.Uint32(stream[4:]))
	require.Positive(t, length)
	require.LessOrEqual(t, length, len(stream)-8)
	return stream[:8+length]
}
