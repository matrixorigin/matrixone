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

package bytejson

import (
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateStoredJSONDocumentRejectsMalformedRanges(t *testing.T) {
	t.Run("truncated container header", func(t *testing.T) {
		err := ValidateStoredJSONDocument(ByteJson{Type: TpCodeArray, Data: make([]byte, headerSize-1)})
		require.Error(t, err)
	})

	t.Run("unknown child type", func(t *testing.T) {
		data := make([]byte, headerSize+valEntrySize)
		endian.PutUint32(data, 1)
		endian.PutUint32(data[docSizeOff:], uint32(len(data)))
		data[headerSize] = 0xff
		endian.PutUint32(data[headerSize+valTypeSize:], uint32(len(data)))

		err := ValidateStoredJSONDocument(ByteJson{Type: TpCodeArray, Data: data})
		require.Error(t, err)
	})

	t.Run("value offset in entry table", func(t *testing.T) {
		data := make([]byte, headerSize+valEntrySize+numberSize)
		endian.PutUint32(data, 1)
		endian.PutUint32(data[docSizeOff:], uint32(len(data)))
		data[headerSize] = TpCodeInt64
		endian.PutUint32(data[headerSize+valTypeSize:], uint32(headerSize))

		err := ValidateStoredJSONDocument(ByteJson{Type: TpCodeArray, Data: data})
		require.Error(t, err)
	})

	t.Run("truncated string payload", func(t *testing.T) {
		data := make([]byte, headerSize+valEntrySize+1)
		endian.PutUint32(data, 1)
		endian.PutUint32(data[docSizeOff:], uint32(len(data)))
		data[headerSize] = TpCodeString
		payloadOffset := headerSize + valEntrySize
		endian.PutUint32(data[headerSize+valTypeSize:], uint32(payloadOffset))
		data[payloadOffset] = 2

		err := ValidateStoredJSONDocument(ByteJson{Type: TpCodeArray, Data: data})
		require.Error(t, err)
	})

	t.Run("invalid string utf8", func(t *testing.T) {
		data := make([]byte, headerSize+valEntrySize+2)
		endian.PutUint32(data, 1)
		endian.PutUint32(data[docSizeOff:], uint32(len(data)))
		data[headerSize] = TpCodeString
		payloadOffset := headerSize + valEntrySize
		endian.PutUint32(data[headerSize+valTypeSize:], uint32(payloadOffset))
		data[payloadOffset] = 1
		data[payloadOffset+1] = 0xff

		err := ValidateStoredJSONDocument(ByteJson{Type: TpCodeArray, Data: data})
		require.Error(t, err)
	})

	t.Run("self reference into header", func(t *testing.T) {
		data := make([]byte, headerSize+valEntrySize)
		endian.PutUint32(data, 1)
		endian.PutUint32(data[docSizeOff:], uint32(len(data)))
		data[headerSize] = TpCodeArray
		endian.PutUint32(data[headerSize+valTypeSize:], 0)

		err := ValidateStoredJSONDocument(ByteJson{Type: TpCodeArray, Data: data})
		require.Error(t, err)
	})

	t.Run("overlapping value ranges", func(t *testing.T) {
		data := make([]byte, headerSize+2*valEntrySize+numberSize)
		endian.PutUint32(data, 2)
		endian.PutUint32(data[docSizeOff:], uint32(len(data)))
		for i := 0; i < 2; i++ {
			entry := headerSize + i*valEntrySize
			data[entry] = TpCodeInt64
			endian.PutUint32(data[entry+valTypeSize:], uint32(headerSize+2*valEntrySize))
		}

		err := ValidateStoredJSONDocument(ByteJson{Type: TpCodeArray, Data: data})
		require.Error(t, err)
	})

	t.Run("non-finite float", func(t *testing.T) {
		data := make([]byte, numberSize)
		endian.PutUint64(data, math.Float64bits(math.NaN()))

		err := ValidateStoredJSONDocument(ByteJson{Type: TpCodeFloat64, Data: data})
		require.Error(t, err)
	})

	t.Run("object key and value overlap", func(t *testing.T) {
		data := make([]byte, headerSize+keyEntrySize+valEntrySize+numberSize+4)
		endian.PutUint32(data, 1)
		endian.PutUint32(data[docSizeOff:], uint32(len(data)))
		keyOffset := headerSize + keyEntrySize + valEntrySize
		endian.PutUint32(data[headerSize:], uint32(keyOffset))
		endian.PutUint16(data[headerSize+keyOriginOff:], 4)
		data[headerSize+keyEntrySize] = TpCodeInt64
		endian.PutUint32(data[headerSize+keyEntrySize+valTypeSize:], uint32(keyOffset+2))

		err := ValidateStoredJSONDocument(ByteJson{Type: TpCodeObject, Data: data})
		require.Error(t, err)
	})
}

func TestValidateStoredJSONDocumentRejectsExcessiveDepth(t *testing.T) {
	atLimitInput := strings.Repeat("[", JSONDocumentMaxNestingDepth) + "1" +
		strings.Repeat("]", JSONDocumentMaxNestingDepth)
	atLimit, err := ParseFromByteSlice([]byte(atLimitInput))
	require.NoError(t, err)
	require.NoError(t, ValidateStoredJSONDocument(atLimit))

	input := strings.Repeat("[", JSONDocumentMaxNestingDepth+1) + "1" +
		strings.Repeat("]", JSONDocumentMaxNestingDepth+1)
	document, err := ParseFromByteSlice([]byte(input))
	require.NoError(t, err)

	err = ValidateStoredJSONDocument(document)
	require.Error(t, err)
	require.True(t, IsJSONDocumentDepthError(err))
}

func TestValidateStoredJSONDocumentAcceptsLargeDocument(t *testing.T) {
	var builder strings.Builder
	builder.WriteByte('[')
	for i := 0; i < 1024; i++ {
		if i > 0 {
			builder.WriteByte(',')
		}
		builder.WriteString("1")
	}
	builder.WriteByte(']')
	document, err := ParseFromByteSlice([]byte(builder.String()))
	require.NoError(t, err)
	require.NoError(t, ValidateStoredJSONDocument(document))
}

func TestStoredJSONValidationWorkBound(t *testing.T) {
	var work uint64
	require.NoError(t, chargeStoredJSONValidationWork(&work, 4, 4))
	require.Error(t, chargeStoredJSONValidationWork(&work, 4, 1))
}
