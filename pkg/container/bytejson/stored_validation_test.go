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
	"bytes"
	"fmt"
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

	t.Run("out-of-order value ranges", func(t *testing.T) {
		data := make([]byte, headerSize+2*valEntrySize+2*numberSize)
		endian.PutUint32(data, 2)
		endian.PutUint32(data[docSizeOff:], uint32(len(data)))
		payloadOffset := headerSize + 2*valEntrySize
		for i := 0; i < 2; i++ {
			entry := headerSize + i*valEntrySize
			data[entry] = TpCodeInt64
		}
		endian.PutUint32(data[headerSize+valTypeSize:], uint32(payloadOffset+numberSize))
		endian.PutUint32(data[headerSize+valEntrySize+valTypeSize:], uint32(payloadOffset))

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

	t.Run("object keys must be sorted", func(t *testing.T) {
		data := make([]byte, headerSize+2*keyEntrySize+2*valEntrySize+2+2*numberSize)
		endian.PutUint32(data, 2)
		endian.PutUint32(data[docSizeOff:], uint32(len(data)))
		keyOffset := headerSize + 2*keyEntrySize + 2*valEntrySize
		for i, key := range [][]byte{[]byte("z"), []byte("a")} {
			entry := headerSize + i*keyEntrySize
			endian.PutUint32(data[entry:], uint32(keyOffset+i))
			endian.PutUint16(data[entry+keyOriginOff:], 1)
			data[keyOffset+i] = key[0]
		}
		valueTable := headerSize + 2*keyEntrySize
		for i := 0; i < 2; i++ {
			entry := valueTable + i*valEntrySize
			data[entry] = TpCodeInt64
			endian.PutUint32(data[entry+valTypeSize:], uint32(keyOffset+2+i*numberSize))
		}

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
	work = ^uint64(0)
	require.Error(t, chargeStoredJSONValidationWork(&work, ^uint64(0)-1, 0))
	require.Error(t, chargeStoredJSONValidationWork(&work, ^uint64(0), 1))
}

func TestValidateStoredJSONDocumentPreservesScalarGuards(t *testing.T) {
	for _, tp := range []byte{TpCodeLiteral, TpCodeInt64, TpCodeUint64, TpCodeFloat64,
		TpCodeString, TpCodeDecimal, TpCodeDate, TpCodeTime, TpCodeDatetime,
		TpCodeBlob, TpCodeOpaque, TpCodeBit, 0xff} {
		for _, data := range [][]byte{nil, {0}, {LiteralFalse}, {0, 0}, {0x80, 0},
			{1, 'x'}, {1, 0xff}, {2, '1', '2'}, make([]byte, numberSize), make([]byte, numberSize+1)} {
			t.Run(fmt.Sprintf("type=%x/data=%x", tp, data), func(t *testing.T) {
				value := ByteJson{Type: tp, Data: data}
				// Existing scalar semantics plus UTF-8 is the conjunction that
				// admission required before traversal consolidation.
				expected := IsValidByteJson(value)
				if tp == TpCodeString && len(data) == 2 && data[1] == 0xff {
					expected = false
				}
				require.Equal(t, expected, ValidateStoredJSONDocument(value) == nil)
				childSize := len(data)
				if tp == TpCodeLiteral {
					if len(data) != 1 {
						return // Inline literals cannot encode extra payload bytes.
					}
					childSize = 0
				}
				array := make([]byte, headerSize+valEntrySize+childSize)
				endian.PutUint32(array, 1)
				endian.PutUint32(array[docSizeOff:], uint32(len(array)))
				array[headerSize] = tp
				if tp == TpCodeLiteral {
					array[headerSize+valTypeSize] = data[0]
				} else {
					endian.PutUint32(array[headerSize+valTypeSize:], headerSize+valEntrySize)
					copy(array[headerSize+valEntrySize:], data)
				}
				// A child's exact encoding may leave unused trailing bytes in
				// the parent. Match the existing structural validator there.
				expected = IsValidByteJson(ByteJson{Type: TpCodeArray, Data: array})
				if tp == TpCodeString && len(data) == 2 && data[1] == 0xff {
					expected = false
				}
				require.Equal(t, expected, ValidateStoredJSONDocument(ByteJson{Type: TpCodeArray, Data: array}) == nil)
			})
		}
	}
	for _, floating := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		data := make([]byte, numberSize)
		endian.PutUint64(data, math.Float64bits(floating))
		require.Error(t, ValidateStoredJSONDocument(ByteJson{Type: TpCodeFloat64, Data: data}))
	}
}

func TestValidateStoredJSONDocumentDoesNotAllocateFrames(t *testing.T) {
	for _, count := range []int{16, 4096} {
		document, err := ParseFromString(`{"keep":1,"large":[` + strings.Repeat("1,", count-1) + "1]}")
		require.NoError(t, err)
		require.NoError(t, ValidateStoredJSONDocument(document))
		var validationErr error
		allocs := testing.AllocsPerRun(10, func() { validationErr = ValidateStoredJSONDocument(document) })
		require.NoError(t, validationErr)
		require.Zero(t, allocs, "width-dependent validation slices must not return")
	}
}

func TestValidateStoredJSONDocumentCanonicalKeyGuards(t *testing.T) {
	for _, mutation := range []string{"duplicate", "invalid utf8", "overlapping key ranges", "overflowing count"} {
		t.Run(mutation, func(t *testing.T) {
			document, err := ParseFromString(`{"a":1,"ab":2}`)
			require.NoError(t, err)
			require.NoError(t, ValidateStoredJSONDocument(document))
			firstOffset := endian.Uint32(document.Data[headerSize:])
			secondEntry := headerSize + keyEntrySize
			switch mutation {
			case "duplicate":
				endian.PutUint16(document.Data[secondEntry+keyOriginOff:], 1)
			case "invalid utf8":
				document.Data[firstOffset] = 0xff
			case "overlapping key ranges":
				// Bytes "ab" are already present at the second key. Make
				// the first key its prefix: sorting passes, ranges do not.
				secondOffset := endian.Uint32(document.Data[secondEntry:])
				endian.PutUint32(document.Data[headerSize:], secondOffset)
			case "overflowing count":
				endian.PutUint32(document.Data, ^uint32(0))
			}
			require.Error(t, ValidateStoredJSONDocument(document))
		})
	}
}

func TestValidateStoredJSONDocumentHistoricalTypedScalars(t *testing.T) {
	// Fixed bytes and explicit outcomes are independent of IsValidByteJson.
	// These are the uvarint/typed layouts of the existing SQL and binary
	// producers, including binary fallback and non-semantic temporal checks.
	for _, test := range []struct {
		name  string
		value ByteJson
		valid bool
	}{
		{"null", ByteJson{TpCodeLiteral, []byte{LiteralNull}}, true},
		{"integer", ByteJson{TpCodeInt64, []byte{1, 0, 0, 0, 0, 0, 0, 0}}, true},
		{"uint max", ByteJson{TpCodeUint64, bytes.Repeat([]byte{0xff}, 8)}, true},
		{"float one", ByteJson{TpCodeFloat64, []byte{0, 0, 0, 0, 0, 0, 0xf0, 0x3f}}, true},
		{"string", ByteJson{TpCodeString, []byte{1, 'x'}}, true},
		{"decimal", ByteJson{TpCodeDecimal, []byte("\x06123.45")}, true},
		{"decimal exponent", ByteJson{TpCodeDecimal, []byte("\x091e1000000")}, true},
		{"date", ByteJson{TpCodeDate, []byte("\x0a2024-01-01")}, true},
		{"time", ByteJson{TpCodeTime, []byte("\x0812:34:56")}, true},
		{"datetime", ByteJson{TpCodeDatetime, []byte("\x132024-01-01 12:34:56")}, true},
		{"temporal encoding only", ByteJson{TpCodeDate, []byte("\x08not-date")}, true},
		{"opaque raw bytes", ByteJson{TpCodeOpaque, []byte{2, 0, 0xff}}, true},
		{"bit raw bytes", ByteJson{TpCodeBit, []byte{1, 0xff}}, true},
		{"legacy blob", ByteJson{TpCodeBlob, []byte("\x04AQ==")}, true},
		{"legacy blob fallback", ByteJson{TpCodeBlob, []byte("\x0anot-base64")}, true},
		{"legacy opaque fallback", ByteJson{TpCodeBlob, []byte("\x18base64:type16:not-base64")}, true},
		{"empty decimal", ByteJson{TpCodeDecimal, []byte{0}}, false},
		{"nonnumeric decimal", ByteJson{TpCodeDecimal, []byte{1, 'x'}}, false},
		{"nonminimal decimal", ByteJson{TpCodeDecimal, []byte{0x81, 0, '1'}}, false},
		{"nonminimal date", ByteJson{TpCodeDate, []byte{0x81, 0, 'x'}}, false},
		{"nonminimal opaque", ByteJson{TpCodeOpaque, []byte{0x81, 0, 0xff}}, false},
		{"truncated bit", ByteJson{TpCodeBit, []byte{2, 0xff}}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			before := bytes.Clone(test.value.Data)
			err := ValidateStoredJSONDocument(test.value) // No vector admission.
			if test.valid {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			require.Equal(t, before, test.value.Data)
			array := make([]byte, headerSize+valEntrySize)
			endian.PutUint32(array, 1)
			array[headerSize] = test.value.Type
			if test.value.Type == TpCodeLiteral {
				array[headerSize+valTypeSize] = test.value.Data[0]
			} else {
				endian.PutUint32(array[headerSize+valTypeSize:], uint32(len(array)))
				array = append(array, test.value.Data...)
			}
			endian.PutUint32(array[docSizeOff:], uint32(len(array)))
			err = ValidateStoredJSONDocument(ByteJson{Type: TpCodeArray, Data: array})
			if test.valid {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}
