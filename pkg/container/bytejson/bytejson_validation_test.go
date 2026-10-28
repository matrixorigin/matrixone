// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bytejson

import (
	"bytes"
	"encoding/binary"
	"math"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/internal/bytejsonvalidate"
	"github.com/stretchr/testify/require"
)

// Preserve the complete original root dispatch and scalar callback from
// 730437ed46, so both benchmarks and differential tests use the old call path.
func byteJsonTypeRankBeforeContainerString(value ByteJson) (jsonTypeRank, bool) {
	switch value.Type {
	case TpCodeLiteral:
		if len(value.Data) != 1 {
			return jsonRankUnknown, false
		}
		switch value.Data[0] {
		case LiteralNull:
			return jsonRankNull, true
		case LiteralTrue, LiteralFalse:
			return jsonRankBoolean, true
		default:
			return jsonRankUnknown, false
		}
	case TpCodeInt64, TpCodeUint64, TpCodeFloat64:
		return jsonRankNumber, isValidNumericEncoding(value)
	case TpCodeDecimal:
		_, ok := ParseNumeric(value)
		return jsonRankNumber, ok
	case TpCodeString:
		return jsonRankString, isValidByteJsonStringEncodingBeforeContainerString(value.Data)
	case TpCodeObject:
		return jsonRankObject, isValidByteJsonContainerBeforeContainerString(value)
	case TpCodeArray:
		return jsonRankArray, isValidByteJsonContainerBeforeContainerString(value)
	case TpCodeDate:
		return jsonRankDate, isValidByteJsonStringEncodingBeforeContainerString(value.Data)
	case TpCodeTime:
		return jsonRankTime, isValidByteJsonStringEncodingBeforeContainerString(value.Data)
	case TpCodeDatetime:
		return jsonRankDatetime, isValidByteJsonStringEncodingBeforeContainerString(value.Data)
	case TpCodeBlob, TpCodeOpaque, TpCodeBit:
		binaryValue, ok := binaryJSONValue(value)
		if !ok {
			return jsonRankUnknown, false
		}
		binaryValue = resolveBinaryJSONValue(binaryValue)
		if binaryValue.subtype == binaryJSONBit {
			return jsonRankBit, true
		}
		return jsonRankBlob, true
	default:
		return jsonRankUnknown, false
	}
}

func isValidByteJsonStringEncodingBeforeContainerString(data []byte) bool {
	_, ok := uvarintPayloadBeforeStringPrefix(data)
	return ok
}

func uvarintPayloadBeforeStringPrefix(data []byte) ([]byte, bool) {
	payloadLength, prefixLength := binary.Uvarint(data)
	if prefixLength <= 0 || prefixLength != uvarintSizeBeforeStringPrefix(payloadLength) ||
		payloadLength != uint64(len(data)-prefixLength) {
		return nil, false
	}
	return data[prefixLength:], true
}

func uvarintSizeBeforeStringPrefix(value uint64) int {
	size := 1
	for value >= 0x80 {
		value >>= 7
		size++
	}
	return size
}

func isValidByteJsonContainerBeforeContainerString(value ByteJson) bool {
	return bytejsonvalidate.Container(value.Type, value.Data, func(tp byte, data []byte) bool {
		_, ok := byteJsonTypeRankBeforeContainerString(ByteJson{Type: TpCode(tp), Data: data})
		return ok
	})
}

func isValidByteJsonBeforeContainerString(value ByteJson) bool {
	_, ok := byteJsonTypeRankBeforeContainerString(value)
	return ok
}

// Model the binary container tables directly: malformed scalar encodings must
// not depend on the public JSON encoder accepting or normalizing them.
func validationScalarContainer(tp TpCode, child ByteJson, copies int, alias bool) ByteJson {
	keyBytes := 0
	tableSize := headerSize + copies*valEntrySize
	if tp == TpCodeObject {
		tableSize += copies * keyEntrySize
		keyBytes = copies
	}
	payloadCopies := copies
	if alias {
		payloadCopies = 1
	}
	data := make([]byte, tableSize+keyBytes+payloadCopies*len(child.Data))
	endian.PutUint32(data, uint32(copies))
	endian.PutUint32(data[docSizeOff:], uint32(len(data)))
	for i := 0; i < copies; i++ {
		valueEntry := headerSize + i*valEntrySize
		if tp == TpCodeObject {
			keyEntry := headerSize + i*keyEntrySize
			endian.PutUint32(data[keyEntry:], uint32(tableSize+i))
			endian.PutUint16(data[keyEntry+keyOriginOff:], 1)
			data[tableSize+i] = byte('a' + i)
			valueEntry = headerSize + copies*keyEntrySize + i*valEntrySize
		}
		offset := tableSize + keyBytes
		if !alias {
			offset += i * len(child.Data)
		}
		data[valueEntry] = child.Type
		endian.PutUint32(data[valueEntry+valTypeSize:], uint32(offset))
		copy(data[offset:], child.Data)
	}
	return ByteJson{Type: tp, Data: data}
}

func TestIsValidByteJsonContainerStringFraming(t *testing.T) {
	for _, tc := range []struct {
		name            string
		data            []byte
		root, container bool
	}{
		{"empty", []byte{0}, true, true},
		{"one byte", []byte{1, 'x'}, true, true},
		{"maximum one byte prefix", append([]byte{127}, bytes.Repeat([]byte{'x'}, 127)...), true, true},
		{"two byte prefix", append([]byte{0x80, 1}, bytes.Repeat([]byte{'x'}, 128)...), true, true},
		{"one byte prefix truncated", append([]byte{127}, bytes.Repeat([]byte{'x'}, 126)...), false, false},
		{"two byte prefix truncated", append([]byte{0x80, 1}, bytes.Repeat([]byte{'x'}, 127)...), false, false},
		{"opaque string bytes", []byte{1, 0xff}, true, true},
		{"overlong empty", []byte{0x80, 0}, false, false},
		{"overlong nonempty", []byte{0x81, 0, 'x'}, false, false},
		{"unterminated", []byte{0x80}, false, false},
		{"truncated", []byte{2, 'x'}, false, false},
		{"overflow", append(bytes.Repeat([]byte{0xff}, 9), 2), false, false},
		{"missing", nil, false, false},
		// A container's entry has an implicit scalar extent. Unreferenced bytes
		// after that extent remain legal, while a root string must consume all data.
		{"unreferenced tail", []byte{1, 'x', 'y'}, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, tp := range []TpCode{TpCodeDate, TpCodeTime, TpCodeDatetime} {
				value := ByteJson{Type: tp, Data: tc.data}
				require.Equal(t, tc.root, IsValidByteJson(value), "root type%d", tp)
				require.Equal(t, isValidByteJsonBeforeContainerString(value), IsValidByteJson(value))
			}
			root := ByteJson{Type: TpCodeString, Data: tc.data}
			require.Equal(t, tc.root, IsValidByteJson(root), "primitive root")
			require.Equal(t, isValidByteJsonBeforeContainerString(root), IsValidByteJson(root))
			for _, tp := range []TpCode{TpCodeArray, TpCodeObject} {
				container := validationScalarContainer(tp, root, 1, false)
				require.Equal(t, tc.container, IsValidByteJson(container), "container type%d", tp)
				require.Equal(t, isValidByteJsonBeforeContainerString(container), IsValidByteJson(container))
			}
		})
	}
	for _, bad := range []ByteJson{
		makeFloatJSONFromBits(math.Float64bits(math.NaN())),
		{Type: TpCodeDecimal, Data: []byte{1, 'x'}},
		{Type: TpCodeBlob, Data: []byte{2, 'x'}},
		{Type: 0xfe, Data: []byte{1, 'x'}},
	} {
		container := validationScalarContainer(TpCodeArray, bad, 1, false)
		require.False(t, IsValidByteJson(container), "scalar type%d", bad.Type)
		require.Equal(t, isValidByteJsonBeforeContainerString(container), IsValidByteJson(container))
	}
	// Legacy BLOB framing accepts non-base64 payloads in the raw fallback domain.
	// Keep that policy distinct from a truncated binary length prefix above.
	legacy := makeBinaryJson(TpCodeBlob, []byte("not-base64"))
	for _, value := range []ByteJson{legacy, validationScalarContainer(TpCodeArray, legacy, 1, false)} {
		require.True(t, isValidByteJsonBeforeContainerString(value))
		require.True(t, IsValidByteJson(value))
	}
	str := ByteJson{Type: TpCodeString, Data: []byte{1, 'x'}}
	for _, alias := range []bool{false, true} {
		container := validationScalarContainer(TpCodeArray, str, 2, alias)
		require.Equal(t, !alias, IsValidByteJson(container), "alias=%v", alias)
		require.Equal(t, isValidByteJsonBeforeContainerString(container), IsValidByteJson(container))
	}
	nested := str
	for i := 0; i < JSONDocumentMaxNestingDepth; i++ {
		nested = validationScalarContainer(TpCodeArray, nested, 1, false)
	}
	require.True(t, IsValidByteJson(nested), "maximum container depth")
	require.True(t, isValidByteJsonBeforeContainerString(nested))
	nested = validationScalarContainer(TpCodeArray, nested, 1, false)
	require.False(t, IsValidByteJson(nested), "over maximum container depth")
	require.False(t, isValidByteJsonBeforeContainerString(nested))
}

func BenchmarkIsValidByteJsonContainer(b *testing.B) {
	denseStrings := make([]any, 64)
	denseStrings128 := make([]any, 64)
	denseStrings1024 := make([]any, 64)
	denseNumbers := make([]any, 64)
	mixed := make([]any, 64)
	for i := range denseStrings {
		denseStrings[i] = strings.Repeat("x", 8)
		denseStrings128[i] = strings.Repeat("x", 128)
		denseStrings1024[i] = strings.Repeat("x", 1024)
		denseNumbers[i] = int64(i)
		if i%2 == 0 {
			mixed[i] = denseStrings[i]
		} else {
			mixed[i] = denseNumbers[i]
		}
	}
	makeValue := func(v any) ByteJson {
		out, err := CreateByteJSON(v)
		if err != nil {
			b.Fatal(err)
		}
		return out
	}
	deep := makeValue("x")
	for i := 0; i < 32; i++ {
		deep = validationScalarContainer(TpCodeArray, deep, 1, false)
	}
	object := makeValue(map[string]any{"attr": map[string]any{"release_ring": "stable", "workflow_variant": "tool_first", "prompt_template_version": "pt_2026_03_2"}, "provider": map[string]any{"cache_hit": true, "stop_reason": "end_turn"}, "latency": int64(256)})
	for _, scenario := range []struct {
		name  string
		value ByteJson
	}{
		{"DenseStrings64", makeValue(denseStrings)},
		{"DenseStrings128x64", makeValue(denseStrings128)},
		{"DenseStrings1024x64", makeValue(denseStrings1024)},
		{"DenseNumbers64", makeValue(denseNumbers)},
		{"Mixed64", makeValue(mixed)},
		{"SmallStringArray", makeValue([]any{"x"})},
		{"SmallNumberArray", makeValue([]any{int64(1)})},
		{"SmallObject", makeValue(map[string]any{"a": "x", "b": int64(1)})},
		{"AgentObject", object},
		{"Depth32", deep},
		{"RootString127", makeValue(strings.Repeat("x", 127))},
		{"RootString128", makeValue(strings.Repeat("x", 128))},
		{"RootStringLarge", makeValue(strings.Repeat("x", 256<<10))},
		{"RootDate", makeBinaryJson(TpCodeDate, []byte("2026-10-27"))},
	} {
		for _, implementation := range []struct {
			name string
			fn   func(ByteJson) bool
		}{
			{"Original", isValidByteJsonBeforeContainerString}, {"Candidate", IsValidByteJson},
		} {
			b.Run(scenario.name+"/"+implementation.name, func(b *testing.B) {
				b.SetBytes(int64(len(scenario.value.Data) + 1))
				b.ReportAllocs()
				for b.Loop() {
					if !implementation.fn(scenario.value) {
						b.Fatal("valid fixture rejected")
					}
				}
			})
		}
	}
}
