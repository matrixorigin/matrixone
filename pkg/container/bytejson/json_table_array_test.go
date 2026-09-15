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

package bytejson

import (
	"context"
	"encoding/binary"
	"math"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

func parseBuilderValue(t *testing.T, text string) ByteJson {
	t.Helper()
	value, err := ParseFromString(text)
	require.NoError(t, err)
	return value
}

func aliasedInt64ArrayForStoragePreflight() ByteJson {
	data := make([]byte, headerSize+2*valEntrySize+numberSize)
	binary.LittleEndian.PutUint32(data[:4], 2)
	binary.LittleEndian.PutUint32(data[docSizeOff:], uint32(len(data)))
	for i := 0; i < 2; i++ {
		entryOffset := headerSize + i*valEntrySize
		data[entryOffset] = byte(TpCodeInt64)
		binary.LittleEndian.PutUint32(data[entryOffset+valTypeSize:], uint32(headerSize+2*valEntrySize))
	}
	binary.LittleEndian.PutUint64(data[headerSize+2*valEntrySize:], 1)
	return ByteJson{Type: TpCodeArray, Data: data}
}

func aliasedNestedArrayForStoragePreflight(depth int) ByteJson {
	value := aliasedInt64ArrayForStoragePreflight()
	for i := 1; i < depth; i++ {
		data := make([]byte, headerSize+2*valEntrySize+len(value.Data))
		binary.LittleEndian.PutUint32(data[:4], 2)
		binary.LittleEndian.PutUint32(data[docSizeOff:], uint32(len(data)))
		for entry := 0; entry < 2; entry++ {
			entryOffset := headerSize + entry*valEntrySize
			data[entryOffset] = byte(TpCodeArray)
			binary.LittleEndian.PutUint32(data[entryOffset+valTypeSize:], uint32(headerSize+2*valEntrySize))
		}
		copy(data[headerSize+2*valEntrySize:], value.Data)
		value = ByteJson{Type: TpCodeArray, Data: data}
	}
	return value
}

func TestStoragePreflightRejectsAliasedSerializedWork(t *testing.T) {
	value := aliasedInt64ArrayForStoragePreflight()
	_, err := StorageCompatibleDataSizeWithLimit(context.Background(), value, math.MaxInt)
	require.Error(t, err)
}

func TestStoragePreflightBoundsAliasedNestedWork(t *testing.T) {
	value := aliasedNestedArrayForStoragePreflight(16)
	require.Equal(t, 296, len(value.Data))
	_, err := StorageCompatibleDataSizeWithLimit(context.Background(), value, math.MaxInt)
	require.Error(t, err)
}

func TestJSONTableArrayBuilderRejectsLimitBeyondUint32Format(t *testing.T) {
	if uint64(^uint(0)>>1) < uint64(math.MaxUint32)+1 {
		t.Skip("host int cannot represent the uint32 format boundary")
	}

	limitAtFormatMax := int(uint64(math.MaxUint32) + 1)
	builder, err := NewJSONTableArrayBuilder(limitAtFormatMax)
	require.NoError(t, err)
	builder.Close()

	_, err = NewJSONTableArrayBuilder(limitAtFormatMax + 1)
	require.Error(t, err)

	exactEntries := int(uint64(math.MaxUint32) - uint64(headerSize) - uint64(valEntrySize))
	available, err := jsonTableArrayPayloadBudget(limitAtFormatMax, exactEntries, 0)
	require.NoError(t, err)
	require.Zero(t, available)
	_, err = jsonTableArrayPayloadBudget(limitAtFormatMax, exactEntries+1, 0)
	require.Error(t, err)
	_, err = jsonTableArrayPayloadBudget(limitAtFormatMax, math.MaxUint32, 0)
	require.Error(t, err)
	_, err = jsonTableArrayPayloadBudget(limitAtFormatMax, 0, math.MaxUint32)
	require.Error(t, err)
}

func TestJSONTableArrayBuilderPreservesEveryCopiedValue(t *testing.T) {
	first := parseBuilderValue(t, `1`)
	second := parseBuilderValue(t, `2`)
	object := parseBuilderValue(t, `{"a":3}`)
	builder, err := NewJSONTableArrayBuilder(1024)
	require.NoError(t, err)
	defer builder.Close()

	require.NoError(t, builder.Append(first))
	// Append owns its input bytes. Mutating a source view after Append must not
	// alter the eventual cell.
	first.Data = append([]byte(nil), first.Data...)
	first.Data[0] = 0xff
	require.NoError(t, builder.Append(second))
	require.NoError(t, builder.Append(object))
	require.Equal(t, 3, builder.Count())
	encodedBytes := builder.Bytes()

	array, err := builder.Build()
	require.NoError(t, err)
	require.Equal(t, `[1, 2, {"a": 3}]`, array.String())
	require.Equal(t, 3, array.GetElemCnt())
	require.Equal(t, "1", array.GetArrayElem(0).String())
	require.Equal(t, "2", array.GetArrayElem(1).String())
	require.Equal(t, `{"a": 3}`, array.GetArrayElem(2).String())
	require.Equal(t, len(array.Data)+1, encodedBytes, "final cell size is accounted for before ownership transfer")
	require.Equal(t, 1+headerSize, builder.Bytes(), "the builder releases its owned payload after Build")

	_, err = builder.Build()
	require.Error(t, err)
	// Close after Build is intentionally idempotent for deferred cleanup.
	builder.Close()
}

func TestJSONTableArrayBuilderEnforcesEncodedCellLimitIncrementally(t *testing.T) {
	first := parseBuilderValue(t, `1`)
	second := parseBuilderValue(t, `2`)
	builder, err := NewJSONTableArrayBuilder(34)
	require.NoError(t, err)
	defer builder.Close()

	require.NoError(t, builder.Append(first))
	require.Equal(t, 22, builder.Bytes())
	err = builder.Append(second)
	require.ErrorIs(t, err, ErrJSONTableCellLimit)
	require.Equal(t, 1, builder.Count())
	require.Equal(t, 22, builder.Bytes())

	array, err := builder.Build()
	require.NoError(t, err)
	require.Equal(t, `[1]`, array.String())
}

func TestJSONTableArrayBuilderChecksCancellationAndLifecycle(t *testing.T) {
	builder, err := NewJSONTableArrayBuilder(128)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, builder.AppendContext(ctx, parseBuilderValue(t, `1`)), context.Canceled)
	require.Zero(t, builder.Count())
	_, err = builder.Build()
	require.Error(t, err, "an empty builder cannot publish a JSON cell")
	builder.Close()
	builder.Close()
	require.Error(t, builder.Append(parseBuilderValue(t, `1`)))
}

func TestJSONTableArrayBuilderRejectsInvalidInputs(t *testing.T) {
	_, err := NewJSONTableArrayBuilder(0)
	require.Error(t, err)
	_, err = NewJSONTableArrayBuilder(1 + headerSize + valEntrySize - 1)
	require.Error(t, err)

	builder, err := NewJSONTableArrayBuilder(128)
	require.NoError(t, err)
	defer builder.Close()
	require.Error(t, builder.Append(ByteJson{Type: TpCodeLiteral}))
	require.Error(t, builder.Append(ByteJson{Type: TpCodeLiteral, Data: []byte{0xff}}))
	require.NoError(t, builder.Append(parseBuilderValue(t, `[]`)))
	require.Equal(t, 1, builder.Count())
}

func TestJSONTableArrayBuilderAccountsForStorageCompatibleExpansion(t *testing.T) {
	for _, test := range []struct {
		name      string
		values    []ByteJson
		wantBytes int
		firstFits bool
	}{
		{
			name: "bit",
			values: []ByteJson{
				{Type: TpCodeBit, Data: appendBinaryString(nil, string([]byte{1, 1}))},
				{Type: TpCodeBit, Data: appendBinaryString(nil, string([]byte{2, 2}))},
			},
			wantBytes: 61,
			firstFits: false,
		},
		{
			name: "opaque",
			values: []ByteJson{
				{Type: TpCodeOpaque, Data: appendBinaryString(nil, string([]byte{1, 1}))},
				{Type: TpCodeOpaque, Data: appendBinaryString(nil, string([]byte{2, 2}))},
			},
			wantBytes: 29,
			firstFits: true,
		},
		{
			name: "bit and opaque",
			values: []ByteJson{
				{Type: TpCodeBit, Data: appendBinaryString(nil, string([]byte{1, 1}))},
				{Type: TpCodeOpaque, Data: appendBinaryString(nil, string([]byte{2, 2}))},
			},
			wantBytes: 45,
			firstFits: false,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			rawLimit := 1 + headerSize + len(test.values)*valEntrySize
			for _, value := range test.values {
				rawLimit += len(value.Data)
			}

			unbounded, err := NewJSONTableArrayBuilder(1024)
			require.NoError(t, err)
			for _, value := range test.values {
				require.NoError(t, unbounded.Append(value))
			}
			array, err := unbounded.Build()
			require.NoError(t, err)
			encoded, err := array.Marshal()
			require.NoError(t, err)
			require.Equal(t, test.wantBytes, len(encoded))
			require.Greater(t, len(encoded), rawLimit)

			exact, err := NewJSONTableArrayBuilder(test.wantBytes)
			require.NoError(t, err)
			for _, value := range test.values {
				require.NoError(t, exact.Append(value))
			}
			exactArray, err := exact.Build()
			require.NoError(t, err)
			exactEncoded, err := exactArray.Marshal()
			require.NoError(t, err)
			require.Equal(t, test.wantBytes, len(exactEncoded))

			oneOver, err := NewJSONTableArrayBuilder(test.wantBytes - 1)
			require.NoError(t, err)
			for i, value := range test.values {
				err = oneOver.Append(value)
				if i == len(test.values)-1 {
					require.ErrorIs(t, err, ErrJSONTableCellLimit)
				} else {
					require.NoError(t, err)
				}
			}
			require.Less(t, oneOver.Count(), len(test.values))
			exact.Close()
			oneOver.Close()

			bounded, err := NewJSONTableArrayBuilder(rawLimit)
			require.NoError(t, err)
			defer bounded.Close()
			err = bounded.Append(test.values[0])
			if test.firstFits {
				require.NoError(t, err)
				require.Equal(t, 1, bounded.Count())
				err = bounded.Append(test.values[1])
				require.ErrorIs(t, err, ErrJSONTableCellLimit)
				require.Equal(t, 1, bounded.Count())
			} else {
				require.ErrorIs(t, err, ErrJSONTableCellLimit)
				require.Zero(t, bounded.Count())
			}
		})
	}
}

func TestJSONTableArrayBuilderAccountsForNestedStorageExpansion(t *testing.T) {
	bit := ByteJson{Type: TpCodeBit, Data: appendBinaryString(nil, string([]byte{1, 1}))}
	nested, err := CreateByteJSON([]any{bit})
	require.NoError(t, err)
	values := []ByteJson{nested, nested}

	rawLimit := 1 + headerSize + len(values)*valEntrySize
	for _, value := range values {
		rawLimit += len(value.Data)
	}

	unbounded, err := NewJSONTableArrayBuilder(1024)
	require.NoError(t, err)
	for _, value := range values {
		require.NoError(t, unbounded.Append(value))
	}
	array, err := unbounded.Build()
	require.NoError(t, err)
	encoded, err := array.Marshal()
	require.NoError(t, err)
	require.Equal(t, 87, len(encoded))
	require.Greater(t, len(encoded), rawLimit)

	exact, err := NewJSONTableArrayBuilder(len(encoded))
	require.NoError(t, err)
	for _, value := range values {
		require.NoError(t, exact.Append(value))
	}
	exactArray, err := exact.Build()
	require.NoError(t, err)
	exactEncoded, err := exactArray.Marshal()
	require.NoError(t, err)
	require.Equal(t, len(encoded), len(exactEncoded))

	oneOver, err := NewJSONTableArrayBuilder(len(encoded) - 1)
	require.NoError(t, err)
	require.NoError(t, oneOver.Append(values[0]))
	require.ErrorIs(t, oneOver.Append(values[1]), ErrJSONTableCellLimit)
	require.Equal(t, 1, oneOver.Count())
	exact.Close()
	oneOver.Close()

	bounded, err := NewJSONTableArrayBuilder(rawLimit)
	require.NoError(t, err)
	defer bounded.Close()
	require.NoError(t, bounded.Append(values[0]))
	require.ErrorIs(t, bounded.Append(values[1]), ErrJSONTableCellLimit)
	require.Equal(t, 1, bounded.Count())
}

func TestJSONTableArrayBuilderRejectsLargeOpaqueBeforeEncoding(t *testing.T) {
	for _, test := range []struct {
		name          string
		payloadLength int
	}{
		{name: "1MiB", payloadLength: 1 << 20},
		{name: "8MiB", payloadLength: 8 << 20},
	} {
		t.Run(test.name, func(t *testing.T) {
			payloadLength := test.payloadLength
			// Construct the input before measuring. The witness must charge the
			// rejected storage conversion, rather than the caller's source buffer.
			data := make([]byte, binary.MaxVarintLen64+payloadLength)
			prefixLength := binary.PutUvarint(data, uint64(payloadLength))
			data = data[:prefixLength+payloadLength]
			value := ByteJson{Type: TpCodeOpaque, Data: data}

			builder, err := NewJSONTableArrayBuilder(14)
			require.NoError(t, err)
			runtime.GC()
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			err = builder.Append(value)
			runtime.ReadMemStats(&after)

			require.ErrorIs(t, err, ErrJSONTableCellLimit)
			require.Zero(t, builder.Count())
			require.Equal(t, 1+headerSize, builder.Bytes())
			builder.Close()

			// A rejected cell may allocate a small error or bookkeeping object,
			// but it must not allocate an input-sized base64 representation.
			require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(payloadLength/2))
			heapDelta := uint64(0)
			if after.HeapAlloc > before.HeapAlloc {
				heapDelta = after.HeapAlloc - before.HeapAlloc
			}
			t.Logf("payload=%d total_alloc_delta=%d heap_alloc_delta=%d", payloadLength, after.TotalAlloc-before.TotalAlloc, heapDelta)
			require.Less(t, heapDelta, uint64(payloadLength/2))
		})
	}
}

func TestJSONTableArrayBuilderNestedObjectStorageSize(t *testing.T) {
	opaque := ByteJson{Type: TpCodeOpaque, Data: appendBinaryString(nil, "ab")}
	nested, err := CreateByteJSON(map[string]any{"key": opaque})
	require.NoError(t, err)

	unbounded, err := NewJSONTableArrayBuilder(1024)
	require.NoError(t, err)
	require.NoError(t, unbounded.Append(nested))
	array, err := unbounded.Build()
	require.NoError(t, err)
	encoded, err := array.Marshal()
	require.NoError(t, err)
	unbounded.Close()

	for _, limit := range []int{len(encoded), len(encoded) - 1} {
		bounded, err := NewJSONTableArrayBuilder(limit)
		require.NoError(t, err)
		err = bounded.Append(nested)
		if limit == len(encoded) {
			require.NoError(t, err)
			require.Equal(t, 1, bounded.Count())
		} else {
			require.ErrorIs(t, err, ErrJSONTableCellLimit)
			require.Zero(t, bounded.Count())
		}
		bounded.Close()
	}
}

func TestJSONTableArrayBuilderCoversNilAndAdmissionBoundaries(t *testing.T) {
	var nilBuilder *JSONTableArrayBuilder
	require.Error(t, nilBuilder.AppendContext(nil, parseBuilderValue(t, `1`)))
	_, err := nilBuilder.Build()
	require.Error(t, err)
	require.Zero(t, nilBuilder.Count())
	require.Zero(t, nilBuilder.Bytes())
	nilBuilder.Close()

	builder, err := NewJSONTableArrayBuilder(128)
	require.NoError(t, err)
	require.NoError(t, builder.AppendContext(nil, parseBuilderValue(t, `1`)))
	builder.count = math.MaxUint32
	require.Error(t, builder.Append(parseBuilderValue(t, `2`)))
	builder.Close()

	value := parseBuilderValue(t, `1`)
	encoded, err := value.Marshal()
	require.NoError(t, err)
	_, err = StorageCompatibleDataSizeWithLimit(nil, value, -1)
	require.Error(t, err)
	dataSize, err := StorageCompatibleDataSizeWithLimit(nil, value, len(encoded)-1)
	require.NoError(t, err)
	require.Equal(t, len(encoded)-1, dataSize)
	bounded, err := MarshalStorageCompatibleWithLimit(nil, value, len(encoded))
	require.NoError(t, err)
	require.Equal(t, encoded, bounded)
	_, err = MarshalStorageCompatibleWithLimit(context.Background(), value, 0)
	require.Error(t, err)

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = MarshalStorageCompatibleWithLimit(cancelled, value, len(encoded))
	require.ErrorIs(t, err, context.Canceled)
}

func TestStoragePreflightRejectsMalformedScalarAndContainerValues(t *testing.T) {
	floatNaN := make([]byte, numberSize)
	binary.LittleEndian.PutUint64(floatNaN, math.Float64bits(math.NaN()))
	values := []ByteJson{
		{Type: TpCodeLiteral, Data: []byte{0xff}},
		{Type: TpCodeInt64, Data: []byte{1}},
		{Type: TpCodeFloat64, Data: floatNaN},
		{Type: TpCodeString, Data: []byte{0x80}},
		{Type: TpCodeOpaque, Data: []byte{0x80}},
		{Type: TpCode(0xff), Data: []byte{1}},
	}
	for _, value := range values {
		_, err := StorageCompatibleDataSizeWithLimit(context.Background(), value, math.MaxInt)
		require.Error(t, err)
	}

	arrayHeader := ByteJson{Type: TpCodeArray, Data: make([]byte, headerSize)}
	_, err := StorageCompatibleDataSizeWithLimit(context.Background(), arrayHeader, math.MaxInt)
	require.Error(t, err)

	arrayTable := make([]byte, headerSize+valEntrySize)
	binary.LittleEndian.PutUint32(arrayTable[docSizeOff:], uint32(len(arrayTable)))
	binary.LittleEndian.PutUint32(arrayTable[:4], 2)
	_, err = StorageCompatibleDataSizeWithLimit(context.Background(), ByteJson{Type: TpCodeArray, Data: arrayTable}, math.MaxInt)
	require.Error(t, err)

	arrayOffset := make([]byte, headerSize+valEntrySize+numberSize)
	binary.LittleEndian.PutUint32(arrayOffset[docSizeOff:], uint32(len(arrayOffset)))
	binary.LittleEndian.PutUint32(arrayOffset[:4], 1)
	arrayOffset[headerSize] = byte(TpCodeInt64)
	binary.LittleEndian.PutUint32(arrayOffset[headerSize+valTypeSize:], 0)
	_, err = StorageCompatibleDataSizeWithLimit(context.Background(), ByteJson{Type: TpCodeArray, Data: arrayOffset}, math.MaxInt)
	require.Error(t, err)

	arrayChild := make([]byte, headerSize+valEntrySize+numberSize)
	binary.LittleEndian.PutUint32(arrayChild[docSizeOff:], uint32(len(arrayChild)))
	binary.LittleEndian.PutUint32(arrayChild[:4], 1)
	arrayChild[headerSize] = byte(TpCode(0xff))
	binary.LittleEndian.PutUint32(arrayChild[headerSize+valTypeSize:], uint32(headerSize+valEntrySize))
	_, err = StorageCompatibleDataSizeWithLimit(context.Background(), ByteJson{Type: TpCodeArray, Data: arrayChild}, math.MaxInt)
	require.Error(t, err)

	arrayLiteral := make([]byte, headerSize+valEntrySize)
	binary.LittleEndian.PutUint32(arrayLiteral[docSizeOff:], uint32(len(arrayLiteral)))
	binary.LittleEndian.PutUint32(arrayLiteral[:4], 1)
	arrayLiteral[headerSize] = byte(TpCodeLiteral)
	arrayLiteral[headerSize+valTypeSize] = 0xff
	_, err = StorageCompatibleDataSizeWithLimit(context.Background(), ByteJson{Type: TpCodeArray, Data: arrayLiteral}, math.MaxInt)
	require.Error(t, err)

	object := make([]byte, headerSize+keyEntrySize+valEntrySize+1)
	binary.LittleEndian.PutUint32(object[docSizeOff:], uint32(len(object)))
	binary.LittleEndian.PutUint32(object[:4], 1)
	object[headerSize] = 0
	object[headerSize+keyOriginOff] = 1
	object[headerSize+keyOriginOff+1] = 0
	_, err = StorageCompatibleDataSizeWithLimit(context.Background(), ByteJson{Type: TpCodeObject, Data: object}, math.MaxInt)
	require.Error(t, err)

	deep := aliasedNestedArrayForStoragePreflight(JSONDocumentMaxNestingDepth + 1)
	_, err = StorageCompatibleDataSizeWithLimit(context.Background(), deep, math.MaxInt)
	require.Error(t, err)
}
