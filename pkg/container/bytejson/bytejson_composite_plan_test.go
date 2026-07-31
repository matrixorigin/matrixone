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
	"testing"

	"github.com/stretchr/testify/require"
)

func encodedByteJSON(t *testing.T, encoder ByteJsonDataEncoder) ByteJson {
	t.Helper()
	data := make([]byte, encoder.DataSize())
	written, err := encoder.EncodeDataInto(data)
	require.NoError(t, err)
	require.Equal(t, len(data), written)
	return ByteJson{Type: encoder.TypeCode(), Data: data}
}

func TestArrayDataEncoderMatchesCreateByteJSON(t *testing.T) {
	stringEncoder, err := NewStringDataEncoder([]byte("value"))
	require.NoError(t, err)
	values := []ByteJsonDataEncoder{
		NewLiteralDataEncoder(LiteralNull),
		NewLiteralDataEncoder(LiteralTrue),
		NewInt64DataEncoder(-7),
		NewUint64DataEncoder(9),
		NewFloat64DataEncoder(1.25),
		stringEncoder,
	}
	encoder, err := NewArrayDataEncoder(values)
	require.NoError(t, err)
	want, err := CreateByteJSON([]any{nil, true, int64(-7), uint64(9), 1.25, "value"})
	require.NoError(t, err)
	require.Equal(t, want, encodedByteJSON(t, encoder))
}

func TestIndexedFloatArrayDataEncoder(t *testing.T) {
	values := []float64{1.5, -2, 3.25}
	encoder, err := NewIndexedFloatArrayDataEncoder(
		len(values),
		func(idx int) float64 { return values[idx] },
	)
	require.NoError(t, err)
	want, err := CreateByteJSON([]any{1.5, -2.0, 3.25})
	require.NoError(t, err)
	require.Equal(t, want, encodedByteJSON(t, encoder))
}

func TestObjectDataEncoderSortsAndKeepsLastDuplicate(t *testing.T) {
	first, err := NewStringDataEncoder([]byte("first"))
	require.NoError(t, err)
	last, err := NewStringDataEncoder([]byte("last"))
	require.NoError(t, err)
	encoder, err := NewObjectDataEncoder([]ObjectDataEncoderEntry{
		{Key: []byte("z"), Value: NewInt64DataEncoder(1)},
		{Key: []byte("a"), Value: first},
		{Key: []byte("a"), Value: last},
	})
	require.NoError(t, err)
	want, err := CreateByteJSON(map[string]any{"a": "last", "z": int64(1)})
	require.NoError(t, err)
	require.Equal(t, want, encodedByteJSON(t, encoder))
}

func TestBinaryDataEncodersAreStorageCompatible(t *testing.T) {
	raw := []byte{0, 1, 2, 250, 251}
	for _, constructor := range []func([]byte) (ByteJsonDataEncoder, error){
		NewOpaqueDataEncoder,
		NewBitDataEncoder,
	} {
		encoder, err := constructor(raw)
		require.NoError(t, err)
		value := encodedByteJSON(t, encoder)
		require.Equal(t, TpCodeBlob, value.Type)
		require.False(t, value.requiresLegacyBinaryEncoding())
	}
}

func TestCompositeDataEncodersPreserveNestedAndTypedValues(t *testing.T) {
	nested, err := CreateByteJSON(map[string]any{
		"key": []any{int64(1), "value"},
	})
	require.NoError(t, err)
	raw, err := NewRawDataEncoder(nested)
	require.NoError(t, err)
	date, err := NewTypedStringDataEncoder(TpCodeDate, []byte("2026-07-31"))
	require.NoError(t, err)
	decimal, err := NewTypedStringDataEncoder(TpCodeDecimal, []byte("123.450"))
	require.NoError(t, err)
	array, err := NewArrayDataEncoder([]ByteJsonDataEncoder{raw, date, decimal})
	require.NoError(t, err)
	encoded := encodedByteJSON(t, array)
	require.Equal(t, nested, encoded.GetArrayElem(0))
	require.Equal(t, TpCodeDate, encoded.GetArrayElem(1).Type)
	require.Equal(t, []byte("2026-07-31"), encoded.GetArrayElem(1).GetString())
	require.Equal(t, TpCodeDecimal, encoded.GetArrayElem(2).Type)
	require.Equal(t, []byte("123.450"), encoded.GetArrayElem(2).GetString())
}

func TestCompositeDataEncodersRejectInvalidPlansAndDestinations(t *testing.T) {
	_, err := NewArrayDataEncoder([]ByteJsonDataEncoder{nil})
	require.Error(t, err)
	_, err = NewObjectDataEncoder([]ObjectDataEncoderEntry{{Key: []byte("key")}})
	require.Error(t, err)
	_, err = NewTypedStringDataEncoder(TpCodeObject, []byte("invalid"))
	require.Error(t, err)

	encoder, err := NewArrayDataEncoder([]ByteJsonDataEncoder{NewInt64DataEncoder(1)})
	require.NoError(t, err)
	_, err = encoder.EncodeDataInto(make([]byte, encoder.DataSize()-1))
	require.Error(t, err)
}
