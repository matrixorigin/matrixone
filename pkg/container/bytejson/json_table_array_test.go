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
	"testing"

	"github.com/stretchr/testify/require"
)

func parseBuilderValue(t *testing.T, text string) ByteJson {
	t.Helper()
	value, err := ParseFromString(text)
	require.NoError(t, err)
	return value
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

	bounded, err := NewJSONTableArrayBuilder(rawLimit)
	require.NoError(t, err)
	defer bounded.Close()
	require.NoError(t, bounded.Append(values[0]))
	require.ErrorIs(t, bounded.Append(values[1]), ErrJSONTableCellLimit)
	require.Equal(t, 1, bounded.Count())
}
