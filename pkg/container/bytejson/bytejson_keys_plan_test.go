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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestObjectKeysArrayEncoder(t *testing.T) {
	object, err := CreateByteJSON(map[string]any{
		"z":      int64(1),
		"a":      true,
		"longer": nil,
	})
	require.NoError(t, err)
	encoder, err := NewObjectKeysArrayEncoder(object)
	require.NoError(t, err)
	encoded := make([]byte, encoder.DataSize())
	written, err := encoder.EncodeDataInto(encoded)
	require.NoError(t, err)
	require.Equal(t, len(encoded), written)
	result := ByteJson{Type: encoder.TypeCode(), Data: encoded}
	visible, err := result.MarshalJSON()
	require.NoError(t, err)
	require.JSONEq(t, `["a", "longer", "z"]`, string(visible))
}

func TestObjectKeysArrayEncoderRejectsInvalidInputAndSize(t *testing.T) {
	array, err := CreateByteJSON([]any{int64(1)})
	require.NoError(t, err)
	_, err = NewObjectKeysArrayEncoder(array)
	require.Error(t, err)

	object, err := CreateByteJSON(map[string]any{"key": int64(1)})
	require.NoError(t, err)
	encoder, err := NewObjectKeysArrayEncoder(object)
	require.NoError(t, err)
	_, err = encoder.EncodeDataInto(make([]byte, encoder.DataSize()-1))
	require.Error(t, err)
}

func TestStringDataEncoder(t *testing.T) {
	encoder, err := NewStringDataEncoder([]byte("a value \" with unicode 世界"))
	require.NoError(t, err)
	encoded := make([]byte, encoder.DataSize())
	written, err := encoder.EncodeDataInto(encoded)
	require.NoError(t, err)
	require.Equal(t, len(encoded), written)
	result := ByteJson{Type: encoder.TypeCode(), Data: encoded}
	require.Equal(t, []byte("a value \" with unicode 世界"), result.GetString())

	_, err = encoder.EncodeDataInto(encoded[:len(encoded)-1])
	require.Error(t, err)
}
