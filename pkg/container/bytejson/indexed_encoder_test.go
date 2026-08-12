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

type testIndexedArray []ByteJson

func (s testIndexedArray) Len() int { return len(s) }
func (s testIndexedArray) Value(index int) (ByteJson, error) {
	return s[index], nil
}

type testIndexedObject struct {
	keys   [][]byte
	values []ByteJson
}

func (s testIndexedObject) Len() int         { return len(s.keys) }
func (s testIndexedObject) Key(i int) []byte { return s.keys[i] }
func (s testIndexedObject) Value(i int) (ByteJson, error) {
	return s.values[i], nil
}

func encodeIndexedTestValue(t *testing.T, encoder ByteJsonDataEncoder) ByteJson {
	t.Helper()
	data := make([]byte, encoder.DataSize())
	n, err := encoder.EncodeDataInto(data)
	require.NoError(t, err)
	require.Equal(t, len(data), n)
	return ByteJson{Type: encoder.TypeCode(), Data: data}
}

func TestIndexedContainerEncoders(t *testing.T) {
	one, err := CreateByteJSONWithCheck(int64(1))
	require.NoError(t, err)
	text, err := CreateByteJSONWithCheck("two")
	require.NoError(t, err)

	array := NewIndexedArrayEncoder(testIndexedArray{one, Null, text})
	encoded := encodeIndexedTestValue(t, array)
	visible, err := encoded.MarshalJSON()
	require.NoError(t, err)
	require.JSONEq(t, `[1,null,"two"]`, string(visible))

	object := NewIndexedObjectEncoder(testIndexedObject{
		keys:   [][]byte{[]byte("a"), []byte("b")},
		values: []ByteJson{text, one},
	})
	encoded = encodeIndexedTestValue(t, object)
	visible, err = encoded.MarshalJSON()
	require.NoError(t, err)
	require.JSONEq(t, `{"a":"two","b":1}`, string(visible))

	invalid := NewIndexedObjectEncoder(testIndexedObject{
		keys:   [][]byte{[]byte("b"), []byte("a")},
		values: []ByteJson{one, text},
	})
	validator := invalid.(ByteJsonDataValidator)
	err = validator.ValidateData()
	require.Error(t, err)
}
