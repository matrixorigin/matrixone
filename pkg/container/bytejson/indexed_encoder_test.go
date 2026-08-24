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
	"errors"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

type mutableIndexedArray struct {
	values []ByteJson
	errAt  int
}

func (s *mutableIndexedArray) Len() int { return len(s.values) }
func (s *mutableIndexedArray) Value(index int) (ByteJson, error) {
	if index == s.errAt {
		return ByteJson{}, errors.New("indexed value failed")
	}
	return s.values[index], nil
}

type oversizedIndexedArray struct{}

func (oversizedIndexedArray) Len() int                    { return math.MaxUint32 + 1 }
func (oversizedIndexedArray) Value(int) (ByteJson, error) { return Null, nil }

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

func TestIndexedContainerEncoderFailureBoundaries(t *testing.T) {
	one, err := CreateByteJSONWithCheck(int64(1))
	require.NoError(t, err)

	nilArray := NewIndexedArrayEncoder(nil).(*indexedContainerEncoder)
	require.Error(t, nilArray.ValidateData())
	require.Zero(t, nilArray.DataSize())
	require.Error(t, NewIndexedArrayEncoder(oversizedIndexedArray{}).(ByteJsonDataValidator).ValidateData())

	invalidLiteral := &mutableIndexedArray{
		values: []ByteJson{{Type: TpCodeLiteral, Data: nil}},
		errAt:  -1,
	}
	require.Error(t, NewIndexedArrayEncoder(invalidLiteral).(ByteJsonDataValidator).ValidateData())
	tooLongKey := make([]byte, math.MaxUint16+1)
	require.Error(t, NewIndexedObjectEncoder(testIndexedObject{
		keys: [][]byte{tooLongKey}, values: []ByteJson{one},
	}).(ByteJsonDataValidator).ValidateData())

	failing := &mutableIndexedArray{values: []ByteJson{one}, errAt: 0}
	require.Error(t, NewIndexedArrayEncoder(failing).(ByteJsonDataValidator).ValidateData())
	failing.errAt = -1
	encoder := NewIndexedArrayEncoder(failing).(*indexedContainerEncoder)
	require.NoError(t, encoder.ValidateData())
	require.NoError(t, encoder.ValidateData())
	require.Error(t, func() error {
		_, err := encoder.EncodeDataInto(make([]byte, encoder.DataSize()-1))
		return err
	}())
}
