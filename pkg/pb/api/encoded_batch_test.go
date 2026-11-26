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

package api

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEncodedBatchParts(t *testing.T) {
	head, body, tail := []byte("head"), []byte("body"), []byte("!")
	b := NewEncodedBatch(head, nil, body, tail)
	require.Equal(t, 9, b.Len())
	require.Equal(t, 9, b.ProtoSize())
	require.Equal(t, []byte("headbody!"), b.Bytes())

	data, err := b.Marshal()
	require.NoError(t, err)
	require.Equal(t, []byte("headbody!"), data)
	buf := make([]byte, 12)
	n, err := b.MarshalTo(buf)
	require.NoError(t, err)
	require.Equal(t, 9, n)
	require.Equal(t, []byte("headbody!"), buf[:n])

	// The parts are borrowed until marshaling.
	body[0] = 'B'
	require.Equal(t, []byte("headBody!"), b.Bytes())

	// One part is returned as is.
	single := NewEncodedBatch(body)
	require.Equal(t, &body[0], &single.Bytes()[0])
	require.Nil(t, EncodedBatch{}.Bytes())

	require.True(t, b.Equal(NewEncodedBatch([]byte("headBody!"))))
	require.False(t, b.Equal(single))
	require.Zero(t, b.Compare(NewEncodedBatch([]byte("headBody!"))))
}

func TestEncodedBatchUnmarshalCopies(t *testing.T) {
	wire := []byte("payload")
	var b EncodedBatch
	require.NoError(t, b.Unmarshal(wire))
	wire[0] = 'X'
	require.Equal(t, []byte("payload"), b.Bytes())
}

func TestEncodedBatchJSON(t *testing.T) {
	b := NewEncodedBatch([]byte("ab"), []byte("cd"))
	data, err := b.MarshalJSON()
	require.NoError(t, err)
	var got EncodedBatch
	require.NoError(t, got.UnmarshalJSON(data))
	require.Equal(t, []byte("abcd"), got.Bytes())
}

func TestEntryCarriesEncodedBatch(t *testing.T) {
	e := Entry{TableName: "t", Attrs: []string{"a"}, Bat: NewEncodedBatch([]byte("12"), []byte("345"))}
	wire, err := e.Marshal()
	require.NoError(t, err)
	require.Equal(t, e.ProtoSize(), len(wire))
	var got Entry
	require.NoError(t, got.Unmarshal(wire))
	require.Equal(t, []byte("12345"), got.Bat.Bytes())
	require.Equal(t, e.Attrs, got.Attrs)
}
