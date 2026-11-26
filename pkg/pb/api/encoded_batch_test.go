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
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// testEncoder encodes its bytes, which it reads only when asked to write.
type testEncoder struct{ data []byte }

func (e *testEncoder) EncodeTo(dst []byte) error {
	if len(dst) != len(e.data) {
		return errors.New("size changed")
	}
	copy(dst, e.data)
	return nil
}

func TestEncodedBatchBytes(t *testing.T) {
	body := []byte("payload")
	b := NewEncodedBatch(body)
	require.Equal(t, 7, b.Len())
	require.Equal(t, 7, b.ProtoSize())
	require.Equal(t, &body[0], &b.Bytes()[0], "the bytes are kept as is")
	require.Nil(t, EncodedBatch{}.Bytes())

	buf := make([]byte, 9)
	n, err := b.MarshalTo(buf)
	require.NoError(t, err)
	require.Equal(t, 7, n)
	require.Equal(t, body, buf[:n])
	_, err = b.MarshalTo(buf[:6])
	require.Error(t, err)

	require.True(t, b.Equal(NewEncodedBatch([]byte("payload"))))
	require.False(t, b.Equal(NewEncodedBatch([]byte("payloaD"))))
	require.Zero(t, b.Compare(NewEncodedBatch([]byte("payload"))))
}

func TestBorrowedEncodedBatch(t *testing.T) {
	enc := &testEncoder{data: []byte("abcd")}
	b := NewBorrowedEncodedBatch(enc, 4)
	require.Equal(t, 4, b.ProtoSize())

	// The encoder is read when the batch is marshaled, not before.
	enc.data[0] = 'A'
	buf := make([]byte, 6)
	n, err := b.MarshalTo(buf)
	require.NoError(t, err)
	require.Equal(t, []byte("Abcd"), buf[:n])
	require.Equal(t, []byte("Abcd"), b.Bytes())
	require.True(t, b.Equal(NewEncodedBatch([]byte("Abcd"))))

	enc.data = []byte("abcde")
	_, err = b.MarshalTo(buf)
	require.Error(t, err, "a batch that changed size fails")
	require.Nil(t, b.Bytes())
}

func TestEncodedBatchUnmarshalViews(t *testing.T) {
	wire := []byte("payload!")
	var b EncodedBatch
	allocs := testing.AllocsPerRun(10, func() {
		require.NoError(t, b.Unmarshal(wire[:7]))
	})
	require.Zero(t, allocs)
	require.Equal(t, &wire[0], &b.Bytes()[0], "Unmarshal keeps a view")
	require.Equal(t, 7, cap(b.Bytes()), "the view cannot grow into the buffer")
}

func TestEncodedBatchJSON(t *testing.T) {
	b := NewBorrowedEncodedBatch(&testEncoder{data: []byte("abcd")}, 4)
	data, err := b.MarshalJSON()
	require.NoError(t, err)
	var got EncodedBatch
	require.NoError(t, got.UnmarshalJSON(data))
	require.Equal(t, []byte("abcd"), got.Bytes())
}

func TestEntryCarriesEncodedBatch(t *testing.T) {
	e := Entry{TableName: "t", Attrs: []string{"a"}, Bat: NewBorrowedEncodedBatch(&testEncoder{data: []byte("12345")}, 5)}
	wire, err := e.Marshal()
	require.NoError(t, err)
	require.Equal(t, e.ProtoSize(), len(wire))
	var got Entry
	require.NoError(t, got.Unmarshal(wire))
	require.Equal(t, []byte("12345"), got.Bat.Bytes())
	require.Equal(t, e.Attrs, got.Attrs)
}

// Detach gives a view or an encoder's encoding bytes of its own, and keeps
// owned bytes as they are.
func TestEncodedBatchDetach(t *testing.T) {
	wire := []byte("payload")
	var view EncodedBatch
	require.NoError(t, view.Unmarshal(wire))
	require.True(t, view.IsView())
	owned := view.Detach()
	require.False(t, owned.IsView())
	clear(wire)
	require.Equal(t, []byte("payload"), owned.Bytes())
	require.Equal(t, &owned.Bytes()[0], &owned.Detach().Bytes()[0], "owned bytes are not copied again")

	enc := &testEncoder{data: []byte("abc")}
	written := NewBorrowedEncodedBatch(enc, 3).Detach()
	enc.data[0] = 'X'
	require.Equal(t, []byte("abc"), written.Bytes())
}
