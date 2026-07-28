// Copyright 2021 - 2026 Matrix Origin
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

package frontend

import (
	"testing"

	"github.com/fagongzi/goetty/v2/buf"
	"github.com/stretchr/testify/require"
)

func TestSQLCodecMaxPayloadSize(t *testing.T) {
	writeHeader := func(t *testing.T, payloadSize int) *buf.ByteBuf {
		t.Helper()
		data := buf.NewByteBuf(PacketHeaderLength)
		require.NoError(t, data.WriteByte(byte(payloadSize)))
		require.NoError(t, data.WriteByte(byte(payloadSize>>8)))
		require.NoError(t, data.WriteByte(byte(payloadSize>>16)))
		require.NoError(t, data.WriteByte(0))
		return data
	}

	t.Run("reject from header before payload arrives", func(t *testing.T) {
		data := writeHeader(t, 65)
		defer data.Close()
		codec := NewSqlCodec(WithSQLCodecMaxPayloadSize(64))

		value, ok, err := codec.Decode(data)
		require.ErrorIs(t, err, ErrPacketTooLarge)
		require.False(t, ok)
		require.Nil(t, value)
		require.Equal(t, PacketHeaderLength, data.Readable())
	})

	t.Run("boundary packet remains compatible", func(t *testing.T) {
		data := writeHeader(t, 4)
		defer data.Close()
		_, err := data.Write([]byte{1, 2, 3, 4})
		require.NoError(t, err)
		codec := NewSqlCodec(WithSQLCodecMaxPayloadSize(4))

		value, ok, err := codec.Decode(data)
		require.NoError(t, err)
		require.True(t, ok)
		require.NotNil(t, value)
		require.Equal(t, []byte{1, 2, 3, 4}, value.(*Packet).Payload)
	})

	t.Run("default remains unbounded by option", func(t *testing.T) {
		data := writeHeader(t, 65)
		defer data.Close()
		codec := NewSqlCodec()

		value, ok, err := codec.Decode(data)
		require.NoError(t, err)
		require.False(t, ok)
		require.Nil(t, value)
	})
}
