// Copyright 2021 - 2023 Matrix Origin
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

package proxy

import (
	"encoding/binary"
	"testing"

	"github.com/fagongzi/goetty/v2/buf"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/stretchr/testify/require"
)

func TestProxyProtocolOptions(t *testing.T) {
	pp := &proxyProtocolCodec{}
	ret := WithProxyProtocolCodec(pp)
	require.NotNil(t, ret)
}

func TestProxyServerCodecAcceptsIPv6AtIndependentMinimum(t *testing.T) {
	data := buf.NewByteBuf(ProxyHeaderLength + int(minimumProxyProtocolBodyLimit))
	header := make([]byte, ProxyHeaderLength)
	copy(header, ProxyProtocolV2Signature)
	header[12] = 0x21
	header[13] = tcpOverIPv6
	binary.BigEndian.PutUint16(header[14:], uint16(minimumProxyProtocolBodyLimit))
	_, err := data.Write(header)
	require.NoError(t, err)
	_, err = data.Write(make([]byte, minimumProxyProtocolBodyLimit))
	require.NoError(t, err)

	configured := Config{
		ClientHandshakePacketLimit: minimumClientHandshakePacketLimit,
		ProxyProtocolBodyLimit:     minimumProxyProtocolBodyLimit,
	}
	res, ok, err := newProxySessionCodec(configured).Decode(data)
	require.NoError(t, err)
	require.True(t, ok)
	require.IsType(t, &ProxyAddr{}, res)
}

func TestProxyProtocolCodec_Decode(t *testing.T) {
	t.Run("oversized body rejected from fixed header", func(t *testing.T) {
		data := buf.NewByteBuf(ProxyHeaderLength)
		header := make([]byte, ProxyHeaderLength)
		copy(header, ProxyProtocolV2Signature)
		header[12] = 0x21
		header[13] = unspec
		binary.BigEndian.PutUint16(header[14:], 65)
		_, err := data.Write(header)
		require.NoError(t, err)

		pp := WithProxyProtocolCodec(
			frontend.NewSqlCodec(),
			WithProxyProtocolMaxBodySize(64),
		)
		res, ok, err := pp.Decode(data)
		require.ErrorIs(t, err, frontend.ErrPacketTooLarge)
		require.False(t, ok)
		require.Nil(t, res)
		require.Equal(t, ProxyHeaderLength, data.Readable())
	})

	t.Run("body at configured limit is accepted", func(t *testing.T) {
		data := buf.NewByteBuf(ProxyHeaderLength + 64)
		header := make([]byte, ProxyHeaderLength)
		copy(header, ProxyProtocolV2Signature)
		header[12] = 0x21
		header[13] = unspec
		binary.BigEndian.PutUint16(header[14:], 64)
		_, err := data.Write(header)
		require.NoError(t, err)
		_, err = data.Write(make([]byte, 64))
		require.NoError(t, err)

		pp := WithProxyProtocolCodec(
			frontend.NewSqlCodec(),
			WithProxyProtocolMaxBodySize(64),
		)
		res, ok, err := pp.Decode(data)
		require.NoError(t, err)
		require.True(t, ok)
		require.IsType(t, &ProxyAddr{}, res)
		require.Zero(t, data.Readable())
	})

	t.Run("fragmented header and body", func(t *testing.T) {
		data := buf.NewByteBuf(100)
		pp := WithProxyProtocolCodec(frontend.NewSqlCodec(
			frontend.WithSQLCodecMaxPayloadSize(64 << 10)))

		header := make([]byte, ProxyHeaderLength)
		copy(header, ProxyProtocolV2Signature)
		header[12] = 0x21
		header[13] = tcpOverIPv4
		binary.BigEndian.PutUint16(header[14:], 12)
		body := []byte{
			10, 11, 12, 13,
			20, 21, 22, 23,
			0x1f, 0x40,
			0x23, 0x28,
		}

		fragments := [][]byte{
			header[:4],
			header[4:12],
			header[12:15],
			header[15:],
			body[:5],
			body[5:],
		}
		buffered := 0
		for i, fragment := range fragments {
			n, err := data.Write(fragment)
			require.NoError(t, err)
			require.Equal(t, len(fragment), n)
			buffered += n

			res, ok, err := pp.Decode(data)
			require.NoError(t, err)
			if i < len(fragments)-1 {
				require.False(t, ok)
				require.Nil(t, res)
				require.Equal(t, buffered, data.Readable(), "incomplete input must not be consumed")
				continue
			}

			require.True(t, ok)
			addr, ok := res.(*ProxyAddr)
			require.True(t, ok)
			require.Equal(t, "10.11.12.13", addr.SourceAddress.String())
			require.Equal(t, uint16(8000), addr.SourcePort)
			require.Equal(t, "20.21.22.23", addr.TargetAddress.String())
			require.Equal(t, uint16(9000), addr.TargetPort)
			require.Zero(t, data.Readable())
		}
	})

	t.Run("all incomplete signature prefixes wait", func(t *testing.T) {
		pp := WithProxyProtocolCodec(frontend.NewSqlCodec(
			frontend.WithSQLCodecMaxPayloadSize(64 << 10)))
		for length := 1; length < len(ProxyProtocolV2Signature); length++ {
			data := buf.NewByteBuf(100)
			_, err := data.Write([]byte(ProxyProtocolV2Signature[:length]))
			require.NoError(t, err)

			res, ok, err := pp.Decode(data)
			require.NoError(t, err, "prefix length %d", length)
			require.False(t, ok, "prefix length %d", length)
			require.Nil(t, res, "prefix length %d", length)
			require.Equal(t, length, data.Readable(), "prefix length %d", length)
		}
	})

	t.Run("mismatched signature delegates to mysql codec", func(t *testing.T) {
		data := buf.NewByteBuf(100)
		_, err := data.Write([]byte{0x0d, 0x0a, 0x0d, 0x00})
		require.NoError(t, err)

		pp := WithProxyProtocolCodec(frontend.NewSqlCodec(
			frontend.WithSQLCodecMaxPayloadSize(64 << 10)))
		res, ok, err := pp.Decode(data)
		require.ErrorIs(t, err, frontend.ErrPacketTooLarge)
		require.False(t, ok)
		require.Nil(t, res)
	})

	t.Run("short header", func(t *testing.T) {
		data := buf.NewByteBuf(100)
		n, err := data.Write([]byte("12345"))
		require.NoError(t, err)
		require.Equal(t, 5, n)

		pp := WithProxyProtocolCodec(frontend.NewSqlCodec())
		res, ok, err := pp.Decode(data)
		require.NoError(t, err)
		require.False(t, ok)
		require.Nil(t, res)
	})

	t.Run("long header", func(t *testing.T) {
		data := buf.NewByteBuf(100)
		n, err := data.Write([]byte("12345678901234567890"))
		require.NoError(t, err)
		require.Equal(t, 20, n)

		pp := WithProxyProtocolCodec(frontend.NewSqlCodec())
		res, ok, err := pp.Decode(data)
		require.NoError(t, err)
		require.False(t, ok)
		require.Nil(t, res)
	})

	t.Run("local address", func(t *testing.T) {
		data := buf.NewByteBuf(100)
		n, err := data.Write([]byte(ProxyProtocolV2Signature))
		require.NoError(t, err)
		require.Equal(t, len(ProxyProtocolV2Signature), n)

		// skip 2 bytes
		n, err = data.Write([]byte{0, 0})
		require.NoError(t, err)
		require.Equal(t, 2, n)
		data.WriteUint16(0)

		pp := WithProxyProtocolCodec(frontend.NewSqlCodec())
		res, ok, err := pp.Decode(data)
		require.NoError(t, err)
		require.True(t, ok)
		addr, ok := res.(*ProxyAddr)
		require.True(t, ok)
		require.Nil(t, addr.SourceAddress)
		require.Zero(t, data.Readable())
	})

	t.Run("ipv4 address", func(t *testing.T) {
		data := buf.NewByteBuf(100)
		n, err := data.Write([]byte(ProxyProtocolV2Signature))
		require.NoError(t, err)
		require.Equal(t, len(ProxyProtocolV2Signature), n)

		// ipv4
		n, err = data.Write([]byte{0, tcpOverIPv4})
		require.NoError(t, err)
		require.Equal(t, 2, n)

		// ipv4 length
		data.WriteUint16(12)
		// source address
		err = data.WriteByte(10)
		require.NoError(t, err)
		err = data.WriteByte(11)
		require.NoError(t, err)
		err = data.WriteByte(12)
		require.NoError(t, err)
		err = data.WriteByte(13)
		require.NoError(t, err)
		// target address
		err = data.WriteByte(20)
		require.NoError(t, err)
		err = data.WriteByte(21)
		require.NoError(t, err)
		err = data.WriteByte(22)
		require.NoError(t, err)
		err = data.WriteByte(23)
		require.NoError(t, err)
		// source port
		data.WriteUint16(8000)
		// target port
		data.WriteUint16(9000)

		pp := &proxyProtocolCodec{}
		res, ok, err := pp.Decode(data)
		require.NoError(t, err)
		require.True(t, ok)
		require.NotNil(t, res)
		addr, ok := res.(*ProxyAddr)
		require.True(t, ok)
		require.Equal(t, "10.11.12.13", addr.SourceAddress.To4().String())
		require.Equal(t, 8000, int(addr.SourcePort))
		require.Equal(t, "20.21.22.23", addr.TargetAddress.To4().String())
		require.Equal(t, 9000, int(addr.TargetPort))
	})

	t.Run("ipv6 address", func(t *testing.T) {
		data := buf.NewByteBuf(100)
		n, err := data.Write([]byte(ProxyProtocolV2Signature))
		require.NoError(t, err)
		require.Equal(t, len(ProxyProtocolV2Signature), n)

		// ipv6
		n, err = data.Write([]byte{0, tcpOverIPv6})
		require.NoError(t, err)
		require.Equal(t, 2, n)

		// ipv4 length
		data.WriteUint16(36)
		// source address
		for i := 0; i < 16; i++ {
			err = data.WriteByte(byte(10 + i))
			require.NoError(t, err)
		}
		// target address
		for i := 0; i < 16; i++ {
			err = data.WriteByte(byte(50 + i))
			require.NoError(t, err)
		}
		// source port
		data.WriteUint16(8000)
		// target port
		data.WriteUint16(9000)

		pp := &proxyProtocolCodec{}
		res, ok, err := pp.Decode(data)
		require.NoError(t, err)
		require.True(t, ok)
		require.NotNil(t, res)
		addr, ok := res.(*ProxyAddr)
		require.True(t, ok)
		require.Equal(t, "a0b:c0d:e0f:1011:1213:1415:1617:1819", addr.SourceAddress.To16().String())
		require.Equal(t, 8000, int(addr.SourcePort))
		require.Equal(t, "3233:3435:3637:3839:3a3b:3c3d:3e3f:4041", addr.TargetAddress.To16().String())
		require.Equal(t, 9000, int(addr.TargetPort))
	})

	t.Run("error length", func(t *testing.T) {
		data := buf.NewByteBuf(100)
		n, err := data.Write([]byte(ProxyProtocolV2Signature))
		require.NoError(t, err)
		require.Equal(t, len(ProxyProtocolV2Signature), n)

		// ipv4 with a complete declared body that is too short for its address.
		n, err = data.Write([]byte{0, tcpOverIPv4})
		require.NoError(t, err)
		require.Equal(t, 2, n)
		data.WriteUint16(4)
		_, err = data.Write(make([]byte, 4))
		require.NoError(t, err)

		pp := &proxyProtocolCodec{}
		res, ok, err := pp.Decode(data)
		require.Error(t, err)
		require.False(t, ok)
		require.Nil(t, res)
	})
}
