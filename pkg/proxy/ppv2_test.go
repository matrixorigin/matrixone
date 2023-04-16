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

func TestProxyProtocolCodec_Decode(t *testing.T) {
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

		pp := &proxyProtocolCodec{}
		res, ok, err := pp.Decode(data)
		require.NoError(t, err)
		require.True(t, ok)
		require.NotNil(t, res)
		addr, ok := res.(*ProxyAddr)
		require.True(t, ok)
		require.Nil(t, addr.SourceAddress)
		require.Equal(t, 0, int(addr.SourcePort))
		require.Nil(t, addr.TargetAddress)
		require.Equal(t, 0, int(addr.TargetPort))
	})

	t.Run("remote address", func(t *testing.T) {
		data := buf.NewByteBuf(100)
		n, err := data.Write([]byte(ProxyProtocolV2Signature))
		require.NoError(t, err)
		require.Equal(t, len(ProxyProtocolV2Signature), n)

		// skip 2 bytes
		n, err = data.Write([]byte{0, 0})
		require.NoError(t, err)
		require.Equal(t, 2, n)
		// ipv4
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

	t.Run("error length", func(t *testing.T) {
		data := buf.NewByteBuf(100)
		n, err := data.Write([]byte(ProxyProtocolV2Signature))
		require.NoError(t, err)
		require.Equal(t, len(ProxyProtocolV2Signature), n)

		// skip 2 bytes
		n, err = data.Write([]byte{0, 0})
		require.NoError(t, err)
		require.Equal(t, 2, n)
		data.WriteUint16(33)

		pp := &proxyProtocolCodec{}
		res, ok, err := pp.Decode(data)
		require.Error(t, err)
		require.False(t, ok)
		require.Nil(t, res)
	})
}
