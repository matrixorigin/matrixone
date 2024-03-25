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
	"math"
	"net"
	"testing"

	"github.com/lni/goutils/leaktest"
	"github.com/stretchr/testify/require"
)

func TestMySQLConnPreRecv(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("protocol_error/length-0", func(t *testing.T) {
		var data [10]byte
		src, dst := net.Pipe()
		go func() {
			n, err := dst.Write(data[:])
			require.NoError(t, err)
			require.Equal(t, 10, n)
		}()
		sc := newMySQLConn("source", src, 10, nil, nil, nil)
		size, err := sc.preRecv()
		require.NoError(t, err, "mysql protocol error")
		require.Equal(t, 4, size)
	})

	t.Run("protocol_error/length-max", func(t *testing.T) {
		var data [10]byte
		l := math.MaxInt32
		data[0] = byte(l)
		data[1] = byte(l >> 8)
		data[2] = byte(l >> 16)
		src, dst := net.Pipe()
		go func() {
			n, err := dst.Write(data[:])
			require.NoError(t, err)
			require.Equal(t, 10, n)
		}()
		// sc := newMySQLConn("source", src, 10, nil, nil, nil)
		sc := newMySQLConn("source", src, 10, nil, nil, nil)
		size, err := sc.preRecv()
		require.NoError(t, err)
		require.Equal(t, mysqlHeadLen+(1<<24-1), size)
	})

	t.Run("protocol_error/length-normal", func(t *testing.T) {
		var data [10]byte
		l := 6
		data[0] = byte(l)
		data[1] = byte(l >> 8)
		data[2] = byte(l >> 16)
		src, dst := net.Pipe()
		go func() {
			n, err := dst.Write(data[:])
			require.NoError(t, err)
			require.Equal(t, 10, n)
		}()
		sc := newMySQLConn("source", src, 10, nil, nil, nil)
		size, err := sc.preRecv()
		require.NoError(t, err)
		require.Equal(t, 10, size)
	})

	t.Run("protocol_error/begin", func(t *testing.T) {
		data := makeSimplePacket("begin")
		src, dst := net.Pipe()
		go func() {
			n, err := dst.Write(data[:])
			require.NoError(t, err)
			require.Equal(t, 10, n)
		}()
		sc := newMySQLConn("source", src, 10, nil, nil, nil)
		size, err := sc.preRecv()
		require.NoError(t, err)
		require.Equal(t, 10, size)
	})

	t.Run("protocol_error/commit", func(t *testing.T) {
		data := makeSimplePacket("commit")
		src, dst := net.Pipe()
		go func() {
			n, err := dst.Write(data[:])
			require.NoError(t, err)
			require.Equal(t, 11, n)
		}()
		sc := newMySQLConn("source", src, 20, nil, nil, nil)
		size, err := sc.preRecv()
		require.NoError(t, err)
		require.Equal(t, 11, size)
	})

	t.Run("protocol_error/rollback", func(t *testing.T) {
		data := makeSimplePacket("rollback")
		src, dst := net.Pipe()
		go func() {
			n, err := dst.Write(data[:])
			require.NoError(t, err)
			require.Equal(t, 13, n)
		}()
		sc := newMySQLConn("source", src, 20, nil, nil, nil)
		size, err := sc.preRecv()
		require.NoError(t, err)
		require.Equal(t, 13, size)
	})
}

func TestMySQLConnReceive(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("small buffer", func(t *testing.T) {
		q := "select 1"
		data := makeSimplePacket(q)
		src, dst := net.Pipe()
		go func() {
			n, err := dst.Write(data[:])
			require.NoError(t, err)
			require.Equal(t, 13, n)
		}()
		sc := newMySQLConn("source", src, 6, nil, nil, nil)
		res, err := sc.receive()
		require.NoError(t, err)
		require.Equal(t, q, string(res[5:]))
	})

	t.Run("enough buffer", func(t *testing.T) {
		q := "select 1"
		data := makeSimplePacket(q)
		src, dst := net.Pipe()
		go func() {
			n, err := dst.Write(data[:])
			require.NoError(t, err)
			require.Equal(t, 13, n)
		}()
		sc := newMySQLConn("source", src, 100, nil, nil, nil)
		res, err := sc.receive()
		require.NoError(t, err)
		require.Equal(t, q, string(res[5:]))
	})
}

func TestMySQLConnSend(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("small buffer", func(t *testing.T) {
		q := "select 1"
		data := makeSimplePacket(q)
		// data write to src1, src1 pipe to dst1,
		// dst1 sendTo src2, src2 pipe to dst2, dst2 read data.
		src1, dst1 := net.Pipe()
		src2, dst2 := net.Pipe()

		go func() {
			n, err := src1.Write(data[:])
			require.NoError(t, err)
			require.Equal(t, 13, n)
		}()
		go func() {
			var res [30]byte
			n, err := dst2.Read(res[:])
			require.NoError(t, err)
			require.Equal(t, 8, n)
			require.Equal(t, "sel", string(res[5:n]))

			n, err = dst2.Read(res[:])
			require.NoError(t, err)
			require.Equal(t, 5, n)
			require.Equal(t, "ect 1", string(res[:n]))
		}()
		d1 := newMySQLConn("source", dst1, 8, nil, nil, nil)
		_, err := d1.sendTo(src2)
		require.NoError(t, err)
	})

	t.Run("enough buffer", func(t *testing.T) {
		q := "select 1"
		data := makeSimplePacket(q)
		// data write to src1, src1 pipe to dst1,
		// dst1 sendTo src2, src2 pipe to dst2, dst2 read data.
		src1, dst1 := net.Pipe()
		src2, dst2 := net.Pipe()

		go func() {
			n, err := src1.Write(data[:])
			require.NoError(t, err)
			require.Equal(t, 13, n)
		}()
		go func() {
			var res [30]byte
			n, err := dst2.Read(res[:])
			require.NoError(t, err)
			require.Equal(t, 13, n)
			require.Equal(t, q, string(res[5:n]))
		}()
		d1 := newMySQLConn("source", dst1, 30, nil, nil, nil)
		_, err := d1.sendTo(src2)
		require.NoError(t, err)
	})
}

func TestMySQLConnSize(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("available read", func(t *testing.T) {
		src, dst := net.Pipe()
		go func() {
			n, err := src.Write([]byte("123456789012"))
			require.NoError(t, err)
			require.Equal(t, 12, n)
		}()
		s := newMySQLConn("source", dst, 30, nil, nil, nil)
		require.Equal(t, 0, s.readAvail())

		require.NoError(t, s.receiveAtLeast(3))
		require.Equal(t, 12, s.readAvail())
	})

	t.Run("available write", func(t *testing.T) {
		src, dst := net.Pipe()
		go func() {
			n, err := src.Write([]byte("123456789012"))
			require.NoError(t, err)
			require.Equal(t, 12, n)
		}()
		s := newMySQLConn("source", dst, 30, nil, nil, nil)
		require.Equal(t, 30, s.writeAvail())

		require.NoError(t, s.receiveAtLeast(3))
		require.Equal(t, 18, s.writeAvail())
	})
}
