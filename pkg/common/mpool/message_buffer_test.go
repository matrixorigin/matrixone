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

package mpool

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

// newTestMessageBuffer returns a buffer on a fresh pool and checks, at the
// end of the test, that the buffer is freed and the pool holds nothing.
func newTestMessageBuffer(t *testing.T, maxRetained int) (*MessageBuffer, *MPool) {
	mp := MustNewZero()
	buf := NewMessageBuffer(mp, maxRetained)
	t.Cleanup(func() {
		buf.Free()
		require.Zero(t, mp.CurrNB(), "message buffer leaked")
	})
	return &buf, mp
}

// newCappedPool returns a pool that cannot allocate more than 1MB.
func newCappedPool(t *testing.T) *MPool {
	mp, err := NewMPool("capped_message_buffer", 1<<20, NoFixed)
	require.NoError(t, err)
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
	return mp
}

func TestMessageBufferTypedWritesAndGrowth(t *testing.T) {
	buf, mp := newTestMessageBuffer(t, 1<<20)
	require.Same(t, mp, buf.Pool())
	require.Zero(t, buf.Len())
	require.Nil(t, buf.Bytes())

	var want bytes.Buffer
	require.NoError(t, buf.WriteByte(7))
	want.WriteByte(7)
	require.NoError(t, buf.WriteUint32(0x01020304))
	want.Write(binary.NativeEndian.AppendUint32(nil, 0x01020304))
	require.NoError(t, buf.WriteInt32(-5))
	want.Write(binary.NativeEndian.AppendUint32(nil, uint32(0xfffffffb)))
	require.NoError(t, buf.WriteUint64(1<<40+3))
	want.Write(binary.NativeEndian.AppendUint64(nil, 1<<40+3))
	require.NoError(t, buf.WriteInt64(-9))
	want.Write(binary.NativeEndian.AppendUint64(nil, uint64(0xfffffffffffffff7)))
	// Grow several times: every growth keeps the bytes written so far.
	for i := 0; i < 12; i++ {
		chunk := bytes.Repeat([]byte{byte('a' + i)}, 1<<i)
		n, err := buf.Write(chunk)
		require.NoError(t, err)
		require.Equal(t, len(chunk), n)
		want.Write(chunk)
		require.Equal(t, want.Bytes(), buf.Bytes())
	}
	require.Equal(t, want.Len(), buf.Len())
	require.Equal(t, int64(cap(buf.Bytes())), mp.CurrNB(), "only the current block is held")
}

// EnsureCapacity allocates once for a known size: the writes that follow do
// not allocate again.
func TestMessageBufferEnsureCapacityAllocatesOnce(t *testing.T) {
	buf, mp := newTestMessageBuffer(t, 1<<20)
	require.NoError(t, buf.EnsureCapacity(1000))
	require.NoError(t, buf.EnsureCapacity(10), "a smaller capacity is already there")
	allocs := mp.Stats().NumAlloc.Load()
	for i := 0; i < 100; i++ {
		_, err := buf.Write(make([]byte, 10))
		require.NoError(t, err)
	}
	require.Equal(t, allocs, mp.Stats().NumAlloc.Load())
	require.Equal(t, int64(1), allocs)
}

func TestMessageBufferResize(t *testing.T) {
	buf, mp := newTestMessageBuffer(t, 1<<20)
	data, err := buf.Resize(100)
	require.NoError(t, err)
	require.Len(t, data, 100)
	data[0] = 1
	again, err := buf.Resize(50)
	require.NoError(t, err)
	require.Len(t, again, 50)
	require.True(t, &data[0] == &again[0], "a smaller message reuses the buffer")
	empty, err := buf.Resize(0)
	require.NoError(t, err)
	require.Empty(t, empty)
	require.Equal(t, int64(1), mp.Stats().NumAlloc.Load())
}

// Reset keeps a buffer up to maxRetained for the next message and frees a
// larger one.
func TestMessageBufferResetRetainsOrFrees(t *testing.T) {
	buf, mp := newTestMessageBuffer(t, 4096)
	_, err := buf.Resize(4000)
	require.NoError(t, err)
	buf.Reset()
	require.Zero(t, buf.Len())
	require.NotZero(t, mp.CurrNB(), "a buffer within maxRetained is kept")
	_, err = buf.Resize(4000)
	require.NoError(t, err)
	require.Equal(t, int64(1), mp.Stats().NumAlloc.Load(), "and reused")

	_, err = buf.Resize(10000)
	require.NoError(t, err)
	buf.Reset()
	require.Zero(t, mp.CurrNB(), "a buffer past maxRetained is freed")
	require.Nil(t, buf.Bytes())
	_, err = buf.Resize(10)
	require.NoError(t, err, "and the buffer allocates again")
}

func TestMessageBufferFreeIsIdempotentAndReusable(t *testing.T) {
	buf, mp := newTestMessageBuffer(t, 1<<20)
	buf.Free()
	_, err := buf.Write([]byte("abc"))
	require.NoError(t, err)
	buf.Free()
	buf.Free()
	require.Zero(t, mp.CurrNB())
	require.Zero(t, buf.Len())
	_, err = buf.Write([]byte("again"))
	require.NoError(t, err)
	require.Equal(t, []byte("again"), buf.Bytes())
}

// A failed allocation leaves the buffer's message and memory as they were.
func TestMessageBufferAllocationFailure(t *testing.T) {
	mp := newCappedPool(t)
	buf := NewMessageBuffer(mp, 4<<20)
	defer buf.Free()
	_, err := buf.Write([]byte("kept"))
	require.NoError(t, err)
	held := mp.CurrNB()

	require.Error(t, buf.EnsureCapacity(2<<20))
	_, err = buf.Write(make([]byte, 2<<20))
	require.Error(t, err)
	require.Equal(t, []byte("kept"), buf.Bytes()[:4])
	require.Equal(t, held, mp.CurrNB(), "no memory is taken or lost")
	_, err = buf.Resize(2 << 20)
	require.Error(t, err)
	require.Zero(t, buf.Len(), "a failed Resize starts an empty message")
}

func TestMessageBufferWithoutPool(t *testing.T) {
	var buf MessageBuffer
	require.Nil(t, buf.Pool())
	require.Error(t, buf.EnsureCapacity(1))
	_, err := buf.Write([]byte("x"))
	require.Error(t, err)
	buf.Reset()
	buf.Free()
}

// Growth frees the old block: bytes taken from Bytes before a write are
// invalid after it, which poison-on-free makes visible.
func TestMessageBufferGrowthFreesOldBlock(t *testing.T) {
	EnableDebugPoisonOnFree()
	defer DisableDebugPoisonOnFree()
	buf, _ := newTestMessageBuffer(t, 1<<20)
	_, err := buf.Write(bytes.Repeat([]byte{1}, 64))
	require.NoError(t, err)
	stale := buf.Bytes()
	_, err = buf.Write(make([]byte, 1<<16))
	require.NoError(t, err)
	require.Equal(t, bytes.Repeat([]byte{1}, 64), buf.Bytes()[:64], "the new block keeps the message")
	require.NotEqual(t, bytes.Repeat([]byte{1}, 64), stale, "the old block was freed")
}
