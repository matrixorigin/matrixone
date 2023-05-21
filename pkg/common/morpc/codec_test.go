// Copyright 2021 - 2022 Matrix Origin
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

package morpc

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/fagongzi/goetty/v2/buf"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncodeAndDecode(t *testing.T) {
	codec := newTestCodec()
	buf := buf.NewByteBuf(1)

	ctx, cancel := context.WithTimeout(context.TODO(), time.Hour*10)
	defer cancel()

	msg := RPCMessage{Ctx: ctx, Message: newTestMessage(1)}
	err := codec.Encode(msg, buf, nil)
	assert.NoError(t, err)

	v, ok, err := codec.Decode(buf)
	assert.True(t, ok)
	assert.Equal(t, msg.Message, v.(RPCMessage).Message)
	assert.NoError(t, err)
	assert.NotNil(t, v.(RPCMessage).Ctx)
	assert.NotNil(t, v.(RPCMessage).Cancel)
}

func TestEncodeAndDecodeWithStream(t *testing.T) {
	codec := newTestCodec()
	buf := buf.NewByteBuf(1)

	ctx, cancel := context.WithTimeout(context.TODO(), time.Hour*10)
	defer cancel()

	msg := RPCMessage{Ctx: ctx, Message: newTestMessage(1), stream: true, streamSequence: 1}
	err := codec.Encode(msg, buf, nil)
	assert.NoError(t, err)

	v, ok, err := codec.Decode(buf)
	assert.True(t, ok)
	assert.Equal(t, msg.Message, v.(RPCMessage).Message)
	assert.True(t, v.(RPCMessage).stream)
	assert.Equal(t, uint32(1), v.(RPCMessage).streamSequence)
	assert.NoError(t, err)
	assert.NotNil(t, v.(RPCMessage).Ctx)
	assert.NotNil(t, v.(RPCMessage).Cancel)
}

func TestEncodeAndDecodeWithChecksum(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), time.Hour*10)
	defer cancel()
	codec := newTestCodec(WithCodecEnableChecksum(),
		WithCodecIntegrationHLC(clock.NewHLCClock(func() int64 { return 0 }, 0)))
	buf1 := buf.NewByteBuf(32)

	msg := newTestMessage(1)
	err := codec.Encode(RPCMessage{Ctx: ctx, Message: msg}, buf1, nil)
	assert.NoError(t, err)

	buf.Uint64ToBytesTo(0, buf1.RawSlice(5, 5+8))
	_, ok, err := codec.Decode(buf1)
	assert.False(t, ok)
	assert.Error(t, err)
}

func TestEncodeAndDecodeWithCompress(t *testing.T) {
	p, err := mpool.NewMPool("test", 0, 0)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.TODO(), time.Hour*10)
	defer cancel()
	codec := newTestCodec(WithCodecEnableCompress(p))
	buf1 := buf.NewByteBuf(32)

	msg := newTestMessage(1)
	err = codec.Encode(RPCMessage{Ctx: ctx, Message: msg}, buf1, nil)
	assert.NoError(t, err)

	buf.Uint64ToBytesTo(0, buf1.RawSlice(5, 5+8))
	resp, ok, err := codec.Decode(buf1)
	assert.NoError(t, err)
	assert.True(t, ok)

	assert.Equal(t, msg, resp.(RPCMessage).Message)
}

func TestEncodeAndDecodeWithCompressAndHasPayload(t *testing.T) {
	// XXX Zhang Xu
	//
	// in codec, readMessage, dstPayload is freed c.pool.Free(dstPayload)
	// but it is returned again in SetPayloadField
	// We have to enable the NoFixed flag so that mpool Free does not
	// really destroy the memory.
	p, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.TODO(), time.Hour*10)
	defer cancel()

	codec := newTestCodec(WithCodecEnableCompress(p))
	buf1 := buf.NewByteBuf(32)
	buf2 := buf.NewByteBuf(32)

	msg := RPCMessage{Ctx: ctx, Message: newTestMessage(1)}
	msg.Message.(*testMessage).payload = []byte(strings.Repeat("payload", 100))
	err = codec.Encode(msg, buf1, buf2)
	assert.NoError(t, err)

	v, ok, err := codec.Decode(buf2)
	assert.True(t, ok)
	assert.Equal(t, msg.Message, v.(RPCMessage).Message)
	assert.NoError(t, err)
	assert.NotNil(t, v.(RPCMessage).Ctx)
	assert.NotNil(t, v.(RPCMessage).Cancel)
}

func TestEncodeAndDecodeAndChecksumMismatch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), time.Hour*10)
	defer cancel()

	codec := newTestCodec(WithCodecEnableChecksum())
	buf1 := buf.NewByteBuf(32)

	msg := RPCMessage{Ctx: ctx, Message: newTestMessage(1)}
	err := codec.Encode(msg, buf1, nil)
	assert.NoError(t, err)

	buf.Uint64ToBytesTo(0, buf1.RawSlice(5, 5+8))

	v, ok, err := codec.Decode(buf1)
	assert.False(t, ok)
	assert.Error(t, err)
	assert.Nil(t, v)
}

func TestEncodeAndDecodeWithPayload(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), time.Hour*10)
	defer cancel()

	codec := newTestCodec()
	buf1 := buf.NewByteBuf(32)
	buf2 := buf.NewByteBuf(32)

	msg := RPCMessage{Ctx: ctx, Message: newTestMessage(1)}
	msg.Message.(*testMessage).payload = []byte("payload")
	err := codec.Encode(msg, buf1, buf2)
	assert.NoError(t, err)

	v, ok, err := codec.Decode(buf2)
	assert.True(t, ok)
	assert.Equal(t, msg.Message, v.(RPCMessage).Message)
	assert.NoError(t, err)
	assert.NotNil(t, v.(RPCMessage).Ctx)
	assert.NotNil(t, v.(RPCMessage).Cancel)
}

func TestEncodeAndDecodeWithPayloadAndChecksum(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), time.Hour*10)
	defer cancel()

	codec := newTestCodec(WithCodecEnableChecksum())
	buf1 := buf.NewByteBuf(32)
	buf2 := buf.NewByteBuf(32)

	msg := RPCMessage{Ctx: ctx, Message: newTestMessage(1)}
	msg.Message.(*testMessage).payload = []byte("payload")
	err := codec.Encode(msg, buf1, buf2)
	assert.NoError(t, err)

	v, ok, err := codec.Decode(buf2)
	assert.True(t, ok)
	assert.Equal(t, msg.Message, v.(RPCMessage).Message)
	assert.NoError(t, err)
	assert.NotNil(t, v.(RPCMessage).Ctx)
	assert.NotNil(t, v.(RPCMessage).Cancel)
}

func TestEncodeAndDecodeWithEmptyPayloadAndChecksum(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), time.Hour*10)
	defer cancel()

	codec := newTestCodec(WithCodecEnableChecksum())
	buf1 := buf.NewByteBuf(32)
	buf2 := buf.NewByteBuf(32)

	msg := RPCMessage{Ctx: ctx, Message: newTestMessage(1)}
	err := codec.Encode(msg, buf1, buf2)
	assert.NoError(t, err)
	io.Copy(buf2, buf1)

	v, ok, err := codec.Decode(buf2)
	assert.True(t, ok)
	assert.Equal(t, msg.Message, v.(RPCMessage).Message)
	assert.NoError(t, err)
	assert.NotNil(t, v.(RPCMessage).Ctx)
	assert.NotNil(t, v.(RPCMessage).Cancel)
}

func TestEncodeAndDecodeWithEmptyPayloadAndChecksumMismatch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), time.Hour*10)
	defer cancel()

	codec := newTestCodec(WithCodecEnableChecksum())
	buf1 := buf.NewByteBuf(32)
	buf2 := buf.NewByteBuf(32)

	msg := RPCMessage{Ctx: ctx, Message: newTestMessage(1)}
	err := codec.Encode(msg, buf1, buf2)
	assert.NoError(t, err)
	io.Copy(buf2, buf1)

	buf.Uint64ToBytesTo(0, buf2.RawSlice(5, 5+8))
	_, ok, err := codec.Decode(buf2)
	assert.False(t, ok)
	assert.Error(t, err)
}

func TestNewWithMaxBodySize(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), time.Hour*10)
	defer cancel()

	maxBodySize := 1024 * 1024 * 20
	codec := newTestCodec(WithCodecMaxBodySize(maxBodySize + 1024))
	buf1 := buf.NewByteBuf(32)
	buf2 := buf.NewByteBuf(32)

	msg := RPCMessage{Ctx: ctx, Message: newTestMessage(1)}
	msg.Message.(*testMessage).payload = make([]byte, 1024*1024*11)
	err := codec.Encode(msg, buf1, buf2)
	assert.NoError(t, err)

	v, ok, err := codec.Decode(buf2)
	assert.True(t, ok)
	assert.Equal(t, msg.Message, v.(RPCMessage).Message)
	assert.NoError(t, err)
	assert.NotNil(t, v.(RPCMessage).Ctx)
	assert.NotNil(t, v.(RPCMessage).Cancel)
}

func TestBufferScale(t *testing.T) {
	stopper := stopper.NewStopper("")
	defer stopper.Stop()

	ctx, cancel := context.WithTimeout(context.TODO(), time.Hour*10)
	defer cancel()
	c1 := newTestCodec(WithCodecEnableChecksum(),
		WithCodecIntegrationHLC(clock.NewUnixNanoHLCClockWithStopper(stopper, 0)),
		WithCodecPayloadCopyBufferSize(16*1024))
	c2 := newTestCodec(WithCodecEnableChecksum(),
		WithCodecIntegrationHLC(clock.NewUnixNanoHLCClockWithStopper(stopper, 0)),
		WithCodecPayloadCopyBufferSize(16*1024))
	out := buf.NewByteBuf(32)
	conn := buf.NewByteBuf(32)

	n := 100
	var messages []RPCMessage
	for i := 0; i < n; i++ {
		msg := RPCMessage{Ctx: ctx, Message: newTestMessage(uint64(i))}
		payload := make([]byte, 1024*1024)
		payload[len(payload)-1] = byte(i)
		msg.Message.(*testMessage).payload = payload
		messages = append(messages, msg)

		require.NoError(t, c1.Encode(msg, out, conn))
	}
	_, err := out.WriteTo(conn)
	if err != nil {
		require.Equal(t, io.EOF, err)
	}

	for i := 0; i < n; i++ {
		msg, ok, err := c2.Decode(conn)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, messages[i].Message, msg.(RPCMessage).Message)
	}
}

func TestEncodeAndDecodeInternal(t *testing.T) {
	codec := newTestCodec()
	buf := buf.NewByteBuf(1)

	ctx, cancel := context.WithTimeout(context.TODO(), time.Hour*10)
	defer cancel()

	msg := RPCMessage{Ctx: ctx, Message: &flagOnlyMessage{flag: flagPing}, internal: true}
	err := codec.Encode(msg, buf, nil)
	assert.NoError(t, err)

	v, ok, err := codec.Decode(buf)
	assert.True(t, ok)
	assert.Equal(t, msg.Message, v.(RPCMessage).Message)
	assert.NoError(t, err)
	assert.NotNil(t, v.(RPCMessage).Ctx)
	assert.NotNil(t, v.(RPCMessage).Cancel)
}
