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
	"testing"
	"time"

	"github.com/fagongzi/goetty/v2/buf"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/stretchr/testify/assert"
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
	assert.NotNil(t, v.(RPCMessage).cancel)
}

func TestEncodeAndDecodeAndChecksum(t *testing.T) {
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
	assert.NotNil(t, v.(RPCMessage).cancel)
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
	assert.NotNil(t, v.(RPCMessage).cancel)
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
	assert.NotNil(t, v.(RPCMessage).cancel)
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
