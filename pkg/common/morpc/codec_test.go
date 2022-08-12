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
	"io"
	"testing"

	"github.com/fagongzi/goetty/v2/buf"
	"github.com/stretchr/testify/assert"
)

func TestEncodeAndDecode(t *testing.T) {
	codec := newTestCodec()
	buf := buf.NewByteBuf(32)

	msg := newTestMessage(1)
	err := codec.Encode(msg, buf, nil)
	assert.NoError(t, err)

	v, ok, err := codec.Decode(buf)
	assert.True(t, ok)
	assert.Equal(t, msg, v)
	assert.NoError(t, err)
}

func TestEncodeAndDecodeAndChecksum(t *testing.T) {
	codec := newTestCodecWithChecksum()
	buf1 := buf.NewByteBuf(32)

	msg := newTestMessage(1)
	err := codec.Encode(msg, buf1, nil)
	assert.NoError(t, err)

	buf.Uint64ToBytesTo(0, buf1.RawSlice(5, 5+8))
	_, ok, err := codec.Decode(buf1)
	assert.False(t, ok)
	assert.Error(t, err)
}

func TestEncodeAndDecodeAndChecksumMismatch(t *testing.T) {
	codec := newTestCodecWithChecksum()
	buf := buf.NewByteBuf(32)

	msg := newTestMessage(1)
	err := codec.Encode(msg, buf, nil)
	assert.NoError(t, err)

	v, ok, err := codec.Decode(buf)
	assert.True(t, ok)
	assert.Equal(t, msg, v)
	assert.NoError(t, err)
}

func TestEncodeAndDecodeWithPayload(t *testing.T) {
	codec := newTestCodec()
	buf1 := buf.NewByteBuf(32)
	buf2 := buf.NewByteBuf(32)

	msg := newTestMessage(1)
	msg.payload = []byte("payload")
	err := codec.Encode(msg, buf1, buf2)
	assert.NoError(t, err)

	v, ok, err := codec.Decode(buf2)
	assert.True(t, ok)
	assert.Equal(t, msg, v)
	assert.NoError(t, err)
}

func TestEncodeAndDecodeWithPayloadAndChecksum(t *testing.T) {
	codec := newTestCodecWithChecksum()
	buf1 := buf.NewByteBuf(32)
	buf2 := buf.NewByteBuf(32)

	msg := newTestMessage(1)
	msg.payload = []byte("payload")
	err := codec.Encode(msg, buf1, buf2)
	assert.NoError(t, err)

	v, ok, err := codec.Decode(buf2)
	assert.True(t, ok)
	assert.Equal(t, msg, v)
	assert.NoError(t, err)
}

func TestEncodeAndDecodeWithEmptyPayloadAndChecksum(t *testing.T) {
	codec := newTestCodecWithChecksum()
	buf1 := buf.NewByteBuf(32)
	buf2 := buf.NewByteBuf(32)

	msg := newTestMessage(1)
	err := codec.Encode(msg, buf1, buf2)
	assert.NoError(t, err)
	io.Copy(buf2, buf1)

	v, ok, err := codec.Decode(buf2)
	assert.True(t, ok)
	assert.Equal(t, msg, v)
	assert.NoError(t, err)
}

func TestEncodeAndDecodeWithEmptyPayloadAndChecksumMismatch(t *testing.T) {
	codec := newTestCodecWithChecksum()
	buf1 := buf.NewByteBuf(32)
	buf2 := buf.NewByteBuf(32)

	msg := newTestMessage(1)
	err := codec.Encode(msg, buf1, buf2)
	assert.NoError(t, err)
	io.Copy(buf2, buf1)

	buf.Uint64ToBytesTo(0, buf2.RawSlice(5, 5+8))
	_, ok, err := codec.Decode(buf2)
	assert.False(t, ok)
	assert.Error(t, err)
}
