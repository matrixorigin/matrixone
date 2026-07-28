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
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/fagongzi/goetty/v2/buf"
	"github.com/matrixorigin/matrixone/pkg/common/malloc"
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
	ctx, cancel := context.WithTimeout(context.TODO(), time.Hour*10)
	defer cancel()
	codec := newTestCodec(WithCodecEnableCompress(malloc.GetDefault(nil)))
	buf1 := buf.NewByteBuf(32)

	msg := newTestMessage(1)
	err := codec.Encode(RPCMessage{Ctx: ctx, Message: msg}, buf1, nil)
	assert.NoError(t, err)

	buf.Uint64ToBytesTo(0, buf1.RawSlice(5, 5+8))
	resp, ok, err := codec.Decode(buf1)
	assert.NoError(t, err)
	assert.True(t, ok)

	assert.Equal(t, msg, resp.(RPCMessage).Message)
}

func TestCodecRejectsTruncatedOptionalHeaders(t *testing.T) {
	tests := []struct {
		name       string
		flag       byte
		headerSize int
	}{
		{name: "checksum", flag: flagChecksumEnabled, headerSize: checksumFieldBytes},
		{name: "payload size", flag: flagHashPayload, headerSize: payloadSizeFieldBytes},
		{name: "stream sequence", flag: flagStreamingMessage, headerSize: 4},
		{
			name:       "combined",
			flag:       flagChecksumEnabled | flagHashPayload | flagStreamingMessage,
			headerSize: checksumFieldBytes + payloadSizeFieldBytes + 4,
		},
		{
			name:       "custom headers",
			flag:       flagHasCustomHeader,
			headerSize: 8 + 1, // deadline followed by the trace-length byte
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			codec := newTestCodec()
			for frameSize := 1; frameSize < 1+test.headerSize; frameSize++ {
				data := make([]byte, frameSize)
				data[0] = test.flag
				_, ok, err := codec.Decode(newCodecFrame(t, data))
				require.False(t, ok, "frame size %d", frameSize)
				require.Error(t, err, "frame size %d", frameSize)
			}
		})
	}
}

func TestCodecRejectsTruncatedTraceHeader(t *testing.T) {
	// The trace header declares four bytes but only three follow its length byte.
	// Before the cumulative bounds check this reached an out-of-range slice.
	data := append([]byte{flagHasCustomHeader}, make([]byte, 8)...)
	data = append(data, 4, 0, 0, 0)

	_, ok, err := newTestCodec().Decode(newCodecFrame(t, data))
	require.False(t, ok)
	require.Error(t, err)
}

func TestCodecCancelsContextAfterDecodeError(t *testing.T) {
	header := &failingContextHeaderCodec{}
	codec := newTestCodec().(*messageCodec)
	codec.bc.headerCodecs = []HeaderCodec{&deadlineContextCodec{}, header}

	data := []byte{flagHasCustomHeader}
	deadline := buf.NewByteBuf(8)
	require.NoError(t, deadline.WriteInt64(int64(time.Hour)))
	data = append(data, deadline.RawSlice(0, 8)...)

	_, ok, err := codec.Decode(newCodecFrame(t, data))
	require.False(t, ok)
	require.Error(t, err)
	require.NotNil(t, header.ctx)
	select {
	case <-header.ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("decode error did not cancel the decoded context")
	}
}

func TestCodecRejectsPayloadFlagForNonPayloadMessage(t *testing.T) {
	codec := NewMessageCodec("", func() Message { return &codecBodyMessage{} })
	data := []byte{flagHashPayload, 0, 0, 0, 1}
	data = append(data, make([]byte, 9)...)

	_, ok, err := codec.Decode(newCodecFrame(t, data))
	require.False(t, ok)
	require.Error(t, err)
}

func TestCodecRejectsCompressedMessageWhenDisabled(t *testing.T) {
	codec := NewMessageCodec("", func() Message { return &codecBodyMessage{} })
	data := append([]byte{flagCompressEnabled}, make([]byte, 8)...)

	_, ok, err := codec.Decode(newCodecFrame(t, data))
	require.False(t, ok)
	require.Error(t, err)
}

func TestCodecRejectsTruncatedInternalMessageBody(t *testing.T) {
	for _, flag := range []byte{flagPing, flagPong} {
		for bodySize := 0; bodySize < 8; bodySize++ {
			data := append([]byte{flag}, make([]byte, bodySize)...)
			_, ok, err := newTestCodec().Decode(newCodecFrame(t, data))
			require.False(t, ok, "flag %d, body size %d", flag, bodySize)
			require.Error(t, err, "flag %d, body size %d", flag, bodySize)
		}
	}
}

func TestCodecRejectsOversizedDecompressedBody(t *testing.T) {
	const maxBodySize = 1024
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()

	encoder := NewMessageCodec(
		"",
		func() Message { return &codecBodyMessage{} },
		WithCodecEnableCompress(malloc.GetDefault(nil)),
		WithCodecMaxBodySize(128*1024),
	)
	decoder := NewMessageCodec(
		"",
		func() Message { return &codecBodyMessage{} },
		WithCodecEnableCompress(malloc.GetDefault(nil)),
		WithCodecMaxBodySize(maxBodySize),
	)
	wire := buf.NewByteBuf(32)
	msg := &codecBodyMessage{id: 1, body: []byte(strings.Repeat("a", 64*1024))}
	require.NoError(t, encoder.Encode(RPCMessage{Ctx: ctx, Message: msg}, wire, nil))

	_, ok, err := decoder.Decode(wire)
	require.False(t, ok)
	require.Error(t, err)
}

func TestCodecRejectsOversizedDecompressedPayload(t *testing.T) {
	const maxBodySize = 1024
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()

	encoder := newTestCodec(
		WithCodecEnableCompress(malloc.GetDefault(nil)),
		WithCodecMaxBodySize(128*1024),
	)
	decoder := newTestCodec(
		WithCodecEnableCompress(malloc.GetDefault(nil)),
		WithCodecMaxBodySize(maxBodySize),
	)
	out := buf.NewByteBuf(32)
	wire := buf.NewByteBuf(32)
	msg := RPCMessage{Ctx: ctx, Message: newTestMessage(1)}
	msg.Message.(*testMessage).payload = []byte(strings.Repeat("a", 64*1024))
	require.NoError(t, encoder.Encode(msg, out, wire))

	_, ok, err := decoder.Decode(wire)
	require.False(t, ok)
	require.Error(t, err)
}

func TestCodecRejectsCombinedDecompressedMessageOverLimit(t *testing.T) {
	const maxBodySize = 1024
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()

	encoder := NewMessageCodec(
		"",
		func() Message { return &codecBodyPayloadMessage{} },
		WithCodecEnableCompress(malloc.GetDefault(nil)),
		WithCodecMaxBodySize(128*1024),
	)
	decoder := NewMessageCodec(
		"",
		func() Message { return &codecBodyPayloadMessage{} },
		WithCodecEnableCompress(malloc.GetDefault(nil)),
		WithCodecMaxBodySize(maxBodySize),
	)
	out := buf.NewByteBuf(32)
	wire := buf.NewByteBuf(32)
	msg := &codecBodyPayloadMessage{
		codecBodyMessage: codecBodyMessage{id: 1, body: []byte(strings.Repeat("a", 600))},
		payload:          []byte(strings.Repeat("b", 600)),
	}
	require.NoError(t, encoder.Encode(RPCMessage{Ctx: ctx, Message: msg}, out, wire))

	_, ok, err := decoder.Decode(wire)
	require.False(t, ok)
	require.Error(t, err)
}

func TestEncodeAndDecodeWithCompressAndHasPayload(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), time.Hour*10)
	defer cancel()

	codec := newTestCodec(WithCodecEnableCompress(malloc.GetDefault(nil)))
	buf1 := buf.NewByteBuf(32)
	buf2 := buf.NewByteBuf(32)

	msg := RPCMessage{Ctx: ctx, Message: newTestMessage(1)}
	msg.Message.(*testMessage).payload = []byte(strings.Repeat("payload", 100))
	err := codec.Encode(msg, buf1, buf2)
	assert.NoError(t, err)

	v, ok, err := codec.Decode(buf2)
	assert.True(t, ok)
	assert.Equal(t, msg.Message, v.(RPCMessage).Message)
	assert.NoError(t, err)
	assert.NotNil(t, v.(RPCMessage).Ctx)
	assert.NotNil(t, v.(RPCMessage).Cancel)
}

func newCodecFrame(t *testing.T, data []byte) *buf.ByteBuf {
	t.Helper()
	frame := buf.NewByteBuf(len(data) + totalSizeFieldBytes)
	require.NoError(t, frame.WriteInt(len(data)))
	_, err := frame.Write(data)
	require.NoError(t, err)
	return frame
}

type codecBodyMessage struct {
	id   uint64
	body []byte
}

type codecBodyPayloadMessage struct {
	codecBodyMessage
	payload []byte
}

func (m *codecBodyPayloadMessage) ProtoSize() int {
	return m.codecBodyMessage.ProtoSize() + len(m.payload)
}

func (m *codecBodyPayloadMessage) GetPayloadField() []byte {
	return m.payload
}

func (m *codecBodyPayloadMessage) SetPayloadField(data []byte) {
	m.payload = append(m.payload[:0], data...)
}

type failingContextHeaderCodec struct {
	ctx context.Context
}

func (c *failingContextHeaderCodec) Encode(*RPCMessage, *buf.ByteBuf) (int, error) {
	return 0, errors.New("injected header encode failure")
}

func (c *failingContextHeaderCodec) Decode(msg *RPCMessage, _ []byte) (int, error) {
	c.ctx = msg.Ctx
	return 0, errors.New("injected header decode failure")
}

func (m *codecBodyMessage) SetID(id uint64)     { m.id = id }
func (m *codecBodyMessage) GetID() uint64       { return m.id }
func (m *codecBodyMessage) DebugString() string { return "codec-body-message" }
func (m *codecBodyMessage) ProtoSize() int      { return 8 + len(m.body) }

func (m *codecBodyMessage) MarshalTo(data []byte) (int, error) {
	buf.Uint64ToBytesTo(m.id, data)
	copy(data[8:], m.body)
	return m.ProtoSize(), nil
}

func (m *codecBodyMessage) Unmarshal(data []byte) error {
	if len(data) < 8 {
		return io.ErrUnexpectedEOF
	}
	m.id = buf.Byte2Uint64(data)
	m.body = append(m.body[:0], data[8:]...)
	return nil
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
