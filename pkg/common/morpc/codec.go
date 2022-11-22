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
	"sync"

	"github.com/cespare/xxhash/v2"
	"github.com/fagongzi/goetty/v2/buf"
	"github.com/fagongzi/goetty/v2/codec"
	"github.com/fagongzi/goetty/v2/codec/length"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
)

var (
	flagPayloadMessage byte = 1
	flagWithChecksum   byte = 2
	flagCustomHeader   byte = 4

	defaultMaxMessageSize = 1024 * 1024 * 100
	checksumFieldBytes    = 8
	totalSizeFieldBytes   = 4
	payloadSizeFieldBytes = 4
)

func GetMessageSize() int {
	return defaultMaxMessageSize
}

// WithCodecEnableChecksum enable checksum
func WithCodecEnableChecksum() CodecOption {
	return func(c *messageCodec) {
		c.bc.enableChecksum = true
	}
}

// WithCodecPayloadCopyBufferSize set payload copy buffer size, if is a PayloadMessage
func WithCodecPayloadCopyBufferSize(value int) CodecOption {
	return func(c *messageCodec) {
		c.bc.payloadBufSize = value
	}
}

// WithCodecIntegrationHLC intrgration hlc
func WithCodecIntegrationHLC(clock clock.Clock) CodecOption {
	return func(c *messageCodec) {
		c.AddHeaderCodec(&hlcCodec{clock: clock})
	}
}

// WithCodecMaxBodySize set rpc max body size
func WithCodecMaxBodySize(size int) CodecOption {
	return func(c *messageCodec) {
		if size == 0 {
			size = defaultMaxMessageSize
		}
		c.codec = length.NewWithSize(c.bc, 0, 0, 0, size)
		c.bc.maxBodySize = size
	}
}

type messageCodec struct {
	codec codec.Codec
	bc    *baseCodec
}

// NewMessageCodec create message codec. The message encoding format consists of a message header and a message body.
// Format:
//  1. Size, 4 bytes, required. Inlucde header and body.
//  2. Message header
//     2.1. Flag, 1 byte, required.
//     2.2. Checksum, 8 byte, optional. Set if has a checksun flag
//     2.3. PayloadSize, 4 byte, optional. Set if the message is a morpc.PayloadMessage.
//     2.4. Custom headers, optional. Set if has custom header codecs
//  3. Message body
//     3.1. message body, required.
//     3.2. payload, optional. Set if has paylad flag.
func NewMessageCodec(messageFactory func() Message, options ...CodecOption) Codec {
	bc := &baseCodec{
		messageFactory: messageFactory,
		maxBodySize:    defaultMaxMessageSize,
	}
	c := &messageCodec{codec: length.NewWithSize(bc, 0, 0, 0, defaultMaxMessageSize), bc: bc}
	c.AddHeaderCodec(&deadlineContextCodec{})
	c.AddHeaderCodec(&traceCodec{})

	for _, opt := range options {
		opt(c)
	}
	return c
}

func (c *messageCodec) Decode(in *buf.ByteBuf) (any, bool, error) {
	return c.codec.Decode(in)
}

func (c *messageCodec) Encode(data interface{}, out *buf.ByteBuf, conn io.Writer) error {
	return c.bc.Encode(data, out, conn)
}

func (c *messageCodec) AddHeaderCodec(hc HeaderCodec) {
	c.bc.headerCodecs = append(c.bc.headerCodecs, hc)
}

type baseCodec struct {
	enableChecksum bool
	payloadBufSize int
	maxBodySize    int
	messageFactory func() Message
	headerCodecs   []HeaderCodec
}

func (c *baseCodec) Decode(in *buf.ByteBuf) (any, bool, error) {
	msg := RPCMessage{Message: c.messageFactory()}
	offset := 0
	data := getDecodeData(in)

	// 2.1
	flag, n := readFlag(data, offset)
	offset += n

	// 2.2
	expectChecksum, n := readChecksum(flag, data, offset)
	offset += n

	// 2.3
	payloadSize, n := readPayloadSize(flag, data, offset)
	offset += n

	// 2.4
	n, err := c.readCustomHeaders(flag, &msg, data, offset)
	if err != nil {
		return nil, false, err
	}
	offset += n

	// 3.1 and 3.2
	if err := readMessage(flag, data, offset, expectChecksum, payloadSize, &msg); err != nil {
		return nil, false, err
	}

	in.SetReadIndex(in.GetMarkIndex())
	in.ClearMark()
	return msg, true, nil
}

func (c *baseCodec) Encode(data interface{}, out *buf.ByteBuf, conn io.Writer) error {
	msg, ok := data.(RPCMessage)
	if !ok {
		return moerr.NewInternalError("not support %T %+v", data, data)
	}

	startWriteOffset := out.GetWriteOffset()
	totalSize := 0
	// The total message size cannot be determined at the beginning and needs to wait until all the
	// dynamic content is determined before the total size can be determined. After the total size is
	// determined, we need to write the total size data in the location of totalSizeAt
	totalSizeAt := skip(totalSizeFieldBytes, out)

	// 2.1 flag
	flag := c.getFlag(msg.Message)
	out.WriteByte(flag)
	totalSize += 1

	// 2.2 checksum, similar to totalSize, we do not currently know the size of the message body.
	checksumAt := -1
	if flag&flagWithChecksum != 0 {
		checksumAt = skip(checksumFieldBytes, out)
		totalSize += checksumFieldBytes
	}

	// 2.3 payload
	var payloadData []byte
	var payloadMsg PayloadMessage
	var hasPayload bool
	if payloadMsg, hasPayload = msg.Message.(PayloadMessage); hasPayload {
		// set payload filed to nil to avoid payload being written to the out buffer, and write directly
		// to the socket afterwards to reduce one payload.
		payloadData = payloadMsg.GetPayloadField()
		payloadMsg.SetPayloadField(nil)

		out.WriteInt(len(payloadData))
		totalSize += payloadSizeFieldBytes + len(payloadData)
	}

	// skip all written data by this message
	discardWritten := func() {
		out.SetWriteIndexByOffset(startWriteOffset)
		if hasPayload {
			payloadMsg.SetPayloadField(payloadData)
		}
	}

	// 2.4 Custom header size
	n, err := c.encodeCustomHeaders(&msg, out)
	if err != nil {
		return err
	}
	totalSize += n

	// 3.1 message body
	bodySize, body, err := c.writeBody(out, msg.Message, totalSize)
	if err != nil {
		discardWritten()
		return err
	}

	// now, header and body are all determined, we need to fill the totalSize and checksum
	// fill total size
	totalSize += bodySize
	writeIntAt(totalSizeAt, out, totalSize)

	// fill checksum
	if checksumAt != -1 {
		if err := writeChecksum(checksumAt, out, body, payloadData); err != nil {
			discardWritten()
			return err
		}
	}

	// 3.2 payload
	if hasPayload {
		// resume payload to payload message
		payloadMsg.SetPayloadField(payloadData)
		if err := writePayload(out, payloadData, conn, c.payloadBufSize); err != nil {
			return err
		}
	}

	return nil
}

func (c *baseCodec) getFlag(msg Message) byte {
	flag := byte(0)
	if c.enableChecksum {
		flag |= flagWithChecksum
	}
	if len(c.headerCodecs) > 0 {
		flag |= flagCustomHeader
	}
	if _, ok := msg.(PayloadMessage); ok {
		flag |= flagPayloadMessage
	}
	return flag
}

func (c *baseCodec) encodeCustomHeaders(msg *RPCMessage, out *buf.ByteBuf) (int, error) {
	if len(c.headerCodecs) == 0 {
		return 0, nil
	}

	size := 0
	for _, hc := range c.headerCodecs {
		v, err := hc.Encode(msg, out)
		if err != nil {
			return 0, err
		}
		size += v
	}
	return size, nil
}

func (c *baseCodec) readCustomHeaders(flag byte, msg *RPCMessage, data []byte, offset int) (int, error) {
	if flag&flagCustomHeader == 0 {
		return 0, nil
	}

	readed := 0
	for _, hc := range c.headerCodecs {
		n, err := hc.Decode(msg, data[offset+readed:])
		if err != nil {
			return 0, err
		}
		readed += n
	}
	return readed, nil
}

var (
	checksumPool = sync.Pool{
		New: func() any {
			return xxhash.New()
		},
	}
)

func acquireChecksum() *xxhash.Digest {
	return checksumPool.Get().(*xxhash.Digest)
}

func releaseChecksum(checksum *xxhash.Digest) {
	checksum.Reset()
	checksumPool.Put(checksum)
}

func skip(n int, out *buf.ByteBuf) int {
	_, offset := setWriterIndexAfterGow(out, n)
	return offset
}

func writeIntAt(offset int, out *buf.ByteBuf, value int) {
	idx := out.GetReadIndex() + offset
	buf.Int2BytesTo(value, out.RawSlice(idx, idx+4))
}

func writeUint64At(offset int, out *buf.ByteBuf, value uint64) {
	idx := out.GetReadIndex() + offset
	buf.Uint64ToBytesTo(value, out.RawSlice(idx, idx+8))
}

func (c *baseCodec) writeBody(out *buf.ByteBuf, msg Message, writtenSize int) (int, []byte, error) {
	maxCanWrite := c.maxBodySize - writtenSize
	size := msg.Size()
	if size > maxCanWrite {
		return 0, nil,
			moerr.NewInternalError("message body %d is too large, max is %d",
				size+writtenSize,
				c.maxBodySize)
	}

	index, _ := setWriterIndexAfterGow(out, size)
	data := out.RawSlice(index, index+size)
	if _, err := msg.MarshalTo(data); err != nil {
		return 0, nil, err
	}
	return size, data, nil
}

func writePayload(out *buf.ByteBuf, payload []byte, conn io.Writer, copyBuffer int) error {
	if len(payload) == 0 {
		return nil
	}

	// reset here to avoid buffer expansion as much as possible
	defer out.Reset()

	// first, write header and body to socket
	if _, err := out.WriteTo(conn); err != nil {
		return err
	}

	// write payload to socket
	if err := buf.WriteTo(payload, conn, copyBuffer); err != nil {
		return err
	}
	return nil
}

func writeChecksum(offset int, out *buf.ByteBuf, body, payload []byte) error {
	checksum := acquireChecksum()
	defer releaseChecksum(checksum)

	_, err := checksum.Write(body)
	if err != nil {
		return err
	}
	if len(payload) > 0 {
		_, err = checksum.Write(payload)
		if err != nil {
			return err
		}
	}
	writeUint64At(offset, out, checksum.Sum64())
	return nil
}

func getDecodeData(in *buf.ByteBuf) []byte {
	return in.RawSlice(in.GetReadIndex(), in.GetMarkIndex())
}

func readFlag(data []byte, offset int) (byte, int) {
	return data[offset], 1
}

func readChecksum(flag byte, data []byte, offset int) (uint64, int) {
	if flag&flagWithChecksum == 0 {
		return 0, 0
	}

	return buf.Byte2Uint64(data[offset:]), checksumFieldBytes
}

func readPayloadSize(flag byte, data []byte, offset int) (int, int) {
	if flag&flagPayloadMessage == 0 {
		return 0, 0
	}

	return buf.Byte2Int(data[offset:]), payloadSizeFieldBytes
}

func validChecksum(body, payload []byte, expectChecksum uint64) error {
	checksum := acquireChecksum()
	defer releaseChecksum(checksum)

	_, err := checksum.Write(body)
	if err != nil {
		return err
	}
	if len(payload) > 0 {
		_, err := checksum.Write(payload)
		if err != nil {
			return err
		}
	}
	actulChecksum := checksum.Sum64()
	if actulChecksum != expectChecksum {
		return moerr.NewInternalError("checksum mismatch, expect %d, got %d",
			expectChecksum,
			actulChecksum)
	}
	return nil
}

func readMessage(flag byte, data []byte, offset int, expectChecksum uint64, payloadSize int, msg *RPCMessage) error {
	body := data[offset : len(data)-payloadSize]
	payload := data[len(data)-payloadSize:]
	if flag&flagWithChecksum != 0 {
		if err := validChecksum(body, payload, expectChecksum); err != nil {
			return err
		}
	}

	if err := msg.Message.Unmarshal(body); err != nil {
		return err
	}

	if payloadSize > 0 {
		msg.Message.(PayloadMessage).SetPayloadField(payload)
	}
	return nil
}

func setWriterIndexAfterGow(out *buf.ByteBuf, n int) (int, int) {
	offset := out.Readable()
	out.Grow(n)
	out.SetWriteIndex(out.GetReadIndex() + offset + n)
	return out.GetReadIndex() + offset, offset
}
