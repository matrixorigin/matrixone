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
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/pierrec/lz4/v4"
)

const (
	flagHashPayload byte = 1 << iota
	flagChecksumEnabled
	flagHasCustomHeader
	flagCompressEnabled
	flagStreamingMessage
	flagPing
	flagPong
)

var (
	defaultMaxMessageSize = 1024 * 1024 * 100
	checksumFieldBytes    = 8
	totalSizeFieldBytes   = 4
	payloadSizeFieldBytes = 4

	approximateHeaderSize = 128
)

func GetMessageSize() int {
	return defaultMaxMessageSize
}

// WithCodecEnableChecksum enable checksum
func WithCodecEnableChecksum() CodecOption {
	return func(c *messageCodec) {
		c.bc.checksumEnabled = true
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

// WithCodecEnableCompress enable compress body and payload
func WithCodecEnableCompress(pool *mpool.MPool) CodecOption {
	return func(c *messageCodec) {
		c.bc.compressEnabled = true
		c.bc.pool = pool
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
//     2.4. Streaming sequence, 4 byte, optional. Set if the message is in a streaming.
//     2.5. Custom headers, optional. Set if has custom header codecs
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

func (c *messageCodec) Valid(msg Message) error {
	n := msg.Size() + approximateHeaderSize
	if n >= c.bc.maxBodySize {
		return moerr.NewInternalErrorNoCtx("message body %d is too large, max is %d",
			n,
			c.bc.maxBodySize)
	}
	return nil
}

func (c *messageCodec) AddHeaderCodec(hc HeaderCodec) {
	c.bc.headerCodecs = append(c.bc.headerCodecs, hc)
}

type baseCodec struct {
	pool            *mpool.MPool
	checksumEnabled bool
	compressEnabled bool
	payloadBufSize  int
	maxBodySize     int
	messageFactory  func() Message
	headerCodecs    []HeaderCodec
}

func (c *baseCodec) Decode(in *buf.ByteBuf) (any, bool, error) {
	msg := RPCMessage{}
	offset := 0
	data := getDecodeData(in)

	// 2.1
	flag, n := c.readFlag(&msg, data, offset)
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

	// 2.5
	offset += readStreaming(flag, &msg, data, offset)

	// 3.1 and 3.2
	if err := c.readMessage(flag, data, offset, expectChecksum, payloadSize, &msg); err != nil {
		return nil, false, err
	}

	in.SetReadIndex(in.GetMarkIndex())
	in.ClearMark()
	return msg, true, nil
}

func (c *baseCodec) Encode(data interface{}, out *buf.ByteBuf, conn io.Writer) error {
	msg, ok := data.(RPCMessage)
	if !ok {
		return moerr.NewInternalErrorNoCtx("not support %T %+v", data, data)
	}

	startWriteOffset := out.GetWriteOffset()
	totalSize := 0
	// The total message size cannot be determined at the beginning and needs to wait until all the
	// dynamic content is determined before the total size can be determined. After the total size is
	// determined, we need to write the total size data in the location of totalSizeAt
	totalSizeAt := skip(totalSizeFieldBytes, out)

	// 2.1 flag
	flag := c.getFlag(msg)
	out.MustWriteByte(flag)
	totalSize += 1

	// 2.2 checksum, similar to totalSize, we do not currently know the size of the message body.
	checksumAt := -1
	if flag&flagChecksumEnabled != 0 {
		checksumAt = skip(checksumFieldBytes, out)
		totalSize += checksumFieldBytes
	}

	// 2.3 payload
	var payloadData []byte
	var compressedPayloadData []byte
	var payloadMsg PayloadMessage
	var hasPayload bool

	// skip all written data by this message
	discardWritten := func() {
		out.SetWriteIndexByOffset(startWriteOffset)
		if hasPayload {
			payloadMsg.SetPayloadField(payloadData)
		}
	}

	if payloadMsg, hasPayload = msg.Message.(PayloadMessage); hasPayload {
		// set payload filed to nil to avoid payload being written to the out buffer, and write directly
		// to the socket afterwards to reduce one payload.
		payloadData = payloadMsg.GetPayloadField()
		payloadMsg.SetPayloadField(nil)
		compressedPayloadData = payloadData

		if c.compressEnabled && len(payloadData) > 0 {
			v, err := c.compress(payloadData)
			if err != nil {
				discardWritten()
				return err
			}
			defer c.pool.Free(v)
			compressedPayloadData = v
		}

		out.WriteInt(len(compressedPayloadData))
		totalSize += payloadSizeFieldBytes + len(compressedPayloadData)
	}

	// 2.4 Custom header size
	n, err := c.encodeCustomHeaders(&msg, out)
	if err != nil {
		return err
	}
	totalSize += n

	// 2.5 streaming message
	if msg.stream {
		out.WriteUint32(msg.streamSequence)
		totalSize += 4
	}

	// 3.1 message body
	body, err := c.writeBody(out, msg.Message, totalSize)
	if err != nil {
		discardWritten()
		return err
	}

	// now, header and body are all determined, we need to fill the totalSize and checksum
	// fill total size
	totalSize += len(body)
	writeIntAt(totalSizeAt, out, totalSize)

	// fill checksum
	if checksumAt != -1 {
		if err := writeChecksum(checksumAt, out, body, compressedPayloadData); err != nil {
			discardWritten()
			return err
		}
	}

	// 3.2 payload
	if hasPayload {
		// resume payload to payload message
		payloadMsg.SetPayloadField(payloadData)
		if err := writePayload(out, compressedPayloadData, conn, c.payloadBufSize); err != nil {
			return err
		}
	}

	return nil
}

func (c *baseCodec) compress(src []byte) ([]byte, error) {
	n := lz4.CompressBlockBound(len(src))
	dst, err := c.pool.Alloc(n)
	if err != nil {
		return nil, err
	}
	dst, err = c.compressTo(src, dst)
	if err != nil {
		c.pool.Free(dst)
		return nil, err
	}
	return dst, nil
}

func (c *baseCodec) uncompress(src []byte) ([]byte, error) {
	// The lz4 library requires a []byte with a large enough dst when
	// decompressing, otherwise it will return an ErrInvalidSourceShortBuffer, we
	// can't confirm how large a dst we need to give initially, so when we encounter
	// an ErrInvalidSourceShortBuffer, we expand the size and retry.
	n := len(src) * 2
	for {
		dst, err := c.pool.Alloc(n)
		if err != nil {
			return nil, err
		}
		dst, err = uncompress(src, dst)
		if err == nil {
			return dst, nil
		}

		c.pool.Free(dst)
		if err != lz4.ErrInvalidSourceShortBuffer {
			return nil, err
		}
		n *= 2
	}
}

func (c *baseCodec) compressTo(src, dst []byte) ([]byte, error) {
	dst, err := compress(src, dst)
	if err != nil {
		return nil, err
	}
	return dst, nil
}

func (c *baseCodec) compressBound(size int) int {
	return lz4.CompressBlockBound(size)
}

func (c *baseCodec) getFlag(msg RPCMessage) byte {
	flag := byte(0)
	if c.checksumEnabled {
		flag |= flagChecksumEnabled
	}
	if c.compressEnabled {
		flag |= flagCompressEnabled
	}
	if len(c.headerCodecs) > 0 {
		flag |= flagHasCustomHeader
	}
	if _, ok := msg.Message.(PayloadMessage); ok {
		flag |= flagHashPayload
	}
	if msg.stream {
		flag |= flagStreamingMessage
	}
	if msg.internal {
		if m, ok := msg.Message.(*flagOnlyMessage); ok {
			flag |= m.flag
		}
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
	if flag&flagHasCustomHeader == 0 {
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

func (c *baseCodec) writeBody(
	out *buf.ByteBuf,
	msg Message,
	writtenSize int) ([]byte, error) {
	maxCanWrite := c.maxBodySize - writtenSize
	size := msg.Size()
	if size == 0 {
		return nil, nil
	}

	if size > maxCanWrite {
		return nil,
			moerr.NewInternalErrorNoCtx("message body %d is too large, max is %d",
				size+writtenSize,
				c.maxBodySize)
	}

	if !c.compressEnabled {
		index, _ := setWriterIndexAfterGow(out, size)
		data := out.RawSlice(index, index+size)
		if _, err := msg.MarshalTo(data); err != nil {
			return nil, err
		}
		return data, nil
	}

	// we use mpool to compress body, then write the dst into the buffer
	origin, err := c.pool.Alloc(size)
	if err != nil {
		return nil, err
	}
	defer c.pool.Free(origin)
	if _, err := msg.MarshalTo(origin); err != nil {
		return nil, err
	}

	n := c.compressBound(len(origin))
	dst, err := c.pool.Alloc(n)
	if err != nil {
		return nil, err
	}
	defer c.pool.Free(dst)

	dst, err = compress(origin, dst)
	if err != nil {
		return nil, err
	}

	index := out.GetWriteOffset()
	out.MustWrite(dst)
	return out.RawSlice(out.GetReadIndex()+index, out.GetWriteIndex()), nil
}

func (c *baseCodec) readMessage(flag byte, data []byte, offset int, expectChecksum uint64, payloadSize int, msg *RPCMessage) error {
	if offset == len(data) {
		return nil
	}

	body := data[offset : len(data)-payloadSize]
	payload := data[len(data)-payloadSize:]
	if flag&flagChecksumEnabled != 0 {
		if err := validChecksum(body, payload, expectChecksum); err != nil {
			return err
		}
	}

	if flag&flagCompressEnabled != 0 {
		dstBody, err := c.uncompress(body)
		if err != nil {
			return err
		}
		defer c.pool.Free(dstBody)
		body = dstBody

		if payloadSize > 0 {
			dstPayload, err := c.uncompress(payload)
			if err != nil {
				return err
			}
			defer c.pool.Free(dstPayload)
			payload = dstPayload
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

func (c *baseCodec) readFlag(msg *RPCMessage, data []byte, offset int) (byte, int) {
	flag := data[offset]
	if flag&flagPing != 0 {
		msg.Message = &flagOnlyMessage{flag: flagPing}
		msg.internal = true
	} else if flag&flagPong != 0 {
		msg.Message = &flagOnlyMessage{flag: flagPong}
		msg.internal = true
	} else {
		msg.Message = c.messageFactory()
	}
	return flag, 1
}

func readChecksum(flag byte, data []byte, offset int) (uint64, int) {
	if flag&flagChecksumEnabled == 0 {
		return 0, 0
	}

	return buf.Byte2Uint64(data[offset:]), checksumFieldBytes
}

func readPayloadSize(flag byte, data []byte, offset int) (int, int) {
	if flag&flagHashPayload == 0 {
		return 0, 0
	}

	return buf.Byte2Int(data[offset:]), payloadSizeFieldBytes
}

func readStreaming(flag byte, msg *RPCMessage, data []byte, offset int) int {
	if flag&flagStreamingMessage == 0 {
		return 0
	}
	msg.stream = true
	msg.streamSequence = buf.Byte2Uint32(data[offset:])
	return 4
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
		return moerr.NewInternalErrorNoCtx("checksum mismatch, expect %d, got %d",
			expectChecksum,
			actulChecksum)
	}
	return nil
}

func setWriterIndexAfterGow(out *buf.ByteBuf, n int) (int, int) {
	offset := out.Readable()
	out.Grow(n)
	out.SetWriteIndex(out.GetReadIndex() + offset + n)
	return out.GetReadIndex() + offset, offset
}
