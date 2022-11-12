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
	}
}

type messageCodec struct {
	codec codec.Codec
	bc    *baseCodec
}

// NewMessageCodec create message codec
func NewMessageCodec(messageFactory func() Message, options ...CodecOption) Codec {
	bc := &baseCodec{
		messageFactory: messageFactory,
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
	messageFactory func() Message
	headerCodecs   []HeaderCodec
}

func (c *baseCodec) Decode(in *buf.ByteBuf) (any, bool, error) {
	message := RPCMessage{Message: c.messageFactory()}
	data := in.RawSlice(in.GetReadIndex(), in.GetMarkIndex())
	flag := data[0]
	data = data[1:]

	var checksum *xxhash.Digest
	expectChecksum := uint64(0)
	if flag&flagWithChecksum != 0 {
		expectChecksum = buf.Byte2Uint64(data)
		data = data[8:]
		checksum = acquireChecksum()
		defer releaseChecksum(checksum)
	}

	var payloadData []byte
	if flag&flagPayloadMessage != 0 {
		msize := buf.Byte2Int(data)
		// custom header + msg + payload
		data = data[4:]
		v := len(data) - msize
		payloadData = data[v:]
		data = data[:v]
	}

	if flag&flagCustomHeader != 0 {
		for _, hc := range c.headerCodecs {
			n, err := hc.Decode(&message, data)
			if err != nil {
				return nil, false, err
			}
			data = data[n:]
		}
	}

	if flag&flagWithChecksum != 0 {
		_, err := checksum.Write(data)
		if err != nil {
			return nil, false, err
		}
		if len(payloadData) > 0 {
			_, err := checksum.Write(payloadData)
			if err != nil {
				return nil, false, err
			}
		}
		actulChecksum := checksum.Sum64()
		if actulChecksum != expectChecksum {
			return nil, false, moerr.NewInternalError("checksum mismatch, expect %d, got %d",
				expectChecksum,
				actulChecksum)
		}
	}

	err := message.Message.Unmarshal(data)
	if err != nil {
		return nil, false, err
	}

	if len(payloadData) > 0 {
		message.Message.(PayloadMessage).SetPayloadField(payloadData)
	}

	in.SetReadIndex(in.GetMarkIndex())
	in.ClearMark()
	return message, true, nil
}

func (c *baseCodec) Encode(data interface{}, out *buf.ByteBuf, conn io.Writer) error {
	// format:
	// 4 bytes length
	// 1 bytes flag
	// 8 bytes checksum if has check flag
	// 4 bytes message size if has payload flag
	// custom headers
	// message body
	// payload body

	if rpcMessage, ok := data.(RPCMessage); ok {
		message := rpcMessage.Message
		var checksum *xxhash.Digest
		checksumIdx := 0
		flag := byte(0)
		size := 1 // 1 bytes flag

		if c.enableChecksum {
			flag |= flagWithChecksum
			size += 8
			checksum = acquireChecksum()
			defer releaseChecksum(checksum)
		}

		// handle payload
		var payloadData []byte
		var payload PayloadMessage
		hasPayload := false
		if payload, hasPayload = message.(PayloadMessage); hasPayload {
			payloadData = payload.GetPayloadField()
			hasPayload = len(payloadData) > 0
			if hasPayload {
				payload.SetPayloadField(nil)
				flag |= flagPayloadMessage
				hasPayload = true
				size += 4 + len(payloadData) // 4 bytes payload size + payload bytes
			}
		}

		if len(c.headerCodecs) > 0 {
			flag |= flagCustomHeader
		}

		msize := message.Size()
		size += msize

		// 4 bytes total length
		sizeIdx := out.GetWriteIndex()
		out.Grow(4)
		out.SetWriteIndex(sizeIdx + 4)
		// 1 byte flag
		out.MustWriteByte(flag)
		// 8 bytes checksum
		if c.enableChecksum {
			checksumIdx = out.GetWriteIndex()
			out.Grow(8)
			out.SetWriteIndex(checksumIdx + 8)
		}
		// 4 bytes payload message size
		if hasPayload {
			out.WriteInt(len(payloadData))
		}

		if len(c.headerCodecs) > 0 {
			for _, hc := range c.headerCodecs {
				v, err := hc.Encode(&rpcMessage, out)
				if err != nil {
					return err
				}
				size += v
			}
		}
		// message size
		buf.Int2BytesTo(size, out.RawSlice(sizeIdx, sizeIdx+4))

		// message body
		index := out.GetWriteIndex()
		out.Grow(msize)
		out.SetWriteIndex(index + msize)
		if _, err := message.MarshalTo(out.RawSlice(index, index+msize)); err != nil {
			return err
		}

		if c.enableChecksum {
			_, err := checksum.Write(out.RawSlice(index, index+msize))
			if err != nil {
				return err
			}
			if hasPayload {
				_, err = checksum.Write(payloadData)
				if err != nil {
					return err
				}
			}
			buf.Uint64ToBytesTo(checksum.Sum64(), out.RawSlice(checksumIdx, checksumIdx+8))
		}

		// payload body
		if hasPayload {
			// recover payload
			payload.SetPayloadField(payloadData)
			if _, err := out.WriteTo(conn); err != nil {
				return err
			}
			if err := buf.WriteTo(payloadData, conn, c.payloadBufSize); err != nil {
				return err
			}
		}
		return nil
	}

	return moerr.NewInternalError("not support %T %+v", data, data)
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
