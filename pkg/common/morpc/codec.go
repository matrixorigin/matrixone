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
	"fmt"
	"io"
	"sync"

	"github.com/cespare/xxhash/v2"
	"github.com/fagongzi/goetty/v2/buf"
	"github.com/fagongzi/goetty/v2/codec"
	"github.com/fagongzi/goetty/v2/codec/length"
)

var (
	flagPayloadMessage byte = 1
	flagWithChecksum   byte = 2
)

type messageCodec struct {
	codec codec.Codec
	bc    codec.Codec
}

// NewMessageCodec create a message codec. If the message is a PayloadMessage, payloadCopyBufSize
// determines how much data is copied from the payload to the socket each time.
func NewMessageCodec(messageFactory func() Message, payloadCopyBufSize int) Codec {
	return newMessageCodec(messageFactory, payloadCopyBufSize, false)
}

// NewMessageCodec create a message codec. If the message is a PayloadMessage, payloadCopyBufSize
// determines how much data is copied from the payload to the socket each time.
func NewMessageCodecWithChecksum(messageFactory func() Message, payloadCopyBufSize int) Codec {
	return newMessageCodec(messageFactory, payloadCopyBufSize, true)
}

func newMessageCodec(messageFactory func() Message, payloadCopyBufSize int, enableChecksum bool) Codec {
	bc := &baseCodec{
		messageFactory: messageFactory,
		payloadBufSize: payloadCopyBufSize,
		enableChecksum: enableChecksum,
	}
	return &messageCodec{codec: length.New(bc), bc: bc}
}

func (c *messageCodec) Decode(in *buf.ByteBuf) (any, bool, error) {
	return c.codec.Decode(in)
}

func (c *messageCodec) Encode(data interface{}, out *buf.ByteBuf, conn io.Writer) error {
	return c.bc.Encode(data, out, conn)
}

type baseCodec struct {
	enableChecksum bool
	payloadBufSize int
	messageFactory func() Message
}

func (c *baseCodec) Decode(in *buf.ByteBuf) (any, bool, error) {
	message := c.messageFactory()
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
		data = data[4:]
		payloadData = data[msize:]
		data = data[:msize]
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
			return nil, false, fmt.Errorf("checksum mismatch, expect %d, got %d",
				expectChecksum,
				actulChecksum)
		}
	}

	err := message.Unmarshal(data)
	if err != nil {
		return nil, false, err
	}

	if len(payloadData) > 0 {
		message.(PayloadMessage).SetPayloadField(payloadData)
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
	// message body
	// payload body

	if message, ok := data.(Message); ok {
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
		msize := message.Size()
		size += msize

		// 4 bytes total length
		out.WriteInt(size)
		// 1 byte flag
		out.MustWriteByte(flag)
		// 8 bytes checksum
		if c.enableChecksum {
			checksumIdx = out.GetWriteIndex()
			out.Grow(8)
			out.SetWriteIndex(checksumIdx + 8)
		}
		// 4 bytes message size
		if hasPayload {
			out.WriteInt(msize)
		}

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

	return fmt.Errorf("not support %T %+v", data, data)
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
