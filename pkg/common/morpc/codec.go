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

	"github.com/fagongzi/goetty/v2/buf"
	"github.com/fagongzi/goetty/v2/codec"
	"github.com/fagongzi/goetty/v2/codec/length"
)

var (
	flagPayloadMessage byte = 1
)

type messageCodec struct {
	encoder codec.Encoder
	deocder codec.Decoder
}

// NewMessageCodec create a message codec. If the message is a PayloadMessage, payloadCopyBufSize
// determines how much data is copied from the payload to the socket each time.
func NewMessageCodec(messageFactory func() Message, payloadCopyBufSize int) Codec {
	bc := &baseCodec{messageFactory: messageFactory, payloadBufSize: payloadCopyBufSize}
	_, decoder := length.New(bc, bc)
	return &messageCodec{encoder: bc, deocder: decoder}
}

func (c *messageCodec) Decode(in *buf.ByteBuf) (bool, interface{}, error) {
	return c.deocder.Decode(in)
}

func (c *messageCodec) Encode(data interface{}, out *buf.ByteBuf) error {
	return c.encoder.Encode(data, out)
}

type baseCodec struct {
	payloadBufSize int
	messageFactory func() Message
}

func (c *baseCodec) Decode(in *buf.ByteBuf) (bool, interface{}, error) {
	message := c.messageFactory()

	data := in.GetMarkedRemindData()
	flag := data[0]
	data = data[1:]
	var payloadData []byte
	if flag == flagPayloadMessage {
		msize := buf.Byte2Int(data)
		data = data[4:]
		payloadData = data[msize:]
		data = data[:msize]
	}

	err := message.Unmarshal(data)
	if err != nil {
		return false, nil, err
	}

	if len(payloadData) > 0 {
		message.(PayloadMessage).SetPayloadField(payloadData)
	}

	in.MarkedBytesReaded()
	return true, message, nil
}

func (c *baseCodec) Encode(data interface{}, out *buf.ByteBuf) error {
	if message, ok := data.(Message); ok {
		flag := byte(0)
		size := 1 // 1 bytes flag
		var payloadData []byte
		hasPayload := false
		if payload, ok := message.(PayloadMessage); ok {
			payloadData = payload.GetPayloadField()
			if len(payloadData) > 0 {
				payload.SetPayloadField(nil)
				flag = flagPayloadMessage
				hasPayload = true
				size += 4 + len(payloadData) // 4 bytes payload size + payload bytes
				defer payload.SetPayloadField(payloadData)
			}
		}

		msize := message.Size()
		size += msize

		// 4 bytes message size
		buf.MustWriteInt(out, size)
		// 1 byte flag
		buf.MustWriteByte(out, flag)
		// 4 bytes message size
		if hasPayload {
			buf.MustWriteInt(out, msize)
		}
		// message
		index := out.GetWriteIndex()
		out.Expansion(msize)
		if _, err := message.MarshalTo(out.RawBuf()[index : index+msize]); err != nil {
			return err
		}
		out.SetWriterIndex(index + msize)

		// payload
		if hasPayload {
			if _, err := out.FlushToSink(); err != nil {
				return err
			}

			if _, err := out.WriteToSink(payloadData, c.payloadBufSize); err != nil {
				return err
			}
		}
		return nil
	}

	return fmt.Errorf("not support %T %+v", data, data)
}
