// Copyright 2021 Matrix Origin
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

package frontend

import (
	"context"
	"io"

	"github.com/fagongzi/goetty/v2/buf"
	"github.com/fagongzi/goetty/v2/codec"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

var (
	errorInvalidLength0             = moerr.NewInvalidInput(context.Background(), "invalid length: 0")
	errorLenOfWrittenNotEqLenOfData = moerr.NewInternalError(context.Background(), "len of written != len of the data")
	// ErrPacketTooLarge identifies a MySQL packet rejected before its payload
	// is decoded. Callers can use errors.Is to translate it to the protocol's
	// ER_SERVER_NET_PACKET_TOO_LARGE response.
	ErrPacketTooLarge = moerr.NewInvalidInputNoCtx("mysql packet too large")
)

const PacketHeaderLength = 4

// SQLCodecOption customizes MySQL packet decoding. With no options the codec
// retains its historical behavior.
type SQLCodecOption func(*sqlCodec)

// WithSQLCodecMaxPayloadSize rejects a packet from its four-byte header before
// waiting for or retaining a payload larger than maxPayloadSize. A non-positive
// value leaves the protocol's existing three-byte length limit unchanged.
func WithSQLCodecMaxPayloadSize(maxPayloadSize int) SQLCodecOption {
	return func(c *sqlCodec) {
		c.maxPayloadSize = maxPayloadSize
	}
}

func NewSqlCodec(options ...SQLCodecOption) codec.Codec {
	c := &sqlCodec{}
	for _, option := range options {
		if option != nil {
			option(c)
		}
	}
	return c
}

type sqlCodec struct {
	maxPayloadSize int
}

type Packet struct {
	Length     int32
	SequenceID int8
	Payload    []byte
}

func (c *sqlCodec) Decode(in *buf.ByteBuf) (interface{}, bool, error) {
	readable := in.Readable()
	if readable < PacketHeaderLength {
		return nil, false, nil
	}

	header := in.PeekN(0, PacketHeaderLength)
	length := int32(uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16)
	if length == 0 {
		return nil, false, errorInvalidLength0
	}
	if c.maxPayloadSize > 0 && int(length) > c.maxPayloadSize {
		return nil, false, ErrPacketTooLarge
	}

	sequenceID := int8(header[3])

	if readable < int(length)+PacketHeaderLength {
		return nil, false, nil
	}

	in.Skip(PacketHeaderLength)
	in.SetMarkIndex(in.GetReadIndex() + int(length))
	payload := in.ReadMarkedData()

	packet := &Packet{
		Length:     length,
		SequenceID: sequenceID,
		Payload:    payload,
	}

	return packet, true, nil
}

func (c *sqlCodec) Encode(data interface{}, out *buf.ByteBuf, writer io.Writer) error {
	x := data.([]byte)
	xlen := len(x)
	tlen, err := out.Write(data.([]byte))
	if err != nil {
		return err
	}
	if tlen != xlen {
		return errorLenOfWrittenNotEqLenOfData
	}
	return nil
}
