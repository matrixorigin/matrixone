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
	"fmt"

	"github.com/fagongzi/goetty/buf"
	"github.com/fagongzi/goetty/codec"
)

const PacketHeaderLength = 4

func NewSqlCodec() (codec.Encoder, codec.Decoder) {
	c := &sqlCodec{}
	return c, c
}

type sqlCodec struct {
}

type Packet struct {
	Length     int32
	SequenceID int8
	Payload    []byte
}

func (c *sqlCodec) Decode(in *buf.ByteBuf) (bool, interface{}, error) {
	readable := in.Readable()
	if readable < PacketHeaderLength {
		return false, nil, nil
	}

	header, err := in.PeekN(0, PacketHeaderLength)
	if err != nil {
		return false, "", err
	}

	length := int32(uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16)
	sequenceID := int8(header[3])

	if readable < int(length)+PacketHeaderLength {
		return false, nil, nil
	}

	err = in.Skip(PacketHeaderLength)
	if err != nil {
		return true, nil, err
	}

	err = in.MarkN(int(length))
	if err != nil {
		if length == 0 {
			packet := &Packet{
				Length:     0,
				SequenceID: sequenceID,
				Payload:    make([]byte, 0),
			}
			return true, packet, nil
		}
		return false, nil, err
	}

	_, payload, _ := in.ReadMarkedBytes()

	packet := &Packet{
		Length:     length,
		SequenceID: sequenceID,
		Payload:    payload,
	}

	return true, packet, nil
}

func (c *sqlCodec) Encode(data interface{}, out *buf.ByteBuf) error {
	x := data.([]byte)
	xlen := len(x)
	tlen, err := out.Write(data.([]byte))
	if err != nil {
		return err
	}
	if tlen != xlen {
		return fmt.Errorf("len of written != len of the data")
	}
	return nil
}
