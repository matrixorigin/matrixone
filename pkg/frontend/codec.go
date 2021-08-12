package frontend

import (
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
	Length int32
	SequenceID int8
	Payload []byte
}

func (c *sqlCodec) Decode(in *buf.ByteBuf) (bool, interface{}, error) {
	readable := in.Readable()
	header, err := in.PeekN(0, PacketHeaderLength)
	if err != nil {
		return false, "", err
	}

	length := int32(uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16)
	sequenceID := int8(header[3])

	if readable < int(length) + PacketHeaderLength {
		return false, nil, nil
	}

	in.Skip(PacketHeaderLength)
	in.MarkN(int(length))
	_, payload, err := in.ReadMarkedBytes()

	packet := &Packet{
		Length:     length,
		SequenceID: sequenceID,
		Payload:    payload,
	}

	return true, packet, nil
}

func (c *sqlCodec) Encode(data interface{}, out *buf.ByteBuf) error {
	_, err := out.Write(data.([]byte))
	if err != nil {
		return err
	}
	return nil
}
