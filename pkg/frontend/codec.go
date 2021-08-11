package frontend

import (
	"fmt"
	"github.com/fagongzi/goetty/buf"
	"github.com/fagongzi/goetty/codec"
)
// TODO

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
	_, header, err := in.ReadBytes(4)
	if err != nil {
		return false, "", err
	}

	length := int32(uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16)
	sequenceID := int8(header[3])

	_, payload, err := in.ReadBytes(int(length))

	packet := &Packet{
		Length:     length,
		SequenceID: sequenceID,
		Payload:    payload,
	}

	return true, packet, nil
}

func (c *sqlCodec) Encode(data interface{}, out *buf.ByteBuf) error {
	fmt.Println("Encoder send Length: ", data.([]byte)[:2], ", SequenceId:", data.([]byte)[3], ", payload:", data.([]byte)[4:])
	_, err := out.Write(data.([]byte))
	if err != nil {
		return err
	}
	return nil
}
